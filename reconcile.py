"""Catch-up after downtime without replaying history, under one request budget.

aegis-ssvr C1 (design note v2 on the item). After a long stop the shared change
cursor fell ~17.6k transactions behind and the inbox held 1.9M rows that
coalesce to ~17k candidate evaluations. Replaying them one change at a time
drove quipu to 5.7 req/s. Instead:

* RequestBudget: ONE token bucket in quipu requests for the whole process,
  persisted in emitter.db (a restart or crash loop never refills it; a new
  database starts at 0 tokens). Every call PRE-RESERVES its hard cap and is
  refunded the unused part afterwards. Callers are ranked by class; a lower
  class does not take tokens while a higher class is waiting.
* Reconciler: STREAM -> RECONCILE -> DRAINING -> STREAM. Entry is checked only
  in STREAM and not inside the cooldown. RECONCILE pins the head watermark W,
  discovers every change emitter's candidates from current state (at or after W;
  pinned historical reads exceed the server's query timeout) in budgeted pages,
  while changes after W are replayed from the stream once the cursor is at W; queues
  them (and every tracked item) as priority-2 deadlines, then in ONE
  transaction purges the superseded inbox, recomputes its projections and jumps
  the cursor to W. DRAINING evaluates those deadlines below live work.

Reconcile mode is all-or-nothing: if any change emitter lacks the reconcile or
budget-cap contract the cursor never jumps (it would skip changes that emitter
needed), and chaski keeps the bounded replay.
"""
from __future__ import annotations

import json
import logging
import math
import threading

LOG = logging.getLogger("chaski.reconcile")

# Scheduling classes, highest priority first.
FEED, SINK, LIVE, DISCOVER, RECONCILE = 1, 2, 3, 4, 5
CLASS_NAMES = {FEED: "feed", SINK: "sink", LIVE: "live", DISCOVER: "discover", RECONCILE: "reconcile"}
DEMAND_TTL_S = 120.0       # floor: a waiting class blocks lower classes until it stops re-asking
BLOCKED_RECHECK_S = 5.0    # a caller blocked only by priority re-checks this often (no busy-spin)
WINDOW_S = 900.0           # the bound is stated over any 15-minute window
STARVATION_S = 600.0       # the reconcile class gets at least one grant per this interval
JUMP_RETRY_S = 300.0       # a rolled-back cursor jump waits this long before retrying

BUDGET_SCHEMA = """
CREATE TABLE IF NOT EXISTS request_budget (
 id INTEGER PRIMARY KEY CHECK(id=1), tokens REAL NOT NULL, updated REAL NOT NULL);
"""

RECONCILE_SCHEMA = """
CREATE TABLE IF NOT EXISTS reconcile_run (
 id INTEGER PRIMARY KEY CHECK(id=1), run_id INTEGER NOT NULL, target_tx INTEGER NOT NULL,
 state TEXT NOT NULL CHECK(state IN ('reconcile','draining','stream')),
 started REAL NOT NULL, completed REAL);
CREATE TABLE IF NOT EXISTS reconcile_emitter (
 run_id INTEGER NOT NULL, emitter TEXT NOT NULL, cursor TEXT, complete INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(run_id, emitter));
"""


class BudgetError(ValueError):
    pass


class RequestBudget:
    """Persisted, prioritized token bucket counted in quipu requests."""

    def __init__(self, conn, rate: float, burst: int, max_window_rate: float | None = None):
        if isinstance(rate, bool) or not (isinstance(rate, (int, float)) and math.isfinite(rate) and rate > 0):
            raise BudgetError("request budget rate must be a positive finite number")
        if type(burst) is not int or burst < 1:
            raise BudgetError("request budget burst must be a positive integer")
        # The stated bound: requests in ANY 15-minute window <= rate*900 + burst.
        if max_window_rate is not None and rate + burst / WINDOW_S > max_window_rate + 1e-9:
            raise BudgetError(f"rate {rate} + burst {burst}/{WINDOW_S:.0f}s exceeds the"
                              f" {max_window_rate} req/s window bound")
        self.conn, self.rate, self.burst = conn, float(rate), burst
        self.lock = threading.Lock()   # the stream reader thread also reserves
        conn.executescript(BUDGET_SCHEMA)
        row = conn.execute("SELECT tokens, updated FROM request_budget WHERE id=1").fetchone()
        # A new database starts EMPTY. A restart keeps the persisted tokens and does
        # NOT credit the downtime: refill counts only from this process's first use.
        self.tokens = 0.0 if row is None else min(float(row[0]), burst)
        self.updated = None
        # A waiting class blocks lower classes while it keeps re-asking; it re-asks
        # at least every refill time, so the TTL must exceed that (aegis-ssvr C1).
        self.demand_ttl = max(DEMAND_TTL_S, 1.5 * burst / self.rate)
        self.demand: dict[int, float] = {}        # last failed attempt per class
        self.waiting_since: dict[int, float] = {}  # first failed attempt of the current wait
        self.last_grant: dict[int, float] = {}
        self.reserved_total = {c: 0 for c in CLASS_NAMES}
        self.refunded_total = 0
        # Completed adapter cost reports only. Unknown calls never become zero.
        self.adapter_requests_total = 0
        self.adapter_accounting_unknown_total = 0
        self.violations: dict[str, int] = {}

    def _refill(self, now: float) -> None:
        if self.updated is None:
            self.updated = now
        elif now > self.updated:
            self.tokens = min(self.burst, self.tokens + (now - self.updated) * self.rate)
            self.updated = now

    def _persist(self) -> None:
        self.conn.execute("INSERT INTO request_budget VALUES (1,?,?) ON CONFLICT(id) DO UPDATE"
                          " SET tokens=excluded.tokens, updated=excluded.updated",
                          (self.tokens, self.updated))
        self.conn.commit()

    def _blocked_by_higher(self, cls: int, now: float) -> bool:
        if cls == RECONCILE and now - self.waiting_since.get(RECONCILE, now) >= STARVATION_S:
            return False   # starvation guard: reconcile work waiting this long still advances
        return any(c < cls and now - t < self.demand_ttl for c, t in self.demand.items())

    def try_reserve(self, cls: int, k: int, now: float) -> bool:
        """Take k tokens for a call that will make at most k requests."""
        if cls not in CLASS_NAMES:
            raise BudgetError(f"unknown budget class {cls!r}")
        if type(k) is not int or not 1 <= k <= self.burst:
            raise BudgetError(f"a reservation must be 1..{self.burst} requests, got {k!r}")
        with self.lock:
            self._refill(now)
            if self._blocked_by_higher(cls, now) or self.tokens < k:
                self.demand[cls] = now                 # refreshed on every attempt
                self.waiting_since.setdefault(cls, now)
                return False   # nothing spent; the refill is persisted with the next grant
            self.tokens -= k
            self.demand.pop(cls, None)
            self.waiting_since.pop(cls, None)
            self.last_grant[cls] = now
            self.reserved_total[cls] += k
            self._persist()
            return True

    def settle(self, reserved: int, cost, label: str) -> int:
        """Refund reserved - actual. A missing or over-cap cost is charged in full
        and counted as a violation; the caller suspends that emitter."""
        actual = cost.get("quipu_requests") if isinstance(cost, dict) else None
        if type(actual) is not int or not 0 <= actual <= reserved:
            self.adapter_accounting_unknown()
            self.violations[label] = self.violations.get(label, 0) + 1
            LOG.warning("adapter %s reported no valid cost (%r) for a %d-request reservation",
                        label, cost, reserved)
            return reserved
        with self.lock:
            self.adapter_requests_total += actual
        refund = reserved - actual
        if refund:
            with self.lock:
                self.tokens = min(self.burst, self.tokens + refund)
                self.refunded_total += refund
                self._persist()
        return actual

    def adapter_accounting_unknown(self) -> None:
        """A failed/malformed adapter has no exact cost; retain the budget charge."""
        with self.lock:
            self.adapter_accounting_unknown_total += 1

    def due(self, k: int, now: float, cls: int | None = None) -> float:
        """When to try again: refill time for k tokens, and never sooner than a
        bounded re-check while only a higher class's demand blocks (no busy-spin)."""
        with self.lock:
            self._refill(now)
            at = now if self.tokens >= k else now + (k - self.tokens) / self.rate
            if cls is not None and self._blocked_by_higher(cls, now):
                at = max(at, now + BLOCKED_RECHECK_S)
            return at

    def metrics(self, esc) -> list[str]:
        with self.lock:
            requests = self.adapter_requests_total
            unknown = self.adapter_accounting_unknown_total
        lines = [
                 "# HELP chaski_adapter_quipu_requests_total Requests in valid adapter cost reports; "
                 "also check accounting_unknown.",
                 "# TYPE chaski_adapter_quipu_requests_total counter",
                 f"chaski_adapter_quipu_requests_total {requests}",
                 "# HELP chaski_adapter_quipu_accounting_unknown_total Calls with unknown request cost; "
                 "never treat as zero requests.",
                 "# TYPE chaski_adapter_quipu_accounting_unknown_total counter",
                 f"chaski_adapter_quipu_accounting_unknown_total {unknown}",
                 f"chaski_budget_tokens {self.tokens:.3f}",
                 f"chaski_budget_rate_requests_per_second {self.rate}",
                 f"chaski_budget_burst_requests {self.burst}",
                 f"chaski_budget_refunded_total {self.refunded_total}"]
        lines += [f'chaski_budget_reserved_total{{class="{CLASS_NAMES[c]}"}} {n}'
                  for c, n in sorted(self.reserved_total.items())]
        lines += [f'chaski_budget_violation_total{{emitter="{esc(label)}"}} {n}'
                  for label, n in sorted(dict(self.violations).items())]
        return lines


class BudgetedWrite:
    """The existing global write interval, also charged to the request budget."""

    def __init__(self, inner, budget: RequestBudget):
        self.inner, self.budget = inner, budget
        self.interval_s = inner.interval_s

    @property
    def _last(self):
        return self.inner._last

    def try_take(self, now: float) -> bool:
        if now - self.inner._last < self.inner.interval_s:
            return False
        if not self.budget.try_reserve(SINK, 1, now):
            return False
        return self.inner.try_take(now)


def eligible(runners) -> tuple[bool, list[str]]:
    """All change emitters must declare both contracts, or none reconcile."""
    missing = [r.emitter.label for r in runners
               if r.subscription.get("reconcile_contract") != 1 or r.subscription.get("budget_cap") != 1]
    return not missing, missing


def recompute_inbox_projection(conn) -> None:
    """Rebuild counts and frontier from change_inbox (same SELECTs as the backfill)."""
    conn.execute("DELETE FROM change_inbox_frontier")
    conn.execute("DELETE FROM change_inbox_counts")
    conn.execute("INSERT INTO change_inbox_frontier"
                 " SELECT emitter,json_extract(payload,'$.entity'),MIN(tx),MIN(seq) FROM change_inbox"
                 " WHERE json_extract(payload,'$.entity') IS NOT NULL"
                 " GROUP BY emitter,json_extract(payload,'$.entity')")
    conn.execute("INSERT INTO change_inbox_counts SELECT emitter,COUNT(*) FROM change_inbox GROUP BY emitter")


def assert_inbox_projection(conn) -> None:
    counts = dict(conn.execute("SELECT emitter,pending FROM change_inbox_counts WHERE pending>0"))
    actual = dict(conn.execute("SELECT emitter,COUNT(*) FROM change_inbox GROUP BY emitter"))
    if counts != actual:
        raise RuntimeError(f"inbox count projection mismatch: {counts} != {actual}")
    frontier = dict(conn.execute("SELECT emitter,COUNT(*) FROM change_inbox_frontier GROUP BY emitter"))
    distinct = dict(conn.execute(
        "SELECT emitter,COUNT(DISTINCT json_extract(payload,'$.entity')) FROM change_inbox"
        " WHERE json_extract(payload,'$.entity') IS NOT NULL GROUP BY emitter"))
    if frontier != distinct:
        raise RuntimeError(f"inbox frontier projection mismatch: {frontier} != {distinct}")


def _delete_trigger_sql(conn) -> str:
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='trigger'"
                       " AND name='change_inbox_projection_delete'").fetchone()
    if row is None:
        raise RuntimeError("inbox projection delete trigger is missing")
    return row[0]


class Reconciler:
    """Process-level catch-up state machine over the shared change cursor."""

    def __init__(self, conn, feed, runners, budget: RequestBudget, *, max_lag_tx=5000,
                 max_backlog=5000, cooldown_s=86400, cooldown_override_lag_tx=50000,
                 page_requests=None, head=None):
        self.conn, self.feed, self.runners, self.budget = conn, feed, list(runners), budget
        self.max_lag_tx, self.max_backlog = max_lag_tx, max_backlog
        self.cooldown_s, self.cooldown_override_lag_tx = cooldown_s, cooldown_override_lag_tx
        self.page_requests = budget.burst if page_requests is None else page_requests
        if type(self.page_requests) is not int or not 1 <= self.page_requests <= budget.burst:
            raise BudgetError("discover page reservation must be an integer within the burst")
        self._head = head
        conn.executescript(RECONCILE_SCHEMA)
        self.is_eligible, self.blocking = eligible(self.runners)
        self.unknown = 0
        self.purge_seconds = None
        self.retry_at = 0.0
        # Discovery progress (aegis-p1u4): the change cursor is static while a run
        # discovers, so without these "slow" and "stuck" look identical from outside.
        self.discover_pages = {}         # emitter -> successful pages since start
        self.discover_items = {}         # emitter -> items those pages returned
        self.discover_last_success = {}  # emitter -> wall time of the last committed page

    # --- state ------------------------------------------------------------------
    def run(self):
        return self.conn.execute(
            "SELECT run_id,target_tx,state,started,completed FROM reconcile_run WHERE id=1").fetchone()

    @property
    def state(self) -> str:
        row = self.run()
        return row[2] if row else "stream"

    @property
    def paused(self) -> bool:
        """The change feed is paused while discovering against a pinned W."""
        return self.state == "reconcile"

    def head(self) -> int:
        if self._head is not None:
            return self._head()
        out = self.feed.quipu._request(
            "GET", "/changes?since=9223372036854775807&capture=old_and_new_values&limit=1")
        watermark = out.get("watermark_tx")
        if type(watermark) is not int or watermark < 0:
            raise RuntimeError("change head watermark missing")
        return watermark

    def backlog(self) -> int:
        row = self.conn.execute("SELECT MAX(pending) FROM change_inbox_counts").fetchone()
        return row[0] or 0

    def remaining(self) -> int:
        # A suspended emitter (broken cost contract) keeps its deadlines for after a
        # restart but must not wedge DRAINING for the healthy ones.
        labels = [r.emitter.label for r in self.runners if not getattr(r, "suspended", False)]
        if not labels:
            return 0
        marks = ",".join("?" * len(labels))
        return self.conn.execute(
            f"SELECT COUNT(*) FROM adapter_deadlines WHERE priority=2 AND emitter IN ({marks})",
            labels).fetchone()[0]

    # --- entry ------------------------------------------------------------------
    def sample_head(self, now: float, every_s: float = 60.0) -> bool:
        """While paused, read the head at most once per `every_s`, as one FEED
        request from the shared budget, so the reported lag stays measurable
        (a paused feed accepts no pages, so nothing else observes the head)."""
        note = getattr(self.feed, "note_head", None)
        if note is None or not self.paused:
            return False
        last = getattr(self.feed, "head_observed_at", None)
        if last is not None and now - last < every_s:
            return False
        if not self.budget.try_reserve(FEED, 1, now):
            return False
        try:
            note(self.head(), now)
        except Exception as exc:  # noqa: BLE001
            self.unknown += 1
            LOG.warning("paused head sample UNKNOWN (lag kept UNKNOWN or stale): %s", exc)
            return False
        return True

    def reconcile_due(self, now: float) -> str | None:
        """'new' for a changed subscription (no stored pass), 'daily' when the
        catalogue pass is due. The pass is stamped when a run COMPLETES."""
        due = None
        for runner in self.runners:
            row = self.conn.execute("SELECT last_success FROM adapter_reconcile WHERE emitter=?",
                                    (runner.emitter.label,)).fetchone()
            if row is None:
                return "new"
            if now - row[0] >= runner.emitter.interval_s:
                due = "daily"
        return due

    def should_enter(self, now: float) -> bool:
        """Only from STREAM. A changed subscription enters at once; the daily pass
        and the lag/backlog triggers all respect the cooldown after a completed run,
        so a drain longer than a day cannot re-trigger itself."""
        if not self.is_eligible or self.state != "stream":
            return False
        due = self.reconcile_due(now)
        if due == "new":
            return True
        lag = self.feed.lag
        backlog = self.backlog()
        triggered = due == "daily" or (lag is not None and lag > self.max_lag_tx) or backlog > self.max_backlog
        if not triggered:
            return False
        row = self.run()
        if row and row[4] is not None and now - row[4] < self.cooldown_s:
            return lag is not None and lag > self.cooldown_override_lag_tx
        return True

    def enter(self, now: float) -> bool:
        if not self.budget.try_reserve(FEED, 1, now):
            return False
        try:
            target = self.head()
        except Exception as exc:  # noqa: BLE001
            self.unknown += 1
            LOG.warning("reconcile entry UNKNOWN (stream mode kept): %s", exc)
            return False
        cursor = self.conn.execute("SELECT tx FROM change_cursor WHERE id=1").fetchone()
        if cursor is not None and target < cursor[0]:
            self.unknown += 1
            LOG.warning("reconcile entry refused: head %s is behind the cursor %s", target, cursor[0])
            return False
        note = getattr(self.feed, "note_head", None)
        if note is not None:
            note(target, now)
        row = self.run()
        run_id = (row[0] + 1) if row else 1
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("INSERT INTO reconcile_run VALUES (1,?,?,'reconcile',?,NULL)"
                              " ON CONFLICT(id) DO UPDATE SET run_id=excluded.run_id,"
                              " target_tx=excluded.target_tx, state='reconcile',"
                              " started=excluded.started, completed=NULL", (run_id, target, now))
            self.conn.execute("DELETE FROM reconcile_emitter WHERE run_id<>?", (run_id,))
            self.conn.executemany("INSERT OR IGNORE INTO reconcile_emitter VALUES (?,?,NULL,0)",
                                  [(run_id, r.emitter.label) for r in self.runners])
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        LOG.warning("RECONCILE run %s: pinned W=%s, backlog %s, lag %s", run_id, target,
                    self.backlog(), self.feed.lag)
        return True

    # --- discovery ----------------------------------------------------------------
    def _queue(self, runner, items, include_tracked):
        label = runner.emitter.label
        rows = {(label, item) for item in items}
        if include_tracked:
            rows |= {(label, r[0]) for r in self.conn.execute(
                "SELECT item FROM verdicts WHERE emitter=? AND"
                " (tracked=1 OR (? AND recovery_at IS NULL))",
                (label, getattr(runner.emitter, "resolve_recovery", False)))}
        # Never demote: an existing priority-0/1 or older deadline keeps MIN(priority,due).
        self.conn.executemany(
            "INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES (?,?,?,2)"
            " ON CONFLICT(emitter,item) DO UPDATE SET due=MIN(due,excluded.due),"
            " priority=MIN(priority,excluded.priority)",
            [(label, item, self._now) for label, item in sorted(rows)])

    def discover_step(self, now: float) -> bool:
        """Run at most one discover page; True when every emitter is complete."""
        run_id, target = self.run()[:2]
        pending = self.conn.execute(
            "SELECT emitter,cursor FROM reconcile_emitter WHERE run_id=? AND complete=0 ORDER BY emitter",
            (run_id,)).fetchall()
        if not pending:
            return True
        label, cursor = pending[0]
        runner = next(r for r in self.runners if r.emitter.label == label)
        if getattr(runner, "suspended", False):
            return False   # UNKNOWN until restart: the run cannot complete without it
        if not self.budget.try_reserve(DISCOVER, self.page_requests, now):
            return False
        request = {"discover": True, "now": now, "as_of_tx": target,
                   "cursor": None if cursor is None else json.loads(cursor),
                   "budget": {"max_requests": self.page_requests}}
        from incremental import call, strings
        try:
            out = call(runner.emitter, request)
        except Exception as exc:  # noqa: BLE001
            # A failed page spends its whole reservation; the run stays in RECONCILE.
            self.budget.adapter_accounting_unknown()
            self.unknown += 1
            LOG.warning("reconcile discover %s UNKNOWN (page retried later): %s", label, exc)
            return False
        before = self.budget.violations.get(label, 0)
        self.budget.settle(self.page_requests, out.get("cost"), label)
        if self.budget.violations.get(label, 0) > before:
            # Same contract as ChangeRunner._call: the page is discarded, the emitter suspended.
            runner.suspended = True
            self.unknown += 1
            LOG.warning("reconcile discover %s broke its request-cost contract; suspended", label)
            return False
        try:
            items = strings(out.get("items"))
        except Exception as exc:  # noqa: BLE001
            self.unknown += 1
            LOG.warning("reconcile discover %s refused a malformed page: %s", label, exc)
            return False
        complete = out.get("complete") is True
        next_cursor = out.get("next_cursor")
        if complete == (next_cursor is not None):
            self.unknown += 1
            LOG.warning("reconcile discover %s refused: page must carry exactly one of"
                        " complete or next_cursor", label)
            return False
        self._now = now
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self._queue(runner, items, include_tracked=cursor is None)
            self.conn.execute("UPDATE reconcile_emitter SET cursor=?, complete=? WHERE run_id=? AND emitter=?",
                              (None if complete else json.dumps(next_cursor), int(complete), run_id, label))
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        pages = self.discover_pages[label] = self.discover_pages.get(label, 0) + 1
        self.discover_items[label] = self.discover_items.get(label, 0) + len(items)
        self.discover_last_success[label] = now
        LOG.info("reconcile discover %s page %d ok: run %s, %d items, cost %s, %s",
                 label, pages, run_id, len(items), out.get("cost"),
                 "complete" if complete else "more pages")
        return complete and len(pending) == 1

    # --- the cursor jump ----------------------------------------------------------
    def commit_jump(self, now: float) -> None:
        import time as _time
        run_id, target = self.run()[:2]
        started = _time.monotonic()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            incomplete = self.conn.execute(
                "SELECT COUNT(*) FROM reconcile_emitter WHERE run_id=? AND complete=0", (run_id,)).fetchone()[0]
            if incomplete:
                raise RuntimeError("cursor jump before every emitter discovered")
            cursor = self.conn.execute("SELECT tx FROM change_cursor WHERE id=1").fetchone()
            if cursor is not None and cursor[0] > target:
                raise RuntimeError("cursor moved past the pinned watermark during reconcile")
            if self.conn.execute("SELECT 1 FROM change_inbox WHERE tx>? LIMIT 1", (target,)).fetchone():
                raise RuntimeError("inbox holds changes after the pinned watermark")
            trigger = _delete_trigger_sql(self.conn)
            # Bulk purge without the per-row frontier trigger, then rebuild the
            # projections exactly and restore the identical trigger text.
            self.conn.execute("DROP TRIGGER change_inbox_projection_delete")
            self.conn.execute("DELETE FROM change_inbox WHERE tx<=?", (target,))
            recompute_inbox_projection(self.conn)
            self.conn.execute(trigger)
            assert_inbox_projection(self.conn)
            self.conn.execute("INSERT OR REPLACE INTO change_cursor VALUES (1,?)", (target,))
            self.conn.execute("UPDATE reconcile_run SET state='draining' WHERE id=1")
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.purge_seconds = _time.monotonic() - started
        self.feed.cursor_tx = target
        self.feed.lag = -1
        LOG.warning("RECONCILE run %s: cursor jumped to W=%s, inbox purged in %.1fs; draining %s deadlines",
                    run_id, target, self.purge_seconds, self.remaining())

    # --- driver -------------------------------------------------------------------
    def _run_matches_config(self) -> bool:
        run_id = self.run()[0]
        stored = {r[0] for r in self.conn.execute(
            "SELECT emitter FROM reconcile_emitter WHERE run_id=?", (run_id,))}
        return self.is_eligible and stored == {r.emitter.label for r in self.runners}

    def abort(self, reason: str) -> None:
        """Abandon an unfinished run without moving the cursor. Deadlines already
        queued stay (idempotent); a new run starts from STREAM if still triggered."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute("DELETE FROM reconcile_emitter")
            self.conn.execute("UPDATE reconcile_run SET state='stream', completed=NULL WHERE id=1")
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        self.unknown += 1
        LOG.warning("RECONCILE run aborted, cursor unchanged: %s", reason)

    def step(self, now: float) -> str | None:
        """Advance the machine. Returns 'jumped' when the cursor moved to W."""
        state = self.state
        if state == "stream":
            if self.should_enter(now):
                self.enter(now)
            return None
        if state == "reconcile":
            if not self._run_matches_config():
                # Emitters added, removed or no longer eligible since the run began:
                # never jump the shared cursor past an emitter the run did not discover.
                self.abort("change emitters differ from the persisted run, or one is ineligible")
                return None
            if now < self.retry_at:
                return None
            if self.discover_step(now):
                try:
                    self.commit_jump(now)
                except Exception as exc:  # noqa: BLE001
                    self.unknown += 1
                    self.retry_at = now + JUMP_RETRY_S   # the purge is heavy: no tight retry loop
                    LOG.warning("reconcile cursor jump UNKNOWN (rolled back, run kept): %s", exc)
                    return None
                return "jumped"
            return None
        if state == "draining" and self.remaining() == 0:
            # Stamp the catalogue pass at COMPLETION, so the next daily pass and
            # the cooldown both count from the end of the drain.
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.executemany("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)",
                                      [(r.emitter.label, now) for r in self.runners])
                self.conn.execute("UPDATE reconcile_run SET state='stream', completed=? WHERE id=1", (now,))
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            LOG.warning("RECONCILE complete: back to stream mode, cooldown %ss", self.cooldown_s)
        return None

    def next_wakeup(self, now: float) -> float:
        state = self.state
        if state == "reconcile":
            return max(now + 0.01, self.retry_at, self.budget.due(self.page_requests, now, DISCOVER))
        return now + 60

    def eta_seconds(self, cost_per_item: float) -> float:
        return self.remaining() * cost_per_item / self.budget.rate

    def metrics(self, esc, cost_per_item: float) -> list[str]:
        state = self.state
        lines = [f'chaski_reconcile_state{{state="{s}"}} {int(s == state)}'
                 for s in ("stream", "reconcile", "draining")]
        lines += [f"chaski_reconcile_eligible {int(self.is_eligible)}",
                  f"chaski_reconcile_deadlines_remaining {self.remaining()}",
                  f"chaski_reconcile_eta_seconds {self.eta_seconds(cost_per_item):.0f}",
                  f"chaski_reconcile_unknown_total {self.unknown}"]
        lines += [f'chaski_reconcile_blocked_by{{emitter="{esc(label)}"}} 1' for label in self.blocking]
        if self.purge_seconds is not None:
            lines.append(f"chaski_reconcile_purge_seconds {self.purge_seconds:.3f}")
        run = self.run()
        if state == "reconcile" and run is not None:
            # Phase per emitter of the active run: 1 once its discovery is complete.
            for label, complete in self.conn.execute(
                    "SELECT emitter,complete FROM reconcile_emitter WHERE run_id=? ORDER BY emitter",
                    (run[0],)):
                lines.append(f'chaski_reconcile_discover_complete{{emitter="{esc(label)}"}} {int(complete)}')
        for label in sorted(self.discover_pages):
            e = esc(label)
            lines += [f'chaski_reconcile_discover_pages_total{{emitter="{e}"}} {self.discover_pages[label]}',
                      f'chaski_reconcile_discover_items_total{{emitter="{e}"}} {self.discover_items[label]}',
                      f'chaski_reconcile_discover_last_success_timestamp_seconds{{emitter="{e}"}}'
                      f' {self.discover_last_success[label]:.3f}']
        return lines
