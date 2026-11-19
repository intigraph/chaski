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
  discovers every change emitter's candidates as of W in budgeted pages, queues
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

LOG = logging.getLogger("chaski.reconcile")

# Scheduling classes, highest priority first.
FEED, SINK, LIVE, DISCOVER, RECONCILE = 1, 2, 3, 4, 5
CLASS_NAMES = {FEED: "feed", SINK: "sink", LIVE: "live", DISCOVER: "discover", RECONCILE: "reconcile"}
DEMAND_TTL_S = 120.0       # a waiting class blocks lower classes for at most this long per wait
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

    def __init__(self, conn, rate: float, burst: int):
        if not (isinstance(rate, (int, float)) and math.isfinite(rate) and rate > 0):
            raise BudgetError("request budget rate must be a positive finite number")
        if type(burst) is not int or burst < 1:
            raise BudgetError("request budget burst must be a positive integer")
        self.conn, self.rate, self.burst = conn, float(rate), burst
        conn.executescript(BUDGET_SCHEMA)
        row = conn.execute("SELECT tokens, updated FROM request_budget WHERE id=1").fetchone()
        if row is None:
            self.tokens, self.updated = 0.0, None   # a new database starts EMPTY
        else:
            self.tokens, self.updated = min(float(row[0]), burst), float(row[1])
        self.demand: dict[int, float] = {}
        self.last_grant: dict[int, float] = {}
        self.reserved_total = {c: 0 for c in CLASS_NAMES}
        self.refunded_total = 0
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
        if cls == RECONCILE and now - self.demand.get(RECONCILE, now) >= STARVATION_S:
            return False   # starvation guard: reconcile work waiting this long still advances
        return any(c < cls and now - t < DEMAND_TTL_S for c, t in self.demand.items())

    def try_reserve(self, cls: int, k: int, now: float) -> bool:
        """Take k tokens for a call that will make at most k requests."""
        if cls not in CLASS_NAMES:
            raise BudgetError(f"unknown budget class {cls!r}")
        if type(k) is not int or not 1 <= k <= self.burst:
            raise BudgetError(f"a reservation must be 1..{self.burst} requests, got {k!r}")
        self._refill(now)
        if self._blocked_by_higher(cls, now) or self.tokens < k:
            self.demand.setdefault(cls, now)
            return False   # nothing spent; the refill is persisted with the next grant
        self.tokens -= k
        self.demand.pop(cls, None)
        self.last_grant[cls] = now
        self.reserved_total[cls] += k
        self._persist()
        return True

    def settle(self, reserved: int, cost, label: str) -> int:
        """Refund reserved - actual. A missing or over-cap cost is charged in full
        and counted as a violation; the caller suspends that emitter."""
        actual = cost.get("quipu_requests") if isinstance(cost, dict) else None
        if type(actual) is not int or not 0 <= actual <= reserved:
            self.violations[label] = self.violations.get(label, 0) + 1
            LOG.warning("adapter %s reported no valid cost (%r) for a %d-request reservation",
                        label, cost, reserved)
            return reserved
        refund = reserved - actual
        if refund:
            self.tokens = min(self.burst, self.tokens + refund)
            self.refunded_total += refund
            self._persist()
        return actual

    def due(self, k: int, now: float) -> float:
        """Earliest time k tokens could be available (ignoring priority)."""
        self._refill(now)
        return now if self.tokens >= k else now + (k - self.tokens) / self.rate

    def metrics(self, esc) -> list[str]:
        lines = [f"chaski_budget_tokens {self.tokens:.3f}",
                 f"chaski_budget_rate_requests_per_second {self.rate}",
                 f"chaski_budget_burst_requests {self.burst}",
                 f"chaski_budget_refunded_total {self.refunded_total}"]
        lines += [f'chaski_budget_reserved_total{{class="{CLASS_NAMES[c]}"}} {n}'
                  for c, n in sorted(self.reserved_total.items())]
        lines += [f'chaski_budget_violation_total{{emitter="{esc(label)}"}} {n}'
                  for label, n in sorted(self.violations.items())]
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
        self.page_requests = page_requests or budget.burst
        if not 1 <= self.page_requests <= budget.burst:
            raise BudgetError("discover page reservation must fit the burst")
        self._head = head
        conn.executescript(RECONCILE_SCHEMA)
        self.is_eligible, self.blocking = eligible(self.runners)
        self.unknown = 0
        self.purge_seconds = None
        self.retry_at = 0.0

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
        labels = [r.emitter.label for r in self.runners]
        if not labels:
            return 0
        marks = ",".join("?" * len(labels))
        return self.conn.execute(
            f"SELECT COUNT(*) FROM adapter_deadlines WHERE priority=2 AND emitter IN ({marks})",
            labels).fetchone()[0]

    # --- entry ------------------------------------------------------------------
    def reconcile_due(self, now: float) -> bool:
        """The daily catalogue pass, or a changed subscription (no stored pass)."""
        for runner in self.runners:
            row = self.conn.execute("SELECT last_success FROM adapter_reconcile WHERE emitter=?",
                                    (runner.emitter.label,)).fetchone()
            if row is None or now - row[0] >= runner.emitter.interval_s:
                return True
        return False

    def should_enter(self, now: float) -> bool:
        """Only from STREAM. Lag/backlog triggers respect the cooldown; the daily
        or new-subscription catalogue pass is the existing backstop, due on its own clock."""
        if not self.is_eligible or self.state != "stream":
            return False
        if self.reconcile_due(now):
            return True
        lag = self.feed.lag
        backlog = self.backlog()
        if not ((lag is not None and lag > self.max_lag_tx) or backlog > self.max_backlog):
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
            self.unknown += 1
            LOG.warning("reconcile discover %s UNKNOWN (page retried later): %s", label, exc)
            return False
        self.budget.settle(self.page_requests, out.get("cost"), label)
        items = strings(out.get("items"))
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
            self.conn.executemany("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)",
                                  [(r.emitter.label, now) for r in self.runners])
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
    def step(self, now: float) -> str | None:
        """Advance the machine. Returns 'jumped' when the cursor moved to W."""
        state = self.state
        if state == "stream":
            if self.should_enter(now):
                self.enter(now)
            return None
        if state == "reconcile":
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
            self.conn.execute("UPDATE reconcile_run SET state='stream', completed=? WHERE id=1", (now,))
            self.conn.commit()
            LOG.warning("RECONCILE complete: back to stream mode, cooldown %ss", self.cooldown_s)
        return None

    def next_wakeup(self, now: float) -> float:
        state = self.state
        if state == "reconcile":
            return max(now + 0.01, self.retry_at, self.budget.due(self.page_requests, now))
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
        return lines
