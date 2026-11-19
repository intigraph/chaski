"""aegis-ssvr C1: reconcile mode and the global request budget.

Each test names the review finding (design note v2, B1-B6) it enforces.
"""
import json

import pytest

import incremental as inc
import reconcile as rc
from emitter import Emitter, ProtocolError, connect


class Sink:
    def deliver(self, event):
        pass


def change(tx, entity):
    return {"tx": tx, "sequence": 0, "entity": entity, "attribute": "status",
            "graph": "ROOT", "op": "assert", "value": "closed"}


class Adapters:
    """Fake budget-capped adapters: discover pages over a fixed population."""

    def __init__(self, population, page=3, cost=5, contract=True):
        self.population, self.page, self.cost, self.contract = population, page, cost, contract
        self.calls = []

    def __call__(self, emitter, request=None):
        if request is None:
            d = {"version": 1, "attributes": ["status"], "graphs": ["ROOT"]}
            if self.contract is True or emitter.label in (self.contract or ()):
                d |= {"budget_cap": 1, "reconcile_contract": 1}
            return d
        self.calls.append((emitter.label, request))
        cost = {"quipu_requests": self.cost}
        if request.get("discover"):
            items = sorted(self.population[emitter.label])
            start = request.get("cursor") or 0
            chunk = items[start:start + self.page]
            out = {"items": chunk, "cost": cost}
            if start + self.page >= len(items):
                out["complete"] = True
            else:
                out["next_cursor"] = start + self.page
            return out
        if "route" in request:
            return {"items": sorted({c["entity"] for c in request["route"]}), "cost": cost}
        scope = list(request["items"])
        return {"scope": scope, "next_checks": {i: None for i in scope}, "cost": cost,
                "records": [{"item": i, "verdict": "BLOCKED", "event_id": "e-" + i} for i in scope]}


class Feed:
    def __init__(self, lag=-1):
        self.lag, self.cursor_tx = lag, -1


def build(tmp_path, monkeypatch, labels=("a", "b"), contract=True, rate=1.0, burst=40, head=10_000,
          population=None, cursor=100):
    conn = connect(tmp_path / "emitter.db")
    population = population or {label: {f"{label}{i}" for i in range(7)} for label in labels}
    adapters = Adapters(population, contract=contract)
    monkeypatch.setattr(inc, "call", adapters)
    budget = rc.RequestBudget(conn, rate, burst)
    runners = [inc.ChangeRunner(Emitter(label, ["unused"], 86400, "owner", change_driven=True), conn, Sink(),
                                request_budget=budget if contract is True else None, call_requests=10)
               for label in labels]
    conn.execute("INSERT OR REPLACE INTO change_cursor VALUES (1,?)", (cursor,))
    conn.commit()
    feed = Feed()
    recon = rc.Reconciler(conn, feed, runners, budget, page_requests=10, head=lambda: head)
    return conn, budget, runners, recon, adapters, feed


def fill_inbox(conn, labels, rows=50):
    conn.execute("BEGIN")
    for label in labels:
        conn.executemany("INSERT INTO change_inbox VALUES (?,?,0,?)",
                         [(label, tx, json.dumps(change(tx, f"{label}{tx % 9}"))) for tx in range(1, rows + 1)])
    conn.commit()


def run_until(recon, start, stop_state, step=1.0, limit=100_000):
    now = start
    while recon.state != stop_state and now < start + limit:
        recon.step(now)
        now += step
    return now


# --- B1: pre-reservation, a persisted bucket that starts at 0, bounded windows ---------

def test_new_bucket_starts_empty_and_restart_does_not_refill(tmp_path):
    conn = connect(tmp_path / "e.db")
    b = rc.RequestBudget(conn, 0.25, 40)
    assert not b.try_reserve(rc.LIVE, 10, 1000.0)      # starts at 0, not full
    assert b.try_reserve(rc.LIVE, 10, 1040.0)          # 40 s x 0.25 = 10 tokens
    # A crash loop: every restart reopens the SAME persisted bucket.
    for _ in range(5):
        again = rc.RequestBudget(conn, 0.25, 40)
        assert not again.try_reserve(rc.LIVE, 10, 1040.0)
    assert rc.RequestBudget(conn, 0.25, 40).tokens == pytest.approx(0.0)


def test_reservation_is_a_hard_cap_within_the_burst(tmp_path):
    b = rc.RequestBudget(connect(tmp_path / "e.db"), 1.0, 40)
    for bad in (0, 41, 1.5, "10"):
        with pytest.raises(rc.BudgetError):
            b.try_reserve(rc.LIVE, bad, 0.0)


def test_unused_reservation_is_refunded_and_a_bad_cost_is_charged_in_full(tmp_path):
    b = rc.RequestBudget(connect(tmp_path / "e.db"), 1.0, 40)
    b.try_reserve(rc.LIVE, 1, 0.0)
    assert b.try_reserve(rc.LIVE, 30, 40.0) and b.tokens == pytest.approx(10.0)
    assert b.settle(30, {"quipu_requests": 12}, "x") == 12 and b.tokens == pytest.approx(28.0)
    for bad in (None, {}, {"quipu_requests": 31}, {"quipu_requests": -1}, {"quipu_requests": True}):
        before = b.tokens
        assert b.settle(30, bad, "x") == 30 and b.tokens == before
    assert b.violations["x"] == 5


def test_adapter_call_carries_the_cap_and_a_cost_violation_suspends_the_emitter(tmp_path, monkeypatch):
    conn, budget, runners, *_ = build(tmp_path, monkeypatch, labels=("a",))
    r = runners[0]
    conn.execute("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a','a1',0,0)")
    budget.tokens, budget.updated = 40.0, 0.0
    seen = []

    def lying(emitter, request=None):
        seen.append(request)
        return {"scope": ["a1"], "records": [], "next_checks": {"a1": None}}   # no cost
    monkeypatch.setattr(inc, "call", lying)
    r.tick(1.0)
    assert seen[0]["budget"] == {"max_requests": 10}
    assert r.suspended and budget.violations["a"] == 1
    seen.clear()
    budget.tokens = 40.0
    r.tick(500.0)
    assert seen == []                                     # suspended: no more calls


def test_not_admitted_is_not_a_failure(tmp_path, monkeypatch):
    conn, budget, runners, _, adapters, _ = build(tmp_path, monkeypatch, labels=("a",))
    conn.execute("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a','a1',0,0)")
    runners[0].tick(1.0)                                  # bucket empty
    assert adapters.calls == [] and runners[0].runs["unknown"] == 0
    assert runners[0].next_wakeup(1.0) >= 1.0 + 9         # waits for 10 tokens at 1/s


def test_any_15_minute_window_stays_within_the_bound(tmp_path):
    """4 emitters + feed + sinks all greedy for 2 h never exceed rate*900+burst."""
    rate, burst = 0.25, 40
    b = rc.RequestBudget(connect(tmp_path / "e.db"), rate, burst)
    grants = []
    now = 0.0
    while now < 7200:
        for cls, k in ((rc.FEED, 1), (rc.SINK, 1), (rc.LIVE, 10), (rc.LIVE, 10),
                       (rc.DISCOVER, 10), (rc.RECONCILE, 10)):
            if b.try_reserve(cls, k, now):
                grants.append((now, k))
        now += 1.0
    worst = max(sum(k for t, k in grants if s <= t < s + 900) for s in range(0, 7200 - 900, 10))
    assert worst <= rate * 900 + burst
    assert worst / 900 <= 0.3


def test_lower_class_waits_for_a_higher_class_with_a_starvation_guard(tmp_path):
    b = rc.RequestBudget(connect(tmp_path / "e.db"), 1.0, 40)
    b.tokens, b.updated = 0.0, 0.0
    assert not b.try_reserve(rc.LIVE, 20, 5.0)            # LIVE now waiting (5 tokens)
    assert not b.try_reserve(rc.RECONCILE, 1, 5.0)       # lower class blocked by LIVE...
    assert not b.try_reserve(rc.RECONCILE, 1, 6.0)
    assert b.try_reserve(rc.LIVE, 20, 21.0)
    b.demand[rc.LIVE] = 600.0                             # LIVE waiting again, freshly
    assert not b.try_reserve(rc.RECONCILE, 1, 604.0)
    assert b.try_reserve(rc.RECONCILE, 1, 5.0 + rc.STARVATION_S)   # ...but never starved


# --- B2: no per-emitter opt-out -------------------------------------------------------

def test_one_emitter_without_the_contract_disables_reconcile_for_all(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch, contract=("a",))
    fill_inbox(conn, ("a", "b"), rows=6000)
    assert not recon.is_eligible and recon.blocking == ["b"]
    budget.tokens, budget.updated = 40.0, 0.0
    for now in range(0, 3600, 10):
        recon.step(float(now))
    assert recon.state == "stream"
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (100,)   # never jumps
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone() == (12000,)


def test_global_budget_refuses_an_adapter_without_a_hard_cap(tmp_path, monkeypatch):
    conn = connect(tmp_path / "e.db")
    monkeypatch.setattr(inc, "call", Adapters({"a": set()}, contract=False))
    with pytest.raises(ProtocolError, match="budget_cap"):
        inc.ChangeRunner(Emitter("a", ["unused"], 86400, "owner", change_driven=True), conn, Sink(),
                         request_budget=rc.RequestBudget(conn, 1.0, 40))


# --- the run, B4 purge/projection, B5 identity and no false clears ---------------------

def test_reconcile_run_discovers_purges_jumps_and_drains(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=3000)
    # A tracked verdict that discover no longer returns is re-evaluated, not cleared.
    conn.execute("INSERT INTO verdicts (emitter,item,verdict,generation,updated) VALUES ('a','gone',"
                 "'BLOCKED',1,0)")
    conn.execute("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a','a1',5,0)")
    conn.commit()
    assert recon.should_enter(0.0)
    end = run_until(recon, 0.0, "draining")
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (10_000,)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone() == (0,)
    rc.assert_inbox_projection(conn)
    assert dict(conn.execute("SELECT emitter,pending FROM change_inbox_counts")) == {}
    queued = set(conn.execute("SELECT emitter,item,priority FROM adapter_deadlines"))
    assert ("a", "gone", 2) in queued and ("a", "a1", 0) in queued            # never demoted
    assert {(e, i) for e, i, _ in queued} >= {("b", f"b{i}") for i in range(7)}
    assert conn.execute("SELECT verdict FROM verdicts WHERE item='gone'").fetchone() == ("BLOCKED",)
    # Pages: 7 items at 3 per page = 3 pages per emitter, each pinned to W.
    pages = [r for _, r in adapters.calls if r.get("discover")]
    assert len(pages) == 6 and all(r["as_of_tx"] == 10_000 and r["budget"] == {"max_requests": 10}
                                   for r in pages)
    # The delete trigger is restored: a later delete still maintains the projection.
    conn.execute("INSERT INTO change_inbox VALUES ('a',10001,0,?)", (json.dumps(change(10001, "z")),))
    conn.execute("DELETE FROM change_inbox WHERE tx=10001")
    rc.assert_inbox_projection(conn)
    # Drain: runners evaluate everything, then the machine returns to stream.
    now = end
    while recon.state != "stream" and now < end + 100_000:
        for r in runners:
            r.tick(now)
        recon.step(now)
        now += 5
    assert recon.state == "stream" and recon.remaining() == 0
    assert any(r.batch > inc.BATCH for r in runners)      # batch grew from measured cost


def test_crash_mid_discovery_resumes_the_same_run_and_watermark(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    now = 0.0
    while len([r for _, r in adapters.calls if r.get("discover")]) < 2:
        recon.step(now)
        now += 1
    run_id, target = recon.run()[:2]
    # "Restart": a new reconciler over the same database; head has moved on.
    again = rc.Reconciler(conn, feed, runners, budget, page_requests=10, head=lambda: 99_999)
    assert again.state == "reconcile" and again.run()[:2] == (run_id, target)
    run_until(again, now, "draining")
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (target,)


def test_projection_mismatch_rolls_the_jump_back(tmp_path, monkeypatch):
    conn, budget, runners, recon, *_ = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    monkeypatch.setattr(rc, "recompute_inbox_projection",
                        lambda c: c.execute("DELETE FROM change_inbox_frontier"))
    now = 0.0
    while recon.unknown == 0 and now < 2000:
        recon.step(now)
        now += 1
    assert recon.state == "reconcile" and recon.unknown == 1
    recon.step(now + 1)
    assert recon.unknown == 1                              # backed off, no tight retry
    assert recon.next_wakeup(now + 1) >= now - 1 + rc.JUMP_RETRY_S
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (100,)
    assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone() == (12000,)
    assert conn.execute("SELECT 1 FROM sqlite_master WHERE name='change_inbox_projection_delete'").fetchone()


def test_a_malformed_discover_page_does_not_advance(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    original = adapters.__call__

    def both(emitter, request=None):
        out = original(emitter, request)
        if request and request.get("discover"):
            out |= {"complete": True, "next_cursor": 1}
        return out
    monkeypatch.setattr(inc, "call", both)
    for now in range(0, 600):
        recon.step(float(now))
    assert recon.state == "reconcile" and recon.unknown >= 1
    assert not conn.execute("SELECT 1 FROM reconcile_emitter WHERE complete=1").fetchone()


def test_runners_and_feed_pause_while_discovering(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    recon.step(10.0)                                       # enter (1 token)... not yet
    run = 10.0
    while recon.state != "reconcile":
        run += 1
        recon.step(run)
    assert recon.paused
    for r in runners:
        r.paused = True
    budget.tokens = 40.0
    adapters.calls.clear()
    for r in runners:
        r.tick(run + 1)
    assert not [c for c in adapters.calls if "route" in c[1]]


# --- B3: entry only from STREAM, cooldown and hysteresis --------------------------------

def test_cooldown_blocks_backlog_reentry_but_not_a_large_lag(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    conn.execute("INSERT INTO reconcile_run VALUES (1,1,5000,'stream',0,1000)")
    conn.executemany("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)", [("a", 1000), ("b", 1000)])
    fill_inbox(conn, ("a", "b"), rows=6000)
    assert not recon.should_enter(2000.0)                  # backlog, inside cooldown
    feed.lag = 60_000
    assert recon.should_enter(2000.0)                      # lag beyond the override
    feed.lag = -1
    assert recon.should_enter(1000.0 + 86400)              # cooldown over (and daily pass due)


def test_draining_and_reconcile_never_reenter(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    for state in ("reconcile", "draining"):
        conn.execute("INSERT OR REPLACE INTO reconcile_run VALUES (1,1,5000,?,0,NULL)", (state,))
        feed.lag = 10**9
        assert not recon.should_enter(10**6)


def test_quiet_stream_with_a_fresh_daily_pass_does_not_enter(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    conn.executemany("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)", [("a", 100), ("b", 100)])
    feed.lag = 10
    assert not recon.should_enter(200.0)
