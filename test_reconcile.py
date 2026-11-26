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


# --- review round 2 (Gemini on #77) --------------------------------------------------

def test_discover_cost_violation_discards_the_page_and_suspends(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    original = adapters.__call__

    def overspend(emitter, request=None):
        out = original(emitter, request)
        if request and request.get("discover"):
            out["cost"] = {"quipu_requests": 11}            # cap was 10
        return out
    monkeypatch.setattr(inc, "call", overspend)
    for now in range(0, 400):
        recon.step(float(now))
    assert runners[0].suspended and budget.violations["a"] == 1
    assert recon.state == "reconcile"                       # cannot complete, never jumps
    assert not conn.execute("SELECT 1 FROM reconcile_emitter WHERE complete=1 OR cursor IS NOT NULL").fetchone()
    assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (100,)
    calls = len(adapters.calls)
    for now in range(400, 800):
        recon.step(float(now))
    assert len([c for c in adapters.calls[calls:] if c[0] == "a"]) == 0   # suspended: no more pages


def test_restart_after_downtime_does_not_refill(tmp_path):
    conn = connect(tmp_path / "e.db")
    b = rc.RequestBudget(conn, 1.0, 40)
    b.try_reserve(rc.LIVE, 1, 0.0)
    assert b.try_reserve(rc.LIVE, 5, 10.0)                  # 10 refilled, 5 left
    later = rc.RequestBudget(conn, 1.0, 40)                 # a day of downtime
    assert later.tokens == pytest.approx(5.0)
    assert not later.try_reserve(rc.LIVE, 10, 86_400.0)     # downtime is not credited
    assert later.try_reserve(rc.LIVE, 10, 86_405.0)


def test_rate_and_burst_must_fit_the_window_bound(tmp_path):
    conn = connect(tmp_path / "e.db")
    rc.RequestBudget(conn, 0.25, 40, max_window_rate=0.3)   # 0.294 req/s worst window
    with pytest.raises(rc.BudgetError, match="window"):
        rc.RequestBudget(conn, 0.3, 40, max_window_rate=0.3)


def test_waiting_live_keeps_priority_through_a_long_refill_and_nothing_spins(tmp_path):
    b = rc.RequestBudget(connect(tmp_path / "e.db"), 0.25, 40)
    assert b.demand_ttl >= 1.5 * 40 / 0.25
    b.tokens, b.updated = 0.0, 0.0
    t = 0.0
    while not b.try_reserve(rc.LIVE, 40, t):                # LIVE re-asks at its due time
        assert not b.try_reserve(rc.RECONCILE, 10, t)       # reconcile never jumps ahead
        nxt = b.due(40, t, rc.LIVE)
        assert nxt > t                                      # no busy-spin
        t = nxt
    assert t >= 160
    b.demand[rc.SINK] = t                                   # a higher class just failed
    b.tokens = 40.0
    assert b.due(10, t, rc.LIVE) >= t + rc.BLOCKED_RECHECK_S   # blocked by priority: bounded re-check


def test_a_drain_longer_than_a_day_does_not_retrigger(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    end = run_until(recon, 0.0, "draining")
    assert conn.execute("SELECT COUNT(*) FROM adapter_reconcile").fetchone() == (0,)   # not stamped yet
    conn.execute("DELETE FROM adapter_deadlines WHERE priority=2")
    done = end + 2 * 86400                                   # drained two days later
    recon.step(done)
    assert recon.state == "stream"
    assert set(conn.execute("SELECT emitter,last_success FROM adapter_reconcile")) == {("a", done), ("b", done)}
    assert not recon.should_enter(done + 1)                  # no immediate re-entry
    assert recon.should_enter(done + 86400)                  # next daily pass, after the cooldown


def test_emitter_set_change_aborts_a_persisted_run(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    now = 0.0
    while recon.state != "reconcile":
        recon.step(now)
        now += 1
    for subset in ([runners[0]], runners + [runners[0].__class__.__new__(runners[0].__class__)]):
        if len(subset) > 2:
            extra = subset[-1]
            extra.emitter = Emitter("c", ["unused"], 86400, "owner", change_driven=True)
            extra.subscription = {"budget_cap": 1, "reconcile_contract": 1}
        again = rc.Reconciler(conn, feed, subset, budget, page_requests=10, head=lambda: 10_000)
        assert again.state == "reconcile"                     # the persisted run
        again.step(now)                                       # no StopIteration, no jump
        assert again.state == "stream"
        assert conn.execute("SELECT tx FROM change_cursor").fetchone() == (100,)
        assert conn.execute("SELECT COUNT(*) FROM change_inbox").fetchone() == (12000,)
        conn.execute("INSERT OR REPLACE INTO reconcile_run VALUES (1,9,10000,'reconcile',0,NULL)")
        conn.executemany("INSERT OR REPLACE INTO reconcile_emitter VALUES (9,?,NULL,0)", [("a",), ("b",)])


def test_suspended_emitter_does_not_wedge_draining(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    fill_inbox(conn, ("a", "b"), rows=6000)
    run_until(recon, 0.0, "draining")
    runners[0].suspended = True
    conn.execute("DELETE FROM adapter_deadlines WHERE emitter='b' AND priority=2")
    recon.step(10**6)
    assert recon.state == "stream"
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines WHERE emitter='a' AND priority=2").fetchone()[0] > 0


def test_mixed_batch_never_charges_reconcile_work_as_live(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch, labels=("a",))
    r = runners[0]
    r.batch = 10
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,?)",
                     [("live1", 0), ("rec1", 2), ("rec2", 2)])
    budget.tokens, budget.updated = 40.0, 0.0
    r.tick(1.0)
    sent = [c[1]["items"] for c in adapters.calls if "items" in c[1]]
    assert sent == [["live1"]]
    assert budget.reserved_total[rc.LIVE] == 10 and budget.reserved_total[rc.RECONCILE] == 0


def test_stream_reconnect_waits_for_a_reserved_permit(tmp_path):
    import change_stream
    b = rc.RequestBudget(connect(tmp_path / "e.db"), 1.0, 40)
    quipu = type("Q", (), {"base": "http://127.0.0.1:9"})()
    s = change_stream.ChangeStream(quipu, 5, budgeted=True)
    try:
        assert s.want_connect.wait(2) and s.connects == 0     # asks, does not connect
        s.grant(b, 0.0)                                       # bucket empty: no permit
        assert s.connects == 0 and s.want_connect.is_set()
        s.grant(b, 5.0)                                       # 5 tokens: one reserved
        for _ in range(50):
            if s.connects:
                break
            import time
            time.sleep(0.05)
        assert s.connects == 1 and b.reserved_total[rc.FEED] == 1
    finally:
        s.close()


def test_daily_pass_also_waits_for_the_cooldown(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch)
    conn.executemany("INSERT OR REPLACE INTO adapter_reconcile VALUES (?,?)", [("a", 0), ("b", 0)])
    conn.execute("INSERT INTO reconcile_run VALUES (1,1,5000,'stream',0,200000)")
    assert recon.reconcile_due(200_100) == "daily"
    assert not recon.should_enter(200_100)                   # completed 100 s ago: cooldown holds
    assert recon.should_enter(200_000 + 86_400)
    conn.execute("DELETE FROM adapter_reconcile WHERE emitter='b'")
    assert recon.should_enter(200_100)                       # a new subscription does not wait


def test_adapter_metrics_count_reported_cost_not_reserved_budget(tmp_path, monkeypatch):
    conn, budget, runners, _, _, _ = build(tmp_path, monkeypatch, labels=("a",))
    assert not budget.try_reserve(rc.LIVE, 1, 1000)
    budget.try_reserve(rc.LIVE, 1, 1100)
    # A ten-request reservation makes only two reads; the counter must be two,
    # while the existing refund and persistent safety budget retain their meaning.
    monkeypatch.setattr(inc, "call", lambda *_: {"cost": {"quipu_requests": 2}})
    runners[0]._call(rc.LIVE, {"items": []}, 1100)
    metrics = "\n".join(budget.metrics(str))
    assert "chaski_adapter_quipu_requests_total 2\n" in metrics
    assert "chaski_adapter_quipu_accounting_unknown_total 0\n" in metrics
    assert budget.refunded_total == 8
    conn.close()


def test_failed_adapter_keeps_charge_and_exposes_unknown_cost(tmp_path, monkeypatch):
    conn, budget, runners, _, _, _ = build(tmp_path, monkeypatch, labels=("a",))
    assert not budget.try_reserve(rc.LIVE, 1, 1000)
    budget.try_reserve(rc.LIVE, 1, 1100)
    tokens = budget.tokens
    def fail(*_):
        raise ProtocolError("failed after an unknown number of HTTP attempts")
    monkeypatch.setattr(inc, "call", fail)
    with pytest.raises(ProtocolError):
        runners[0]._call(rc.LIVE, {"items": []}, 1100)
    assert budget.tokens == tokens - 10
    assert budget.refunded_total == 0
    metrics = "\n".join(budget.metrics(str))
    assert "chaski_adapter_quipu_requests_total 0\n" in metrics
    assert "chaski_adapter_quipu_accounting_unknown_total 1\n" in metrics
    conn.close()


def test_malformed_cost_is_unknown_not_reported_requests(tmp_path):
    budget = rc.RequestBudget(connect(tmp_path / "e.db"), 1, 40)
    assert budget.settle(10, {"quipu_requests": 11}, "a") == 10
    assert budget.adapter_requests_total == 0
    assert budget.adapter_accounting_unknown_total == 1
    assert budget.violations == {"a": 1}


def test_failed_discovery_exposes_unknown_cost(tmp_path, monkeypatch):
    conn, budget, _, recon, _, _ = build(tmp_path, monkeypatch, labels=("a",))
    fill_inbox(conn, ("a",))
    def fail(*_):
        raise ProtocolError("discovery transport failed")
    monkeypatch.setattr(inc, "call", fail)
    for now in range(1000, 1100):
        recon.step(now)
        if budget.adapter_accounting_unknown_total:
            break
    assert budget.adapter_accounting_unknown_total > 0
    assert budget.adapter_requests_total == 0
    assert budget.refunded_total == 0
    conn.close()


# --- aegis-p1u4: discovery progress is visible from outside ------------------------------

def test_discovery_progress_moves_with_each_successful_page(tmp_path, monkeypatch, caplog):
    conn, budget, _, recon, adapters, _ = build(tmp_path, monkeypatch, labels=("a", "b"))
    fill_inbox(conn, ("a", "b"), rows=3000)
    def page_metrics():
        return {line.rsplit(" ", 1)[0]: float(line.rsplit(" ", 1)[1])
                for line in recon.metrics(str, 10.0) if "discover" in line}
    assert recon.should_enter(0.0)
    now = 0.0
    while recon.state == "stream" and now < 1000:   # the entering step discovers nothing
        recon.step(now)
        now += 1.0
    assert recon.state == "reconcile" and page_metrics() == {'chaski_reconcile_discover_complete{emitter="a"}': 0,
                              'chaski_reconcile_discover_complete{emitter="b"}': 0}
    caplog.set_level("INFO", logger="chaski.reconcile")
    seen = []
    while recon.state == "reconcile" and now < 100_000:
        recon.step(now)
        m = page_metrics()
        stamp = m.get('chaski_reconcile_discover_last_success_timestamp_seconds{emitter="a"}')
        if stamp is not None and (not seen or seen[-1][1] != stamp):
            seen.append((m['chaski_reconcile_discover_pages_total{emitter="a"}'], stamp))
        now += 1.0
    pages = [r for label, r in adapters.calls if label == "a" and r.get("discover")]
    # One timestamp move per committed page, strictly increasing, pages counted 1..3.
    assert [p for p, _ in seen] == [1, 2, 3] == list(range(1, len(pages) + 1))
    assert all(a < b for (_, a), (_, b) in zip(seen, seen[1:]))
    oks = [r.getMessage() for r in caplog.records if " page " in r.getMessage() and " ok:" in r.getMessage()]
    assert len(oks) == 6 and oks[0].startswith("reconcile discover a page 1 ok:")
    assert "3 items" in oks[0] and "more pages" in oks[0] and "complete" in oks[2]
    # After the jump the run is no longer discovering: no phase series, counters kept.
    m = page_metrics()
    assert not any(k.startswith("chaski_reconcile_discover_complete") for k in m)
    assert m['chaski_reconcile_discover_items_total{emitter="b"}'] == 7


def test_a_failed_discover_page_does_not_move_progress(tmp_path, monkeypatch):
    conn, budget, _, recon, _, _ = build(tmp_path, monkeypatch, labels=("a",))
    fill_inbox(conn, ("a",), rows=6000)
    def fail(*_):
        raise ProtocolError("discovery transport failed")
    monkeypatch.setattr(inc, "call", fail)
    for now in range(1000, 1100):
        recon.step(now)
    assert recon.unknown > 0
    assert not any("pages_total" in line or "last_success" in line for line in recon.metrics(str, 10.0))


# --- aegis-ssvr B: the reported change lag is never the -1 sentinel ----------------------

class HeadFeed(Feed):
    note_head = inc.ChangeFeed.note_head
    lag_report = inc.ChangeFeed.lag_report

    def __init__(self, cursor_tx=-1):
        super().__init__()
        self.cursor_tx, self.head_tx, self.head_observed_at = cursor_tx, -1, None


def test_lag_is_unknown_not_minus_one_until_a_head_is_observed():
    feed = HeadFeed(cursor_tx=100)
    assert feed.lag_report() is None
    feed.note_head(10_000, 5.0)
    assert feed.lag_report() == 9_900
    feed.note_head(9_000, 6.0)                         # an older watermark never moves it back
    assert (feed.head_tx, feed.head_observed_at) == (10_000, 5.0)
    assert HeadFeed().lag_report() is None             # cursor UNKNOWN stays UNKNOWN


def test_paused_reconcile_samples_the_head_once_per_interval(tmp_path, monkeypatch):
    conn, budget, runners, _recon, adapters, _feed = build(tmp_path, monkeypatch)
    heads = iter([5_000, 6_000, 7_000])
    reads = []

    def head():
        reads.append(1)
        return next(heads)
    feed = HeadFeed(cursor_tx=100)
    recon = rc.Reconciler(conn, feed, runners, budget, page_requests=10, head=head)
    budget.try_reserve(rc.LIVE, 1, 900.0)              # the persisted bucket starts empty
    assert not recon.sample_head(1000.0)               # stream state: never sampled
    conn.execute("INSERT OR REPLACE INTO reconcile_run VALUES (1,1,4000,'reconcile',0,NULL)")
    assert recon.paused
    assert recon.sample_head(1000.0) and feed.lag_report() == 4_900
    assert not recon.sample_head(1030.0)               # inside the interval: no request
    assert recon.sample_head(1061.0) and feed.lag_report() == 5_900
    assert len(reads) == 2


def test_a_failed_head_sample_keeps_the_last_measurement(tmp_path, monkeypatch):
    conn, budget, runners, _recon, adapters, _feed = build(tmp_path, monkeypatch)

    def head():
        raise RuntimeError("quipu 408")
    feed = HeadFeed(cursor_tx=100)
    feed.note_head(900, 10.0)
    recon = rc.Reconciler(conn, feed, runners, budget, page_requests=10, head=head)
    budget.try_reserve(rc.LIVE, 1, 900.0)              # the persisted bucket starts empty
    conn.execute("INSERT OR REPLACE INTO reconcile_run VALUES (1,1,4000,'reconcile',0,NULL)")
    assert not recon.sample_head(1000.0)
    assert feed.lag_report() == 800 and recon.unknown == 1


# --- aegis-g68w.14: reserve against measured cost; bounded reconcile share --------------

def test_reservation_follows_measured_cost_and_returns_to_the_cap_after_a_failure(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch, labels=("a",))
    r = runners[0]
    r.batch = 2
    def due(*items):
        conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,0)",
                         [(i,) for i in items])
    due("x1", "x2")
    budget.tokens, budget.updated = 40.0, 0.0
    r.tick(1.0)                                            # no history: the full cap
    caps = [c[1]["budget"]["max_requests"] for c in adapters.calls if "items" in c[1]]
    assert caps == [10]
    # The fake adapter reports 5 requests for 2 items: 2.5 per item. Two items with
    # 25% headroom reserve ceil(2.5 * 2 * 1.25) = 7, and that is the cap it receives.
    r.batch = 2
    due("x3", "x4")
    budget.tokens = 40.0
    r.tick(2.0)
    caps = [c[1]["budget"]["max_requests"] for c in adapters.calls if "items" in c[1]]
    assert caps == [10, 7]
    assert budget.reserved_total[rc.LIVE] == 17
    # A failed call forgets the estimate: the next call reserves the full cap again.
    def fail(emitter, request=None):
        if request is not None and "items" in request:
            raise ProtocolError("transport failed")
        return adapters(emitter, request)
    monkeypatch.setattr(inc, "call", fail)
    due("x5")
    budget.tokens = 40.0
    r.tick(3.0)
    monkeypatch.setattr(inc, "call", adapters)
    r.retry_at = 0.0
    budget.tokens = 40.0
    r.tick(4.0)
    caps = [c[1]["budget"]["max_requests"] for c in adapters.calls if "items" in c[1]]
    assert caps[-1] == 10


def test_reconcile_share_counts_route_calls_as_live_traffic(tmp_path, monkeypatch):
    """The bounded share is over ALL live adapter calls, not only evaluate batches.

    With a change arriving every tick, each tick makes a live route call AND an
    evaluate call. Counting only evaluate batches let 9 live calls (5 routes and
    4 live evaluates) run between reconcile batches, about half the intended share
    (aegis-g68w.14, measured 4.7% of reservations in production against ~20%).
    """
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch, labels=("a",))
    r = runners[0]
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,2)",
                     [(f"rec{i}",) for i in range(6)])
    for step in range(30):
        # Continuous live traffic: a fresh routed change and a fresh live deadline.
        conn.execute("INSERT INTO change_inbox VALUES ('a',?,0,?)",
                     (20_000 + step, json.dumps(change(20_000 + step, f"chg{step}"))))
        conn.execute("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,0)",
                     (f"live{step}",))
        conn.commit()
        budget.tokens, budget.updated = 40.0, float(step)
        r.tick(float(step) + 0.5)
    kinds = []
    for _, request in adapters.calls:
        if "route" in request:
            kinds.append("route")
        elif "items" in request:
            kinds.append("reconcile" if all(i.startswith("rec") for i in request["items"]) else "live")
    assert "route" in kinds and kinds.count("reconcile") >= 2
    gaps, run = [], 0
    for kind in kinds[kinds.index("reconcile") + 1:]:
        if kind == "reconcile":
            gaps.append(run)
            run = 0
        else:
            run += 1
    # A route call can land between the guard's check and the reconcile batch in
    # the same tick, so the bound is RECONCILE_EVERY live calls plus that one.
    assert gaps and max(gaps) <= r.RECONCILE_EVERY + 1, (gaps, kinds)


def test_reconcile_work_runs_within_a_bounded_number_of_live_batches(tmp_path, monkeypatch):
    conn, budget, runners, recon, adapters, feed = build(tmp_path, monkeypatch, labels=("a",))
    r = runners[0]
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,2)",
                     [(f"rec{i}",) for i in range(3)])
    classes = []
    for step in range(12):
        # A live backlog that never empties: a fresh live deadline before every tick.
        conn.execute("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,0)",
                     (f"live{step}",))
        budget.tokens, budget.updated = 40.0, float(step)
        before = dict(budget.reserved_total)
        r.tick(float(step) + 0.5)
        classes.append(rc.RECONCILE if budget.reserved_total[rc.RECONCILE] > before.get(rc.RECONCILE, 0)
                       else rc.LIVE)
    # Strict priority never ran reconcile work here; now it runs after every 4 live batches.
    assert classes.index(rc.RECONCILE) == r.RECONCILE_EVERY
    assert classes.count(rc.RECONCILE) >= 2
    assert conn.execute("SELECT COUNT(*) FROM adapter_deadlines WHERE priority=2").fetchone()[0] < 3
    # Each reconcile batch carries only reconcile items, charged to the reconcile class.
    sent = [c[1]["items"] for c in adapters.calls if "items" in c[1]]
    assert all(all(i.startswith("rec") for i in s) or all(i.startswith("live") for i in s) for s in sent)
