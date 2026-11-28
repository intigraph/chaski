"""aegis-exe6.1: a change emitter's reservation must fit the bucket it can reach.

Observed (aegis-exe6): the first call of a kind reserved call_requests = 40 = the
burst under LIVE while FEED/SINK traffic held the bucket at <= 20 tokens. Every
attempt was NotAdmitted, silently, and no emitter ran after a restart.
"""
import incremental as inc
import reconcile as rc
from emitter import Emitter, connect
from test_reconcile import Adapters, Sink


def runner_with(tmp_path, monkeypatch, call_requests=40, burst=40, rate=1.0):
    conn = connect(tmp_path / "emitter.db")
    adapters = Adapters({"a": set()}, cost=3)
    monkeypatch.setattr(inc, "call", adapters)
    budget = rc.RequestBudget(conn, rate, burst)
    r = inc.ChangeRunner(Emitter("a", ["unused"], 86400, "owner", change_driven=True), conn, Sink(),
                         request_budget=budget, call_requests=call_requests)
    return conn, budget, r, adapters


def due(conn, *items, priority=0):
    conn.executemany("INSERT INTO adapter_deadlines (emitter,item,due,priority) VALUES ('a',?,0,?)"
                     " ON CONFLICT(emitter,item) DO UPDATE SET due=0",
                     [(i, priority) for i in items])
    conn.commit()


def feed_holds_bucket_at(budget, level, now):
    """FEED/SINK traffic: a higher class takes everything above `level`."""
    # What the bucket holds now; a real FEED caller only ever calls try_reserve.
    held = budget.tokens if budget.updated is None else min(
        budget.burst, budget.tokens + (now - budget.updated) * budget.rate)
    excess = int(held - level)
    if excess >= 1:
        assert budget.try_reserve(rc.FEED, excess, now)


def caps(adapters):
    return [c[1]["budget"]["max_requests"] for c in adapters.calls if "items" in c[1]]


def test_bucket_held_at_20_with_call_requests_40_still_runs_the_emitter(tmp_path, monkeypatch):
    conn, budget, r, adapters = runner_with(tmp_path, monkeypatch)
    due(conn, *[f"x{i}" for i in range(60)])
    now = 0.0
    while now < 3 * budget.peak_window and not r.runs["ok"]:
        feed_holds_bucket_at(budget, 20, now)
        r.tick(now)
        now += 1.0
    assert r.runs["ok"] > 0, "an emitter whose reservation exceeds the attainable bucket never runs"
    assert caps(adapters) and max(caps(adapters)) <= 20
    # Sized to the cap: no more than the default batch's share of 20/40.
    assert len([c for c in adapters.calls if "items" in c[1]][0][1]["items"]) <= inc.BATCH * 20 // 40
    # It keeps running under the same traffic, not once.
    ran = r.runs["ok"]
    for _ in range(300):
        due(conn, f"y{int(now)}")
        feed_holds_bucket_at(budget, 20, now)
        r.tick(now)
        now += 1.0
    assert r.runs["ok"] > ran + 5
    assert max(caps(adapters)) <= 20


def test_an_unloaded_bucket_still_reserves_the_full_call_cap(tmp_path, monkeypatch):
    conn, budget, r, adapters = runner_with(tmp_path, monkeypatch)
    budget.tokens, budget.updated = 40.0, 0.0
    due(conn, "x1")
    r.tick(1.0)
    assert caps(adapters) == [40]


def test_a_starved_emitter_gets_one_minimal_batch_within_starvation_s(tmp_path, monkeypatch):
    conn, budget, r, adapters = runner_with(tmp_path, monkeypatch)
    due(conn, *[f"x{i}" for i in range(10)])
    budget.tokens, budget.updated = 40.0, 0.0
    t = 1.0
    ran_at = None
    while t <= 1.0 + rc.STARVATION_S + 5:
        budget.demand[rc.FEED] = t        # FEED always waiting: strict priority blocks LIVE
        r.tick(t)
        if r.runs["ok"]:
            ran_at = t
            break
        t += 1.0
    assert ran_at is not None and ran_at - 1.0 <= rc.STARVATION_S + 1
    sent = [c[1]["items"] for c in adapters.calls if "items" in c[1]]
    assert sent == [["x0"]]                       # one minimal batch, not the backlog
    assert r.not_admitted.get("priority", 0) >= rc.STARVATION_S - 1
    # Bounded: the guard re-arms; the very next tick is refused again.
    budget.demand[rc.FEED] = t + 1
    r.tick(t + 1)
    assert r.runs["ok"] == 1


def test_not_admitted_is_counted_by_reason_and_exported(tmp_path, monkeypatch):
    conn, budget, r, adapters = runner_with(tmp_path, monkeypatch)
    due(conn, "x1")
    budget.tokens, budget.updated = 0.0, 0.0
    r.tick(0.0)                                   # empty bucket
    assert r.not_admitted == {"tokens": 1}
    budget.tokens = 40.0
    budget.demand[rc.SINK] = 0.5
    r.tick(0.5)                                   # a higher class is waiting
    assert r.not_admitted == {"tokens": 1, "priority": 1}
    assert r.runs.get("unknown", 0) == 0 and not r.runs.get("ok")   # refusals are not failures
    text = "\n".join(r.metrics(lambda s: s))
    assert 'chaski_emitter_not_admitted_total{emitter="a",reason="tokens"} 1' in text
    assert 'chaski_emitter_not_admitted_total{emitter="a",reason="priority"} 1' in text
    assert 'chaski_emitter_last_run_age_seconds{emitter="a"}' in text
    assert "chaski_budget_attainable_requests" in "\n".join(budget.metrics(lambda s: s))


def test_attainable_level_follows_the_bucket_peak(tmp_path):
    conn = connect(tmp_path / "emitter.db")
    b = rc.RequestBudget(conn, 1.0, 40)
    assert b.attainable(0.0) == 40                # UNKNOWN before a window: assume the burst
    now = 0.0
    while now < 2.5 * b.peak_window:
        feed_holds_bucket_at(b, 12, now)
        now += 1.0
    assert b.attainable(now - 1.0) == 12          # what a lower class can hold behind FEED
    # Traffic stops: the bucket refills and the cap returns to the burst.
    assert b.attainable(now + 100) == 40
