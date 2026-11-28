"""aegis-exe6.2: one item costing more than the cap must wait, not fail forever.

Observed (harding, aegis-exe6): with call_requests 15, `lapsed` needed 16 requests
for a single item and every call failed under a hard cap of 15; entity-review-due
reserved 1 for a call costing 2. A failed call is retried every 60 s with the
same cap, so the work is never lost but never succeeds: a livelock that burns
its reservation each time.
"""
import logging

import pytest

import incremental as inc
import reconcile as rc
from emitter import Emitter, connect
from test_admission import caps, due, feed_holds_bucket_at
from test_reconcile import Adapters, Sink


class CostlyAdapters(Adapters):
    """A budget-capped adapter whose items have real costs: a call whose cost
    exceeds its hard cap fails, as the real adapters do."""

    def __init__(self, item_cost, transient=0):
        super().__init__({"a": set()})
        self.item_cost, self.failed, self.transient = item_cost, [], transient

    def __call__(self, emitter, request=None):
        if request is None or "items" not in request:
            return super().__call__(emitter, request)
        need = sum(self.item_cost(i) for i in request["items"])
        if self.transient:
            self.transient -= 1
            self.calls.append((emitter.label, request))
            self.failed.append((request["budget"]["max_requests"], "transient"))
            raise RuntimeError("adapter exited 3: quipu 502")
        if need > request["budget"]["max_requests"]:
            self.calls.append((emitter.label, request))
            self.failed.append((request["budget"]["max_requests"], need))
            raise RuntimeError("adapter exited 3: request cap exhausted")
        out = super().__call__(emitter, request)
        out["cost"] = {"quipu_requests": need}
        return out


def runner(tmp_path, monkeypatch, cost, call_requests=40, burst=40, transient=0):
    conn = connect(tmp_path / "emitter.db")
    adapters = CostlyAdapters(lambda item: cost, transient)
    monkeypatch.setattr(inc, "call", adapters)
    budget = rc.RequestBudget(conn, 1.0, burst)
    r = inc.ChangeRunner(Emitter("a", ["unused"], 86400, "owner", change_driven=True), conn, Sink(),
                         request_budget=budget, call_requests=call_requests)
    return conn, budget, r, adapters


@pytest.mark.parametrize("cost,held", [(16, 15), (2, 1)])
def test_an_item_costing_more_than_the_cap_waits_then_succeeds_without_failed_calls(
        tmp_path, monkeypatch, cost, held):
    conn, budget, r, adapters = runner(tmp_path, monkeypatch, cost)
    budget.tokens, budget.updated = 40.0, 0.0
    due(conn, "x1")
    r.tick(0.0)                                   # measured: one item costs `cost`
    assert r.runs["ok"] == 1 and r._min_cost["evaluate"] == cost
    # FEED/SINK now hold the bucket at `held` < cost, so the attainable cap drops below it.
    due(conn, "x2")
    now = 1.0
    while now < 3 * budget.peak_window:
        feed_holds_bucket_at(budget, held, now)
        r.tick(now)
        now += 1.0
    assert r._cap(now - 1) == held < cost
    assert adapters.failed == [], "a call capped below one item's cost fails"
    assert r.runs["ok"] == 1, "nothing can succeed while the bucket holds less than one item's cost"
    assert r.not_admitted.get("cost_exceeds_cap", 0) > 0
    # The traffic stops: once the bucket holds >= cost the item runs, under a cap >= cost.
    while r.runs["ok"] == 1 and now < 10 * budget.peak_window:
        r.tick(now)
        now += 1.0
    assert r.runs["ok"] == 2
    assert caps(adapters)[-1] >= cost
    assert adapters.failed == [] and r.runs.get("unknown", 0) == 0, "no reservation was burned on a failure"
    text = "\n".join(r.metrics(lambda s: s))
    assert 'chaski_emitter_not_admitted_total{emitter="a",reason="cost_exceeds_cap"} ' in text


def test_an_item_costing_more_than_call_requests_is_a_loud_configuration_error_not_a_livelock(
        tmp_path, monkeypatch, caplog):
    # A's flag: call_requests 15 with a single item costing 16.
    conn, budget, r, adapters = runner(tmp_path, monkeypatch, 16, call_requests=15)
    budget.tokens, budget.updated = 40.0, 0.0
    due(conn, "x1")
    caplog.set_level(logging.ERROR, logger=inc.LOG.name)
    now = 0.0
    while not r.runs["ok"] and now < 600:
        r.tick(now)
        now += 1.0
    # Nothing could know the cost before the first one-item call failed under 15;
    # that failure is the evidence, and it is the only one.
    assert r.runs["ok"] == 1 and len(adapters.failed) == 1 and adapters.failed[0] == (15, 16)
    assert caps(adapters)[-1] == 16
    assert any("CONFIGURATION ERROR" in m and "call_requests=15" in m for m in caplog.messages)
    text = "\n".join(r.metrics(lambda s: s))
    assert 'chaski_emitter_min_call_cost{emitter="a",kind="evaluate"} 16' in text
    for i in range(5):                            # every later item reserves >= 16; none fails
        due(conn, f"y{i}")
        start = r.runs["ok"]
        while r.runs["ok"] == start and now < 2000:
            r.tick(now)
            now += 1.0
    assert r.runs["ok"] == 6 and len(adapters.failed) == 1


def test_an_item_costing_more_than_the_burst_retries_at_the_whole_burst_without_wedging(tmp_path, monkeypatch):
    # Nothing can admit it, but an untyped failure can not prove that either: it is
    # retried at the whole burst on the normal retry cadence, never above the burst.
    conn, budget, r, adapters = runner(tmp_path, monkeypatch, 41)
    budget.tokens, budget.updated = 40.0, 0.0
    due(conn, "x1")
    for now in range(0, 400):
        r.tick(float(now))
    assert 1 < len(adapters.failed) <= 400 // 60 + 1, "one attempt per retry interval, no storm"
    assert all(held == 40 for held, _ in adapters.failed)
    assert r._min_cost["evaluate"] == 40 and r.runs["ok"] == 0
    assert 'chaski_emitter_min_call_cost{emitter="a",kind="evaluate"} 0' in "\n".join(r.metrics(lambda s: s))


@pytest.mark.parametrize("call_requests", [40, 15])
def test_transient_one_item_failures_do_not_strand_the_kind(tmp_path, monkeypatch, caplog, call_requests):
    # Review of #105: a Quipu 5xx or timeout on a one-item call exits nonzero just like
    # cap exhaustion. It must not be read as "this item costs more than the burst".
    conn, budget, r, adapters = runner(tmp_path, monkeypatch, 3, call_requests=call_requests,
                                       transient=60)
    budget.tokens, budget.updated = 40.0, 0.0
    due(conn, "x1")
    caplog.set_level(logging.WARNING, logger=inc.LOG.name)
    now = 0.0
    while adapters.transient and now < 60 * 200:
        r.tick(now)
        now += 1.0
    assert adapters.transient == 0, "the kind kept being called through the outage"
    assert r._min_cost["evaluate"] <= budget.burst
    assert not any("CONFIGURATION ERROR" in m for m in caplog.messages)
    assert 'chaski_emitter_min_call_cost{emitter="a",kind="evaluate"} 0' in "\n".join(r.metrics(lambda s: s))
    while not r.runs["ok"] and now < 60 * 300:   # the outage ends: it recovers
        r.tick(now)
        now += 1.0
    assert r.runs["ok"] == 1
    assert r._min_cost["evaluate"] == r._measured_cost["evaluate"] == 3, "the measured cost replaces the hint"
    assert not r.not_admitted.get("cost_exceeds_cap") or r._cap(now) >= 3


def test_an_unmeasured_emitter_exports_zero(tmp_path, monkeypatch):
    conn, budget, r, adapters = runner(tmp_path, monkeypatch, 3)
    r._publish()
    text = "\n".join(r.metrics(lambda s: s))
    assert 'chaski_emitter_min_call_cost{emitter="a",kind="evaluate"} 0' in text
    assert 'chaski_emitter_min_call_cost{emitter="a",kind="route"} 0' in text
