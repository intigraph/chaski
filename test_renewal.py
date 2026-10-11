"""Lease refresh must require fresh evidence and preserve event identity."""

from unittest.mock import patch

import emitter


class Sink:
    def __init__(self):
        self.deliveries = []
        self.renewals = []
        self.fail = False

    def delivery_scope(self):
        return {"kind": "test", "receiver": "lease"}

    def prepare_recovery(self, event):
        return {"delivery_scope": self.delivery_scope()}

    def resolve(self, event):
        self.deliveries.append(dict(event))

    def deliver(self, event):
        self.deliveries.append(dict(event))

    def renew(self, event):
        if self.fail:
            raise ConnectionError("controlled failure")
        self.renewals.append(dict(event))


def setup():
    definition = emitter.Emitter(
        label="lease",
        command=[],
        interval_s=5,
        owner="crew",
        from_verdict="BELOW",
        to_verdict="ABOVE",
        resolve_recovery=True,
        generation_event_ids=True,
        renew_interval_s=30,
        persist_runs=True,
    )
    conn = emitter.connect(":memory:")
    sink = Sink()
    return definition, conn, sink, emitter.Runner(definition, conn, sink)


def tick(runner, now, verdict=None, fail=False):
    records = [] if verdict is None else [{"item": "a", "event_id": "source-a", "verdict": verdict, "owner": "crew"}]
    with patch.object(
        emitter, "run_adapter", side_effect=ConnectionError("unknown") if fail else None, return_value=records
    ):
        runner.tick(now)


def test_refresh_same_identity_without_new_transition():
    _, conn, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    tick(runner, 100, "ABOVE")
    assert sink.renewals == sink.deliveries
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 1


def test_unknown_missing_and_skipped_measurements_never_renew():
    _, _, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    tick(runner, 100, "UNKNOWN")
    tick(runner, 200, fail=True)
    tick(runner, 201, "ABOVE")  # interval prevents a new measurement
    tick(runner, 300)
    assert sink.renewals == []


def test_recovery_and_new_generation_cannot_refresh_old_identity():
    _, _, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    old = sink.deliveries[0]["event_id"]
    tick(runner, 100, "BELOW")
    assert sink.deliveries[1]["prior"]["event_id"] == old
    tick(runner, 200, "ABOVE")
    new = sink.deliveries[2]["event_id"]
    assert new != old
    tick(runner, 300, "ABOVE")
    assert [r["event_id"] for r in sink.renewals] == [new]


def test_pending_firing_cannot_be_renewed():
    definition, conn, sink, _ = setup()
    emitter.observe(conn, definition, [{"item": "a", "verdict": "BELOW"}], 10)
    emitter.observe(conn, definition, [{"item": "a", "verdict": "ABOVE", "event_id": "source-a"}], 20)
    assert emitter.renew_active(conn, sink, 100, definition, {"a"}) == (0, 0)
    assert sink.renewals == []


def test_restart_retains_unknown_and_renewal_backoff():
    definition, conn, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    sink.fail = True
    tick(runner, 100, "ABOVE")
    tick(runner, 110, fail=True)
    restarted = emitter.Runner(definition, conn, sink)
    assert restarted.runs == {"ok": 3, "unknown": 1}
    sink.fail = False
    tick(restarted, 120, "ABOVE")
    assert sink.renewals == []
    tick(restarted, 140, "ABOVE")
    assert len(sink.renewals) == 1


def test_persistent_global_budget_survives_runner_restart():
    _, conn, _, _ = setup()
    assert emitter.PersistentWriteBudget(conn, 30).try_take(100)
    restarted = emitter.PersistentWriteBudget(conn, 30)
    assert not restarted.try_take(110)
    assert restarted.try_take(130)


def test_nonpositive_old_row_cannot_starve_eligible_refresh():
    definition, conn, sink, _ = setup()
    before = [{"item": item, "verdict": "BELOW"} for item in ("a", "b")]
    above = [{"item": item, "verdict": "ABOVE", "event_id": "source-" + item} for item in ("a", "b")]
    emitter.observe(conn, definition, before, 10)
    emitter.observe(conn, definition, above, 20)
    emitter.deliver_pending(conn, sink, 20, definition.label)
    assert emitter.renew_active(conn, sink, 100, definition, {"b"}, limit=1) == (1, 0)
    assert sink.renewals[0]["item"] == "b"


def test_changed_receiver_scope_never_refreshes_prior_firing():
    _, _, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    sink.delivery_scope = lambda: {"kind": "test", "receiver": "other"}
    tick(runner, 100, "ABOVE")
    assert sink.renewals == []
    assert runner._snapshot["renewal_failures"] == 1


def test_one_shot_queued_counter_does_not_reset_on_restart():
    definition, conn, sink, runner = setup()
    tick(runner, 10, "BELOW")
    tick(runner, 20, "ABOVE")
    tick(runner, 100, "BELOW")
    restarted = emitter.Runner(definition, conn, sink)
    assert restarted.queued_total == runner.queued_total == 2
