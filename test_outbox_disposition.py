"""Expiry is retained evidence, never a receiver acknowledgement."""

import json
import sqlite3

import pytest
import emitter as em


@pytest.fixture
def state(tmp_path):
    conn = em.connect(tmp_path / "inbox.db")
    definition = em.Emitter(
        label="source",
        command=[],
        interval_s=5,
        owner="owner",
        from_verdict="NO",
        to_verdict="YES",
        externally_triggered=True,
    )
    em.observe(conn, definition, [{"item": "item", "verdict": "NO"}], 1)
    em.observe(conn, definition, [{"item": "item", "verdict": "YES", "event_id": "event"}], 2)
    yield conn, definition
    conn.close()


def plan_for(conn):
    row = conn.execute("SELECT event_id FROM outbox").fetchone()[0]
    return {
        "schema": em.DISPOSITION_PLAN,
        "emitter": "source",
        "created_before": 3,
        "expected_count": 1,
        "reason": "obsolete backlog",
        "ruling_ref": "ruling-1",
        "source_utc": "2026-01-01T00:00:00Z",
        "rows": [{"event_id": row, "row_sha256": em.disposition_digest(em.disposition_row(conn, row))}],
    }


def apply(conn, plan=None):
    plan = plan or plan_for(conn)
    return em.apply_disposition_plan(conn, plan, em.disposition_digest(plan))


class Sink:
    def __init__(self):
        self.sent = []
        self.setups = 0

    def delivery_scope(self):
        return {"receiver": "control"}

    def setup_pending(self):
        return not self.setups

    def setup(self):
        self.setups += 1

    def deliver(self, event):
        self.sent.append(event["event_id"])


def test_expiry_fences_delivery_binding_setup_budget_wakeup_and_metrics(state):
    conn, definition = state
    before = list(conn.execute("SELECT * FROM outbox"))
    plan = plan_for(conn)
    apply(conn, plan)
    apply(conn, plan)  # lost answer is idempotent
    sink = Sink()
    budget = em.WriteBudget(1)
    old_budget = budget._last
    assert em.deliver_pending(conn, sink, 10, "source", budget) == (0, 0)
    runner = em.Runner(definition, conn, sink, budget)
    assert runner.delivery_due(10) is None
    assert runner.setup_due(10) is None
    assert runner.next_wakeup(10) == 15
    assert sink.sent == [] and sink.setups == 0 and budget._last == old_budget
    assert list(conn.execute("SELECT * FROM outbox")) == before
    assert runner._snapshot["pending"] == 0 and runner._snapshot["delivered"] == 0
    assert runner._snapshot["disposed"] == 1
    assert any("outbox_disposed_total" in line and line.endswith(" 1") for line in runner.metrics(str))
    assert em.verify_dispositions(conn) == 1
    # Undisposed fresh traffic remains deliverable with ordinary binding/receipt.
    em.observe(conn, definition, [{"item": "fresh", "verdict": "NO"}], 11)
    em.observe(conn, definition, [{"item": "fresh", "verdict": "YES", "event_id": "new"}], 12)
    assert em.deliver_pending(conn, sink, 20, "source", budget) == (0, 0)  # setup takes its own slot
    assert em.deliver_pending(conn, sink, 21, "source", budget) == (1, 0)
    assert sink.sent == ["new"] and sink.setups == 1
    assert conn.execute(
        "SELECT delivered_at FROM outbox WHERE event_id=?", (plan["rows"][0]["event_id"],)
    ).fetchone() == (None,)


def test_original_positive_control_delivers_without_ledger(state):
    conn, _ = state
    sink = Sink()
    assert em.deliver_pending(conn, sink, 10, "source") == (1, 0)
    assert sink.sent and sink.setups == 1
    assert conn.execute("SELECT attempts,delivery_scope FROM outbox").fetchone() == (
        1,
        json.dumps(sink.delivery_scope(), sort_keys=True),
    )


@pytest.mark.parametrize(
    "change",
    [
        "UPDATE outbox SET attempts=1",
        "UPDATE outbox SET delivered_at=3",
        "UPDATE outbox SET delivery_scope='{}'",
        "UPDATE outbox SET payload='{}'",
        "UPDATE outbox SET emitter='foreign'",
        "UPDATE outbox SET created=999",
        "UPDATE outbox SET recovery_queued=1",
    ],
)
def test_changed_or_attempted_rows_refuse_without_partial_disposition(state, change):
    conn, _ = state
    plan = plan_for(conn)
    conn.execute(change)
    with pytest.raises(ValueError):
        apply(conn, plan)
    assert conn.execute("SELECT COUNT(*) FROM outbox_dispositions").fetchone() == (0,)


def test_atomic_append_rollback_and_conflicting_replay(state):
    conn, _ = state
    plan = plan_for(conn)
    conn.execute("CREATE TEMP TRIGGER crash AFTER INSERT ON outbox_dispositions BEGIN SELECT RAISE(ABORT,'crash'); END")
    with pytest.raises(sqlite3.IntegrityError):
        apply(conn, plan)
    assert conn.execute("SELECT COUNT(*) FROM outbox_dispositions").fetchone() == (0,)
    conn.execute("DROP TRIGGER crash")
    apply(conn, plan)
    plan["reason"] = "different decision"
    with pytest.raises(ValueError, match="conflicting"):
        apply(conn, plan)
    for sql in ("UPDATE outbox_dispositions SET reason=reason", "DELETE FROM outbox_dispositions"):
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            conn.execute(sql)


def test_drift_stays_fenced_and_restart_refuses(state):
    conn, _ = state
    apply(conn)
    conn.execute("UPDATE outbox SET payload='{}'")
    assert em.deliver_pending(conn, Sink(), 10, "source") == (0, 0)
    with pytest.raises(ValueError, match="changed"):
        em.verify_dispositions(conn)
    db = conn.execute("PRAGMA database_list").fetchone()[2]
    with pytest.raises(ValueError, match="changed"):
        em.connect(db)


def test_duplicate_foreign_recovery_and_wrong_plan_hash_refuse(state):
    conn, _ = state
    plan = plan_for(conn)
    with pytest.raises(ValueError, match="hash"):
        em.apply_disposition_plan(conn, plan, "0" * 64)
    plan["rows"] *= 2
    plan["expected_count"] = 2
    with pytest.raises(ValueError, match="duplicate"):
        apply(conn, plan)
    conn.execute("UPDATE outbox SET payload=?", (json.dumps({"resolution_of": "prior"}),))
    with pytest.raises(ValueError, match="recovery"):
        apply(conn)


def test_cli_plan_readonly_and_apply_requires_exclusive_custody(state, tmp_path, capsys):
    from consumer_lease import ConsumerLease, ConsumerLeaseError

    conn, _ = state
    db = tmp_path / "inbox.db"
    cursor = tmp_path / "cursor.json"
    cursor.write_text("{}")
    plan = tmp_path / "plan.json"
    assert (
        em.disposition_main(
            [
                "plan",
                "--db",
                str(db),
                "--plan",
                str(plan),
                "--emitter",
                "source",
                "--created-before",
                "3",
                "--expected-count",
                "1",
                "--reason",
                "obsolete",
                "--ruling-ref",
                "ruling-1",
            ]
        )
        == 0
    )
    digest = json.loads(capsys.readouterr().out)["plan_sha256"]
    assert conn.execute("SELECT COUNT(*) FROM outbox_dispositions").fetchone() == (0,)
    args = ["apply", "--db", str(db), "--cursor", str(cursor), "--plan", str(plan), "--expected-plan-sha256", digest]
    with ConsumerLease(cursor, db):
        with pytest.raises(ConsumerLeaseError):
            em.disposition_main(args)
    assert em.disposition_main(args) == 0
    assert em.disposition_main(["verify", "--db", str(db)]) == 0
    assert conn.execute("SELECT delivered_at,attempts FROM outbox").fetchone() == (None, 0)


def test_extra_matching_population_refuses_every_append(state):
    conn, definition = state
    plan = plan_for(conn)
    em.observe(conn, definition, [{"item": "extra", "verdict": "NO"}], 2)
    em.observe(conn, definition, [{"item": "extra", "verdict": "YES", "event_id": "extra"}], 2.5)
    with pytest.raises(ValueError, match="population"):
        apply(conn, plan)
    assert conn.execute("SELECT COUNT(*) FROM outbox_dispositions").fetchone() == (0,)


def test_disposed_original_cannot_be_renamed_by_supported_migration(state):
    import state_key_migration as migration

    conn, _ = state
    apply(conn)
    conn.row_factory = sqlite3.Row
    old = migration.row(conn, "source", "item")
    plan = {
        "schema": migration.SCHEMA,
        "mappings": [
            {
                "emitter": "source",
                "from": "item",
                "to": "urn:fixture:item",
                "expected": migration.digest({"from": migration.stable(old), "to": None}),
            }
        ],
    }
    report = migration.inspect(conn, plan)
    assert report["conflicts"][0]["reason"] == "append-only disposition freezes this event identity"
    assert em.verify_dispositions(conn) == 1


def test_version_exits_before_operational_inputs(capsys):
    from importlib.metadata import version

    with pytest.raises(SystemExit) as outcome:
        em.disposition_main(["--version"])
    assert outcome.value.code == 0
    assert capsys.readouterr().out == "chaski-outbox-disposition " + version("chaski-reactor") + "\n"
