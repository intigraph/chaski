"""aegis-se8: a reviewed expiry of stale scheduled firings is evidence, never a receipt or a rerun."""
import hashlib
import json
import sqlite3
from dataclasses import asdict

import pytest

import emitter as em
import scheduled_expiry as sx
from scheduled_receiver import ExecutionScope, run_id
from slot_receiver import SlotLedger, SlotRetired, SlotUnknown, slot_id

LABEL = "hb"
SCOPE = ExecutionScope(job="http://example/job-hb", revision="sha256:" + "1" * 64,
                       host="http://example/host", manifest_sha256="2" * 64,
                       agent="scheduler-pilot", workflow="urn:shuttle:workflow:pilot")
BOUND = {"receiver": "scheduled-event-v1", "emitter": LABEL, "recipient": "owner",
         "binding": {"receiver": "async-scheduled-v1", **asdict(SCOPE)}}
HOUR = 3600
BASE = 1_794_000_000 - 1_794_000_000 % HOUR
CUTOFF = BASE + 10 * HOUR


def binding(slot):
    return {**asdict(SCOPE), "scheduled_at": slot}


def add_firing(conn, slot, *, attempts=3, scope=BOUND, created=None):
    identity = run_id(SCOPE, slot)
    payload = {"emitter": LABEL, "event": "scheduled", "event_id": identity, "evidence": "slot",
               "generation": 1, "item": SCOPE.job, "observed_at": slot, "recipient": "owner"}
    conn.execute("INSERT INTO outbox(event_id,emitter,item,generation,payload,created,attempts,"
                 "next_attempt,last_error,delivery_scope) VALUES (?,?,?,?,?,?,?,?,?,?)",
                 (identity, LABEL, SCOPE.job, 1, json.dumps(payload, sort_keys=True),
                  slot if created is None else created, attempts, 0, "scheduled receiver capacity reached",
                  None if scope is None else json.dumps(scope, sort_keys=True)))
    return identity


def claim(ledger, slot, result=None):
    def action():
        if result is None:
            raise RuntimeError("child outcome lost")
        return result
    try:
        ledger.run(slot_id(SCOPE.job, SCOPE.revision, slot), binding(slot), action)
    except RuntimeError:
        pass


class Recorder:
    def __init__(self):
        self.sent = []

    def delivery_scope(self):
        return BOUND

    def deliver(self, event):
        self.sent.append(event["event_id"])


@pytest.fixture
def world(tmp_path):
    """Two stale UNKNOWN claims, two stale never-claimed firings and one fresh firing."""
    db, slots_path = tmp_path / "inbox.db", tmp_path / "slots.db"
    conn = em.connect(db)
    ledger = SlotLedger(slots_path)
    stale = [BASE + i * HOUR for i in range(4)]
    ids = [add_firing(conn, s) for s in stale]
    for s in stale[:2]:
        claim(ledger, s)
    fresh = add_firing(conn, CUTOFF + HOUR, attempts=0)
    slots = sx._open_slots_rw(slots_path)
    yield {"conn": conn, "slots": slots, "ledger": ledger, "db": db, "slots_path": slots_path,
           "stale": stale, "ids": ids, "fresh": fresh, "tmp": tmp_path}
    slots.close()
    conn.close()


EXPECTED = {"never-claimed": 2, "claimed-unknown": 2}


def plan(w, expected=EXPECTED):
    return sx.build_plan(w["conn"], w["slots"], LABEL, CUTOFF, expected, "stale heartbeat", "aegis-se8")


def apply(w, p=None):
    p = p or plan(w)
    return sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_plan_classifies_exactly_and_refuses_other_counts(world):
    p = plan(world)
    assert p["counts"] == EXPECTED
    assert {r["event_id"] for r in p["rows"]} == set(world["ids"])
    assert world["fresh"] not in {r["event_id"] for r in p["rows"]}
    with pytest.raises(ValueError, match="reviewed counts changed"):
        plan(world, {"never-claimed": 2, "claimed-unknown": 1})


def test_cli_plan_is_read_only_then_apply_and_verify(world):
    w = world
    w["conn"].close()
    w["slots"].close()
    before = (sha(w["db"]), sha(w["slots_path"]))
    out = w["tmp"] / "plan.json"
    args = ["--db", str(w["db"]), "--slots", str(w["slots_path"])]
    assert sx.main(["plan", *args, "--plan", str(out), "--emitter", LABEL, "--created-before", str(CUTOFF),
                    "--expect-never-claimed", "2", "--expect-claimed-unknown", "2",
                    "--reason", "stale", "--ruling-ref", "aegis-se8"]) == 0
    assert (sha(w["db"]), sha(w["slots_path"])) == before
    assert out.stat().st_mode & 0o777 == 0o600
    p = json.loads(out.read_text())
    cursor = w["tmp"] / "cursor.json"
    cursor.write_text("{}")
    assert sx.main(["apply", *args, "--cursor", str(cursor), "--plan", str(out),
                    "--expected-plan-sha256", sx.check_plan(p)]) == 0
    assert sx.main(["verify", *args]) == 0
    w["conn"] = em.connect(w["db"])
    w["slots"] = sx._open_slots_rw(w["slots_path"])
    assert sx.verify(w["conn"], w["slots"]) == {"verified_dispositions": 4, "verified_slot_retirements": 2}


def test_apply_fences_stale_firings_and_retires_only_unknown_claims(world):
    w = world
    result = apply(w)
    assert result["actions"] == {"slot-retired-unknown": 2, "expired-not-delivered": 4}
    sink = Recorder()
    delivered, failed = em.deliver_pending(w["conn"], sink, CUTOFF + 2 * HOUR, LABEL)
    assert sink.sent == [w["fresh"]] and (delivered, failed) == (1, 0)
    # A retired claim refuses as retired and never calls the child again.
    called = []
    with pytest.raises(SlotRetired):
        w["ledger"].run(slot_id(SCOPE.job, SCOPE.revision, w["stale"][0]), binding(w["stale"][0]),
                        lambda: called.append(1) or {})
    assert called == []
    # Never-claimed slots get no claim and no result from the expiry.
    assert w["slots"].execute("SELECT COUNT(*) FROM slots").fetchone()[0] == 2
    assert w["slots"].execute("SELECT COUNT(*) FROM slots WHERE result IS NOT NULL").fetchone()[0] == 0
    # Originals are retained: still undelivered, attempts unchanged.
    assert w["conn"].execute("SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL AND attempts=3"
                             ).fetchone()[0] == 4


def test_unretired_unknown_claim_still_reports_reconciliation(world):
    w = world
    with pytest.raises(SlotUnknown) as caught:
        w["ledger"].run(slot_id(SCOPE.job, SCOPE.revision, w["stale"][0]), binding(w["stale"][0]),
                        lambda: {})
    assert type(caught.value) is SlotUnknown


def test_replay_is_idempotent(world):
    p = plan(world)
    apply(world, p)
    again = sx.apply_plan(world["conn"], world["slots"], p, sx.check_plan(p))
    assert again["actions"] == {"slot-already-retired": 2, "already-expired": 4}


def test_crash_between_ledgers_completes_on_replay(world, monkeypatch):
    w = world
    p = plan(w)
    real = em.verify_scheduled_dispositions

    def boom(conn):
        raise RuntimeError("crash after slot commit")
    monkeypatch.setattr(em, "verify_scheduled_dispositions", boom)
    with pytest.raises(RuntimeError):
        sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))
    assert w["slots"].execute("SELECT COUNT(*) FROM slot_retirements").fetchone()[0] == 2
    assert w["conn"].execute("SELECT COUNT(*) FROM scheduled_dispositions").fetchone()[0] == 0
    monkeypatch.setattr(em, "verify_scheduled_dispositions", real)
    again = sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))
    assert again["actions"] == {"slot-already-retired": 2, "expired-not-delivered": 4}


def test_changed_row_refuses_without_any_receipt(world):
    w = world
    p = plan(w)
    w["conn"].execute("UPDATE outbox SET created=created+1 WHERE event_id=?", (w["ids"][0],))
    with pytest.raises(ValueError, match="changed"):
        sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))
    assert w["slots"].execute("SELECT COUNT(*) FROM slot_retirements").fetchone()[0] == 0
    assert w["conn"].execute("SELECT COUNT(*) FROM scheduled_dispositions").fetchone()[0] == 0


def test_retry_counters_may_move_between_review_and_apply(world):
    w = world
    p = plan(w)
    w["conn"].execute("UPDATE outbox SET attempts=attempts+5, last_error='moved'")
    assert apply(w, p)["actions"]["expired-not-delivered"] == 4


def test_new_firing_in_scope_refuses(world):
    w = world
    p = plan(w)
    add_firing(w["conn"], BASE + 5 * HOUR)
    with pytest.raises(ValueError, match="reviewed population"):
        sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))


def test_claim_resolved_after_review_refuses(world):
    w = world
    p = plan(w)
    w["slots"].execute("UPDATE slots SET result='{}' WHERE slot=?",
                       (slot_id(SCOPE.job, SCOPE.revision, w["stale"][0]),))
    with pytest.raises(ValueError, match="publication retry"):
        sx.apply_plan(w["conn"], w["slots"], p, sx.check_plan(p))


def test_slot_with_result_refuses_the_plan(world):
    w = world
    s = BASE + 6 * HOUR
    add_firing(w["conn"], s)
    claim(w["ledger"], s, {"outcome": "succeeded", "exit_code": 0})
    with pytest.raises(ValueError, match="publication retry"):
        plan(w)


def test_unbound_or_foreign_firing_refuses_the_plan(world):
    w = world
    add_firing(w["conn"], BASE + 7 * HOUR, scope=None)
    with pytest.raises(ValueError, match="unbound"):
        plan(w)


def test_foreign_receiver_refuses_the_plan(tmp_path):
    conn = em.connect(tmp_path / "inbox.db")
    slots = sx._open_slots_rw(tmp_path / "slots.db")
    SlotLedger(tmp_path / "slots.db")
    add_firing(conn, BASE, scope={**BOUND, "receiver": "control"})
    with pytest.raises(ValueError, match="async scheduled receiver"):
        sx.build_plan(conn, slots, LABEL, CUTOFF, EXPECTED, "r", "ref")


def test_wrong_plan_hash_refuses(world):
    p = plan(world)
    with pytest.raises(ValueError, match="hash mismatch"):
        sx.apply_plan(world["conn"], world["slots"], p, "0" * 64)


def test_startup_refuses_a_changed_disposed_firing(world):
    w = world
    apply(w)
    w["conn"].execute("UPDATE outbox SET payload=? WHERE event_id=?", ("{}", w["ids"][0]))
    with pytest.raises(ValueError, match="consumer remains fenced"):
        em.connect(w["db"])


def test_ledgers_are_append_only(world):
    w = world
    apply(w)
    for conn, table in ((w["conn"], "scheduled_dispositions"), (w["slots"], "slot_retirements")):
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute(f"DELETE FROM {table}")
        with pytest.raises(sqlite3.DatabaseError, match="append-only"):
            conn.execute(f"UPDATE {table} SET reason='x'")


def test_disposed_metric_counts_scheduled_expiry(world):
    w = world
    apply(w)
    count = w["conn"].execute(
        "SELECT COUNT(*) FROM outbox o WHERE o.emitter=? AND o.delivered_at IS NULL AND "
        + em.undisposed("o"), (LABEL,)).fetchone()[0]
    assert count == 1


def test_cli_apply_takes_custody_for_a_stream_mode_consumer_without_a_cursor_file(world):
    """The Mac pilot keeps its cursor in the inbox: only the lease sidecar exists."""
    from consumer_lease import ConsumerLease, ConsumerLeaseError
    w = world
    w["conn"].close()
    w["slots"].close()
    out = w["tmp"] / "plan.json"
    args = ["--db", str(w["db"]), "--slots", str(w["slots_path"])]
    assert sx.main(["plan", *args, "--plan", str(out), "--emitter", LABEL, "--created-before", str(CUTOFF),
                    "--expect-never-claimed", "2", "--expect-claimed-unknown", "2",
                    "--reason", "stale", "--ruling-ref", "aegis-se8"]) == 0
    digest = sx.check_plan(json.loads(out.read_text()))
    cursor = w["tmp"] / "event-cursor.json"
    assert not cursor.exists()
    apply_args = ["apply", *args, "--cursor", str(cursor), "--plan", str(out), "--expected-plan-sha256", digest]
    # A running consumer holds the same custody: apply refuses with no receipt.
    with ConsumerLease(cursor, w["db"]):
        with pytest.raises(ConsumerLeaseError):
            sx.main(apply_args)
    w["conn"] = em.connect(w["db"])
    assert w["conn"].execute("SELECT COUNT(*) FROM scheduled_dispositions").fetchone()[0] == 0
    w["conn"].close()
    assert sx.main(apply_args) == 0
    assert not cursor.exists()
    w["conn"] = em.connect(w["db"])
    w["slots"] = sx._open_slots_rw(w["slots_path"])
    assert sx.verify(w["conn"], w["slots"]) == {"verified_dispositions": 4, "verified_slot_retirements": 2}
