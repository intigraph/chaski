"""aegis-ssvr A: the one-time recovery scope migration never relaxes the scope check."""
import json
from pathlib import Path

import pytest

import emitter as em
import recovery_migration as rm
from consumer_lease import ConsumerLease

LEGACY = {"kind": "chain", "sinks": [
    {"kind": "quipu", "base": "http://q", "namespace": "http://ns/", "reaction": "control"},
    {"kind": "alertmanager", "base": "http://am", "emitter": "control"}]}
CURRENT = {"kind": "chain", "sinks": [
    {**LEGACY["sinks"][0], "vocabulary": rm.LEGACY_VOCABULARY}, LEGACY["sinks"][1]]}
CANONICAL = {"kind": "chain", "sinks": [
    {**LEGACY["sinks"][0], "vocabulary": "intigraph-owned-a389be53/v1"}, LEGACY["sinks"][1]]}
EMPTY = {c: 0 for c in rm.CLASSES}


class Scoped(em.JsonlSink):
    def __init__(self, path, scope):
        super().__init__(path)
        self.scope = scope

    def delivery_scope(self):
        return self.scope


def rec(item, verdict):
    return {"item": item, "verdict": verdict, "event_id": "fire-" + item}


@pytest.fixture
def world(tmp_path):
    """Three items fired under the pre-#79 scope, then recovered: three frozen recoveries."""
    db = tmp_path / "inbox.db"
    conn = em.connect(db)
    e = em.Emitter("control", [], 0, "owner", resolve_recovery=True)
    old = Scoped(tmp_path / "old.jsonl", LEGACY)
    items = ["a", "b", "c"]
    em.observe(conn, e, [rec(i, "BLOCKED") for i in items], 100)
    em.observe(conn, e, [rec(i, "UNBLOCKED") for i in items], 101)
    assert em.deliver_pending(conn, old, 102, "control") == (3, 0)
    em.observe(conn, e, [rec(i, "BLOCKED") for i in items], 103)
    assert conn.execute("SELECT COUNT(*) FROM recovery_outbox WHERE delivered_at IS NULL").fetchone()[0] == 3
    current = Scoped(tmp_path / "old.jsonl", CURRENT)
    yield {"db": db, "conn": conn, "e": e, "current": current, "tmp": tmp_path}
    conn.close()


def send(w, sink=None, now=200):
    return em.deliver_pending(w["conn"], sink or w["current"], now, "control", resolve_recovery=True)


def plan(w, refreeze=3, null=0, missing=0):
    p = rm.build_plan(w["conn"], {"refreeze": refreeze, "null-scope": null, "missing-prior": missing},
                      "stale pre-#79 scope", "ssvr c136/c139")
    return p, rm._check_plan(p)


def recovery_ids(w):
    return sorted(r[0] for r in w["conn"].execute("SELECT event_id FROM recovery_outbox"))


def test_pre_vocabulary_scope_refuses_before_migration(world):
    assert send(world) == (0, 3)
    errors = {r[0] for r in world["conn"].execute("SELECT last_error FROM recovery_outbox")}
    assert errors == {"prior delivery receiver scope changed or unproven"}


def test_refreeze_lets_the_unchanged_exact_check_deliver(world):
    send(world)
    p, digest = plan(world)
    result = rm.apply_plan(world["conn"], p, digest, world["tmp"] / "log.jsonl")
    assert result["actions"] == {"refrozen": 3}
    for (payload,) in world["conn"].execute("SELECT payload FROM recovery_outbox"):
        assert json.loads(payload)["delivery_scope"] == CURRENT
    # A different (canonical) receiver vocabulary still refuses: no equality exception.
    canonical = Scoped(world["tmp"] / "old.jsonl", CANONICAL)
    assert send(world, canonical, now=10_000) == (0, 3)
    assert send(world, now=20_000) == (3, 0)
    log = [json.loads(line) for line in (world["tmp"] / "log.jsonl").read_text().splitlines()]
    assert [e["action"] for e in log] == ["refrozen"] * 3 and all(e["before_sha256"] != e["after_sha256"] for e in log)


def test_replay_is_idempotent_and_needs_a_new_log(world):
    p, digest = plan(world)
    rm.apply_plan(world["conn"], p, digest)
    before = world["conn"].execute("SELECT * FROM recovery_outbox ORDER BY event_id").fetchall()
    assert rm.apply_plan(world["conn"], p, digest)["actions"] == {"already-refrozen": 3}
    assert world["conn"].execute("SELECT * FROM recovery_outbox ORDER BY event_id").fetchall() == before


def test_null_and_missing_prior_expire_retained_and_fenced(world):
    ids = recovery_ids(world)
    world["conn"].execute("UPDATE recovery_outbox SET payload=json_set(payload,'$.delivery_scope',json('null'))"
                          " WHERE event_id=?", (ids[0],))
    world["conn"].execute("UPDATE recovery_outbox SET last_error=? WHERE event_id=?",
                          ("missing or ambiguous prior graph firing", ids[1]))
    p, digest = plan(world, refreeze=1, null=1, missing=1)
    assert rm.apply_plan(world["conn"], p, digest)["actions"] == {"expired-not-delivered": 2, "refrozen": 1}
    assert recovery_ids(world) == ids                     # nothing deleted
    assert send(world) == (1, 0)                          # only the refrozen row is sent
    assert send(world, now=50_000) == (0, 0)              # expired rows are never retried
    assert rm.population(world["conn"]) == []
    runner = em.Runner(world["e"], world["conn"], world["current"])
    snap = runner._snapshot_values()
    assert snap["pending"] == 0 and snap["disposed"] == 2
    with pytest.raises(Exception, match="append-only"):
        world["conn"].execute("DELETE FROM recovery_dispositions")
    assert em.verify_recovery_dispositions(world["conn"]) == 2


def test_tampered_expired_row_refuses_startup(world):
    ids = recovery_ids(world)
    world["conn"].execute("UPDATE recovery_outbox SET payload=json_set(payload,'$.delivery_scope',json('null'))")
    p, digest = plan(world, refreeze=0, null=3)
    rm.apply_plan(world["conn"], p, digest)
    world["conn"].execute("UPDATE recovery_outbox SET item='other' WHERE event_id=?", (ids[0],))
    world["conn"].close()
    with pytest.raises(ValueError, match="consumer remains fenced"):
        em.connect(world["db"])
    world["conn"] = em.sqlite3.connect(world["db"])


def test_outside_ruling_shape_refuses_the_whole_plan(world):
    ids = recovery_ids(world)
    world["conn"].execute("UPDATE recovery_outbox SET payload=json_set(payload,'$.delivery_scope',json(?))"
                          " WHERE event_id=?", (json.dumps(CANONICAL), ids[0]))
    with pytest.raises(ValueError, match="pre-vocabulary shape"):
        plan(world)


def test_count_mismatch_and_hash_mismatch_refuse(world):
    with pytest.raises(ValueError, match="reviewed counts changed"):
        plan(world, refreeze=2)
    p, digest = plan(world)
    with pytest.raises(ValueError, match="hash mismatch"):
        rm.apply_plan(world["conn"], p, "0" * 64)
    assert rm.population(world["conn"]) and send(world) == (0, 3)


def test_new_recovery_after_review_refuses_without_mutation(world):
    p, digest = plan(world)
    world["conn"].execute(
        "INSERT INTO recovery_outbox(event_id,emitter,item,generation,payload,created,prior_event)"
        " SELECT 'late',emitter,'late',generation,payload,created,'other' FROM recovery_outbox LIMIT 1")
    before = world["conn"].execute("SELECT payload FROM recovery_outbox ORDER BY event_id").fetchall()
    with pytest.raises(ValueError, match="outside the reviewed plan"):
        rm.apply_plan(world["conn"], p, digest)
    assert world["conn"].execute("SELECT payload FROM recovery_outbox ORDER BY event_id").fetchall() == before


def test_changed_row_after_review_refuses(world):
    p, digest = plan(world)
    world["conn"].execute("UPDATE recovery_outbox SET item='moved' WHERE event_id=?", (recovery_ids(world)[0],))
    with pytest.raises(ValueError, match="reviewed row changed"):
        rm.apply_plan(world["conn"], p, digest)
    assert send(world) == (0, 3)


def test_retry_counters_do_not_invalidate_a_reviewed_plan(world):
    p, digest = plan(world)
    send(world)                                           # attempts/last_error/next_attempt move
    assert rm.apply_plan(world["conn"], p, digest)["actions"] == {"refrozen": 3}


def test_cli_plan_apply_verify(world, capsys):
    db, tmp = world["db"], world["tmp"]
    world["conn"].close()
    planfile, log, cursor = tmp / "plan.json", tmp / "apply.jsonl", tmp / "cursor.json"
    cursor.write_text("{}")
    base = ["--reason", "stale", "--ruling-ref", "ssvr c139"]
    assert rm.main(["plan", "--db", str(db), "--plan", str(planfile), "--expect-refreeze", "3",
                    "--expect-null-scope", "0", "--expect-missing-prior", "0", *base]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["counts"] == {**EMPTY, "refreeze": 3} and oct(planfile.stat().st_mode & 0o777) == "0o600"
    args = ["apply", "--db", str(db), "--cursor", str(cursor), "--plan", str(planfile),
            "--expected-plan-sha256", out["plan_sha256"], "--log", str(log)]
    with ConsumerLease(cursor, db):
        with pytest.raises(Exception):
            rm.main(args)                                 # a running consumer holds the lease
    assert not log.exists()
    assert rm.main(args) == 0
    assert json.loads(capsys.readouterr().out)["actions"] == {"refrozen": 3}
    assert len(log.read_text().splitlines()) == 3
    assert rm.main(["verify", "--db", str(db)]) == 0
    assert json.loads(capsys.readouterr().out)["undelivered_unmigrated"] == {"refrozen-awaiting-delivery": 3}
    world["conn"] = em.connect(db)
    assert Path(log).stat().st_mode & 0o777 == 0o600
