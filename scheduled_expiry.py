"""Reviewed expiry of stale attempted/bound scheduled firings (aegis-se8).

``chaski-outbox-disposition`` expires only firings that were never attempted
and never bound to a receiver. A scheduled emitter binds its receiver before
the first send, and the async receiver counts "capacity reached" as an
attempt, so a stale scheduled backlog is outside that tool. Its slots may also
hold a committed claim with no result (UNKNOWN), which ``slot_receiver``
refuses forever. This tool applies an exact, reviewed plan to that backlog:

* ``never-claimed``: the slot ledger has no claim for the firing's slot. The
  child never started through this receiver.
* ``claimed-unknown``: the slot ledger has a claim with no result. Whether the
  child ran is UNKNOWN. The claim gets an append-only ``retired-unknown``
  receipt in ``slot_retirements``; it is never a result, so neither a replay
  nor a publication retry can follow from it.

Both classes get an append-only ``expired-not-delivered`` receipt in
``scheduled_dispositions``. The runtime then never delivers, schedules or
counts the firing as pending. Originals (rows, payloads, bindings, attempts,
claims) are retained unchanged. Any undelivered firing of the emitter at or
before the cutoff that fits neither class refuses the whole plan: an unbound
row, a foreign receiver, an identifier that does not re-encode exactly, or a
slot that already holds a result (that is a publication retry, not expiry).

``plan`` is read-only on both databases and records each row's identity hash
(immutable fields only, so retry counters may move between review and apply).
``apply`` takes the consumer lease, refuses a changed row, population or
claim, writes the slot receipts and then the firing receipts, each in one
transaction; replaying the identical plan is a no-op, so a crash between the
two completes on replay. No sink, child or network call occurs.

# arming: agent-invoked — exact reviewed plan, consumer held, backups taken.
"""
from __future__ import annotations

import base64
import datetime
import json
import os
import re
import sqlite3
import time
from dataclasses import fields
from pathlib import Path

import emitter as em
from scheduled_receiver import ExecutionScope, run_id
from slot_receiver import RETIREMENT_SCHEMA, slot_id

PLAN_SCHEMA = "chaski-scheduled-expiry/v1"
CLASSES = ("never-claimed", "claimed-unknown")
MAX_ROWS = 10000
SCOPE_FIELDS = tuple(f.name for f in fields(ExecutionScope))


def _row(conn, event_id):
    return em.disposition_row(conn, event_id)


def _ledger_present(conn, table):
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                        (table,)).fetchone() is not None


def scheduled_slot(row):
    """The exact slot of a bound scheduled firing, or ValueError.

    The receiver scope is the one bound before the first send; the identifier
    must re-encode exactly from it, as ScheduledEventSink requires.
    """
    if row["delivery_scope"] is None:
        raise ValueError(f"{row['event_id']}: unbound firing; not a scheduled-receiver row")
    scope = json.loads(row["delivery_scope"])
    binding = scope.get("binding") if isinstance(scope, dict) else None
    if (not isinstance(scope, dict) or scope.get("receiver") != "scheduled-event-v1"
            or scope.get("emitter") != row["emitter"] or not isinstance(binding, dict)
            or binding.get("receiver") != "async-scheduled-v1"
            or set(binding) != {"receiver", *SCOPE_FIELDS}):
        raise ValueError(f"{row['event_id']}: not bound to the async scheduled receiver")
    execution = ExecutionScope(**{k: binding[k] for k in SCOPE_FIELDS})
    payload = json.loads(row["payload"])
    if (not isinstance(payload, dict) or payload.get("event") != "scheduled"
            or payload.get("event_id") != row["event_id"]
            or "prior" in payload or "resolution_of" in payload):
        raise ValueError(f"{row['event_id']}: not a scheduled firing")
    identity = row["event_id"]
    if not isinstance(identity, str) or not identity.startswith("scheduled-v1-"):
        raise ValueError(f"{identity}: not a canonical scheduled identifier")
    encoded = identity[len("scheduled-v1-"):]
    decoded = json.loads(base64.b64decode(encoded + "=" * (-len(encoded) % 4),
                                          altchars=b"-_", validate=True))
    scheduled_at = decoded.get("scheduled_at") if isinstance(decoded, dict) else None
    if type(scheduled_at) is not int or run_id(execution, scheduled_at) != identity:
        raise ValueError(f"{identity}: identifier differs from the bound scope")
    return slot_id(execution.job, execution.revision, scheduled_at)


def _slot_state(slots, slot):
    """('absent'|'unknown'|'retired'|'result', binding digest or None)."""
    hit = slots.execute("SELECT binding,result FROM slots WHERE slot=?", (slot,)).fetchone()
    if hit is None:
        return "absent", None
    binding, result = hit
    if result is not None:
        return "result", binding
    if (_ledger_present(slots, "slot_retirements") and
            slots.execute("SELECT 1 FROM slot_retirements WHERE slot=?", (slot,)).fetchone()):
        return "retired", binding
    return "unknown", binding


def population(conn, emitter, cutoff, replay_digest=None):
    """Every undelivered, undisposed firing of the emitter created at or before cutoff.

    Replay also includes the rows this exact plan already expired.
    """
    replay = ""
    args = [emitter, cutoff]
    if replay_digest is not None:
        replay = (" OR EXISTS (SELECT 1 FROM scheduled_dispositions r WHERE r.event_id=o.event_id"
                  " AND r.plan_sha256=?)")
        args.append(replay_digest)
    fence = em.undisposed("o") if _ledger_present(conn, "scheduled_dispositions") else (
        "NOT EXISTS (SELECT 1 FROM outbox_dispositions d WHERE d.event_id=o.event_id)")
    return [r[0] for r in conn.execute(
        "SELECT o.event_id FROM outbox o WHERE o.emitter=? AND o.created<=? AND o.delivered_at IS NULL"
        f" AND o.recovery_queued=0 AND (({fence}){replay}) ORDER BY o.event_id LIMIT ?",
        (*args, MAX_ROWS + 1))]


def classify(row, slots):
    slot = scheduled_slot(row)
    state, binding = _slot_state(slots, slot)
    if state == "absent":
        return "never-claimed", slot, None
    if state in ("unknown", "retired"):
        return "claimed-unknown", slot, binding
    raise ValueError(f"{row['event_id']}: its slot holds a result; a publication retry, not expiry")


def build_plan(conn, slots, emitter, cutoff, expected, reason, ruling_ref):
    ids = population(conn, emitter, cutoff)
    if len(ids) > MAX_ROWS:
        raise ValueError("population exceeds the reviewed bound")
    rows = []
    for event_id in ids:
        row = _row(conn, event_id)
        cls, slot, binding = classify(row, slots)
        rows.append({"event_id": event_id, "class": cls, "identity_sha256": em.scheduled_identity(row),
                     "slot": slot, "slot_binding": binding})
    counts = {c: sum(1 for r in rows if r["class"] == c) for c in CLASSES}
    if counts != expected:
        raise ValueError(f"reviewed counts changed: expected {expected}, found {counts}")
    return {"schema": PLAN_SCHEMA, "emitter": emitter, "created_before": cutoff, "counts": counts,
            "reason": reason, "ruling_ref": ruling_ref,
            "source_utc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
            "rows": rows}


def check_plan(plan):
    keys = {"schema", "emitter", "created_before", "counts", "reason", "ruling_ref", "source_utc", "rows"}
    if not isinstance(plan, dict) or set(plan) != keys or plan["schema"] != PLAN_SCHEMA:
        raise ValueError("invalid scheduled expiry plan")
    for field in ("emitter", "reason", "ruling_ref", "source_utc"):
        if not isinstance(plan[field], str) or not plan[field].strip() or len(plan[field]) > 4096:
            raise ValueError("missing expiry provenance")
    if not re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z", plan["source_utc"]):
        raise ValueError("source UTC must be explicit")
    if type(plan["created_before"]) not in (int, float) or plan["created_before"] != plan["created_before"]:
        raise ValueError("invalid creation cutoff")
    rows = plan["rows"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError("invalid exact population bound")
    seen = set()
    for r in rows:
        if (not isinstance(r, dict)
                or set(r) != {"event_id", "class", "identity_sha256", "slot", "slot_binding"}
                or r["class"] not in CLASSES or not isinstance(r["event_id"], str)
                or r["event_id"] in seen or not re.fullmatch("[0-9a-f]{64}", str(r["identity_sha256"]))
                or not re.fullmatch("slot-[0-9a-f]{64}", str(r["slot"]))
                or (r["class"] == "never-claimed") != (r["slot_binding"] is None)
                or (r["slot_binding"] is not None
                    and not re.fullmatch("[0-9a-f]{64}", str(r["slot_binding"])))):
            raise ValueError("invalid or duplicate row entry")
        seen.add(r["event_id"])
    if plan["counts"] != {c: sum(1 for r in rows if r["class"] == c) for c in CLASSES}:
        raise ValueError("plan counts do not match its rows")
    return em.disposition_digest(plan)


def _verify_rows(conn, slots, plan, digest):
    """Every planned row unchanged and in its class; no unplanned row in scope."""
    planned = {r["event_id"] for r in plan["rows"]}
    current = set(population(conn, plan["emitter"], plan["created_before"], digest))
    if current != planned:
        raise ValueError(f"{len(current ^ planned)} firings differ from the reviewed population")
    for r in plan["rows"]:
        row = _row(conn, r["event_id"])
        if row is None or row["emitter"] != plan["emitter"] or em.scheduled_identity(row) != r["identity_sha256"]:
            raise ValueError(f"{r['event_id']}: reviewed row changed or disappeared")
        cls, slot, binding = classify(row, slots)
        if (cls, slot, binding) != (r["class"], r["slot"], r["slot_binding"]):
            raise ValueError(f"{r['event_id']}: slot claim no longer matches the review")


def apply_plan(conn, slots, plan, expected_sha256):
    """Apply under the caller's consumer lease; idempotent on replay.

    Slot receipts commit first: a claim retired without its firing receipt only
    refuses delivery (as before), and replay then completes the firing receipts.
    """
    digest = check_plan(plan)
    if digest != expected_sha256:
        raise ValueError("reviewed plan hash mismatch")
    if conn.in_transaction or slots.in_transaction:
        raise ValueError("expiry requires its own transactions")
    now = time.time()
    actions = {}

    def count(action):
        actions[action] = actions.get(action, 0) + 1

    conn.execute("BEGIN IMMEDIATE")   # hold the inbox while the claims are retired
    try:
        _verify_rows(conn, slots, plan, digest)
        slots.execute("BEGIN IMMEDIATE")
        try:
            for r in plan["rows"]:
                if r["class"] != "claimed-unknown":
                    continue
                prior = slots.execute("SELECT binding,plan_sha256 FROM slot_retirements WHERE slot=?",
                                      (r["slot"],)).fetchone()
                if prior is not None:
                    if tuple(prior) != (r["slot_binding"], digest):
                        raise ValueError(f"{r['slot']}: conflicting append-only retirement")
                    count("slot-already-retired")
                    continue
                slots.execute("INSERT INTO slot_retirements VALUES (?,?,?,?,?,?,?,?)",
                              (r["slot"], r["slot_binding"], digest, "retired-unknown", plan["reason"],
                               plan["ruling_ref"], plan["source_utc"], now))
                count("slot-retired-unknown")
            slots.execute("COMMIT")
        except BaseException:
            slots.execute("ROLLBACK")
            raise
        for r in plan["rows"]:
            prior = conn.execute("SELECT identity_sha256,plan_sha256,class,slot FROM scheduled_dispositions"
                                 " WHERE event_id=?", (r["event_id"],)).fetchone()
            if prior is not None:
                if tuple(prior) != (r["identity_sha256"], digest, r["class"], r["slot"]):
                    raise ValueError(f"{r['event_id']}: conflicting append-only disposition")
                count("already-expired")
                continue
            conn.execute("INSERT INTO scheduled_dispositions VALUES (?,?,?,?,?,?,?,?,?,?)",
                         (r["event_id"], r["identity_sha256"], digest, "expired-not-delivered", r["class"],
                          r["slot"], plan["reason"], plan["ruling_ref"], plan["source_utc"], now))
            count("expired-not-delivered")
        em.verify_scheduled_dispositions(conn)
        conn.execute("COMMIT")
    except BaseException:
        if conn.in_transaction:
            conn.execute("ROLLBACK")
        raise
    return {"plan_sha256": digest, "actions": actions}


def verify(conn, slots=None):
    """Read-only: every receipt still fences an unchanged, undelivered firing and claim."""
    count = em.verify_scheduled_dispositions(conn)
    retired = 0
    if slots is not None:
        for event_id, slot in conn.execute(
                "SELECT event_id,slot FROM scheduled_dispositions WHERE class='claimed-unknown'"):
            state, binding = _slot_state(slots, slot)
            hit = (slots.execute("SELECT binding FROM slot_retirements WHERE slot=?", (slot,)).fetchone()
                   if _ledger_present(slots, "slot_retirements") else None)
            if state != "retired" or hit is None or hit[0] != binding:
                raise ValueError(f"{event_id}: retired claim changed, resolved or lost its receipt")
            retired += 1
    return {"verified_dispositions": count, "verified_slot_retirements": retired if slots is not None else None}


def _open_ro(path):
    conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, isolation_level=None)
    conn.execute("PRAGMA query_only=ON")
    conn.execute("BEGIN")
    return conn


def _open_slots_rw(path):
    conn = sqlite3.connect(str(path), timeout=10, isolation_level=None)
    conn.execute("PRAGMA synchronous=FULL")
    conn.executescript(RETIREMENT_SCHEMA)
    return conn


def main(argv=None):
    """plan (read-only dry run), apply (exact plan, consumer lease) or verify."""
    import argparse
    from consumer_lease import ConsumerLease
    from chaski_version import add_version_argument
    ap = argparse.ArgumentParser(description="Reviewed expiry of stale scheduled firings (aegis-se8)")
    add_version_argument(ap, "chaski-scheduled-expiry")
    ap.add_argument("action", choices=("plan", "apply", "verify"))
    ap.add_argument("--db", type=Path, required=True, help="emitter inbox (emitter.sqlite)")
    ap.add_argument("--slots", type=Path, help="the emitter's scheduled slot ledger")
    ap.add_argument("--cursor", type=Path)
    ap.add_argument("--plan", type=Path)
    ap.add_argument("--expected-plan-sha256")
    ap.add_argument("--emitter")
    ap.add_argument("--created-before", type=float)
    ap.add_argument("--expect-never-claimed", type=int)
    ap.add_argument("--expect-claimed-unknown", type=int)
    ap.add_argument("--reason")
    ap.add_argument("--ruling-ref")
    args = ap.parse_args(argv)
    for path in (args.db, args.slots):
        if path is not None and not path.is_file():
            ap.error(f"existing database required, no initialization: {path}")
    if args.action in ("plan", "apply") and args.slots is None:
        ap.error("plan and apply require the emitter's --slots ledger")
    if args.action == "apply":
        if not (args.cursor and args.cursor.is_file() and args.plan and args.expected_plan_sha256):
            ap.error("apply requires cursor custody, the reviewed plan and its exact hash")
        if args.plan.stat().st_size > 8 * 1024 * 1024:
            ap.error("plan exceeds 8MiB bound")
        plan = json.loads(args.plan.read_text())
        with ConsumerLease(args.cursor, args.db):
            conn, slots = em.connect(args.db), _open_slots_rw(args.slots)
            try:
                result = apply_plan(conn, slots, plan, args.expected_plan_sha256)
            finally:
                slots.close()
                conn.close()
    else:
        conn = _open_ro(args.db)
        slots = _open_ro(args.slots) if args.slots is not None else None
        try:
            if args.action == "plan":
                expected = (args.expect_never_claimed, args.expect_claimed_unknown)
                if (not (args.plan and args.emitter and args.reason and args.ruling_ref)
                        or args.created_before is None or any(c is None for c in expected)):
                    ap.error("plan requires output path, emitter, cutoff, reason, ruling ref and both counts")
                plan = build_plan(conn, slots, args.emitter, args.created_before, dict(zip(CLASSES, expected)),
                                  args.reason, args.ruling_ref)
                digest = check_plan(plan)
                with os.fdopen(os.open(args.plan, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as out:
                    json.dump(plan, out, indent=1, sort_keys=True)
                result = {"plan_sha256": digest, "counts": plan["counts"],
                          "status": "dry-run; no inbox or ledger mutation"}
            else:
                if not _ledger_present(conn, "scheduled_dispositions"):
                    result = {"verified_dispositions": 0, "verified_slot_retirements": None}
                else:
                    result = verify(conn, slots)
        finally:
            for c in (conn, slots):
                if c is not None:
                    c.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
