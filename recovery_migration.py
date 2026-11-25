"""One-time, reviewed migration of retained recovery rows (aegis-ssvr A).

chaski #79 (8673f3c8) added ``vocabulary`` to the Quipu sink's delivery scope.
Recovery rows frozen before it carry the older scope shape, so the unchanged
scope check refuses them forever ("prior delivery receiver scope changed or
unproven") although the receiver did not move. This tool does NOT relax that
check. It applies an exact, reviewed plan to the retained rows:

* ``refreeze``: a row whose frozen scope is the pre-#79 shape (every Quipu sink
  is exactly {kind, base, namespace, reaction}, every other sink unchanged)
  gets ``vocabulary`` added to each Quipu sink in its recorded scope. Nothing
  else in the row changes; it is delivered later by the normal path, which
  still compares scopes exactly.
* ``expire``: a row with no frozen scope (``null-scope``), or whose prior graph
  firing could not be proven (``missing-prior``), is retained with an
  append-only ``expired-not-delivered`` receipt in ``recovery_dispositions``.
  The runtime then never delivers, schedules or counts it as pending.

Any undelivered recovery that fits neither class refuses the whole plan. The
plan is read-only and records each row's identity hash (immutable fields only,
so retry counters may move between review and apply). ``apply`` takes the
consumer lease, runs in one transaction, refuses a changed row or population,
and replaying the identical plan is a no-op. No sink or network call occurs.

# arming: agent-invoked — exact reviewed plan, consumer held, backup taken.
"""
from __future__ import annotations

import copy
import datetime
import json
import os
import re
import sqlite3
import time
from pathlib import Path

import emitter as em

PLAN_SCHEMA = "chaski-recovery-migration/v1"
LEGACY_VOCABULARY = "aegis-legacy/v1"
MISSING_PRIOR = "missing or ambiguous prior graph firing"
LEGACY_QUIPU_KEYS = {"kind", "base", "namespace", "reaction"}
CLASSES = {"refreeze": "refreeze", "null-scope": "expire", "missing-prior": "expire"}
MAX_ROWS = 10000


def _row(conn, event_id):
    cur = conn.execute("SELECT * FROM recovery_outbox WHERE event_id=?", (event_id,))
    row = cur.fetchone()
    return None if row is None else dict(zip((d[0] for d in cur.description), row))


identity = em.recovery_identity


def refrozen_scope(scope, vocabulary=LEGACY_VOCABULARY):
    """The pre-#79 scope with ``vocabulary`` added to each Quipu sink, else None."""
    if not isinstance(scope, dict):
        return None
    sinks = scope.get("sinks") if scope.get("kind") == "chain" else None
    if scope.get("kind") == "chain":
        if set(scope) != {"kind", "sinks"} or not isinstance(sinks, list) or not sinks:
            return None
    out = copy.deepcopy(scope)
    targets = out["sinks"] if scope.get("kind") == "chain" else [out]
    changed = 0
    for sink in targets:
        if not isinstance(sink, dict):
            return None
        if sink.get("kind") == "quipu":
            if set(sink) != LEGACY_QUIPU_KEYS:
                return None   # already versioned, or an unknown shape: not this migration
            sink["vocabulary"] = vocabulary
            changed += 1
    return out if changed else None


def classify(row):
    """(class, after_payload or None). Raises on a row outside the ruling."""
    if row["delivered_at"] is not None:
        raise ValueError(f"{row['event_id']}: delivered rows are never migrated")
    event = json.loads(row["payload"])
    if not isinstance(event, dict) or "prior" not in event:
        raise ValueError(f"{row['event_id']}: not a recovery payload")
    if "receipt" in event:
        raise ValueError(f"{row['event_id']}: a frozen recovery receipt needs separate adjudication")
    scope = event.get("delivery_scope")
    if scope is None:
        return "null-scope", None
    if MISSING_PRIOR in (row.get("last_error") or ""):
        return "missing-prior", None
    after = refrozen_scope(scope)
    if after is None:
        raise ValueError(f"{row['event_id']}: frozen scope is not the pre-vocabulary shape")
    return "refreeze", json.dumps({**event, "delivery_scope": after}, sort_keys=True)


def population(conn):
    return [r[0] for r in conn.execute(
        "SELECT event_id FROM recovery_outbox WHERE delivered_at IS NULL AND "
        + em.undisposed("recovery_outbox") + " ORDER BY event_id LIMIT ?", (MAX_ROWS + 1,))]


def build_plan(conn, expected, reason, ruling_ref):
    ids = population(conn)
    if len(ids) > MAX_ROWS:
        raise ValueError("population exceeds the reviewed bound")
    rows = []
    for event_id in ids:
        row = _row(conn, event_id)
        cls, after = classify(row)
        entry = {"event_id": event_id, "emitter": row["emitter"], "prior_event": row["prior_event"],
                 "class": cls, "row_sha256": identity(row)}
        if after is not None:
            entry["after_sha256"] = identity({**row, "payload": after})
        rows.append(entry)
    counts = {c: sum(1 for r in rows if r["class"] == c) for c in CLASSES}
    if counts != expected:
        raise ValueError(f"reviewed counts changed: expected {expected}, found {counts}")
    return {"schema": PLAN_SCHEMA, "vocabulary": LEGACY_VOCABULARY, "counts": counts,
            "reason": reason, "ruling_ref": ruling_ref,
            "source_utc": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
            "rows": rows}


def _check_plan(plan):
    fields = {"schema", "vocabulary", "counts", "reason", "ruling_ref", "source_utc", "rows"}
    if not isinstance(plan, dict) or set(plan) != fields or plan["schema"] != PLAN_SCHEMA:
        raise ValueError("invalid recovery migration plan")
    if plan["vocabulary"] != LEGACY_VOCABULARY:
        raise ValueError("this migration only adds the legacy vocabulary")
    for field in ("reason", "ruling_ref", "source_utc"):
        if not isinstance(plan[field], str) or not plan[field].strip() or len(plan[field]) > 4096:
            raise ValueError("missing migration provenance")
    rows = plan["rows"]
    if not isinstance(rows, list) or not 1 <= len(rows) <= MAX_ROWS:
        raise ValueError("invalid exact population bound")
    seen = set()
    for r in rows:
        keys = {"event_id", "emitter", "prior_event", "class", "row_sha256"}
        if r.get("class") == "refreeze":
            keys.add("after_sha256")
        if (not isinstance(r, dict) or set(r) != keys or r["class"] not in CLASSES
                or r["event_id"] in seen
                or any(not re.fullmatch("[0-9a-f]{64}", r[k]) for k in keys if k.endswith("sha256"))):
            raise ValueError("invalid or duplicate row entry")
        seen.add(r["event_id"])
    if plan["counts"] != {c: sum(1 for r in rows if r["class"] == c) for c in CLASSES}:
        raise ValueError("plan counts do not match its rows")
    return em.disposition_digest(plan)


def apply_plan(conn, plan, expected_sha256, log_path=None):
    """Apply atomically under the caller's consumer lease; idempotent on replay."""
    digest = _check_plan(plan)
    if digest != expected_sha256:
        raise ValueError("reviewed plan hash mismatch")
    if conn.in_transaction:
        raise ValueError("migration requires its own transaction")
    log, now = [], time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        planned = {r["event_id"] for r in plan["rows"]}
        extra = set(population(conn)) - planned
        if extra:
            raise ValueError(f"{len(extra)} undelivered recoveries outside the reviewed plan")
        for r in plan["rows"]:
            row = _row(conn, r["event_id"])
            if row is None or row["emitter"] != r["emitter"] or row["prior_event"] != r["prior_event"]:
                raise ValueError(f"{r['event_id']}: reviewed row disappeared or changed scope")
            current = identity(row)
            if r["class"] == "refreeze":
                if current == r["after_sha256"]:
                    action = "already-refrozen"
                elif current == r["row_sha256"]:
                    _, after = classify(row)
                    if identity({**row, "payload": after}) != r["after_sha256"]:
                        raise ValueError(f"{r['event_id']}: re-freeze result differs from review")
                    conn.execute("UPDATE recovery_outbox SET payload=? WHERE event_id=?", (after, r["event_id"]))
                    action = "refrozen"
                else:
                    raise ValueError(f"{r['event_id']}: reviewed row changed")
            else:
                if current != r["row_sha256"]:
                    raise ValueError(f"{r['event_id']}: reviewed row changed")
                prior = conn.execute("SELECT plan_sha256,class FROM recovery_dispositions WHERE event_id=?",
                                     (r["event_id"],)).fetchone()
                if prior is not None:
                    if tuple(prior) != (digest, r["class"]):
                        raise ValueError(f"{r['event_id']}: conflicting append-only disposition")
                    action = "already-expired"
                else:
                    conn.execute("INSERT INTO recovery_dispositions VALUES (?,?,?,?,?,?,?,?,?)",
                                 (r["event_id"], r["row_sha256"], digest, "expired-not-delivered",
                                  r["class"], plan["reason"], plan["ruling_ref"], plan["source_utc"], now))
                    action = "expired-not-delivered"
            log.append({"event_id": r["event_id"], "emitter": r["emitter"], "prior_event": r["prior_event"],
                        "class": r["class"], "action": action, "before_sha256": r["row_sha256"],
                        "after_sha256": r.get("after_sha256"), "plan_sha256": digest, "at": now})
        em.verify_recovery_dispositions(conn)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    if log_path is not None:
        with os.fdopen(os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as out:
            out.writelines(json.dumps(entry, sort_keys=True) + "\n" for entry in log)
    actions = {}
    for entry in log:
        actions[entry["action"]] = actions.get(entry["action"], 0) + 1
    return {"plan_sha256": digest, "actions": actions}


def main(argv=None):
    """plan (read-only dry run), apply (exact plan, consumer lease) or verify."""
    import argparse
    from consumer_lease import ConsumerLease
    from chaski_version import add_version_argument
    ap = argparse.ArgumentParser(description="One-time reviewed recovery scope migration (aegis-ssvr)")
    add_version_argument(ap, "chaski-recovery-migration")
    ap.add_argument("action", choices=("plan", "apply", "verify"))
    ap.add_argument("--db", type=Path, required=True)
    ap.add_argument("--cursor", type=Path)
    ap.add_argument("--plan", type=Path)
    ap.add_argument("--log", type=Path)
    ap.add_argument("--expected-plan-sha256")
    ap.add_argument("--expect-refreeze", type=int)
    ap.add_argument("--expect-null-scope", type=int)
    ap.add_argument("--expect-missing-prior", type=int)
    ap.add_argument("--reason")
    ap.add_argument("--ruling-ref")
    args = ap.parse_args(argv)
    if not args.db.is_file():
        ap.error("existing inbox required; no initialization")
    if args.action == "apply":
        if not (args.cursor and args.cursor.is_file() and args.plan and args.expected_plan_sha256 and args.log):
            ap.error("apply requires cursor custody, the reviewed plan, its exact hash and a new log path")
        if args.plan.stat().st_size > 8 * 1024 * 1024:
            ap.error("plan exceeds 8MiB bound")
        plan = json.loads(args.plan.read_text())
        with ConsumerLease(args.cursor, args.db):
            conn = em.connect(args.db)
            try:
                result = apply_plan(conn, plan, args.expected_plan_sha256, args.log)
            finally:
                conn.close()
    else:
        conn = sqlite3.connect(args.db.resolve().as_uri() + "?mode=ro", uri=True)
        try:
            conn.execute("PRAGMA query_only=ON")
            conn.execute("BEGIN")
            if args.action == "plan":
                counts = (args.expect_refreeze, args.expect_null_scope, args.expect_missing_prior)
                if not (args.plan and args.reason and args.ruling_ref) or any(c is None for c in counts):
                    ap.error("plan requires output path, reason, ruling ref and all three expected counts")
                plan = build_plan(conn, dict(zip(CLASSES, counts)), args.reason, args.ruling_ref)
                digest = _check_plan(plan)
                with os.fdopen(os.open(args.plan, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as out:
                    json.dump(plan, out, indent=1, sort_keys=True)
                result = {"plan_sha256": digest, "counts": plan["counts"], "status": "dry-run; no inbox mutation"}
            else:
                result = {"verified_recovery_dispositions": em.verify_recovery_dispositions(conn),
                          "undelivered_unmigrated": {}}
                for event_id in population(conn):
                    try:
                        cls, _ = classify(_row(conn, event_id))
                    except ValueError:
                        scope = json.loads(_row(conn, event_id)["payload"]).get("delivery_scope")
                        sinks = (scope or {}).get("sinks", [scope])
                        refrozen = any(isinstance(x, dict) and x.get("kind") == "quipu"
                                       and x.get("vocabulary") == LEGACY_VOCABULARY for x in sinks)
                        cls = "refrozen-awaiting-delivery" if refrozen else "outside-ruling"
                    result["undelivered_unmigrated"][cls] = result["undelivered_unmigrated"].get(cls, 0) + 1
        finally:
            conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
