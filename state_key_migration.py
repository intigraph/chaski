"""Exact, opt-in emitter identity migration; never emit or replay events.

# arming: agent-invoked — reviewed plan plus explicit --apply and backup.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from urllib.parse import urlsplit

SCHEMA = "chaski-state-key-map/v1"
RELATED = ("outbox", "recovery_outbox", "adapter_deadlines")


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def row(conn, emitter, item):
    found = conn.execute("SELECT * FROM verdicts WHERE emitter=? AND item=?", (emitter, item)).fetchone()
    return dict(found) if found is not None else None


def stable(value):
    # Observation time advances on every healthy poll. Logical state must not.
    return None if value is None else {k: v for k, v in value.items() if k != "updated"}


def validate(plan):
    if set(plan) != {"schema", "mappings"} or plan["schema"] != SCHEMA or not isinstance(plan["mappings"], list):
        raise ValueError("invalid exact-mapping plan")
    if not 1 <= len(plan["mappings"]) <= 1000:
        raise ValueError("plan must contain 1..1000 reviewed mappings")
    sources, targets = set(), set()
    for mapping in plan["mappings"]:
        if set(mapping) != {"emitter", "from", "to", "expected"}:
            raise ValueError("invalid mapping fields")
        emitter, source, target = (mapping[k] for k in ("emitter", "from", "to"))
        if not all(isinstance(v, str) and v and not any(c.isspace() for c in v) for v in (emitter, source, target)):
            raise ValueError("mapping identities must be nonempty strings without whitespace")
        if ":" in source or urlsplit(target).scheme not in ("https", "http", "urn"):
            raise ValueError("only exact short-key to absolute-IRI mappings are supported")
        if (emitter, source) in sources or (emitter, target) in targets:
            raise ValueError("duplicate or many-to-one mapping")
        sources.add((emitter, source))
        targets.add((emitter, target))
        if not isinstance(mapping["expected"], str) or len(mapping["expected"]) != 64:
            raise ValueError("missing reviewed logical-state fingerprint")


def inspect(conn, plan):
    validate(plan)
    result = {"plan_sha256": digest(plan), "rename": 0, "merge": 0, "noop": 0, "conflicts": [], "rows": []}
    for mapping in plan["mappings"]:
        emitter, source, target = (mapping[k] for k in ("emitter", "from", "to"))
        old, new = row(conn, emitter, source), row(conn, emitter, target)
        state = digest({"from": stable(old), "to": stable(new)})
        conflict = None
        if state != mapping["expected"]:
            conflict = "logical state changed since reviewed dry-run"
        elif old is None:
            if new is None:
                conflict = "neither source nor target exists"
            else:
                result["noop"] += 1
        elif not old["tracked"]:
            conflict = "source is not tracked; requires separate adjudication"
        elif new is not None:
            # Only a silent first observation may be superseded by the actual
            # historical state. Never choose between two event histories.
            if new["generation"] != 0 or new["last_event"] is not None or new["recovery_at"] is not None:
                conflict = "target has transition or recovery history"
            elif not new["tracked"]:
                conflict = "target is not a tracked baseline"
            elif any(
                conn.execute(f"SELECT 1 FROM {table} WHERE emitter=? AND item=? LIMIT 1", (emitter, target)).fetchone()
                for table in ("outbox", "recovery_outbox")
            ):
                conflict = "target has queued or delivered event history"
            elif conn.execute(
                "SELECT 1 FROM adapter_deadlines WHERE emitter=? AND item=?", (emitter, target)
            ).fetchone():
                conflict = "target has a scheduled deadline; requires separate adjudication"
            else:
                result["merge"] += 1
        else:
            result["rename"] += 1
        if conflict:
            result["conflicts"].append({"emitter": emitter, "from": source, "to": target, "reason": conflict})
        result["rows"].append({"emitter": emitter, "from": source, "to": target, "source": old, "target": new})
    return result


def summarize(report):
    return {key: value for key, value in report.items() if key != "rows"}


def snapshot_locked(db, destination):
    """Called with BEGIN IMMEDIATE held, before the first data change."""
    destination = Path(destination)
    fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(fd)
    # The write reservation prevents another commit. A separate read connection
    # can back up the committed state without backing up an active transaction.
    source = sqlite3.connect(Path(db).resolve().as_uri() + "?mode=ro", uri=True)
    backup = sqlite3.connect(destination)
    try:
        source.backup(backup)
        if backup.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("backup integrity failure")
    finally:
        backup.close()
        source.close()
    return {"path": str(destination), "sha256": hashlib.sha256(destination.read_bytes()).hexdigest()}


def migrate(db, plan, *, apply=False, backup=None):
    validate(plan)
    db = Path(db).resolve()
    conn = sqlite3.connect(db.as_uri() + ("?mode=rw" if apply else "?mode=ro"), uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
        exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name='state_key_migrations' AND type='table'"
        ).fetchone()
        if exists and conn.execute("SELECT 1 FROM state_key_migrations WHERE plan_id=?", (digest(plan),)).fetchone():
            conn.rollback()
            return {"plan_sha256": digest(plan), "already_applied": True, "changed": 0}
        report = inspect(conn, plan)
        if not apply:
            conn.rollback()
            return summarize(report)
        if report["conflicts"]:
            raise ValueError("conflicting keys; no rows changed: " + json.dumps(report["conflicts"]))
        if not backup:
            raise ValueError("apply requires a new backup path")
        receipt = snapshot_locked(db, backup)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS state_key_migrations "
            "(plan_id TEXT PRIMARY KEY, before_json TEXT NOT NULL, backup_json TEXT NOT NULL)"
        )
        for entry in report["rows"]:
            if entry["source"] is None:
                continue
            emitter, source, target = (entry[k] for k in ("emitter", "from", "to"))
            if entry["target"] is not None:
                conn.execute("DELETE FROM verdicts WHERE emitter=? AND item=?", (emitter, target))
            conn.execute("UPDATE verdicts SET item=? WHERE emitter=? AND item=?", (target, emitter, source))
            # IDs, payload bytes, generation, attempts, cursor, deliveries and
            # receiver scopes remain byte-for-byte unchanged. Only join keys move.
            for table in RELATED:
                conn.execute(f"UPDATE {table} SET item=? WHERE emitter=? AND item=?", (target, emitter, source))
        conn.execute(
            "INSERT INTO state_key_migrations VALUES (?,?,?)",
            (digest(plan), json.dumps(report["rows"], sort_keys=True), json.dumps(receipt, sort_keys=True)),
        )
        conn.commit()
        return {**summarize(report), "backup": receipt, "changed": report["rename"] + report["merge"]}
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    plan_input = parser.add_mutually_exclusive_group(required=True)
    plan_input.add_argument("--plan")
    plan_input.add_argument("--plan-json", help="reviewed exact plan as JSON; no event payloads or credentials")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--dry-run", action="store_true", help="inspect only (the default)")
    parser.add_argument("--backup")
    args = parser.parse_args()
    try:
        result = migrate(
            args.db,
            json.loads(args.plan_json if args.plan_json is not None else Path(args.plan).read_text()),
            apply=args.apply,
            backup=args.backup,
        )
    except (ValueError, sqlite3.Error, OSError) as error:
        parser.exit(1, str(error) + "\n")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
