#!/usr/bin/env python3
"""Demonstrate a lost identity transition and exact-key migration on private SQLite."""

import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import emitter as em
import incremental
import state_key_migration as migration


def main():
    with tempfile.TemporaryDirectory(prefix="state-key-demo-") as directory:
        db = Path(directory) / "demo.db"
        conn = em.connect(db)
        conn.executescript(incremental.SCHEMA)
        conn.row_factory = __import__("sqlite3").Row
        emitter = em.Emitter(label="fixture", command=["unused"], interval_s=60, owner="fixture")
        short, iri = "fixture-item", "urn:fixture:canonical-item"

        def observe(key, verdict):
            return em.observe(
                conn,
                emitter,
                [
                    {
                        "item": key,
                        "verdict": verdict,
                        "event_id": "fixture-transition",
                        "evidence": "fixture",
                        "assignee": "fixture",
                    }
                ],
                10,
                scope=[key],
            )

        observe(short, "BLOCKED")
        before = observe(iri, "UNBLOCKED")
        assert before == []
        print("BEFORE same logical item changed identity: BLOCKED short key + UNBLOCKED IRI => no event")
        print(json.dumps({"queued": before, "outbox_count": conn.execute("SELECT count(*) FROM outbox").fetchone()[0]}))
        plan = {
            "schema": migration.SCHEMA,
            "mappings": [
                {
                    "emitter": "fixture",
                    "from": short,
                    "to": iri,
                    "expected": migration.digest(
                        {
                            "from": migration.stable(migration.row(conn, "fixture", short)),
                            "to": migration.stable(migration.row(conn, "fixture", iri)),
                        }
                    ),
                }
            ],
        }
        print("DRY RUN", json.dumps(migration.migrate(db, plan), sort_keys=True))
        migration.migrate(db, plan, apply=True, backup=Path(directory) / "rollback.db")
        after = observe(iri, "UNBLOCKED")
        replay = observe(iri, "UNBLOCKED")
        assert after == ["fixture-transition"] and replay == []
        assert conn.execute("SELECT count(*) FROM outbox").fetchone()[0] == 1
        print("AFTER same canonical UNBLOCKED observation uses retained historical BLOCKED state")
        print(json.dumps({"queued": after, "replay": replay, "outbox_count": 1, "event_sent": False}))
        conn.close()


if __name__ == "__main__":
    main()
