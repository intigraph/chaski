"""Synthetic-only plan/I/O control; never opens the production database."""

import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import incremental

query = next(
    value
    for value in incremental.ChangeRunner.route_pending.__code__.co_consts
    if isinstance(value, str) and value.startswith("SELECT tx,seq,payload")
)


def written():
    try:
        return int(dict(line.split(": ", 1) for line in Path("/proc/self/io").read_text().splitlines())["write_bytes"])
    except FileNotFoundError:
        return None  # non-Linux: plan and row parity remain observable


def probe(db):
    conn = sqlite3.connect("file:" + str(db) + "?mode=ro", uri=True)
    plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + query, ("fixture", "fixture", incremental.BATCH))]
    before = written()
    started = time.monotonic()
    rows = conn.execute(query, ("fixture", "fixture", incremental.BATCH)).fetchall()
    after = written()
    result = {
        "seconds": time.monotonic() - started,
        "write_bytes": after - before if before is not None and after is not None else None,
        "row_count": len(rows),
        "result_sha256": hashlib.sha256(json.dumps(rows).encode()).hexdigest(),
        "plan": plan,
    }
    conn.close()
    return result


with tempfile.TemporaryDirectory(prefix="inbox-index-demo-") as root:
    os.environ["SQLITE_TMPDIR"] = root
    db = Path(root) / "synthetic.db"
    conn = sqlite3.connect(db)
    conn.executescript(incremental.SCHEMA)
    conn.execute("DROP INDEX change_inbox_entity")  # model the old durable schema
    conn.executemany(
        "INSERT INTO change_inbox VALUES (?,?,?,?)",
        (
            (
                "fixture",
                i // 4,
                i % 4,
                json.dumps(
                    {
                        "entity": "https://example.test/entity/" + ("x" * 480) + str(i % 12000),
                        "tx": i // 4,
                        "sequence": i % 4,
                        "op": "assert",
                        "value": "fixture",
                    }
                ),
            )
            for i in range(60000)
        ),
    )
    conn.commit()
    conn.close()
    before = probe(db)
    conn = sqlite3.connect(db)
    conn.executescript(incremental.SCHEMA)
    conn.close()
    after = probe(db)
    assert before["result_sha256"] == after["result_sha256"], (before, after)
    assert before["row_count"] > 0
    print(
        json.dumps(
            {
                "fixture_rows": 60000,
                "batch": incremental.BATCH,
                "incremental_sha256": hashlib.sha256(Path(incremental.__file__).read_bytes()).hexdigest(),
                "before": before,
                "after": after,
            },
            indent=2,
        )
    )
