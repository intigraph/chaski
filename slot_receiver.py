"""Durable at-most-once admission for a scheduled command receiver.

Delivery is at least once; executing a side effect again after a lost reply is
unsafe. A committed claim precedes the callback. A process that dies afterwards
leaves UNKNOWN, never permission to execute again. The caller owns authentication,
manifest admission, signed run transitions and export; this is not a scheduler or
an acknowledgment that a workflow succeeded. Keep the database across rollback.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Callable


class SlotConflict(ValueError):
    """The same slot arrived with a different execution binding."""


class SlotUnknown(RuntimeError):
    """A claimed slot has no durable result; reconcile, do not execute again."""


def _canonical(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def slot_id(job: str, revision: str, scheduled_at: int) -> str:
    """UTC epoch seconds, independent of send time and transport retries."""
    if not isinstance(job, str) or not job or not isinstance(revision, str) or not revision:
        raise ValueError("slot needs a job identity and immutable revision")
    if type(scheduled_at) is not int or scheduled_at < 0:
        raise ValueError("scheduled_at must be nonnegative UTC epoch seconds")
    return "slot-" + hashlib.sha256(_canonical({
        "job": job, "revision": revision, "scheduled_at": scheduled_at,
    }).encode()).hexdigest()


class SlotLedger:
    """One local SQLite ledger; no automatic retry of command execution.

    `binding` must include the admitted immutable manifest and execution scope;
    only its canonical digest is stored. `action` returns a JSON object with the
    actual command outcome, which may be a failure or a hold. A stored result is
    replayable delivery evidence, never by itself signed workflow acceptance.
    """

    def __init__(self, path: Path | str):
        self.path = str(path)
        with closing(self._connect()) as conn, conn:
            conn.execute("CREATE TABLE IF NOT EXISTS slots ("
                         "slot TEXT PRIMARY KEY, binding TEXT NOT NULL, result TEXT)")

    def _connect(self):
        conn = sqlite3.connect(self.path, timeout=10)
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def run(self, slot: str, binding: dict, action: Callable[[], dict]) -> dict:
        if not isinstance(slot, str) or not slot or not isinstance(binding, dict) or not binding:
            raise ValueError("slot and nonempty admitted binding are required")
        digest = hashlib.sha256(_canonical(binding).encode()).hexdigest()
        conn = self._connect()
        try:
            # A unique insert serializes competing processes. The claim MUST be
            # committed before any callback, including signing a started event.
            with conn:
                inserted = conn.execute(
                    "INSERT OR IGNORE INTO slots(slot,binding) VALUES (?,?)",
                    (slot, digest)).rowcount
            if not inserted:
                previous, result = conn.execute(
                    "SELECT binding,result FROM slots WHERE slot=?", (slot,)).fetchone()
                if previous != digest:
                    raise SlotConflict("slot execution binding changed")
                if result is None:
                    raise SlotUnknown("claimed slot needs reconciliation")
                return json.loads(result)
            # Exceptions, timeout, kill or a failure to persist the result leave
            # the committed claim UNKNOWN. None of them authorize another call.
            result = action()
            if not isinstance(result, dict):
                raise ValueError("action must return a JSON outcome object")
            encoded = _canonical(result)
            with conn:
                conn.execute("UPDATE slots SET result=? WHERE slot=?", (encoded, slot))
            # First delivery and replay have the same JSON types and values.
            return json.loads(encoded)
        finally:
            conn.close()
