"""chaski emitter — turn a verdict adapter's output into transition events,
delivered at least once to an idempotent receiver.

Stage 2. A verdict adapter is an external command that prints a JSON list of
records, one per tracked item:

    {"item": "...", "verdict": "BLOCKED" | "UNBLOCKED" | "UNKNOWN",
     "event_id": "...",          # required on UNBLOCKED
     "evidence": "...", "assignee": "...", ...}

The adapter is stateless. Everything that makes a transition an EVENT lives
here, in one SQLite file:

- A first observation of an item is a BASELINE, never an event: we did not see
  it become unblocked, we only met it that way.
- UNKNOWN keeps the last known verdict and never emits. "Could not tell" must
  not round to "unblocked", and BLOCKED -> UNKNOWN -> UNBLOCKED emits exactly
  once, when the positive evidence returns.
- Only a known BLOCKED -> UNBLOCKED emits. Every verdict change advances the
  item's generation, so a later genuine re-block and unblock is a new event.
- An item missing from a complete run is UNTRACKED (closed, or no blockers
  left), never unblocked. If it comes back, that is a new baseline.
  KNOWN LIMIT (sattler, review of 88842e53): a transition that happens WHILE
  an item is absent emits nothing on its return, because the return is a
  baseline. An emitter whose consumers must hear such a return sets
  baseline_emits (the review-age emitter does); the blocked-by one does not,
  because "we did not watch it unblock" is the truthful reading there.
- A failed run (non-zero exit, timeout, unparseable output) is UNKNOWN for
  every item: nothing changes.

The verdict update and the outbox row commit in ONE transaction, so a crash
cannot lose an event or record one twice. The outbox is keyed on the
adapter's event id, which must be DETERMINISTIC from the item and the
transition that caused it, never minted at send time. A re-detected
transition after a crash then produces the same id and INSERT OR IGNORE keeps
one row.

Delivery is at least once. A row is marked delivered only after the sink
returned, which it does only on an observed receipt; a failure is retried
with backoff. A crash between a successful send and the checkpoint re-sends
the same id, and the receiver deduplicates on it. That is what makes the
receiver, not this sender, the place exactly-once is decided.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

LOG = logging.getLogger("chaski.emitter")

BLOCKED, UNBLOCKED, UNKNOWN = "BLOCKED", "UNBLOCKED", "UNKNOWN"
MAX_BACKOFF_S = 3600
BASE_BACKOFF_S = 30
# Deliveries per tick. The reactor ticks every 5 s, so this bounds a backlog
# drain at 1 write/s: quipu applies writes one at a time, and a new writer
# arriving in a burst has wedged it before.
DELIVER_PER_TICK = 5

SCHEMA = """
CREATE TABLE IF NOT EXISTS renewal_receipts (
    event_id TEXT PRIMARY KEY,
    last_received REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL NOT NULL DEFAULT 0,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS runner_status (
    emitter TEXT PRIMARY KEY,
    last_attempt REAL NOT NULL,
    last_success REAL NOT NULL,
    ok INTEGER NOT NULL,
    unknown INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS delivery_budget (
    scope TEXT PRIMARY KEY,
    last_taken REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS verdicts (
    emitter    TEXT NOT NULL,
    item       TEXT NOT NULL,
    verdict    TEXT NOT NULL,
    generation INTEGER NOT NULL,
    evidence   TEXT,
    tracked    INTEGER NOT NULL DEFAULT 1,
    last_event TEXT,
    updated    REAL NOT NULL,
    PRIMARY KEY (emitter, item)
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id     TEXT PRIMARY KEY,
    emitter      TEXT NOT NULL,
    item         TEXT NOT NULL,
    generation   INTEGER NOT NULL,
    payload      TEXT NOT NULL,
    created      REAL NOT NULL,
    delivered_at REAL,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL NOT NULL DEFAULT 0,
    last_error   TEXT
);
CREATE TABLE IF NOT EXISTS recovery_outbox (
    event_id TEXT PRIMARY KEY,
    emitter TEXT NOT NULL,
    item TEXT NOT NULL,
    generation INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created REAL NOT NULL,
    prior_event TEXT NOT NULL,
    delivered_at REAL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    UNIQUE (emitter, prior_event)
);

"""


class ProtocolError(ValueError):
    """The adapter's output breaks the record contract. The whole run is refused."""


@dataclass
class Emitter:
    """One adapter, run on a schedule.

    `key` names the record field that identifies an item ("item" for the
    blocked-by adapter, "entity" for the review-age one). A transition from
    `from_verdict` to `to_verdict` is the event, named `event` for consumers.
    `baseline_emits` makes a first sighting already in `to_verdict` an event
    too: right for "this is overdue" (the owner must hear it however late we
    met it), wrong for "this became unblocked" (we did not see it happen).
    """

    label: str
    command: list[str]
    interval_s: int
    owner: str
    event: str = "unblocked"
    timeout_s: int = 300
    key: str = "item"
    from_verdict: str = BLOCKED
    to_verdict: str = UNBLOCKED
    baseline_emits: bool = False
    generation_event_ids: bool = False
    missing_is_from: bool = False
    change_driven: bool = False
    externally_triggered: bool = False
    renew_interval_s: int = 0
    persist_runs: bool = False
    resolve_recovery: bool = False

    @property
    def verdicts(self) -> set[str]:
        return {self.from_verdict, self.to_verdict, UNKNOWN}


def connect(path: Path | str) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), isolation_level=None)  # explicit transactions below
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(verdicts)")}
    if "last_event" not in cols:  # a state file from before last_event existed
        conn.execute("ALTER TABLE verdicts ADD COLUMN last_event TEXT")
    for table, column, kind in (("verdicts", "recovery_at", "REAL"),
                                ("outbox", "recovery_queued", "INTEGER NOT NULL DEFAULT 0"),
                                ("outbox", "delivery_scope", "TEXT")):
        if column not in {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {kind}")
    conn.execute("CREATE INDEX IF NOT EXISTS outbox_recovery_candidates ON outbox(emitter,item,created,event_id)"
                 " WHERE recovery_queued=0 AND delivered_at IS NOT NULL")
    conn.execute("CREATE INDEX IF NOT EXISTS verdict_recovery_candidates ON verdicts(emitter,recovery_at)")
    return conn


class _ScopedRecords(list):
    """A successful partial inventory; absence outside scope is UNKNOWN."""

    def __init__(self, records, scope):
        super().__init__(records)
        self.scope = frozenset(scope)


def decode_adapter_output(raw: str) -> list[dict]:
    records = json.loads(raw)
    if isinstance(records, dict):
        if set(records) != {"records", "scope"}:
            raise ProtocolError("partial adapter output requires records and scope")
        scope, values = records["scope"], records["records"]
        if (not isinstance(scope, list) or len(scope) > 10000
                or any(not isinstance(item, str) or not item for item in scope)
                or len(set(scope)) != len(scope)
                or not isinstance(values, list) or len(values) > 10000):
            raise ProtocolError("invalid partial adapter inventory")
        return _ScopedRecords(values, scope)
    if not isinstance(records, list):
        raise ProtocolError("adapter output is not a JSON list or partial inventory")
    return records


def run_adapter(emitter: Emitter) -> list[dict]:
    """The adapter's records. Raises on ANY failure: the caller treats it as UNKNOWN."""
    proc = subprocess.run(emitter.command, capture_output=True, text=True,
                          timeout=emitter.timeout_s, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"adapter exited {proc.returncode}: {proc.stderr.strip()[:300]}")
    return decode_adapter_output(proc.stdout)


def _validate(emitter: Emitter, records: list[dict]) -> None:
    seen = set()
    for r in records:
        if not isinstance(r, dict) or not isinstance(r.get(emitter.key), str) or not r[emitter.key]:
            raise ProtocolError(f"record without {emitter.key!r}: {str(r)[:120]}")
        item = r[emitter.key]
        if r.get("verdict") not in emitter.verdicts:
            raise ProtocolError(f"{item}: verdict {r.get('verdict')!r} not in {sorted(emitter.verdicts)}")
        kind = r.get("work_kind")
        if kind is not None and (not isinstance(kind, str) or
                                 not re.fullmatch(r"[A-Za-z][A-Za-z0-9_-]{0,127}", kind)):
            raise ProtocolError(f"{item}: invalid work_kind")
        if r["verdict"] == emitter.to_verdict and not r.get("event_id"):
            # Without a deterministic id the receiver cannot deduplicate, and a
            # send-time id would make at-least-once delivery at-least-twice.
            raise ProtocolError(f"{item}: {emitter.to_verdict} without event_id")
        if item in seen:
            raise ProtocolError(f"{item}: listed twice in one run")
        seen.add(item)


def observe(conn: sqlite3.Connection, emitter: Emitter, records: list[dict], now: float,
            scope: set[str] | None = None) -> list[str]:
    """Apply an adapter run; scope=None is complete, a set bounds a partial run.

    Returns the event ids newly queued. Savepoints compose with an outer queue
    transaction so acknowledgement cannot get ahead of the verdict update.

    All-or-nothing: a protocol error leaves every verdict and the outbox as
    they were.
    """
    if scope is None and isinstance(records, _ScopedRecords):
        scope = records.scope
    _validate(emitter, records)
    if emitter.missing_is_from and scope is None:
        # Opt-in only for complete inventories of a bounded rolling window.
        # Failed adapters never reach observe; partial scope never clears absence.
        present_keys = {r[emitter.key] for r in records}
        records = records + [{emitter.key: item, "verdict": emitter.from_verdict}
                             for (item,) in conn.execute(
                                 "SELECT item FROM verdicts WHERE emitter=? AND tracked=1",
                                 (emitter.label,)).fetchall() if item not in present_keys]
    if scope is not None and any(r[emitter.key] not in scope for r in records):
        raise ProtocolError("adapter record outside its partial scope")
    queued: list[str] = []

    def emit(r: dict, item: str, generation: int) -> None:
        if emitter.generation_event_ids:
            r = {**r, "event_id": hashlib.sha256(
                f"{emitter.label}:{r['event_id']}:{generation}".encode()).hexdigest()}
        payload = {
            "event_id": r["event_id"], "event": emitter.event, "emitter": emitter.label,
            "item": item, "generation": generation,
            # whoever the adapter says should act: the assignee of a work item,
            # the owner of an aged entity
            "recipient": r.get("assignee") or r.get("owner"),
            "evidence": r.get("evidence"), "observed_at": now,
            **({"work_kind": r["work_kind"]} if r.get("work_kind") else {}),
        }
        cur = conn.execute(
            "INSERT OR IGNORE INTO outbox (event_id, emitter, item, generation, payload, created)"
            " VALUES (?,?,?,?,?,?)",
            (r["event_id"], emitter.label, item, generation, json.dumps(payload, sort_keys=True), now))
        conn.execute("UPDATE verdicts SET last_event=? WHERE emitter=? AND item=?",
                     (r["event_id"], emitter.label, item))
        if cur.rowcount:
            queued.append(r["event_id"])

    conn.execute("SAVEPOINT observation")
    try:
        present = set()
        for r in records:
            item, verdict = r[emitter.key], r["verdict"]
            present.add(item)
            row = conn.execute(
                "SELECT verdict, generation, tracked, last_event FROM verdicts WHERE emitter=? AND item=?",
                (emitter.label, item)).fetchone()
            if row is None or not row[2]:
                if verdict == UNKNOWN:
                    continue  # nothing known to baseline from
                conn.execute(
                    "INSERT INTO verdicts (emitter, item, verdict, generation, evidence, tracked, updated)"
                    " VALUES (?,?,?,?,?,1,?) ON CONFLICT(emitter, item) DO UPDATE SET"
                    " verdict=excluded.verdict, evidence=excluded.evidence, tracked=1,"
                    " generation=generation+1, updated=excluded.updated",
                    (emitter.label, item, verdict, 0, r.get("evidence"), now))
                if emitter.baseline_emits and verdict == emitter.to_verdict:
                    gen = conn.execute("SELECT generation FROM verdicts WHERE emitter=? AND item=?",
                                       (emitter.label, item)).fetchone()[0]
                    emit(r, item, gen)
                continue
            last, generation, last_event = row[0], row[1], row[3]
            if verdict == UNKNOWN:
                continue
            if verdict == last:
                conn.execute("UPDATE verdicts SET evidence=?, updated=? WHERE emitter=? AND item=?",
                             (r.get("evidence"), now, emitter.label, item))
                # Same verdict, NEW transition: it went away and came back
                # between two runs (re-blocked and unblocked again; re-verified
                # and lapsed again). The adapter's id says so; the verdict cannot.
                if (not emitter.generation_event_ids and verdict == emitter.to_verdict and last_event is not None
                        and r["event_id"] != last_event):
                    generation += 2
                    conn.execute("UPDATE verdicts SET generation=? WHERE emitter=? AND item=?",
                                 (generation, emitter.label, item))
                    emit(r, item, generation)
                continue
            generation += 1
            conn.execute(
                "UPDATE verdicts SET verdict=?, generation=?, evidence=?, updated=? WHERE emitter=? AND item=?",
                (verdict, generation, r.get("evidence"), now, emitter.label, item))
            if last == emitter.from_verdict and verdict == emitter.to_verdict:
                emit(r, item, generation)
        for (item,) in conn.execute(
                "SELECT item FROM verdicts WHERE emitter=? AND tracked=1", (emitter.label,)).fetchall():
            if item not in present and (scope is None or item in scope):
                conn.execute("UPDATE verdicts SET tracked=0, updated=? WHERE emitter=? AND item=?",
                             (now, emitter.label, item))
        if emitter.resolve_recovery:
            # A recovery clock is fixed at its first verified observation. An
            # UNKNOWN record retains the last known state without inventing one.
            for r in records:
                if r["verdict"] != UNKNOWN:
                    if r["verdict"] == emitter.to_verdict:
                        conn.execute("UPDATE verdicts SET recovery_at=NULL WHERE emitter=? AND item=?",
                                     (emitter.label, r[emitter.key]))
                    else:
                        conn.execute("UPDATE verdicts SET recovery_at=COALESCE(recovery_at,?)"
                                     " WHERE emitter=? AND item=?", (now, emitter.label, r[emitter.key]))
            if scope is None:
                conn.execute("UPDATE verdicts SET recovery_at=COALESCE(recovery_at,?)"
                             " WHERE emitter=? AND tracked=0", (now, emitter.label))
            else:
                conn.executemany("UPDATE verdicts SET recovery_at=COALESCE(recovery_at,?)"
                                 " WHERE emitter=? AND item=? AND tracked=0",
                                 [(now, emitter.label, item) for item in scope])
            queued.extend(queue_recoveries(conn, emitter))
        conn.execute("RELEASE observation")
    except BaseException:
        conn.execute("ROLLBACK TO observation")
        conn.execute("RELEASE observation")
        raise
    return queued


def queue_recoveries(conn: sqlite3.Connection, emitter: Emitter, limit: int = DELIVER_PER_TICK) -> list[str]:
    """Bounded, idempotent recovery of confirmed prior deliveries only.

    Called inside observation's transaction and on later ticks, so a delivery
    receipt arriving after recovery is not lost. Original rows are preserved.
    """
    if not getattr(emitter, "resolve_recovery", False):
        return []
    if type(limit) is not int or not 1 <= limit <= DELIVER_PER_TICK:
        raise ProtocolError("recovery batch limit outside its bounded contract")
    conn.execute("SAVEPOINT recovery_queue")
    try:
        rows = conn.execute(
            "SELECT o.event_id,o.item,o.payload,v.generation,v.recovery_at,o.delivery_scope FROM outbox o"
            " JOIN verdicts v ON v.emitter=o.emitter AND v.item=o.item"
            " LEFT JOIN recovery_outbox r ON r.emitter=o.emitter AND r.prior_event=o.event_id"
            " WHERE o.emitter=? AND o.recovery_queued=0 AND o.delivered_at IS NOT NULL"
            " AND v.recovery_at IS NOT NULL AND (v.tracked=0 OR v.verdict!=?)"
            " AND r.event_id IS NULL ORDER BY o.created,o.event_id LIMIT ?",
            (emitter.label, emitter.to_verdict, limit)).fetchall()
        queued = []
        for prior_id, item, raw, generation, recovered_at, scope in rows:
            identity = json.dumps([emitter.label, prior_id], separators=(",", ":"))
            event_id = "resolved-" + hashlib.sha256(identity.encode()).hexdigest()
            payload = {"event_id": event_id, "event": "resolved", "emitter": emitter.label,
                       "item": item, "generation": generation, "observed_at": recovered_at,
                       "resolved_at": recovered_at, "prior": json.loads(raw),
                       "delivery_scope": json.loads(scope) if scope is not None else None}
            cur = conn.execute("INSERT OR IGNORE INTO recovery_outbox"
                               " (event_id,emitter,item,generation,payload,created,prior_event) VALUES (?,?,?,?,?,?,?)",
                               (event_id, emitter.label, item, generation,
                                json.dumps(payload, sort_keys=True), recovered_at, prior_id))
            if not cur.rowcount:
                existing = conn.execute("SELECT emitter,prior_event FROM recovery_outbox WHERE event_id=?",
                                        (event_id,)).fetchone()
                if existing != (emitter.label, prior_id):
                    raise ProtocolError("recovery event identity collision")
            conn.execute("UPDATE outbox SET recovery_queued=1 WHERE event_id=? AND emitter=?",
                         (prior_id, emitter.label))
            if cur.rowcount:
                queued.append(event_id)
        conn.execute("RELEASE recovery_queue")
    except BaseException:
        conn.execute("ROLLBACK TO recovery_queue")
        conn.execute("RELEASE recovery_queue")
        raise
    return queued


class Sink(Protocol):
    """Deliver one event. Return ONLY after an observed receipt; raise otherwise."""

    def deliver(self, event: dict) -> None: ...


class JsonlSink:
    """An idempotent receiver: a JSONL file that keeps one line per event id.

    The reference receiver, and the form consumers can read before a graph sink
    exists. Deduplication is the RECEIVER's job, so it is here.
    """

    def __init__(self, path: Path | str):
        self.path = Path(path)

    def ids(self) -> set[str]:
        if not self.path.exists():
            return set()
        return {json.loads(line)["event_id"] for line in self.path.read_text().splitlines() if line}

    def delivery_scope(self) -> dict:
        return {"kind": "jsonl", "path": str(self.path.resolve())}

    def prepare_recovery(self, event: dict) -> dict:
        return {"prior_event_id": event["prior"]["event_id"],
                "delivery_scope": self.delivery_scope()}

    def resolve(self, event: dict) -> None:
        if event["receipt"].get("delivery_scope") != self.delivery_scope():
            raise ConnectionError("recovery receiver scope changed")
        self.deliver(event)

    def deliver(self, event: dict) -> None:
        if event["event_id"] in self.ids():
            return  # already received: the re-send after a lost checkpoint
        with self.path.open("a") as f:
            f.write(json.dumps(event, sort_keys=True) + "\n")
            f.flush()


class WriteBudget:
    """ONE budget shared by every emitter in the process: at most one sink
    write per `interval_s`, globally. quipu applies writes one at a time, so the
    bound that matters is the reactor's, not each emitter's. A retry spends
    budget like a first attempt, and so does a sink's one-time setup write."""

    def __init__(self, interval_s: float):
        self.interval_s = interval_s
        self._last = float("-inf")
        self._lock = threading.Lock()

    def try_take(self, now: float) -> bool:
        with self._lock:
            if now - self._last < self.interval_s:
                return False
            self._last = now
            return True


def sink_setup_allowed(conn, sink, now, emitter, resolve_recovery):
    """Use the same receiver/policy gate for setup execution and scheduling."""
    allowed = not resolve_recovery
    first = conn.execute(
        "SELECT delivery_scope FROM outbox WHERE emitter=? AND delivered_at IS NULL"
        " AND next_attempt<=? ORDER BY created LIMIT 1", (emitter, now)).fetchone()
    if first is not None:
        scope_fn = getattr(sink, "delivery_scope", None)
        scope = scope_fn() if scope_fn is not None else None
        allowed = first[0] is None or json.loads(first[0]) == scope
    return allowed


def deliver_pending(conn: sqlite3.Connection, sink: Sink, now: float, emitter: str,
                    budget: WriteBudget | None = None, limit: int = 50,
                    resolve_recovery: bool = False) -> tuple[int, int]:
    """Send THIS emitter's due outbox rows to THIS emitter's sink, spending one
    budget slot per write. Returns (delivered, failed).

    Scoped by emitter because runners share one state database: an unscoped
    pass would hand another emitter's events to this sink and checkpoint them
    there, so their own sink would never see them."""
    if conn.in_transaction:
        raise ProtocolError("delivery requires committed observation and receiver binding")
    # A sink may owe a one-time setup write (the graph sink's Reaction). It
    # takes its own budget slot, never piggy-backing on an event's.
    setup = getattr(sink, "setup_pending", None)
    setup_allowed = sink_setup_allowed(conn, sink, now, emitter, resolve_recovery)
    # Recoveries only read/close an existing firing; no setup write is needed.
    # A redirected bound firing must not set up the changed receiver either.
    if setup is not None and setup() and setup_allowed:
        if budget is not None and not budget.try_take(now):
            return 0, 0
        try:
            sink.setup()
        except Exception as exc:  # noqa: BLE001 — retried next pass, no row is charged
            LOG.warning("sink setup for %s failed: %s", emitter, exc)
            return 0, 1
    tables = ("outbox", "recovery_outbox") if resolve_recovery else ("outbox",)
    query = " UNION ALL ".join(
        f"SELECT event_id,payload,attempts,created,'{table}' AS source_table FROM {table}"
        " WHERE emitter=? AND delivered_at IS NULL AND next_attempt<=?" for table in tables)
    args = [value for _ in tables for value in (emitter, now)] + [limit]
    rows = conn.execute(query + " ORDER BY created LIMIT ?", args).fetchall()
    ok = failed = 0
    for event_id, payload, attempts, _created, table in rows:
        event = json.loads(payload)
        if event.get("resolution_of"):
            prior = conn.execute("SELECT delivered_at FROM outbox WHERE event_id=?",
                                 (event["resolution_of"],)).fetchone()
            if prior is None or prior[0] is None:
                continue
        if budget is not None and not budget.try_take(now):
            break  # the rest wait for a later slot; nothing is charged to them
        try:
            event = json.loads(payload)
            scope_fn = getattr(sink, "delivery_scope", None)
            scope = scope_fn() if scope_fn is not None else None
            if "prior" in event:
                if scope is None or event.get("delivery_scope") != scope:
                    raise ProtocolError("prior delivery receiver scope changed or unproven")
                if "receipt" in event and event["receipt"].get("delivery_scope") != scope:
                    raise ProtocolError("frozen recovery receiver scope changed or unproven")
                prepare = getattr(sink, "prepare_recovery", None)
                if prepare is None:
                    raise ProtocolError("sink does not support exact recovery")
                if "receipt" not in event:
                    event["receipt"] = {**prepare(event), "delivery_scope": scope}
                    # Freeze every sink's prior identity BEFORE the first write.
                    payload = json.dumps(event, sort_keys=True)
                    conn.execute(f"UPDATE {table} SET payload=? WHERE event_id=? AND emitter=?",
                                 (payload, event_id, emitter))
                sink.resolve(event)
            else:
                bound = conn.execute("SELECT delivery_scope FROM outbox WHERE event_id=? AND emitter=?",
                                     (event_id, emitter)).fetchone()[0]
                if bound is not None and json.loads(bound) != scope:
                    raise ProtocolError("firing receiver scope changed")
                if bound is None and scope is not None and attempts == 0:
                    # Autocommit before any external write; a lost answer/restart
                    # retries this receiver only. Legacy attempts stay unbound.
                    conn.execute("UPDATE outbox SET delivery_scope=? WHERE event_id=? AND emitter=?",
                                 (json.dumps(scope, sort_keys=True), event_id, emitter))
                sink.deliver(event)
        except Exception as exc:  # noqa: BLE001 — any failure is a retry
            failed += 1
            backoff = min(MAX_BACKOFF_S, BASE_BACKOFF_S * 2 ** attempts)
            conn.execute(f"UPDATE {table} SET attempts=attempts+1, next_attempt=?, last_error=?"
                         " WHERE event_id=? AND emitter=?",
                         (now + backoff, str(exc)[:300], event_id, emitter))
            LOG.warning("delivery of %s failed (attempt %d): %s", event_id, attempts + 1, exc)
            continue
        # Only now, after the sink returned on a receipt. A crash before this
        # line re-sends the same id next time; the receiver deduplicates.
        conn.execute(f"UPDATE {table} SET delivered_at=?, attempts=attempts+1 WHERE event_id=? AND emitter=?",
                     (now, event_id, emitter))
        ok += 1
    return ok, failed


def renew_active(conn: sqlite3.Connection, sink: Sink, now: float, definition: Emitter,
                 positive: set[str], budget: WriteBudget | None = None,
                 limit: int = DELIVER_PER_TICK) -> tuple[int, int]:
    """Refresh only received firings with fresh positive source evidence.

    Renewal sends the original payload only to a lease-aware receiver. It creates
    no transition/outbox/graph event. Missing, UNKNOWN, recovery, pending firing
    and old generations cannot authorize a refresh.
    """
    if not definition.renew_interval_s or not positive:
        return 0, 0
    renewal = getattr(sink, "renew", None)
    if not callable(renewal):
        raise ProtocolError("renewal requires a lease-aware sink")
    rows = conn.execute(
        "SELECT v.item, o.event_id, o.payload, "
        "CASE WHEN r.last_error IS NOT NULL THEN r.attempts ELSE 0 END, o.delivery_scope "
        "FROM verdicts v JOIN outbox o ON o.event_id=v.last_event "
        "LEFT JOIN renewal_receipts r ON r.event_id=o.event_id "
        "WHERE v.emitter=? AND v.tracked=1 AND v.verdict=? "
        "AND v.item IN (SELECT value FROM json_each(?)) "
        "AND o.delivered_at IS NOT NULL AND COALESCE(r.next_attempt,0)<=? "
        "AND COALESCE(r.last_received,o.delivered_at)<=? "
        "ORDER BY COALESCE(r.last_received,o.delivered_at) LIMIT ?",
        (definition.label, definition.to_verdict, json.dumps(sorted(positive)), now,
         now-definition.renew_interval_s, limit)).fetchall()
    ok = failed = 0
    for item, identity, payload, attempts, bound_scope in rows:
        if item not in positive:
            continue
        event = json.loads(payload)
        if event.get("resolution_of"):
            raise ProtocolError("active generation points to recovery")
        if budget is not None and not budget.try_take(now):
            break
        try:
            current_scope = getattr(sink, "delivery_scope", lambda: None)()
            if bound_scope is None or current_scope != json.loads(bound_scope):
                raise ProtocolError("renewal receiver scope changed or unproven")
            renewal(event)
        except Exception as exc:
            failed += 1
            backoff = min(MAX_BACKOFF_S, BASE_BACKOFF_S * 2 ** min(attempts, 20))
            conn.execute(
                "INSERT INTO renewal_receipts(event_id,attempts,next_attempt,last_error) "
                "VALUES(?,?,?,?) ON CONFLICT(event_id) DO UPDATE SET "
                "attempts=excluded.attempts,next_attempt=excluded.next_attempt,last_error=excluded.last_error",
                (identity, attempts+1, now+backoff, str(exc)[:300]))
            continue
        conn.execute(
            "INSERT INTO renewal_receipts(event_id,last_received,attempts,next_attempt) "
            "VALUES(?,?,0,0) ON CONFLICT(event_id) DO UPDATE SET "
            "last_received=excluded.last_received,attempts=0,next_attempt=0,last_error=NULL",
            (identity, now))
        ok += 1
    return ok, failed


class PersistentWriteBudget(WriteBudget):
    """A local one-shot keeps the same global sink budget across restarts."""
    def __init__(self, conn: sqlite3.Connection, interval_s: float):
        super().__init__(interval_s)
        self.conn = conn

    def try_take(self, now: float) -> bool:
        row = self.conn.execute(
            "INSERT INTO delivery_budget(scope,last_taken) VALUES('global',?) "
            "ON CONFLICT(scope) DO UPDATE SET last_taken=excluded.last_taken "
            "WHERE delivery_budget.last_taken<=excluded.last_taken-?",
            (now, self.interval_s))
        return row.rowcount == 1


class Runner:
    """Schedules one emitter's adapter runs and deliveries, and counts them.

    tick() runs on the reactor's thread and may take minutes (the adapter).
    metrics() runs on the HTTP thread and must never block on it or touch the
    SQLite connection (which belongs to the ticking thread), so tick()
    publishes a snapshot under a small lock and metrics() only reads that."""

    def __init__(self, emitter: Emitter, conn: sqlite3.Connection, sink: Sink,
                 budget: WriteBudget | None = None):
        self.emitter, self.conn, self.sink, self.budget = emitter, conn, sink, budget
        saved = (conn.execute("SELECT last_attempt,last_success,ok,unknown FROM runner_status "
                              "WHERE emitter=?", (emitter.label,)).fetchone()
                 if emitter.persist_runs else None)
        self.last_attempt = saved[0] if saved else 0.0
        self.last_success = saved[1] if saved else 0.0
        self.runs = {"ok": saved[2] if saved else 0, "unknown": saved[3] if saved else 0}
        self.queued_total = (conn.execute(
            "SELECT (SELECT COUNT(*) FROM outbox WHERE emitter=?)"
            " + (SELECT COUNT(*) FROM recovery_outbox WHERE emitter=?)",
            (emitter.label, emitter.label)).fetchone()[0] if emitter.persist_runs else 0)
        self._snap_lock = threading.Lock()
        self._snapshot: dict = {}
        self._publish()

    def delivery_due(self, now):
        """Earliest retained delivery, including global write pacing."""
        values = []
        tables = ('outbox', 'recovery_outbox') if self.emitter.resolve_recovery else ('outbox',)
        for table in tables:
            row = self.conn.execute(
                f"SELECT MIN(CASE WHEN json_extract(payload,'$.resolution_of') IS NOT NULL"
                " AND NOT EXISTS (SELECT 1 FROM outbox prior WHERE"
                f" prior.event_id=json_extract({table}.payload,'$.resolution_of')"
                " AND prior.delivered_at IS NOT NULL) THEN MAX(next_attempt,?)"
                f" ELSE next_attempt END) FROM {table} WHERE emitter=? AND delivered_at IS NULL",
                (now + 5, self.emitter.label)).fetchone()
            if row[0] is not None:
                values.append(row[0])
        if not values:
            return None
        due = min(values)
        if self.budget is not None:
            due = max(due, self.budget._last + self.budget.interval_s)
        # A retained but not deliverable dependency is pending work, not an idle
        # population. Avoid a zero-time loop without changing its durable state.
        return max(now + 0.01, due)

    def next_wakeup(self, now):
        due = (now + 5 if self.emitter.externally_triggered else
               self.last_attempt + max(1, self.emitter.interval_s))
        delivery = self.delivery_due(now)
        if delivery is not None:
            due = min(due, delivery)
        setup = self.setup_due(now)
        if setup is not None:
            due = min(due, setup)
        return max(now + 0.01, due)

    def setup_due(self, now):
        setup = getattr(self.sink, 'setup_pending', None)
        if (setup is None or not setup() or not sink_setup_allowed(
                self.conn, self.sink, now, self.emitter.label,
                self.emitter.resolve_recovery)):
            return None
        interval = max(5, self.budget.interval_s if self.budget else 5)
        return max(now + interval,
                   self.budget._last + self.budget.interval_s if self.budget else now)

    def tick(self, now: float) -> None:
        positive = set()
        if not self.emitter.externally_triggered and now - self.last_attempt >= self.emitter.interval_s:
            self.last_attempt = now
            try:
                records = run_adapter(self.emitter)
                self.queued_total += len(observe(self.conn, self.emitter, records, now))
                if isinstance(records, _ScopedRecords):
                    # Apply conclusive per-item evidence, but never present a
                    # partial inventory as a successful whole-emitter census.
                    self.runs["unknown"] += 1
                else:
                    self.runs["ok"] += 1
                    self.last_success = now
                positive = {r[self.emitter.key] for r in records
                            if r["verdict"] == self.emitter.to_verdict}
            except Exception as exc:  # noqa: BLE001 — a failed run is UNKNOWN for every item
                self.runs["unknown"] += 1
                LOG.warning("emitter %s UNKNOWN (no verdict changed): %s", self.emitter.label, exc)
        self.queued_total += len(queue_recoveries(self.conn, self.emitter))
        deliver_pending(self.conn, self.sink, now, self.emitter.label, self.budget, limit=DELIVER_PER_TICK,
                        resolve_recovery=self.emitter.resolve_recovery)
        renew_active(self.conn, self.sink, now, self.emitter, positive, self.budget)
        if self.emitter.persist_runs:
            self.conn.execute(
                "INSERT INTO runner_status VALUES(?,?,?,?,?) ON CONFLICT(emitter) DO UPDATE SET "
                "last_attempt=excluded.last_attempt,last_success=excluded.last_success,"
                "ok=excluded.ok,unknown=excluded.unknown",
                (self.emitter.label,self.last_attempt,self.last_success,
                 self.runs["ok"],self.runs["unknown"]))
        self._publish()

    def _snapshot_values(self) -> dict:
        pending, oldest = self.conn.execute(
            "SELECT COUNT(*), MIN(created) FROM ("
            " SELECT created FROM outbox WHERE emitter=? AND delivered_at IS NULL"
            " UNION ALL SELECT created FROM recovery_outbox WHERE emitter=? AND delivered_at IS NULL)",
            (self.emitter.label, self.emitter.label)).fetchone()
        delivered = self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM outbox WHERE emitter=? AND delivered_at IS NOT NULL)"
            " + (SELECT COUNT(*) FROM recovery_outbox WHERE emitter=? AND delivered_at IS NOT NULL)",
            (self.emitter.label, self.emitter.label)).fetchone()[0]
        renewal_pending, renewal_failures = self.conn.execute(
            "SELECT COUNT(r.last_received),COALESCE(SUM(CASE WHEN r.last_error IS NOT NULL THEN 1 ELSE 0 END),0) "
            "FROM renewal_receipts r JOIN outbox o ON o.event_id=r.event_id "
            "JOIN verdicts v ON v.last_event=o.event_id "
            "WHERE v.emitter=? AND v.tracked=1 AND v.verdict=?",
            (self.emitter.label,self.emitter.to_verdict)).fetchone()
        return {"renewal_receipts": renewal_pending, "renewal_failures": renewal_failures,
                "runs": dict(self.runs), "last_success": self.last_success,
                "queued_total": self.queued_total, "pending": pending,
                "oldest_created": oldest, "delivered": delivered}

    def _publish(self) -> None:
        # Subclasses compose their fields before any reader can see the snapshot.
        snap = self._snapshot_values()
        with self._snap_lock:
            self._snapshot = snap

    def metrics(self, esc) -> list[str]:
        with self._snap_lock:
            s = dict(self._snapshot)
        label = esc(self.emitter.label)
        lines = [f'chaski_emitter_runs_total{{emitter="{label}",result="{r}"}} {n}' for r, n in s["runs"].items()]
        oldest = s["oldest_created"]
        lines += [
            f'chaski_emitter_last_success_timestamp_seconds{{emitter="{label}"}} {s["last_success"]:.0f}',
            f'chaski_emitter_events_queued_total{{emitter="{label}"}} {s["queued_total"]}',
            f'chaski_emitter_renewal_failures{{emitter="{label}"}} {s["renewal_failures"]}',
            f'chaski_emitter_renewal_receipts{{emitter="{label}"}} {s["renewal_receipts"]}',
            f'chaski_emitter_outbox_pending{{emitter="{label}"}} {s["pending"]}',
            f'chaski_emitter_outbox_delivered_total{{emitter="{label}"}} {s["delivered"]}',
            f'chaski_emitter_outbox_oldest_pending_age_seconds{{emitter="{label}"}} '
            f'{(time.time() - oldest) if oldest else 0:.0f}',
        ]
        return lines
