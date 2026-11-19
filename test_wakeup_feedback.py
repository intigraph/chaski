"""Actual consumer-loop controls for retained obligations; synthetic I/O only."""

import json
from pathlib import Path
import subprocess
import sys

import emitter as em

ROOT = Path(__file__).parent


def probe(name):
    output = subprocess.check_output(
        [sys.executable, str(ROOT / "scripts" / f"probe-{name}-wakeups.py"), str(ROOT)], text=True
    )
    return json.loads(output.splitlines()[-1])


def test_disabled_recovery_consumer_retains_debt_without_busy_loop():
    result = probe("recovery")
    assert result["turns"] == 1
    assert result["pending_recovery"] == 1
    assert result["actual_external_requests"] == 0
    assert result["min_wait_s"] >= 5


def test_incremental_consumer_retries_setup_despite_daily_reconcile():
    # aegis-ssvr: a refused setup write still retries without waiting for the
    # daily reconcile, but backs off (30 s, then 60 s) instead of every 5 s pass.
    result = probe("setup")
    assert result["setup_attempts"] == 2
    assert result["turns"] == 2
    assert result["next_wait"] == 60
    assert result["retained_reconcile"] == 100
    assert result["outbox_count"] == 0


def test_blocked_dependency_is_bounded_and_does_not_consume_budget():
    conn = em.connect(":memory:")
    e = em.Emitter("control", [], 3600, "fixture")
    em.observe(conn, e, [{"item": "a", "verdict": "BLOCKED"}], 100)
    em.observe(conn, e, [{"item": "a", "verdict": "UNBLOCKED", "event_id": "fixture-event"}], 101)
    conn.execute("UPDATE outbox SET payload=json_set(payload,'$.resolution_of','missing')")

    class Sink:
        def deliver(self, event):
            raise AssertionError("dependency unresolved")

    budget = em.WriteBudget(5)
    budget._last = 90
    runner = em.Runner(e, conn, Sink(), budget)
    runner.last_attempt = 100
    assert runner.delivery_due(102) == 107
    runner.tick(102)
    assert budget._last == 90
    assert conn.execute("SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL").fetchone()[0] == 1
    conn.close()


def test_setup_wakeup_respects_recovery_policy_and_bound_receiver():
    conn = em.connect(':memory:')

    class Sink:
        def setup_pending(self):
            return True

        def delivery_scope(self):
            return {'receiver': 'new'}

    e = em.Emitter('control', [], 86400, 'fixture', resolve_recovery=True)
    budget = em.WriteBudget(5)
    runner = em.Runner(e, conn, Sink(), budget)
    assert runner.setup_due(100) is None
    e.resolve_recovery = False
    budget._last = 110
    assert runner.setup_due(100) == 115
    em.observe(conn, e, [{'item': 'a', 'verdict': 'BLOCKED'}], 100)
    em.observe(conn, e, [{'item': 'a', 'verdict': 'UNBLOCKED', 'event_id': 'fixture-event'}], 101)
    conn.execute('UPDATE outbox SET delivery_scope=?', ('{"receiver":"original"}',))
    assert runner.setup_due(102) is None
    conn.close()


def test_setup_backoff_doubles_caps_and_resets_on_success():
    em._SETUP_FAILURES.clear()
    delays = [em._setup_failed("x", 1000.0) for _ in range(9)]
    assert delays[:4] == [30, 60, 120, 240]
    assert delays[-1] == em.SETUP_BACKOFF_MAX_S == 3600
    assert em.setup_retry_at("x") == 1000.0 + 3600

    class Sink:
        attempts = 0
        def setup_pending(self):
            return True
        def setup(self):
            Sink.attempts += 1
    conn = em.connect(":memory:")
    em._setup_failed("y", 100.0)                     # retry_at = 130
    assert em.deliver_pending(conn, Sink(), 120.0, "y") == (0, 0)
    assert Sink.attempts == 0                        # still backing off
    em.deliver_pending(conn, Sink(), 131.0, "y")
    assert Sink.attempts == 1 and "y" not in em._SETUP_FAILURES   # success resets
