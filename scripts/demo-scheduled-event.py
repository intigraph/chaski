"""Actual emitter/outbox, thread and ledger; synthetic signing, no network."""
import importlib.util
import json
import logging
from pathlib import Path
import sys
import tempfile
import threading

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from async_scheduled_sink import AsyncScheduledSink  # noqa: E402
from emitter import Emitter, connect, deliver_pending, observe  # noqa: E402
from scheduled_receiver import ExecutionScope, run_id  # noqa: E402
from slot_receiver import SlotLedger  # noqa: E402

# Pending delivery is expected here. Show the measured outbox/receipt state
# below instead of logging the entire reversible fixture identifier twice.
logging.getLogger('chaski.emitter').setLevel(logging.ERROR)

print('Source demo: actual emitter/outbox, worker and ledger; signing is a fixture.')
print('Existing async receiver positive control:', callable(AsyncScheduledSink.deliver))
present = importlib.util.find_spec('scheduled_event_sink') is not None
print('Native scheduled envelope adapter available:', present)
if not present:
    sys.exit(0)
from scheduled_event_sink import ScheduledEventSink  # noqa: E402

with tempfile.TemporaryDirectory() as root:
    root = Path(root)
    entered, release = threading.Event(), threading.Event()
    calls, publications = [], []
    class Port:
        def admit(self, scope, slot): pass
        def started(self, run, scope): pass
        def finished(self, run, scope, outcome): pass
        def publish(self, run, scope, outcome): publications.append(outcome)
    def execute(slot):
        calls.append(slot)
        entered.set()
        assert release.wait(5)
        return {'outcome': 'skipped-by-hold', 'exit_code': 0}
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    definition = Emitter('schedule', ['unused'], 60, 'operator',
                          event='scheduled', baseline_emits=True)
    conn = connect(root/'emitter.sqlite')
    inner = AsyncScheduledSink(SlotLedger(root/'slots.sqlite'), scope,
                               lambda slot: Port(), execute)
    sink = ScheduledEventSink(inner, 'schedule', 'operator')
    try:
        queued = observe(conn, definition, [{'item': scope.job, 'verdict': 'UNBLOCKED',
                         'event_id': run_id(scope, 600), 'owner': 'operator'}], 600)
        print('Observed due declaration; durable outbox rows:', len(queued))
        assert deliver_pending(conn, sink, 600, 'schedule') == (0, 1)
        assert entered.wait(5)
        assert deliver_pending(conn, sink, 630, 'schedule') == (0, 1)
        receipt = conn.execute('SELECT delivered_at FROM outbox').fetchone()[0]
        print('Two delivery attempts; child still blocked:', not release.is_set())
        print('Execution callbacks:', len(calls), '; published:', len(publications), '; receipt:', receipt)
        release.set()
        with inner._lock:
            future = next(iter(inner._pending.values()))
        future.result(timeout=5)
        assert deliver_pending(conn, sink, 690, 'schedule') == (1, 0)
        receipt = conn.execute('SELECT delivered_at FROM outbox').fetchone()[0]
        print('Published actual outcome:', publications[0]['outcome'])
        print('Terminal receipt:', receipt, '; execution callbacks:', len(calls))
        frozen = json.loads(conn.execute('SELECT delivery_scope FROM outbox').fetchone()[0])
        print('Outbox pinned emitter and recipient:', frozen['emitter'], frozen['recipient'])
    finally:
        release.set()
        sink.close()
        conn.close()
