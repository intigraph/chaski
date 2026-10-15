"""Real threads and ledger; synthetic signing callbacks and no external I/O."""
from dataclasses import asdict
import importlib.util
from pathlib import Path
import sys
import tempfile
import threading

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from scheduled_receiver import ExecutionScope, receive  # noqa: E402
from slot_receiver import SlotLedger  # noqa: E402

print('Source demo: real worker thread and SQLite; signing callbacks are fixtures.')
print('Existing receiver positive control:', callable(receive))
present = importlib.util.find_spec('async_scheduled_sink') is not None
print('Async handoff available:', present)
if not present:
    sys.exit(0)
from async_scheduled_sink import AsyncScheduledSink, DeliveryPending  # noqa: E402

with tempfile.TemporaryDirectory() as root:
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
                           'operator', 'urn:example:workflow')
    delivery = {**asdict(scope), 'scheduled_at': 600}
    sink = AsyncScheduledSink(SlotLedger(Path(root)/'slots.sqlite'), scope,
                              lambda slot: Port(), execute, capacity=1)
    try:
        for label in ('First delivery', 'Duplicate delivery'):
            try:
                sink.deliver(delivery)
            except DeliveryPending:
                print(label + ': PENDING, no receipt')
            else:
                raise AssertionError('acknowledged blocked child')
            assert entered.wait(5)
        print('Worker blocked while caller continued:', not release.is_set())
        print('Execution callbacks:', len(calls), '; publications:', len(publications))
        release.set()
        # Demo waits for completion explicitly; deliver never waits on workers.
        with sink._lock:
            future = next(iter(sink._pending.values()))
        future.result(timeout=5)
        sink.deliver(delivery)
        print('Later delivery: RECEIPT after terminal publication')
        print('Published actual outcome:', publications[0]['outcome'])
        print('Final execution callbacks:', len(calls))
    finally:
        release.set()
        sink.close()
