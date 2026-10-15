"""Actual local processes and outbox; Shuttle admission/signing is a fixture."""
import importlib.util
import json
import logging
from pathlib import Path
import sys
import tempfile

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from chaski import make_sink  # noqa: E402

print('Existing sink constructor positive control:', callable(make_sink))
present = importlib.util.find_spec('scheduled_sink_factory') is not None
print('Configured scheduled sink factory available:', present)
if not present:
    sys.exit(0)
from emitter import connect, deliver_pending, observe  # noqa: E402
from scheduled_receiver import run_id  # noqa: E402
from test_scheduled_sink_factory import configured  # noqa: E402

# The acknowledgement counts below show the expected asynchronous pending
# state; omit its long canonical event identifier from this short recording.
logging.getLogger('chaski.emitter').setLevel(logging.ERROR)
with tempfile.TemporaryDirectory() as root:
    load, _, _, scope, trace, counter, _, _ = configured.__wrapped__(Path(root))
    definition, options = load()
    sink = make_sink(definition, options, 'http://unused.invalid')
    conn = connect(Path(root)/'outbox.sqlite')
    try:
        observe(conn, definition, [dict(item=scope.job, owner=definition.owner,
                verdict='UNBLOCKED', event_id=run_id(scope, 600))], 600)
        print('Submit delivery, acknowledged/pending:',
              deliver_pending(conn, sink, 600, definition.label))
        with sink.receiver._lock:
            future = next(iter(sink.receiver._pending.values()))
        future.result(timeout=5)
        print('After terminal publication, acknowledged/pending:',
              deliver_pending(conn, sink, 1600, definition.label))
        phases = [json.loads(x) for x in trace.read_text().splitlines()]
        print('Phase receipts:', [p['action'] for p in phases])
        print('Explicit child outcome:', phases[2]['packet']['outcome'])
        print('Actual child invocations:', counter.read_text())
        print('No production graph, schedule, key or cron was used; signing is a fixture.')
    finally:
        sink.close()
        conn.close()
