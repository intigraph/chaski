"""Real incremental consumer/reader process with a temporary catalogue fixture."""
import importlib.util
import json
import logging
from pathlib import Path
import sys
import tempfile

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from scheduled_projection import ScheduledProjection  # noqa: E402

print('Projection positive control:', callable(ScheduledProjection.evaluate))
present = importlib.util.find_spec('scheduled_catalogue_adapter') is not None
print('Incremental catalogue adapter available:', present)
if not present:
    sys.exit(0)
from emitter import Emitter, connect  # noqa: E402
from incremental import ChangeRunner  # noqa: E402
from test_scheduled_catalogue_adapter import count, setup  # noqa: E402

logging.getLogger('chaski.emitter').setLevel(logging.ERROR)
logging.getLogger('chaski.changes').setLevel(logging.ERROR)
with tempfile.TemporaryDirectory() as root:
    p, _, _, report, counter, command, _ = setup.__wrapped__(Path(root))
    conn = connect(Path(root)/'outbox.sqlite')
    class PendingSink:
        def delivery_scope(self):
            return {'fixture': 'pending'}
        def deliver(self, *_):
            raise ConnectionError('synthetic pending receipt')
    definition = Emitter('fixture', command(), 3600, p.owner, event='scheduled',
                         baseline_emits=True, change_driven=True)
    runner = ChangeRunner(definition, conn, PendingSink())
    try:
        print('Describe reader calls:', count(counter))
        runner.tick(1801)
        print('Bootstrap discover/evaluate reader calls:', count(counter))
        runner.tick(1802)
        runner.tick(2399)
        print('After idle ticks, reader calls:', count(counter))
        runner.tick(2400)
        print('After next due slot, reader calls:', count(counter))
        report.write_text(json.dumps({'schema_version': 1, 'jobs': []}))
        runner.tick(3000)
        print('Missing identity UNKNOWN count:', runner.runs['unknown'])
        print('Retained next deadline:', conn.execute('SELECT due FROM adapter_deadlines').fetchone()[0])
        print('Retained queued slots:', conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0])
        print('Reader is a fixture; no graph, production job, key, profile or cron used.')
    finally:
        conn.close()
