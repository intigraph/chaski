"""Pure pinned projection and actual emitter outbox; no network or execution."""
import importlib.util
from pathlib import Path
import sys
import tempfile

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from emitter import Emitter, connect, observe  # noqa: E402
from scheduled_receiver import ExecutionScope, run_id  # noqa: E402

scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                       'verifier', 'urn:example:workflow')
print('Receiver identity positive control:', run_id(scope, 600).startswith('scheduled-v1-'))
present = importlib.util.find_spec('scheduled_projection') is not None
print('Pinned schedule projection available:', present)
if not present:
    sys.exit(0)
from scheduled_projection import ScheduledProjection  # noqa: E402

p = ScheduledProjection(scope, 'urn:example:catalogue', 'urn:example:manifest',
                        'operator', 600, 600, 'UTC', 1250)
catalogue = {'schema_version': 1, 'source': 'declarations_only', 'graph': p.graph,
             'executable_admission': 'UNKNOWN', 'signed_run_status': 'UNKNOWN',
             'firing_eligibility': 'UNKNOWN',
             'membership': {'status': 'required_identities_verified', 'required': [scope.job]},
             'jobs': [{'iri': scope.job, 'name': 'fixture', 'revision': scope.revision,
                       'owner': p.owner, 'enabled': True, 'host': scope.host, 'target': p.target,
                       'event': 'urn:example:event', 'schedule': 'urn:example:schedule',
                       'interval_s': 600, 'anchor_s': 600, 'timezone': 'UTC'}]}
print('Effective 1250; first eligible aligned slot:', p.evaluate(catalogue, 1250)['next_checks'][scope.job])
with tempfile.TemporaryDirectory() as root:
    conn = connect(Path(root)/'outbox.sqlite')
    e = Emitter('fixture-schedule', ['unused'], 60, p.owner,
                event='scheduled', baseline_emits=True)
    try:
        for now in (1801, 1802, 2401):
            out = p.evaluate(catalogue, now)
            observe(conn, e, out['records'], now, scope=set(out['scope']))
        print('Three observations; two current-slot events:', conn.execute(
            'SELECT COUNT(*) FROM outbox').fetchone()[0])
        print('Next deadline:', out['next_checks'][scope.job])
        catalogue['jobs'] = []
        try:
            p.evaluate(catalogue, 3001)
        except ValueError:
            print('Missing required identity: REFUSED; retained outbox rows:', conn.execute(
                'SELECT COUNT(*) FROM outbox').fetchone()[0])
        print('Execution callbacks: 0; no sink or production schedule enabled.')
    finally:
        conn.close()
