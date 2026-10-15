"""Local temporary child processes only; no signing, network or production job."""
import importlib.util
import json
from pathlib import Path
import sys
import tempfile

source = (Path(sys.argv[2]).resolve() if len(sys.argv) == 3 and sys.argv[1] == '--source'
          else Path(__file__).resolve().parents[1])
sys.path.insert(0, str(source))
from scheduled_receiver import ExecutionScope, run_id  # noqa: E402

scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                       'verifier', 'urn:example:workflow')
print('Receiver identity positive control:', run_id(scope, 600).startswith('scheduled-v1-'))
present = importlib.util.find_spec('fixed_outcome_executor') is not None
print('Fixed outcome executor available:', present)
if not present:
    sys.exit(0)
from fixed_outcome_executor import ExecutionUnknown, FixedOutcomeExecutor  # noqa: E402

with tempfile.TemporaryDirectory() as root:
    for outcome, code in [('succeeded', 0), ('skipped-by-hold', 0), ('failed', 7), (None, 0)]:
        packet = json.dumps(dict(schema_version=1, outcome=outcome, exit_code=code)).encode()
        program = ('import os,sys; '
                   f'os.write(int(os.environ["RESULT_FD"]), {packet!r}); sys.exit({code})')
        if outcome is None:
            program = 'pass'
        call = FixedOutcomeExecutor([sys.executable, '-c', program], cwd=root, env={},
                                    result_fd_env='RESULT_FD', timeout_s=2)
        try:
            print('Child:', call(600))
        except ExecutionUnknown:
            print('Exit-zero child without result: UNKNOWN; preserve claim, no retry.')
print('No production schedule, signing key, graph or cron was used.')
