"""Real subprocess bridge and slot ledger, fixture CLI receipts, no real keys/graph."""
import hashlib
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scheduled_receiver import ExecutionScope, receive
from shuttle_process_port import PortRefused, ShuttleProcessPort
from slot_receiver import SlotLedger

with tempfile.TemporaryDirectory() as root:
    root = Path(root)
    config, cli = root/'config.json', root/'fixture-cli'
    config.write_text('{}')
    config.chmod(0o600)
    cli.write_text('#!'+sys.executable+'\n'+'''import json,sys
from pathlib import Path
assert sys.argv[1:3] == ['--require-existing-keys', 'scheduled']
assert Path(sys.argv[5]).read_text() == '{}'
json.load(sys.stdin)
print(json.dumps({'action': sys.argv[3], 'result': 'verified'}))
''')
    cli.chmod(0o700)
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    port = ShuttleProcessPort(cli, config, hashlib.sha256(config.read_bytes()).hexdigest(), scope, 600)
    calls = []
    def execute():
        calls.append(1)
        return {'outcome': 'skipped-by-hold', 'exit_code': 0}
    packet = {**scope.__dict__, 'scheduled_at': 600}
    ledger = SlotLedger(root/'slots.sqlite')
    first = receive(ledger, scope, 600, packet, port, execute)
    second = receive(ledger, scope, 600, packet, port, execute)
    print('Actual subprocess and durable slot ledger; CLI receipts are a fixture.')
    print('Held result:', first['outcome'])
    print('Delivery attempts: 2; execution callbacks:', len(calls))
    print('Repeat result identical:', first == second)
    config.write_text('{"changed": true}')
    try:
        port.admit(scope, 600)
    except PortRefused:
        print('Changed pinned config: REFUSED before CLI invocation')
    else:
        raise AssertionError('changed config accepted')
