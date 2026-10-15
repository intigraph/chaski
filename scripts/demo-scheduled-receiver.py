"""Synthetic port double: demonstrate sequencing and publication-only replay."""
from dataclasses import asdict
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scheduled_receiver import ExecutionScope, receive  # noqa: E402
from slot_receiver import SlotLedger  # noqa: E402


class Port:
    def admit(self, scope, slot):
        print('fixture admission control passed')

    def started(self, run, scope):
        print('fixture signed-start persistence callback')

    def finished(self, run, scope, outcome):
        print('fixture signed-terminal persistence callback:', outcome['outcome'])

    def publish(self, run, scope, outcome):
        print('fixture export/read-back callback')


with tempfile.TemporaryDirectory() as directory:
    ledger = SlotLedger(Path(directory) / 'slots.db')
    scope = ExecutionScope('urn:example:job', 'revision-1', 'host-a', 'a'*64,
                           'fixture-agent', 'urn:example:workflow')
    delivery = {**asdict(scope), 'scheduled_at': 120}
    executions = []

    def execute():
        executions.append(1)
        print('SYNTHETIC CHILD CALLBACK')
        return {'outcome': 'skipped-by-hold', 'exit_code': 0}

    print('Library demonstration; no real key, signature, process or graph write.')
    print('First delivery:')
    first = receive(ledger, scope, 120, delivery, Port(), execute)
    print('Identical redelivery:')
    second = receive(ledger, scope, 120, delivery, Port(), execute)
    print(json.dumps({'child_callbacks': len(executions), 'same_result': first == second,
                      'outcome': second['outcome']}))
    assert len(executions) == 1 and first == second
