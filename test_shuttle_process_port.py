"""Real process/descriptor controls; subprocess is a fixture CLI, not Shuttle crypto."""
from dataclasses import replace
import hashlib
import json
import sys

import pytest

from scheduled_receiver import ExecutionScope, receive, run_id
from shuttle_process_port import PortRefused, ShuttleProcessPort
from slot_receiver import SlotLedger


@pytest.fixture
def setup(tmp_path):
    trace = tmp_path / 'trace.jsonl'
    config = tmp_path / 'config.json'
    config.write_text(json.dumps({'trace': str(trace), 'mode': 'ok'}))
    config.chmod(0o600)
    executable = tmp_path / 'shuttle-fixture'
    executable.write_text('#!' + sys.executable + '\n' + '''import json,sys,time
from pathlib import Path
assert sys.argv[1:3] == ['--require-existing-keys', 'scheduled']
action = sys.argv[3]
assert sys.argv[4] == '--config'
config = json.loads(Path(sys.argv[5]).read_text())
packet = json.load(sys.stdin)
with open(config['trace'], 'a') as out:
    out.write(json.dumps({'action': action, 'packet': packet})+'\\n')
mode = config['mode']
if mode == 'timeout': time.sleep(10)
if mode == 'fail': sys.exit(3)
if mode == 'large': print('x'*4097)
elif mode == 'duplicate': print('{"action":"bad","action":"'+action+'","result":"verified"}')
elif mode == 'wrong': print(json.dumps({'action': 'wrong', 'result': 'verified'}))
elif mode == 'empty': pass
else: print(json.dumps({'action': action, 'result': 'verified'}))
''')
    executable.chmod(0o700)
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    def port(mode='ok', timeout=5):
        config.write_text(json.dumps({'trace': str(trace), 'mode': mode}))
        return ShuttleProcessPort(executable, config, hashlib.sha256(config.read_bytes()).hexdigest(),
                                  scope, 600, timeout_s=timeout)
    return port, scope, trace, config, executable


def test_real_process_calls_all_phases_and_duplicate_only_publishes(setup, tmp_path):
    factory, scope, trace, _, _ = setup
    port = factory()
    delivery = {**scope.__dict__, 'scheduled_at': 600}
    calls = []
    def execute():
        calls.append(1)
        return {'outcome': 'skipped-by-hold', 'exit_code': 0}
    ledger = SlotLedger(tmp_path/'ledger.sqlite')
    result = receive(ledger, scope, 600, delivery, port, execute)
    assert receive(ledger, scope, 600, delivery, port, execute) == result
    assert calls == [1]
    events = [json.loads(x) for x in trace.read_text().splitlines()]
    assert [e['action'] for e in events] == ['admit', 'started', 'finished', 'publish', 'admit', 'publish']
    assert events[2]['packet']['outcome'] == {'outcome': 'skipped-by-hold', 'exit_code': 0}
    assert events[2]['packet']['run'] == run_id(scope, 600)


@pytest.mark.parametrize('mode', ['fail', 'large', 'duplicate', 'wrong', 'empty', 'timeout'])
def test_failed_or_unverified_admission_never_executes_child(setup, tmp_path, mode):
    factory, scope, _, _, _ = setup
    port = factory(mode, timeout=.1 if mode == 'timeout' else 5)
    with pytest.raises(PortRefused):
        receive(SlotLedger(tmp_path/'ledger.sqlite'), scope, 600,
                {**scope.__dict__, 'scheduled_at': 600}, port,
                lambda: pytest.fail('unverified admission executed child'))


def test_pinned_config_drift_refuses_before_process(setup):
    factory, scope, trace, config, _ = setup
    port = factory()
    config.write_text('{}')
    with pytest.raises(PortRefused, match='pinned bytes'):
        port.admit(scope, 600)
    assert not trace.exists()


@pytest.mark.parametrize('change', ['symlink', 'writable', 'fifo'])
def test_config_custody_refuses_without_blocking_or_launch(setup, change, tmp_path):
    factory, scope, trace, config, _ = setup
    port = factory()
    if change == 'writable':
        config.chmod(0o666)
    else:
        config.unlink()
        if change == 'symlink':
            config.symlink_to(tmp_path/'elsewhere')
        else:
            import os
            os.mkfifo(config)
    with pytest.raises(PortRefused):
        port.admit(scope, 600)
    assert not trace.exists()


def test_scope_slot_and_run_mismatch_refuse_before_process(setup):
    factory, scope, trace, _, _ = setup
    port = factory()
    for callback in [lambda: port.admit(scope, True), lambda: port.admit(scope, 601),
                     lambda: port.admit(replace(scope, host='urn:other'), 600),
                     lambda: port.started('other', scope)]:
        with pytest.raises(PortRefused):
            callback()
    assert not trace.exists()


def test_replaced_config_path_does_not_replace_opened_pinned_config(setup, monkeypatch):
    import subprocess
    factory, scope, trace, config, _ = setup
    port = factory()
    run = subprocess.run
    def replace_path(*args, **kwargs):
        config.rename(config.with_suffix('.original'))
        config.write_text('{}')
        return run(*args, **kwargs)
    monkeypatch.setattr(subprocess, 'run', replace_path)
    port.admit(scope, 600)
    assert json.loads(trace.read_text())['action'] == 'admit'
