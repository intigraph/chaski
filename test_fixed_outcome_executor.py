from itertools import permutations
import json
import sys
import time

import pytest

from fixed_outcome_executor import ExecutionUnknown, FixedOutcomeExecutor, decode_result


def packet(outcome='succeeded', code=0):
    return dict(schema_version=1, outcome=outcome, exit_code=code)


def executor(tmp_path, code, **options):
    return FixedOutcomeExecutor([sys.executable, '-c', code], cwd=tmp_path, env={},
                                result_fd_env='RESULT_FD', timeout_s=2, **options)


def program(value=None, *, raw=None, code=0, prefix=''):
    if raw is None:
        raw = json.dumps(packet() if value is None else value).encode()
    return ('import os,sys; '+prefix+
            f'os.write(int(os.environ["RESULT_FD"]), {raw!r}); sys.exit({code})')


def test_result_invariant_exhausts_normal_exit_domain_and_field_order():
    for code in range(256):
        for outcome in ('succeeded', 'failed', 'skipped-by-hold'):
            value = packet(outcome, code)
            for order in permutations(value):
                raw = json.dumps({k: value[k] for k in order}).encode()
                if (outcome == 'failed') == (code != 0):
                    assert decode_result(raw, code) == {'outcome': outcome, 'exit_code': code}
                else:
                    with pytest.raises(ExecutionUnknown):
                        decode_result(raw, code)


@pytest.mark.parametrize('raw,code', [
    (b'', 0), (b'{}', 0), (b'[]', 0), (b'null', 0), (b'\xff', 0),
    (b'{"schema_version":1,"schema_version":1,"outcome":"succeeded","exit_code":0}', 0),
    (json.dumps(packet()).encode()+b'{}', 0),
    (json.dumps(dict(packet(), schema_version=True)).encode(), 0),
    (json.dumps(dict(packet(), exit_code=False)).encode(), 0),
    (json.dumps(dict(packet(), exit_code=0.0)).encode(), 0),
    (json.dumps(dict(packet(), reason=None)).encode(), 0),
    (json.dumps(dict(packet(), reason='')).encode(), 0),
    (json.dumps(dict(packet(), reason='x'*257)).encode(), 0),
    (json.dumps(dict(packet(), extra=True)).encode(), 0),
    (b'{"schema_version":1,"outcome":"succeeded","exit_code":NaN}', 0),
    (json.dumps(packet()).encode(), 1), (json.dumps(packet()).encode(), -9),
    (json.dumps(packet()).encode(), True), (b' '*4097, 0), (b'['*1500+b']'*1500, 0),
])
def test_unavailable_or_inconsistent_result_is_unknown(raw, code):
    with pytest.raises(ExecutionUnknown):
        decode_result(raw, code)


@pytest.mark.parametrize('outcome,code', [('succeeded', 0), ('failed', 7), ('skipped-by-hold', 0)])
def test_actual_process_exit_and_hold_are_distinct(tmp_path, outcome, code):
    value = dict(packet(outcome, code), reason='fixture')
    assert executor(tmp_path, program(value, code=code))(600) == {'outcome': outcome, 'exit_code': code}


def test_fixed_argv_and_explicit_environment_are_not_delivery_interpolation(tmp_path, monkeypatch):
    monkeypatch.setenv('EXECUTOR_FIXTURE_SECRET', 'synthetic-not-a-credential')
    literal = ['${HOME}', '$$', 'a b', "q'uote"]
    code = ('import os,sys,json; assert "EXECUTOR_FIXTURE_SECRET" not in os.environ; '
            f'assert sys.argv[1:] == {literal!r}; '
            'os.write(int(os.environ["RESULT_FD"]),json.dumps('
            f'{packet()!r}).encode())')
    argv, env = [sys.executable, '-c', code, *literal], {}
    call = FixedOutcomeExecutor(argv, cwd=tmp_path, env=env,
                                result_fd_env='RESULT_FD', timeout_s=2)
    argv[:] = ['not-the-command']
    env['EXECUTOR_FIXTURE_SECRET'] = 'also-synthetic'
    assert call(600) == {'outcome': 'succeeded', 'exit_code': 0}


@pytest.mark.parametrize('code', [
    'pass',  # exit zero without the explicit result is not success
    program(packet('failed', 1), code=0),
    program(raw=b'not-json'),
    program(raw=b'x'*4097),
    program(prefix='os.write(1,b"x"*4096); '),
    program(prefix='os.write(2,b"x"*4096); '),
])
def test_real_process_missing_mismatched_and_bounded_channels(tmp_path, code):
    with pytest.raises(ExecutionUnknown):
        executor(tmp_path, code, output_limit=1024)(600)


def test_timeout_stops_ordinary_descendants_before_their_side_effect(tmp_path):
    marker = tmp_path/'late-side-effect'
    code = ('import os,time; pid=os.fork(); '
            f'(time.sleep(0.7),open({str(marker)!r},"w").write("late")) if pid==0 else time.sleep(5)')
    call = FixedOutcomeExecutor([sys.executable, '-c', code], cwd=tmp_path, env={},
                                result_fd_env='RESULT_FD', timeout_s=0.15)
    started = time.monotonic()
    with pytest.raises(ExecutionUnknown):
        call(600)
    assert time.monotonic()-started < 3
    time.sleep(0.8)
    assert not marker.exists()


def test_valid_result_cannot_acknowledge_a_still_running_parent(tmp_path):
    code = program(prefix='import time; ', code=0).replace('; sys.exit(0)', '; time.sleep(5)')
    call = FixedOutcomeExecutor([sys.executable, '-c', code], cwd=tmp_path, env={},
                                result_fd_env='RESULT_FD', timeout_s=0.15)
    with pytest.raises(ExecutionUnknown):
        call(600)


@pytest.mark.parametrize('valid', [True, False])
def test_durable_receiver_never_repeats_actual_child_after_publication_or_channel_failure(tmp_path, valid):
    from scheduled_receiver import ExecutionScope, receive
    from slot_receiver import SlotLedger, SlotUnknown
    from dataclasses import asdict

    counter = tmp_path/'child-calls'
    prefix = (f'from pathlib import Path; p=Path({str(counter)!r}); '
              'p.write_text(str(int(p.read_text())+1) if p.exists() else "1"); ')
    raw = json.dumps(packet('skipped-by-hold')).encode() if valid else b'not-json'
    call = executor(tmp_path, program(raw=raw, prefix=prefix))
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    ledger = SlotLedger(tmp_path/'slots.sqlite')
    class Port:
        publications = 0
        def admit(self, *_): pass
        def started(self, *_): pass
        def finished(self, *_): pass
        def publish(self, *_):
            self.publications += 1
            if self.publications == 1:
                raise ConnectionError('synthetic lost publication')
    port = Port()
    binding = {**asdict(scope), 'scheduled_at': 600}
    with pytest.raises(ConnectionError if valid else ExecutionUnknown):
        receive(ledger, scope, 600, binding, port, lambda: call(600))
    if valid:
        assert receive(ledger, scope, 600, binding, port, lambda: call(600))['outcome'] == 'skipped-by-hold'
        assert port.publications == 2
    else:
        with pytest.raises(SlotUnknown):
            receive(ledger, scope, 600, binding, port, lambda: call(600))
    assert counter.read_text() == '1'


@pytest.mark.parametrize('key,value', [
    ('argv', 'not-an-argv'), ('argv', ['relative']), ('argv', [sys.executable, 'a\0b']),
    ('env', {'RESULT_FD': '3'}), ('env', {'BAD-KEY': 'x'}), ('env', {'X': 'a\0b'}),
    ('result_fd_env', 'PATH'), ('result_fd_env', 'lower_fd'),
    ('timeout_s', 0), ('timeout_s', True), ('timeout_s', float('inf')),
    ('output_limit', True), ('output_limit', 0), ('output_limit', 8388609),
])
def test_invalid_config_refuses_before_any_child(tmp_path, key, value):
    args = dict(argv=[sys.executable, '-c', 'pass'], cwd=tmp_path, env={},
                result_fd_env='RESULT_FD', timeout_s=1)
    args[key] = value
    with pytest.raises(ValueError):
        FixedOutcomeExecutor(**args)
