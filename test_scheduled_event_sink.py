"""Actual observe/outbox delivery into the async receiver; signing is a fixture."""
import base64
from dataclasses import asdict, replace
import json
import threading

import pytest

from async_scheduled_sink import AsyncScheduledSink
from emitter import Emitter, connect, deliver_pending, observe
from scheduled_event_sink import ScheduledEventSink
from scheduled_receiver import ExecutionScope, run_id
from slot_receiver import SlotLedger


@pytest.fixture
def emitted(tmp_path):
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    definition = Emitter('schedule', ['unused'], 60, 'operator',
                          event='scheduled', baseline_emits=True)
    conn = connect(tmp_path/'emitter.sqlite')
    assert observe(conn, definition, [{'item': scope.job, 'verdict': 'UNBLOCKED',
                    'event_id': run_id(scope, 600), 'owner': 'operator'}], 0)
    event = json.loads(conn.execute('SELECT payload FROM outbox').fetchone()[0])
    yield scope, conn, event
    conn.close()


class RecordingReceiver:
    def __init__(self, scope):
        self.scope, self.calls = scope, []

    def delivery_scope(self):
        return {'scope': asdict(self.scope)}

    def deliver(self, binding):
        self.calls.append(binding)


def encode(value):
    raw = json.dumps(value, sort_keys=True, separators=(',', ':')).encode()
    return 'scheduled-v1-' + base64.urlsafe_b64encode(raw).decode().rstrip('=')


def test_real_emitter_outbox_waits_for_signed_receiver_and_pins_scope(emitted, tmp_path):
    scope, conn, event = emitted
    entered, release, published = (threading.Event() for _ in range(3))
    calls = []

    class Port:
        def admit(self, scope, slot): pass
        def started(self, run, scope): pass
        def finished(self, run, scope, outcome): pass
        def publish(self, run, scope, outcome): published.set()

    def execute(slot):
        calls.append(slot)
        entered.set()
        assert release.wait(5)
        return {'outcome': 'succeeded', 'exit_code': 0}

    inner = AsyncScheduledSink(SlotLedger(tmp_path/'slots.sqlite'), scope,
                               lambda slot: Port(), execute)
    sink = ScheduledEventSink(inner, 'schedule', 'operator')
    try:
        assert deliver_pending(conn, sink, 0, 'schedule') == (0, 1)
        assert entered.wait(5)
        frozen, receipt = conn.execute('SELECT delivery_scope,delivered_at FROM outbox').fetchone()
        assert json.loads(frozen) == sink.delivery_scope() and receipt is None
        assert deliver_pending(conn, sink, 30, 'schedule') == (0, 1)
        assert calls == [600] and not published.is_set()
        release.set()
        with inner._lock:
            future = next(iter(inner._pending.values()))
        future.result(timeout=5)
        assert deliver_pending(conn, sink, 90, 'schedule') == (1, 0)
        assert published.is_set() and calls == [600]
        assert conn.execute('SELECT delivered_at FROM outbox').fetchone() == (90,)
        assert event['event_id'] == run_id(scope, 600)
    finally:
        release.set()
        sink.close()


def test_delivery_time_and_generation_cannot_change_frozen_slot(emitted):
    scope, _, event = emitted
    receiver = RecordingReceiver(scope)
    sink = ScheduledEventSink(receiver, 'schedule', 'operator')
    event.update(observed_at=9876543210, generation=123)
    sink.deliver(event)
    assert receiver.calls == [{**asdict(scope), 'scheduled_at': 600}]


@pytest.mark.parametrize('field,value', [('event', 'unblocked'), ('emitter', 'other'),
    ('item', 'urn:example:foreign'), ('recipient', 'other'), ('command', 'anything'),
    ('generation', True), ('generation', -1), ('observed_at', True),
    ('observed_at', float('nan')), ('observed_at', float('inf')),
    ('evidence', 'x'*8193), ('work_kind', []), ('event_id', 'scheduled-v1-!'),
    ('event_id', 'scheduled-v1-'+'a'*24000)])
def test_misdirected_or_malformed_envelope_never_reaches_receiver(emitted, field, value):
    scope, _, event = emitted
    receiver = RecordingReceiver(scope)
    sink = ScheduledEventSink(receiver, 'schedule', 'operator')
    event[field] = value
    with pytest.raises(ValueError):
        sink.deliver(event)
    assert receiver.calls == []


@pytest.mark.parametrize('field', ['job', 'revision', 'host', 'manifest_sha256', 'agent', 'workflow'])
def test_each_foreign_scope_field_refuses_before_receiver(emitted, field):
    scope, _, event = emitted
    receiver = RecordingReceiver(scope)
    other = 'b'*64 if field == 'manifest_sha256' else 'urn:example:foreign'
    event['event_id'] = run_id(replace(scope, **{field: other}), 600)
    with pytest.raises(ValueError):
        ScheduledEventSink(receiver, 'schedule', 'operator').deliver(event)
    assert receiver.calls == []


@pytest.mark.parametrize('change', ['duplicate', 'padding', 'version_bool', 'slot_bool',
                                     'slot_huge', 'extra', 'array', 'missing', 'nested'])
def test_noncanonical_or_unbounded_run_binding_refuses(emitted, change):
    scope, _, event = emitted
    receiver = RecordingReceiver(scope)
    binding = {**asdict(scope), 'scheduled_at': 600, 'version': 1}
    if change == 'duplicate':
        raw = json.dumps(binding, sort_keys=True, separators=(',', ':'))
        raw = raw.replace('"scheduled_at":600', '"scheduled_at":600,"scheduled_at":600')
        event['event_id'] = 'scheduled-v1-'+base64.urlsafe_b64encode(raw.encode()).decode().rstrip('=')
    elif change == 'padding':
        event['event_id'] += '='
    elif change == 'nested':
        raw = '['*1500 + '0' + ']'*1500
        event['event_id'] = 'scheduled-v1-'+base64.urlsafe_b64encode(raw.encode()).decode().rstrip('=')
    else:
        if change == 'version_bool':
            binding['version'] = True
        elif change == 'slot_bool':
            binding['scheduled_at'] = True
        elif change == 'slot_huge':
            binding['scheduled_at'] = 10**100
        elif change == 'extra':
            binding['command'] = 'anything'
        elif change == 'array':
            binding = [binding]
        elif change == 'missing':
            del binding['version']
        event['event_id'] = encode(binding)
    with pytest.raises(ValueError):
        ScheduledEventSink(receiver, 'schedule', 'operator').deliver(event)
    assert receiver.calls == []


def test_missing_frozen_fields_are_not_filled_from_current_configuration(emitted):
    scope, _, event = emitted
    receiver = RecordingReceiver(scope)
    del event['event_id']
    with pytest.raises(ValueError):
        ScheduledEventSink(receiver, 'schedule', 'operator').deliver(event)
    assert receiver.calls == []
