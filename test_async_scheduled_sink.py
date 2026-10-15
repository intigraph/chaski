"""Real threads, durable claims and emitter receipts; no production execution."""
from contextvars import ContextVar
from dataclasses import asdict
import json
import threading

import pytest

from async_scheduled_sink import AsyncScheduledSink, DeliveryPending
from emitter import connect, deliver_pending
from scheduled_receiver import ExecutionScope
from slot_receiver import SlotLedger, SlotUnknown


@pytest.fixture
def rig(tmp_path):
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'operator', 'urn:example:workflow')
    entered, release, published = (threading.Event() for _ in range(3))
    calls, phases = [], []
    fail_publish = [False]

    class Port:
        def admit(self, scope, slot):
            phases.append('admit')

        def started(self, run, scope):
            phases.append('started')

        def finished(self, run, scope, outcome):
            phases.append('finished')

        def publish(self, run, scope, outcome):
            phases.append('publish')
            if fail_publish[0]:
                raise ConnectionError('publication unavailable')
            published.set()

    def execute(slot):
        calls.append(slot)
        entered.set()
        assert release.wait(5), 'test did not release blocked worker'
        return {'outcome': 'skipped-by-hold', 'exit_code': 0}

    ledger = SlotLedger(tmp_path/'slots.sqlite')
    sink = AsyncScheduledSink(ledger, scope, lambda slot: Port(), execute,
                              workers=1, capacity=1)
    yield sink, scope, entered, release, published, calls, phases, fail_publish, ledger
    release.set()
    sink.close()


def binding(scope, slot=600):
    return {**asdict(scope), 'scheduled_at': slot}


def drain_worker(sink):
    # Wait for actual worker completion without timing guesses; only the test
    # waits, never deliver() or the reactor.
    with sink._lock:
        futures = list(sink._pending.values())
    for future in futures:
        try:
            future.result(timeout=5)
        except ConnectionError:
            pass


def test_emitter_stays_undelivered_while_child_blocks_then_receipts(rig, tmp_path):
    sink, scope, entered, release, published, calls, phases, _, _ = rig
    conn = connect(tmp_path/'emitter.sqlite')
    try:
        conn.execute('INSERT INTO outbox(event_id,emitter,item,generation,payload,created) '
                     'VALUES(?,?,?,?,?,?)',
                     ('event1', 'schedule', scope.job, 1, json.dumps(binding(scope)), 0))
        assert deliver_pending(conn, sink, 0, 'schedule') == (0, 1)
        assert entered.wait(5)
        assert conn.execute('SELECT delivered_at FROM outbox').fetchone() == (None,)
        # A real running child remains blocked, yet delivery can be called
        # again immediately and cannot launch another child.
        assert deliver_pending(conn, sink, 30, 'schedule') == (0, 1)
        assert calls == [600] and not published.is_set()
        release.set()
        drain_worker(sink)
        assert published.is_set()
        assert deliver_pending(conn, sink, 90, 'schedule') == (1, 0)
        assert conn.execute('SELECT delivered_at FROM outbox').fetchone() == (90,)
        assert phases == ['admit', 'started', 'finished', 'publish']
    finally:
        conn.close()


def test_capacity_includes_finished_unacknowledged_work_and_reuses_slot(rig):
    sink, scope, entered, release, _, calls, _, _, _ = rig
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(scope))
    assert entered.wait(5)
    with pytest.raises(DeliveryPending, match='capacity'):
        sink.deliver(binding(scope, 1200))
    release.set()
    drain_worker(sink)
    with pytest.raises(DeliveryPending, match='capacity'):
        sink.deliver(binding(scope, 1200))
    sink.deliver(binding(scope))
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(scope, 1200))
    drain_worker(sink)
    sink.deliver(binding(scope, 1200))
    assert calls == [600, 1200]


def test_publication_retry_and_receiver_recreation_never_repeat_child(rig):
    sink, scope, entered, release, _, calls, _, fail_publish, ledger = rig
    fail_publish[0] = True
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(scope))
    assert entered.wait(5)
    release.set()
    drain_worker(sink)
    with pytest.raises(ConnectionError, match='publication unavailable'):
        sink.deliver(binding(scope))
    fail_publish[0] = False
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(scope))
    drain_worker(sink)
    sink.deliver(binding(scope))
    sink.close()
    replacement = AsyncScheduledSink(ledger, scope, sink.port_factory,
                                     lambda slot: pytest.fail('replayed child'))
    try:
        with pytest.raises(DeliveryPending):
            replacement.deliver(binding(scope))
        drain_worker(replacement)
        replacement.deliver(binding(scope))
        assert calls == [600]
    finally:
        replacement.close()


def test_claimed_unknown_slot_stays_unknown_without_child(rig):
    sink, scope, _, _, _, calls, _, _, ledger = rig
    from slot_receiver import slot_id
    with pytest.raises(RuntimeError):
        ledger.run(slot_id(scope.job, scope.revision, 600), binding(scope),
                   lambda: (_ for _ in ()).throw(RuntimeError('crashed after claim')))
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(scope))
    with pytest.raises(SlotUnknown):
        drain_worker(sink)
    with pytest.raises(SlotUnknown):
        sink.deliver(binding(scope))
    assert calls == []


@pytest.mark.parametrize('change', [{'host': 'urn:example:other'},
                                    {'scheduled_at': True}, {'command': 'anything'},
                                    {'scheduled_at': -1}])
def test_bad_binding_refuses_before_admission(rig, change):
    sink, scope, _, _, _, calls, phases, _, _ = rig
    with pytest.raises(ValueError):
        sink.deliver({**binding(scope), **change})
    assert not sink._pending and calls == [] and phases == []


def test_context_is_copied_at_submit_and_restored_in_caller(rig):
    sink, scope, _, _, _, _, _, _, _ = rig
    flag = ContextVar('strict_admission', default=False)
    observed = []
    def execute(slot):
        observed.append(flag.get())
        flag.set(False)
        return {'outcome': 'succeeded', 'exit_code': 0}
    sink.execute = execute
    token = flag.set(True)
    try:
        with pytest.raises(DeliveryPending):
            sink.deliver(binding(scope))
        drain_worker(sink)
        sink.deliver(binding(scope))
        assert observed == [True] and flag.get() is True
    finally:
        flag.reset(token)


def test_close_refuses_new_work(rig):
    sink, scope, _, _, _, calls, phases, _, _ = rig
    sink.close()
    with pytest.raises(ConnectionError, match='closed'):
        sink.deliver(binding(scope))
    assert calls == [] and phases == []


@pytest.mark.parametrize('workers,capacity', [(0, 1), (True, 1), (2, 1), (1, 65)])
def test_unbounded_or_invalid_capacity_refuses(rig, workers, capacity):
    sink, scope, _, _, _, _, _, _, ledger = rig
    with pytest.raises(ValueError):
        AsyncScheduledSink(ledger, scope, sink.port_factory, sink.execute,
                           workers=workers, capacity=capacity)
