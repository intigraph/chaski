"""aegis-se8: stale scheduled retries cannot starve the newest slot."""
from dataclasses import asdict
import json
import threading

import pytest

import scheduled_sink_factory as factory
from async_scheduled_sink import AsyncScheduledSink, DeliveryPending
from emitter import WriteBudget, connect, deliver_pending
from scheduled_receiver import ExecutionScope
from slot_receiver import SlotLedger

HOUR = 3600
SCOPE = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a' * 64,
                       'operator', 'urn:example:workflow')


class Recorder:
    def __init__(self, ttl=None):
        self.sent = []
        if ttl is not None:
            self.delivery_ttl_s = ttl

    def deliver(self, event):
        self.sent.append(event['id'])


def backlog(conn, now, stale=40):
    for i in range(stale):
        conn.execute('INSERT INTO outbox(event_id,emitter,item,generation,payload,created,attempts)'
                     ' VALUES(?,?,?,?,?,?,?)',
                     (f'old{i:03}', 'hb', 'job', 1, json.dumps({'id': f'old{i:03}'}),
                      now - 30 * HOUR + i, 9))
    conn.execute('INSERT INTO outbox(event_id,emitter,item,generation,payload,created)'
                 ' VALUES(?,?,?,?,?,?)', ('new', 'hb', 'job', 1, json.dumps({'id': 'new'}), now - 60))


def test_ttl_sink_delivers_the_newest_slot_first_tick_and_spends_no_budget_on_stale(tmp_path):
    conn = connect(tmp_path / 'inbox.db')
    now = 10_000_000
    backlog(conn, now)
    sink, budget = Recorder(ttl=2 * HOUR), WriteBudget(1)
    assert deliver_pending(conn, sink, now, 'hb', budget, limit=5) == (1, 0)
    assert sink.sent == ['new']
    # Stale rows are retained exactly: undelivered, attempts unchanged, still pending.
    assert conn.execute("SELECT COUNT(*) FROM outbox WHERE delivered_at IS NULL AND attempts=9"
                        ).fetchone()[0] == 40


def test_without_ttl_oldest_first_is_unchanged(tmp_path):
    conn = connect(tmp_path / 'inbox.db')
    now = 10_000_000
    backlog(conn, now)
    sink = Recorder()
    deliver_pending(conn, sink, now, 'hb', limit=5)
    assert sink.sent == ['old000', 'old001', 'old002', 'old003', 'old004']


def test_firing_inside_ttl_is_still_delivered(tmp_path):
    conn = connect(tmp_path / 'inbox.db')
    now = 10_000_000
    conn.execute('INSERT INTO outbox(event_id,emitter,item,generation,payload,created)'
                 ' VALUES(?,?,?,?,?,?)', ('edge', 'hb', 'job', 1, json.dumps({'id': 'edge'}), now - 2 * HOUR))
    sink = Recorder(ttl=2 * HOUR)
    assert deliver_pending(conn, sink, now, 'hb') == (1, 0)


@pytest.fixture
def receiver(tmp_path):
    release = threading.Event()
    calls = []

    class Port:
        def admit(self, scope, slot): pass
        def started(self, run, scope): pass
        def finished(self, run, scope, outcome): pass
        def publish(self, run, scope, outcome): pass

    def execute(slot):
        calls.append(slot)
        assert release.wait(5)
        return {'outcome': 'succeeded', 'exit_code': 0}

    clock = [0.0]
    ledger = SlotLedger(tmp_path / 'slots.sqlite')
    sink = AsyncScheduledSink(ledger, SCOPE, lambda slot: Port(), execute, workers=1, capacity=1,
                              delivery_ttl_s=2 * HOUR, clock=lambda: clock[0])
    yield sink, release, calls, clock
    release.set()
    sink.close()


def binding(slot):
    return {**asdict(SCOPE), 'scheduled_at': slot}


def finish(sink):
    with sink._lock:
        futures = list(sink._pending.values())
    for f in futures:
        f.result(timeout=5)


def test_finished_work_past_ttl_releases_capacity_without_rerunning(receiver):
    sink, release, calls, clock = receiver
    old, new = 3600, 3600 + 3 * HOUR
    clock[0] = old
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(old))
    release.set()
    finish(sink)
    clock[0] = new
    # The old row is never polled again (past TTL); it must not hold capacity.
    with pytest.raises(DeliveryPending, match='submitted'):
        sink.deliver(binding(new))
    finish(sink)
    sink.deliver(binding(new))
    # A late re-delivery of the evicted slot replays its stored result; no second child.
    with pytest.raises(DeliveryPending, match='submitted'):
        sink.deliver(binding(old))
    finish(sink)
    sink.deliver(binding(old))
    assert calls == [old, new]


def test_finished_work_inside_ttl_still_holds_capacity(receiver):
    sink, release, calls, clock = receiver
    clock[0] = 3600
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(3600))
    release.set()
    finish(sink)
    clock[0] = 3600 + HOUR
    with pytest.raises(DeliveryPending, match='capacity'):
        sink.deliver(binding(3600 + HOUR))


def test_unfinished_work_is_never_evicted(receiver):
    sink, release, calls, clock = receiver
    clock[0] = 3600
    with pytest.raises(DeliveryPending):
        sink.deliver(binding(3600))
    clock[0] = 3600 + 10 * HOUR
    with pytest.raises(DeliveryPending, match='capacity'):
        sink.deliver(binding(3600 + 10 * HOUR))
    assert calls == [3600] or calls == []


@pytest.mark.parametrize('ttl', [0, 59, 30 * 86400 + 1, 7200.0, '7200', True])
def test_factory_refuses_invalid_ttl(ttl, monkeypatch):
    monkeypatch.setattr(factory, 'ShuttleProcessPort', lambda *a, **k: None)
    monkeypatch.setattr(factory, 'FixedOutcomeExecutor', lambda **k: None)
    monkeypatch.setattr(factory, 'execution_manifest_sha256', lambda recipe: 'a' * 64)
    config = {'scope': asdict(SCOPE), 'ledger': '/abs/slots.sqlite',
              'shuttle': {'executable': 'x', 'config': 'y', 'config_sha256': 'z', 'timeout_s': 5},
              'execute': {k: None for k in ('argv', 'cwd', 'env', 'result_fd_env', 'timeout_s', 'output_limit')},
              'workers': 1, 'capacity': 2}
    assert factory.validate(config)[-1] is None
    assert factory.validate({**config, 'delivery_ttl_s': 7200})[-1] == 7200
    with pytest.raises(ValueError, match='delivery_ttl_s'):
        factory.validate({**config, 'delivery_ttl_s': ttl})
