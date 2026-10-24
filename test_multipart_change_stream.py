"""One acknowledged transaction, even when transport splits it into parts."""
import hashlib
import io
import json
from types import SimpleNamespace

import pytest

import change_stream as cs
from emitter import ProtocolError, connect
from incremental import SCHEMA, ChangeFeed


def records():
    return [{'tx': 11, 'sequence': i, 'graph': 'ROOT', 'entity': f'urn:test:{i}',
             'attribute': 'urn:test:state', 'op': 'assert', 'value': 'ready'} for i in range(2)]


def transaction(rows=None):
    rows = records() if rows is None else rows
    digest = hashlib.sha256()
    parts = []
    for index, record in enumerate(rows):
        data = json.dumps({'source_tx': 11, 'index': index, 'digest_before': digest.hexdigest(),
                           'records': [record]}, sort_keys=True, separators=(',', ':'))
        digest.update(data.encode())
        parts.append({'event': 'quipu.changes.part', 'data': data})
    source = {'store_id': 'urn:test:store', 'log_epoch': 'urn:test:epoch'}
    totals = {'source_tx': 11, 'parts': len(parts), 'records': len(rows), 'digest': digest.hexdigest(),
              'source': source}
    begin = {**totals, 'bytes': sum(len(p['data'].encode()) for p in parts)}
    return [{'event': 'quipu.changes.begin', 'data': json.dumps(begin)}, *parts,
            {'event': 'quipu.changes.commit', 'id': '11', 'data': json.dumps(totals)}]


def wire(events):
    return ''.join(''.join(f'{k}: {v}\n' for k, v in event.items()) + '\n' for event in events).encode()


def close(page):
    closer = getattr(page['records'], 'close', None)
    if closer:
        closer()


def test_multipart_yields_one_disk_backed_page_only_at_commit():
    cursor, page = next(cs.frames(io.BytesIO(wire(transaction()))))
    try:
        assert cursor == page['next_tx'] == page['watermark_tx'] == 11
        assert not isinstance(page['records'], list), 'the whole transaction must not occupy memory'
        assert list(page['records']) == records()
        assert page['source']['log_epoch'] == 'urn:test:epoch'
    finally:
        close(page)


def test_transaction_larger_than_one_frame_still_has_bounded_parts():
    rows = records()
    for row in rows:
        row['value'] = 'x' * 600_000
    events = transaction(rows)
    assert len(wire(events)) > cs.FRAME_BYTES
    assert all(len(wire([event])) < cs.FRAME_BYTES for event in events)
    cursor, page = next(cs.frames(io.BytesIO(wire(events))))
    try:
        assert cursor == 11 and list(page['records']) == rows
    finally:
        close(page)


def test_commit_retains_source_watermark_and_rejects_wrong_cursor():
    events = transaction()
    body = json.loads(events[-1]['data'])
    body.update(next_tx=11, watermark_tx=99, capture='test')
    events[-1]['data'] = json.dumps(body)
    _, page = next(cs.frames(io.BytesIO(wire(events))))
    try:
        assert page['watermark_tx'] == 99 and page['capture'] == 'test'
    finally:
        close(page)
    body['next_tx'] = 12
    events[-1]['data'] = json.dumps(body)
    with pytest.raises(ProtocolError):
        next(cs.frames(io.BytesIO(wire(events))))


@pytest.mark.parametrize('case', ['missing_commit', 'part_order', 'part_tx', 'digest_before', 'digest',
                                  'count', 'bytes', 'source', 'commit_id', 'nested_begin', 'interleaved',
                                  'boolean_count', 'duplicate_commit'])
def test_malformed_multipart_never_yields_a_page(case):
    events = transaction()
    if case == 'missing_commit':
        events.pop()
    elif case == 'part_order':
        events[1], events[2] = events[2], events[1]
    elif case == 'commit_id':
        events[-1]['id'] = '12'
    elif case == 'nested_begin':
        events.insert(1, events[0].copy())
    elif case == 'interleaved':
        events.insert(1, {'event': 'quipu.changes', 'id': '11', 'data': '{}'})
    elif case == 'duplicate_commit':
        # An unsolicited commit after the first one cannot produce another page.
        events = [events[-1]]
    else:
        index = 1 if case in {'part_tx', 'digest_before'} else 0 if case in {'bytes', 'boolean_count'} else -1
        body = json.loads(events[index]['data'])
        key, value = {'part_tx': ('source_tx', 12), 'digest_before': ('digest_before', '0' * 64),
                      'digest': ('digest', '0' * 64), 'count': ('records', 3), 'bytes': ('bytes', 1),
                      'source': ('source', {'store_id': 'different', 'log_epoch': 'urn:test:epoch'}),
                      'boolean_count': ('parts', True)}[case]
        body[key] = value
        events[index]['data'] = json.dumps(body)
    with pytest.raises((ProtocolError, ValueError)):
        next(cs.frames(io.BytesIO(wire(events))))


def test_stage_limit_refuses_before_accepting_parts(monkeypatch):
    monkeypatch.setattr(cs, 'MAX_TRANSACTION_BYTES', 1, raising=False)
    with pytest.raises(ProtocolError):
        next(cs.frames(io.BytesIO(wire(transaction()))))


@pytest.mark.parametrize('complete', [False, True])
def test_private_stage_cleanup_and_ownership(monkeypatch, complete):
    opened = []
    factory = cs.tempfile.TemporaryFile

    def tracked(*args, **kwargs):
        file = factory(*args, **kwargs)
        opened.append(file)
        return file

    monkeypatch.setattr(cs.tempfile, 'TemporaryFile', tracked)
    events = transaction()
    if complete:
        _, page = next(cs.frames(io.BytesIO(wire(events))))
        assert not opened[0].closed  # Consumer owns verified data until ACK.
        close(page)
    else:
        with pytest.raises(ProtocolError):
            next(cs.frames(io.BytesIO(wire(events[:-1]))))
    assert len(opened) == 1 and opened[0].closed


def test_heartbeat_enforces_assembly_deadline(monkeypatch):
    clock = iter([0, cs.ASSEMBLY_SECONDS + 1])
    monkeypatch.setattr(cs.time, 'monotonic', lambda: next(clock))
    with pytest.raises(ProtocolError, match='timed out'):
        next(cs.frames(io.BytesIO(wire(transaction()[:1]) + b': heartbeat\n\n')))


def test_disk_floor_refuses_before_creating_stage(monkeypatch):
    monkeypatch.setattr(cs.shutil, 'disk_usage', lambda _: SimpleNamespace(free=0))
    with pytest.raises(ProtocolError, match='staging disk'):
        next(cs.frames(io.BytesIO(wire(transaction()))))


def feed(tmp_path):
    conn = connect(tmp_path / 'state.db')
    conn.executescript(SCHEMA)
    conn.execute('INSERT INTO change_cursor VALUES (1,10)')
    runner = SimpleNamespace(conn=conn, emitter=SimpleNamespace(label='test'), accepts=lambda record: True)
    return conn, ChangeFeed(None, [runner])


def test_verified_parts_commit_inbox_and_cursor_together(tmp_path):
    conn, consumer = feed(tmp_path)
    cursor, page = next(cs.frames(io.BytesIO(wire(transaction()))))
    try:
        consumer.accept_page(page, 10, stream_id=cursor)
        assert conn.execute('SELECT tx FROM change_cursor').fetchone()[0] == 11
        assert conn.execute('SELECT COUNT(*) FROM change_inbox').fetchone()[0] == 2
        assert consumer.take_dirty() == {'test'}
    finally:
        close(page)
        conn.close()


def test_bad_record_rolls_back_staged_inbox_and_cursor(tmp_path):
    conn, consumer = feed(tmp_path)
    rows = records()
    rows[-1]['sequence'] = -1
    cursor, page = next(cs.frames(io.BytesIO(wire(transaction(rows)))))
    try:
        with pytest.raises(ProtocolError):
            consumer.accept_page(page, 10, stream_id=cursor)
        assert conn.execute('SELECT tx FROM change_cursor').fetchone()[0] == 10
        assert conn.execute('SELECT COUNT(*) FROM change_inbox').fetchone()[0] == 0
        assert consumer.take_dirty() == set()
    finally:
        close(page)
        conn.close()
