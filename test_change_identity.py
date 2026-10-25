"""Existing integer custody never authorizes adoption of another store or log."""
import json
import sqlite3
from types import SimpleNamespace

import pytest

import incremental as inc
from emitter import ProtocolError

IDENTITY = {'store_id': 'store-A', 'log_epoch': 'epoch-A'}


def journal(path=':memory:'):
    conn = sqlite3.connect(path, isolation_level=None)
    conn.executescript(inc.SCHEMA)
    conn.execute('INSERT INTO change_cursor VALUES (1,10)')
    return conn


def page(tx, identity=IDENTITY):
    return {'source': identity, 'next_tx': tx, 'watermark_tx': tx,
            'records': [{'tx': tx, 'sequence': 0, 'op': 'assert'}]}


def consumer(conn):
    runner = SimpleNamespace(conn=conn, emitter=SimpleNamespace(label='fixture'), accepts=lambda _: True)
    return inc.ChangeFeed(None, [runner])


@pytest.mark.parametrize('changed', [
    {'store_id': 'store-B', 'log_epoch': 'epoch-A'},
    {'store_id': 'store-A', 'log_epoch': 'epoch-B'},
])
def test_store_or_epoch_swap_cannot_commit_inbox_or_cursor(changed):
    with journal() as conn:
        inc.bind_stream_source(conn, IDENTITY, 10)
        feed = consumer(conn)
        feed.accept_page(page(11), 10, stream_id=11)
        before = conn.execute('SELECT * FROM change_inbox').fetchall()
        with pytest.raises(ProtocolError, match='store or epoch changed'):
            feed.accept_page(page(12, changed), 11, stream_id=12)
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(11,)]
        assert conn.execute('SELECT * FROM change_inbox').fetchall() == before
        assert feed.cursor_tx == 11
        feed.accept_page(page(12), 11, stream_id=12)  # Same-source positive.
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(12,)]


@pytest.mark.parametrize('source', [None, {}, {'store_id': 'store-A'},
                                  {'store_id': '', 'log_epoch': 'epoch-A'},
                                  {**IDENTITY, 'extra': 'ignored'}])
def test_missing_or_malformed_frame_identity_refuses(source):
    with journal() as conn:
        inc.bind_stream_source(conn, IDENTITY, 10)
        with pytest.raises(ProtocolError):
            consumer(conn).accept_page(page(11, source), 10, stream_id=11)
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(10,)]
        assert conn.execute('SELECT COUNT(*) FROM change_inbox').fetchone()[0] == 0


def test_unbound_startup_refuses_before_transport_or_effects(monkeypatch):
    import chaski
    import change_stream
    with journal() as conn:
        def forbidden(*args):
            pytest.fail('unbound custody must not open transport or evaluate effects')
        monkeypatch.setattr(change_stream, 'ChangeStream', forbidden)
        reactor = SimpleNamespace(change_feed=consumer(conn), event_poll_s=60, quipu=None, tick=forbidden)
        with pytest.raises(ProtocolError, match='unbound'):
            chaski._serve_consumer(SimpleNamespace(once=False, changes_stream=True), reactor, [])
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(10,)]
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='change_source'").fetchall() == []


def test_explicit_transition_cli_is_idempotent_and_never_replaces_custody(tmp_path, monkeypatch):
    import chaski
    db, identity, state = tmp_path / 'inbox.db', tmp_path / 'identity.json', tmp_path / 'events.json'
    with journal(db) as conn:
        conn.execute("INSERT INTO change_inbox VALUES ('fixture',10,0,'{}')")
        before = conn.execute('SELECT * FROM change_inbox').fetchall()
    identity.write_text(json.dumps(IDENTITY))
    def forbidden(*args):
        pytest.fail('binding must not read rules or start the consumer')
    monkeypatch.setattr(chaski, 'run_consumer', forbidden)
    args = ['--quipu', 'https://peer.example.test', '--rules', str(tmp_path / 'not-read.yaml'),
            '--state', str(state), '--emitter-db', str(db), '--bind-change-source', str(identity),
            '--expected-change-cursor', '10']
    assert chaski.main(args) == chaski.main(args) == 0
    identity.write_text(json.dumps({**IDENTITY, 'log_epoch': 'epoch-B'}))
    with pytest.raises(ProtocolError):
        chaski.main(args)
    identity.write_text(json.dumps(IDENTITY))
    with pytest.raises(ProtocolError, match='cursor differs'):
        chaski.main([*args[:-1], '9'])
    with sqlite3.connect(db) as conn:
        assert inc.require_stream_source(conn) == ('store-A', 'epoch-A')
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(10,)]
        assert conn.execute('SELECT * FROM change_inbox').fetchall() == before


def test_binding_cli_cannot_create_missing_inbox(tmp_path):
    import chaski
    db, state = tmp_path / 'missing.db', tmp_path / 'events.json'
    with pytest.raises(SystemExit):
        chaski.main(['--quipu', 'https://peer.example.test', '--rules', 'not-read.yaml',
                     '--state', str(state), '--emitter-db', str(db),
                     '--bind-change-source', str(tmp_path / 'not-read.json'),
                     '--expected-change-cursor', '10'])
    assert not db.exists()
    assert not state.with_name(state.name + '.consumer-lease').exists()


def test_stale_initial_binding_does_not_create_source_record():
    with journal() as conn:
        with pytest.raises(ProtocolError, match='cursor differs'):
            inc.bind_stream_source(conn, IDENTITY, 9)
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='change_source'").fetchall() == []
        assert conn.execute('SELECT tx FROM change_cursor').fetchall() == [(10,)]
