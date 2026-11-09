"""Deployment preflight reads predecessor custody while its owner keeps serving."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

from consumer_lease import ConsumerLease
from incremental import bind_stream_source

ROOT = Path(__file__).resolve().parent
IDENTITY = {'store_id': 'fixture-store', 'log_epoch': 'fixture-epoch'}


@pytest.fixture
def custody(tmp_path):
    db = tmp_path / 'inbox.db'
    identity = tmp_path / 'identity.json'
    identity.write_text(json.dumps(IDENTITY))
    # Exact SCHEMA from incremental.py at serving predecessor 6e112f48.
    with sqlite3.connect(db, isolation_level=None) as conn:
        conn.executescript((ROOT / 'fixtures/change-source-6e.sql').read_text())
        conn.execute('INSERT INTO change_cursor VALUES (1,10)')
        conn.execute('INSERT INTO change_inbox VALUES (?,?,?,?)',
                     ('fixture', 10, 0, '{"entity":"urn:fixture:item"}'))
        conn.execute('INSERT INTO adapter_deadlines VALUES (?,?,?,?)',
                     ('fixture', 'urn:fixture:item', 42, 1))
        conn.execute('CREATE TABLE retained_outbox(id TEXT PRIMARY KEY,payload TEXT)')
        conn.execute('INSERT INTO retained_outbox VALUES (?,?)', ('old-event', 'retain me'))
    return db, identity, tmp_path / 'events.json'


def contents(db):
    with sqlite3.connect(db) as conn:
        return list(conn.iterdump())


def check(db, identity, state, *extra):
    result = subprocess.run(
        [sys.executable, str(ROOT / 'chaski.py'), '--quipu', 'http://127.0.0.1:1',
         '--rules', str(state.parent / 'never-read-rules'), '--state', str(state),
         '--emitter-db', str(db), '--check-change-source', str(identity), *extra],
        capture_output=True, text=True, timeout=4)
    # A real CLI mode, not argparse's unknown-option error or a patched helper.
    return result, json.loads(result.stdout)


def test_unbound_old_state_refuses_without_creating_binding_or_lease(custody):
    db, identity, state = custody
    before = contents(db)
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt == {'status': 'REFUSED', 'reason': 'source_unbound'}
    assert contents(db) == before
    assert not state.exists()
    assert not list(state.parent.glob('*.consumer-lease'))


def test_bound_state_checks_while_primary_holds_both_leases(custody):
    db, identity, state = custody
    with sqlite3.connect(db, isolation_level=None) as conn:
        bind_stream_source(conn, IDENTITY, 10)
    before = contents(db)
    with ConsumerLease(state, db):
        result, receipt = check(db, identity, state)
    assert result.returncode == 0, result.stderr
    assert receipt == {'status': 'READY', 'cursor': 10, 'binding_matches': True}
    assert contents(db) == before
    assert not state.exists()


def test_preflight_cannot_open_network_or_start_consumer(custody, monkeypatch, capsys):
    import socket
    import chaski
    db, identity, state = custody
    with sqlite3.connect(db, isolation_level=None) as conn:
        bind_stream_source(conn, IDENTITY, 10)

    def forbidden(*args, **kwargs):
        pytest.fail('read-only preflight must not open network or start a consumer')

    monkeypatch.setattr(socket, 'socket', forbidden)
    monkeypatch.setattr(chaski, 'run_consumer', forbidden)
    assert chaski.main([
        '--quipu', 'http://127.0.0.1:1', '--rules', 'never-read-rules',
        '--state', str(state), '--emitter-db', str(db),
        '--check-change-source', str(identity)]) == 0
    assert json.loads(capsys.readouterr().out)['binding_matches'] is True


@pytest.mark.parametrize('field', ['store_id', 'log_epoch'])
def test_wrong_source_refuses_without_rebinding(custody, field):
    db, identity, state = custody
    with sqlite3.connect(db, isolation_level=None) as conn:
        bind_stream_source(conn, IDENTITY, 10)
    identity.write_text(json.dumps({**IDENTITY, field: 'different'}))
    before = contents(db)
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt['reason'] == 'source_mismatch'
    assert contents(db) == before


def test_missing_inbox_is_not_initialized(custody):
    db, identity, state = custody
    db.unlink()
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt['reason'] == 'missing_inbox'
    assert not db.exists()
    assert not list(state.parent.glob('*.consumer-lease'))


@pytest.mark.parametrize('raw,reason', [
    ('not-json', 'identity_invalid'),
    ('{}', 'identity_invalid'),
    ('x' * 4097, 'identity_exceeds_budget'),
])
def test_bad_identity_refuses_without_reading_rules_or_changing_custody(custody, raw, reason):
    db, identity, state = custody
    identity.write_text(raw)
    before = contents(db)
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt['reason'] == reason
    assert contents(db) == before


@pytest.mark.parametrize('cursor', [-1, 'not-an-integer', None])
def test_invalid_existing_cursor_refuses(custody, cursor):
    db, identity, state = custody
    with sqlite3.connect(db, isolation_level=None) as conn:
        bind_stream_source(conn, IDENTITY, 10)
        if cursor is None:
            conn.execute('DELETE FROM change_cursor')
        else:
            conn.execute('UPDATE change_cursor SET tx=?', (cursor,))
    before = contents(db)
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt['reason'] == 'cursor_invalid'
    assert contents(db) == before


def test_ambiguous_binding_refuses(custody):
    db, identity, state = custody
    with sqlite3.connect(db, isolation_level=None) as conn:
        bind_stream_source(conn, IDENTITY, 10)
        conn.execute('INSERT INTO change_source VALUES (?,?)', ('other', 'epoch'))
    before = contents(db)
    result, receipt = check(db, identity, state)
    assert result.returncode == 2
    assert receipt['reason'] == 'source_ambiguous'
    assert contents(db) == before
