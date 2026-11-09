"""Offline predecessor-state demonstration; never selects a real service database."""
import json
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from consumer_lease import ConsumerLease  # noqa: E402


def run(args, expected):
    result = subprocess.run([sys.executable, str(ROOT / 'chaski.py'), *args],
                            capture_output=True, text=True, timeout=4)
    assert result.returncode == expected, result.stderr
    return result


with tempfile.TemporaryDirectory(prefix='source-preflight-demo-') as temporary:
    root = Path(temporary)
    db, identity, state = root / 'inbox.db', root / 'identity.json', root / 'events.json'
    source = {'store_id': 'fixture-store', 'log_epoch': 'fixture-epoch'}
    identity.write_text(json.dumps(source))
    with sqlite3.connect(db) as conn:
        conn.executescript((ROOT / 'fixtures/change-source-6e.sql').read_text())
        conn.execute('INSERT INTO change_cursor VALUES (1,10)')
        conn.execute('INSERT INTO change_inbox VALUES (?,?,?,?)',
                     ('fixture', 10, 0, '{"entity":"urn:fixture:item"}'))
        conn.execute('INSERT INTO adapter_deadlines VALUES (?,?,?,?)',
                     ('fixture', 'urn:fixture:item', 42, 1))
    args = ['--quipu', 'http://127.0.0.1:1', '--rules', str(root / 'never-read-rules'),
            '--state', str(state), '--emitter-db', str(db)]
    check = [*args, '--check-change-source', str(identity)]
    bind = [*args, '--bind-change-source', str(identity), '--expected-change-cursor', '10']
    print('unbound predecessor:', run(check, 2).stdout.strip())
    with ConsumerLease(state, db):
        refused = run(bind, 2)
        assert 'REFUSED: cursor/inbox custody unavailable' in refused.stderr
        print('bind while primary owns custody: REFUSED before reading rules')
    print('explicit offline fixture transition:', run(bind, 0).stdout.strip())
    with ConsumerLease(state, db):
        print('preflight while primary owns custody:', run(check, 0).stdout.strip())
    identity.write_text(json.dumps({**source, 'log_epoch': 'different'}))
    print('wrong expected epoch:', run(check, 2).stdout.strip())
    with sqlite3.connect(db) as conn:
        assert conn.execute('SELECT tx FROM change_cursor').fetchone() == (10,)
        assert conn.execute('SELECT COUNT(*) FROM change_inbox').fetchone() == (1,)
        assert conn.execute('SELECT COUNT(*) FROM adapter_deadlines').fetchone() == (1,)
        assert conn.execute('SELECT store_id,log_epoch FROM change_source').fetchall() == [
            ('fixture-store', 'fixture-epoch')]
    print('retained: cursor=10, inbox=1, deadline=1, original binding unchanged')
