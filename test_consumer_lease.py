"""Ownership controls use real kernel locks, private files and child processes."""
import os
from pathlib import Path
import subprocess
import sqlite3
import sys

import pytest

import consumer_lease
from consumer_lease import ConsumerLease, ConsumerLeaseError


def test_second_owner_refuses_and_release_allows_next(tmp_path):
    cursor, inbox = tmp_path / 'cursor.json', tmp_path / 'inbox.db'
    with ConsumerLease(cursor, inbox):
        with pytest.raises(ConsumerLeaseError, match='REFUSED'):
            with ConsumerLease(cursor, inbox):
                pytest.fail('second consumer admitted')
    with ConsumerLease(cursor, inbox):
        assert inbox.exists()


@pytest.mark.parametrize('alias_kind', ['symlink', 'hardlink'])
def test_inbox_alias_cannot_split_custody(tmp_path, alias_kind):
    inbox, alias = tmp_path / 'inbox.db', tmp_path / 'alias.db'
    with ConsumerLease(tmp_path / 'one.json', inbox):
        if alias_kind == 'symlink':
            alias.symlink_to(inbox)
        else:
            os.link(inbox, alias)
        with pytest.raises(ConsumerLeaseError):
            with ConsumerLease(tmp_path / 'two.json', alias):
                pytest.fail('alias admitted a second inbox owner')


def test_failed_partial_acquisition_releases_prior_handles(tmp_path):
    cursor = tmp_path / 'a-cursor.json'
    with pytest.raises(ConsumerLeaseError):
        with ConsumerLease(cursor, tmp_path / 'missing-parent' / 'inbox.db'):
            pytest.fail('inaccessible inbox admitted')
    with ConsumerLease(cursor, None):
        pass


def test_body_failure_releases_lease(tmp_path):
    cursor = tmp_path / 'cursor.json'
    with pytest.raises(ValueError):
        with ConsumerLease(cursor, None):
            raise ValueError('startup failure')
    with ConsumerLease(cursor, None):
        pass


def test_cli_refuses_before_loading_rules_or_cursor(tmp_path):
    # Separate interpreter proves process-level exclusion, rather than a mock.
    code = '''
import sys
from pathlib import Path
import chaski, consumer_lease
consumer_lease._darwin_lock_root = lambda: Path(sys.argv[1])/'inode-leases'
chaski.load_rules = lambda path: (_ for _ in ()).throw(AssertionError('read before custody'))
root = Path(sys.argv[1])
raise SystemExit(chaski.main(['--quipu','http://invalid.example','--rules',str(root/'rules.yaml'),'--state',str(root/'cursor.json'),'--emitters',str(root/'emitters.yaml'),'--emitter-db',str(root/'inbox.db')]))
'''
    inbox = tmp_path / 'inbox.db'
    with ConsumerLease(tmp_path / 'cursor.json', inbox):
        owner = sqlite3.connect(inbox)
        owner.execute('CREATE TABLE change_cursor(tx INTEGER)')
        owner.execute('INSERT INTO change_cursor VALUES(41)')
        owner.commit()
        result = subprocess.run([sys.executable, '-c', code, str(tmp_path)],
                                cwd=Path(__file__).parent, capture_output=True,
                                text=True, timeout=5)
        assert owner.execute('SELECT tx FROM change_cursor').fetchone()[0] == 41
        owner.execute('UPDATE change_cursor SET tx=42')
        owner.commit()
        assert owner.execute('SELECT tx FROM change_cursor').fetchone()[0] == 42
        owner.close()
    assert result.returncode == 2
    assert 'REFUSED' in result.stderr
    assert 'read before custody' not in result.stderr


def test_sqlite_wal_and_commits_remain_available_under_lease(tmp_path):
    import emitter
    inbox = tmp_path / 'inbox.db'
    with ConsumerLease(tmp_path / 'cursor.json', inbox):
        conn = emitter.connect(inbox)
        assert conn.execute('PRAGMA journal_mode').fetchone()[0].lower() == 'wal'
        conn.execute('CREATE TABLE lease_control(value INTEGER)')
        conn.execute('INSERT INTO lease_control VALUES(41)')
        conn.close()
        # Closing SQLite must not release process-lifetime consumer custody.
        with pytest.raises(ConsumerLeaseError):
            with ConsumerLease(tmp_path / 'other-cursor.json', inbox):
                pytest.fail('closing SQLite released custody')
        conn = emitter.connect(inbox)
        assert conn.execute('SELECT value FROM lease_control').fetchone()[0] == 41
        conn.execute('UPDATE lease_control SET value=42')
        conn.close()
    with ConsumerLease(tmp_path / 'cursor.json', inbox):
        conn = emitter.connect(inbox)
        assert conn.execute('SELECT value FROM lease_control').fetchone()[0] == 42
        conn.close()


def test_inode_sidecar_never_locks_the_sqlite_file(tmp_path):
    inbox = tmp_path / 'inbox.db'
    fd = os.open(inbox, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        held = consumer_lease._darwin_inbox_lock(fd)
        try:
            db = sqlite3.connect(inbox)
            db.execute('PRAGMA journal_mode=WAL')
            db.execute('CREATE TABLE control(value INTEGER)')
            db.commit()
            db.close()
            with pytest.raises((OSError, ValueError)):
                consumer_lease._darwin_inbox_lock(fd)
        finally:
            os.close(held)
        released = consumer_lease._darwin_inbox_lock(fd)
        os.close(released)
    finally:
        os.close(fd)


@pytest.mark.parametrize('mode', [0o755, 0o777])
def test_inode_namespace_refuses_public_permissions(tmp_path, mode):
    root = tmp_path / 'inode-leases'
    root.mkdir(mode=mode)
    root.chmod(mode)
    fd = os.open(tmp_path / 'inbox.db', os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with pytest.raises(ValueError, match='private'):
            consumer_lease._darwin_inbox_lock(fd)
    finally:
        os.close(fd)


def test_inode_namespace_symlink_refuses(tmp_path):
    target = tmp_path / 'target'
    target.mkdir(mode=0o700)
    (tmp_path / 'inode-leases').symlink_to(target, target_is_directory=True)
    fd = os.open(tmp_path / 'inbox.db', os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with pytest.raises(OSError):
            consumer_lease._darwin_inbox_lock(fd)
    finally:
        os.close(fd)


@pytest.mark.parametrize('alias_kind', ['same', 'symlink', 'hardlink'])
def test_inode_sidecar_excludes_other_process_after_sqlite_closes(tmp_path, alias_kind):
    inbox = tmp_path / 'inbox.db'
    fd = os.open(inbox, os.O_RDWR | os.O_CREAT, 0o600)
    alias = inbox
    if alias_kind != 'same':
        alias = tmp_path / 'alias.db'
        if alias_kind == 'symlink':
            alias.symlink_to(inbox)
        else:
            os.link(inbox, alias)
    code = """
import os,sys
from pathlib import Path
import consumer_lease
consumer_lease._darwin_lock_root=lambda:Path(sys.argv[2])
fd=os.open(sys.argv[1],os.O_RDWR)
try:
    held=consumer_lease._darwin_inbox_lock(fd)
except (OSError,ValueError):
    raise SystemExit(2)
os.close(held);os.close(fd)
"""
    def contender():
        return subprocess.run([sys.executable, '-c', code, str(alias),
                               str(tmp_path / 'inode-leases')],
                              env={**os.environ, 'HOME': str(tmp_path / 'other-home')},
                              capture_output=True, timeout=5)
    held = consumer_lease._darwin_inbox_lock(fd)
    try:
        conn = sqlite3.connect(inbox)
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('CREATE TABLE control(value INTEGER)')
        conn.commit()
        conn.close()
        assert contender().returncode == 2
    finally:
        os.close(held)
        os.close(fd)
    assert contender().returncode == 0


def test_production_inode_namespace_ignores_environment(tmp_path, monkeypatch):
    import pwd
    from conftest import real_darwin_lock_root
    monkeypatch.setenv('HOME', str(tmp_path / 'other-home'))
    monkeypatch.setenv('TMPDIR', str(tmp_path / 'other-temp'))
    monkeypatch.setenv('XDG_STATE_HOME', str(tmp_path / 'other-state'))
    assert real_darwin_lock_root() == (Path(pwd.getpwuid(os.getuid()).pw_dir)
                                     / '.local/state/chaski/inode-leases')
