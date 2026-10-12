"""Ownership controls use real kernel locks, private files and child processes."""
import os
from pathlib import Path
import subprocess
import sqlite3
import sys

import pytest

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
import chaski
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
