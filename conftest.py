"""Private lease namespace for tests; never use the real account's custody files."""
import pytest

import consumer_lease

real_darwin_lock_root = consumer_lease._darwin_lock_root


@pytest.fixture(autouse=True)
def private_inode_namespace(tmp_path, monkeypatch):
    # Production ignores fake HOME so two consumers cannot split custody by
    # changing environments. Test subprocesses explicitly share this fixture.
    monkeypatch.setattr(consumer_lease, '_darwin_lock_root',
                        lambda: tmp_path / 'inode-leases')
