"""Process-lifetime custody for the persistent cursor and emitter inbox."""
from __future__ import annotations

import fcntl
import os
import stat
import sys
from pathlib import Path


class ConsumerLeaseError(RuntimeError):
    """Custody could not be established; no consumer read may proceed."""


def _darwin_lock_root() -> Path:
    # Real account home, not HOME/TMPDIR/XDG: aliases opened by processes with
    # different environments must still resolve to one per-UID lock namespace.
    import pwd
    return Path(pwd.getpwuid(os.getuid()).pw_dir) / '.local/state/chaski/inode-leases'


def _darwin_inbox_lock(fd: int) -> int:
    """Lock custody separately from SQLite's file locks, keyed by real inode.

    Keep the database descriptor open to pin its inode. Never remove these lock
    files: an old descriptor and a replacement pathname could split ownership.
    """
    info = os.fstat(fd)
    root = _darwin_lock_root()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    locked = None
    try:
        directory_info = os.fstat(directory)
        if (directory_info.st_uid != os.getuid()
                or stat.S_IMODE(directory_info.st_mode) != 0o700):
            raise ValueError('inode lease directory must be owned and private')
        name = f'{info.st_dev:x}-{info.st_ino:x}.lease'
        locked = os.open(name, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC
                         | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
        lock_info = os.fstat(locked)
        if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_uid != os.getuid()
                or lock_info.st_mode & 0o077 or lock_info.st_nlink != 1):
            raise ValueError('inode lease file must be owned, private and unaliased')
        fcntl.flock(locked, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result, locked = locked, None
        return result
    finally:
        if locked is not None:
            os.close(locked)
        os.close(directory)


class ConsumerLease:
    """Never unlink lock files: replacing their inode would split ownership.

    Lock the inbox inode so aliases and hard links share its kernel lock.
    On Darwin use a private real-account-home inode-keyed sidecar: flock on
    the database itself conflicts with SQLite's locks on that platform.
    The legacy cursor may not exist yet, so also lock a stable sidecar beside
    its canonical path. If present, lock the cursor inode for hard-link aliases.
    """

    def __init__(self, cursor: Path, inbox: Path | None):
        cursor = cursor.resolve()
        self.inbox = inbox.resolve() if inbox is not None else None
        self.paths = [(cursor.with_name(cursor.name + '.consumer-lease'), True)]
        if cursor.exists():
            self.paths.append((cursor, False))
        if self.inbox is not None:
            self.paths.append((self.inbox, True))
        self.handles: list[int] = []

    def __enter__(self):
        seen = set()
        try:
            for path, create in sorted(self.paths, key=lambda row: str(row[0])):
                flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK
                if create:
                    flags |= os.O_CREAT
                fd = os.open(path, flags, 0o600)
                self.handles.append(fd)
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode):
                    raise ValueError("lease target is not a regular file")
                inode = (info.st_dev, info.st_ino)
                if inode in seen:
                    os.close(self.handles.pop())
                    continue
                if sys.platform == 'darwin' and path == self.inbox:
                    self.handles.append(_darwin_inbox_lock(fd))
                else:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                seen.add(inode)
        except (OSError, ValueError) as exc:
            self.close()
            raise ConsumerLeaseError(
                'REFUSED: cursor/inbox custody unavailable; another consumer or inaccessible lease'
            ) from exc
        return self

    def close(self):
        while self.handles:
            os.close(self.handles.pop())

    def __exit__(self, *exc):
        self.close()
