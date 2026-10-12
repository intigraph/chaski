"""Process-lifetime custody for the persistent cursor and emitter inbox."""
from __future__ import annotations

import fcntl
import os
import stat
from pathlib import Path


class ConsumerLeaseError(RuntimeError):
    """Custody could not be established; no consumer read may proceed."""


class ConsumerLease:
    """Never unlink lock files: replacing their inode would split ownership.

    Lock the inbox itself so aliases and hard links share its kernel lock.
    The legacy cursor may not exist yet, so also lock a stable sidecar beside
    its canonical path. If present, lock the cursor inode for hard-link aliases.
    """

    def __init__(self, cursor: Path, inbox: Path | None):
        cursor = cursor.resolve()
        self.paths = [(cursor.with_name(cursor.name + '.consumer-lease'), True)]
        if cursor.exists():
            self.paths.append((cursor, False))
        if inbox is not None:
            self.paths.append((inbox.resolve(), True))
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
