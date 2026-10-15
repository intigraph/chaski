"""Fixed local Shuttle CLI bridge; no scheduler, launcher or child executor.

The operator pins the executable and config digest. Each call passes the same
opened config descriptor to Shuttle, so replacement of the pathname cannot
change the admitted configuration. Parent-directory custody remains local.
"""
from __future__ import annotations

from dataclasses import asdict
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile

from scheduled_receiver import ExecutionScope, run_id


class PortRefused(RuntimeError):
    """No verified phase receipt; never authorizes child execution or replay."""


class ShuttleProcessPort:
    def __init__(self, executable: Path, config: Path, config_sha256: str,
                 scope: ExecutionScope, scheduled_at: int, *, timeout_s: float = 30):
        scope.validate()
        self.run = run_id(scope, scheduled_at)
        if not Path(executable).is_absolute() or not Path(config).is_absolute():
            raise ValueError("absolute operator-owned executable and config required")
        if (type(config_sha256) is not str or len(config_sha256) != 64 or
                any(c not in '0123456789abcdef' for c in config_sha256)):
            raise ValueError("pinned config SHA256 required")
        if type(timeout_s) not in (int, float) or not 0 < timeout_s <= 60:
            raise ValueError("bounded phase timeout required")
        self.executable, self.config = str(executable), Path(config)
        self.digest, self.scope, self.slot = config_sha256, scope, scheduled_at
        self.timeout_s = timeout_s

    def _call(self, action, scope, run=None, outcome=None):
        if type(scope) is not ExecutionScope or scope != self.scope:
            raise PortRefused("scope differs from pinned delivery")
        if action != 'admit' and run != self.run:
            raise PortRefused("run differs from pinned slot")
        packet = {'scope': asdict(scope), 'scheduled_at': self.slot}
        if action != 'admit':
            packet['run'] = run
        if outcome is not None:
            packet['outcome'] = outcome
        try:
            fd = os.open(self.config, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, 'rb') as config:
                info = os.fstat(config.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_size > 65536 or
                        info.st_uid != os.getuid() or info.st_mode & 0o022):
                    raise PortRefused("config must be bounded, owned and not writable by others")
                raw = config.read(65537)
                if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != self.digest:
                    raise PortRefused("config differs from pinned bytes")
                config.seek(0)
                # Temporary files bound memory use even if a broken SDK emits
                # more output than its small receipt contract permits.
                with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
                    result = subprocess.run(
                        [self.executable, '--require-existing-keys', 'scheduled', action,
                         '--config', f'/dev/fd/{config.fileno()}'],
                        input=json.dumps(packet, allow_nan=False).encode(),
                        stdout=out, stderr=err, pass_fds=(config.fileno(),),
                        timeout=self.timeout_s, check=False)
                    if result.returncode or out.tell() > 4096 or err.tell() > 4096:
                        raise PortRefused("Shuttle phase failed or exceeded receipt bound")
                    out.seek(0)
                    receipt = out.read(4097)
            expected = {'action': action, 'result': 'verified'}
            # Exact canonical receipt rejects duplicate keys, trailing output,
            # wrong action, and a generic exit-zero without verification.
            if receipt.strip() != json.dumps(expected).encode():
                raise PortRefused("Shuttle did not return the exact phase receipt")
        except (OSError, subprocess.TimeoutExpired, ValueError) as exc:
            raise PortRefused(f"Shuttle phase unavailable: {type(exc).__name__}") from None

    def admit(self, scope, scheduled_at):
        if type(scheduled_at) is not int or scheduled_at != self.slot:
            raise PortRefused("delivery slot differs from pinned slot")
        self._call('admit', scope)

    def started(self, run, scope):
        self._call('started', scope, run)

    def finished(self, run, scope, outcome):
        self._call('finished', scope, run, outcome)

    def publish(self, run, scope, outcome):
        self._call('publish', scope, run, outcome)
