"""Fixed trusted command with a bounded, unsigned outcome descriptor.

Manifest/artifact admission and durable slot claims belong to the signed receiver.
Nothing in a delivery can select argv, environment, directory or a descriptor name.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import time
from types import MappingProxyType

MAX_RESULT = 4096


class ExecutionUnknown(RuntimeError):
    """No confirmed outcome; preserve the receiver claim and never rerun it."""


def _pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError('duplicate result field')
        out[key] = value
    return out


def _constant(_):
    raise ValueError('non-finite JSON result')


def decode_result(raw: bytes, actual_code: int) -> dict:
    """Validate the child channel against the actual process exit, not its logs."""
    if (type(raw) is not bytes or not 0 < len(raw) <= MAX_RESULT or
            type(actual_code) is not int or not 0 <= actual_code <= 255):
        raise ExecutionUnknown('bounded result and actual normal process exit required')
    try:
        value = json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant)
        required = {'schema_version', 'outcome', 'exit_code'}
        if (type(value) is not dict or not required <= value.keys() or
                value.keys() - required - {'reason'} or
                type(value['schema_version']) is not int or value['schema_version'] != 1 or
                type(value['exit_code']) is not int or value['exit_code'] != actual_code or
                value['outcome'] not in ('succeeded', 'failed', 'skipped-by-hold') or
                (value['outcome'] == 'failed') != (actual_code != 0) or
                ('reason' in value and (type(value['reason']) is not str or
                                       not 0 < len(value['reason']) <= 256))):
            raise ValueError('inconsistent process result')
        return {'outcome': value['outcome'], 'exit_code': actual_code}
    except (ValueError, UnicodeError, RecursionError):
        raise ExecutionUnknown('invalid or inconsistent child result') from None


class FixedOutcomeExecutor:
    def __init__(self, argv, *, cwd, env: dict, result_fd_env: str,
                 timeout_s: float, output_limit: int = 1048576):
        if (type(argv) not in (list, tuple) or not 0 < len(argv) <= 64 or
                any(type(a) is not str or not a or '\0' in a or len(a) > 8192 for a in argv) or
                sum(map(len, argv)) > 131072 or not os.path.isabs(argv[0])):
            raise ValueError('bounded fixed argv with an absolute executable required')
        cwd = os.fspath(cwd)
        if type(cwd) is not str or not os.path.isabs(cwd) or not Path(cwd).is_dir():
            raise ValueError('existing absolute trusted working directory required')
        if (type(result_fd_env) is not str or
                not re.fullmatch(r'[A-Z][A-Z0-9_]{0,124}_FD', result_fd_env) or
                type(env) is not dict or result_fd_env in env or len(env) > 256 or
                any(type(k) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', k) or
                    type(v) is not str or '\0' in v or len(v) > 32768 for k, v in env.items())):
            raise ValueError('bounded explicit environment and unused result descriptor name required')
        if sum(len(k)+len(v) for k, v in env.items()) > 131072:
            raise ValueError('environment exceeds bound')
        if (type(timeout_s) not in (int, float) or not 0 < timeout_s <= 86400 or
                not math.isfinite(timeout_s) or type(output_limit) is not int or
                not 0 < output_limit <= 8388608):
            raise ValueError('bounded timeout and output limit required')
        self._argv, self._cwd = tuple(argv), cwd
        self._env = MappingProxyType(dict(env))
        self._fd_env, self._timeout, self._output_limit = result_fd_env, timeout_s, output_limit

    def __call__(self, scheduled_at: int) -> dict:
        if type(scheduled_at) is not int or not 0 <= scheduled_at <= 253402300799:
            raise ValueError('bounded integer UTC slot required')
        # The slot is identity metadata, never an argument interpolation source.
        read_fd, write_fd = os.pipe()
        proc = None
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + self._timeout
        try:
            proc = subprocess.Popen(self._argv, cwd=self._cwd,
                env={**self._env, self._fd_env: str(write_fd)}, stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False,
                pass_fds=(write_fd,), start_new_session=True)
            os.close(write_fd)
            write_fd = None
            for fd, kind in ((read_fd, 'result'), (proc.stdout.fileno(), 'output')):
                os.set_blocking(fd, False)
                selector.register(fd, selectors.EVENT_READ, kind)
            result, output = bytearray(), 0
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ExecutionUnknown('command or inherited descriptors exceeded timeout')
                for key, _ in selector.select(min(remaining, 0.1)):
                    try:
                        chunk = os.read(key.fd, 65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fd)
                    elif key.data == 'result':
                        result.extend(chunk)
                        if len(result) > MAX_RESULT:
                            raise ExecutionUnknown('child result exceeded limit')
                    else:
                        output += len(chunk)
                        if output > self._output_limit:
                            raise ExecutionUnknown('combined child output exceeded limit')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ExecutionUnknown('command exceeded timeout')
            code = proc.wait(timeout=remaining)
            # A normal exit is not completion if ordinary descendants remain.
            try:
                os.killpg(proc.pid, 0)
            except ProcessLookupError:
                pass
            else:
                raise ExecutionUnknown('command left descendants in its process group')
            return decode_result(bytes(result), code)
        except (OSError, subprocess.SubprocessError):
            raise ExecutionUnknown('command invocation or completion unavailable') from None
        finally:
            selector.close()
            os.close(read_fd)
            if write_fd is not None:
                os.close(write_fd)
            if proc is not None:
                cleanup_failed = False
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                except OSError:
                    cleanup_failed = True
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    cleanup_failed = True
                finally:
                    proc.stdout.close()
                if cleanup_failed:
                    raise ExecutionUnknown('child cleanup unconfirmed') from None
