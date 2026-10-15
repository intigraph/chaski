"""Pinned, read-only ST catalogue transport for Chaski's incremental protocol."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import selectors
import signal
import stat
import subprocess
import sys
import time

from scheduled_projection import MAX_TIME, ScheduledProjection
from scheduled_receiver import ExecutionScope

SCHEMA = 'https://schema.org/'
QUECHUA = 'https://scbrown.github.io/quechua/ns#'
TYPE = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
ATTRIBUTES = [TYPE, *[SCHEMA+x for x in ('name', 'identifier', 'location', 'target',
    'potentialAction', 'eventSchedule', 'repeatFrequency', 'scheduleTimezone', 'startDate')],
    QUECHUA+'sourceKind', QUECHUA+'owner', QUECHUA+'enabled']


def _pairs(pairs):
    out = {}
    for key, value in pairs:
        if key in out:
            raise ValueError('duplicate JSON key')
        out[key] = value
    return out


def _constant(_):
    raise ValueError('non-finite JSON value')


def _float(value):
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('non-finite JSON value')
    return value


def _decode(raw):
    try:
        return json.loads(raw, object_pairs_hook=_pairs, parse_constant=_constant, parse_float=_float)
    except (ValueError, UnicodeError, RecursionError):
        raise ValueError('invalid bounded JSON') from None


def _keys(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValueError('complete supported adapter object required')


def load_config(path, digest):
    if (not Path(path).is_absolute() or type(digest) is not str or
            not re.fullmatch('[0-9a-f]{64}', digest)):
        raise ValueError('absolute pinned configuration required')
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
                info.st_mode & 0o022 or info.st_size > 65536):
            raise ValueError('configuration custody unavailable')
        raw = source.read(65537)
    if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError('configuration changed')
    config = _decode(raw)
    _keys(config, ('version', 'projection', 'reader'))
    if type(config['version']) is not int or config['version'] != 1:
        raise ValueError('adapter configuration version unsupported')
    _keys(config['projection'], ('scope', 'graph', 'target', 'owner', 'anchor_s', 'interval_s',
                                'timezone', 'effective_at'))
    projection = dict(config['projection'])
    projection['scope'] = ExecutionScope(**projection['scope'])
    pinned = ScheduledProjection(**projection)
    reader = config['reader']
    _keys(reader, ('executable', 'root', 'env', 'timeout_s', 'output_limit'))
    for key in ('executable', 'root'):
        if type(reader[key]) is not str or '\0' in reader[key] or not Path(reader[key]).is_absolute():
            raise ValueError('absolute fixed reader paths required')
    if not Path(reader['root']).is_dir():
        raise ValueError('existing reader root required')
    env = reader['env']
    if (type(env) is not dict or len(env) > 64 or any(
            type(k) is not str or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,127}', k) or
            type(v) is not str or '\0' in v or len(v) > 8192 for k, v in env.items()) or
            sum(len(k)+len(v) for k, v in env.items()) > 65536):
        raise ValueError('bounded explicit reader environment required')
    timeout, limit = reader['timeout_s'], reader['output_limit']
    if (type(timeout) not in (int, float) or not 0 < timeout <= 60 or not math.isfinite(timeout) or
            type(limit) is not int or not 0 < limit <= 8388608):
        raise ValueError('bounded reader timeout and output required')
    return pinned, reader


def read_catalogue(projection, reader):
    """The only invocation is ST's explicit graph, required-identity JSON list."""
    argv = [reader['executable'], '--root', reader['root'], 'work', 'jobs',
            '--graph', projection.graph, '--expect-job', projection.scope.job, '--json', 'list']
    proc = None
    selector = selectors.DefaultSelector()
    deadline = time.monotonic()+reader['timeout_s']
    try:
        proc = subprocess.Popen(argv, cwd=reader['root'], env=dict(reader['env']),
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True)
        os.set_blocking(proc.stdout.fileno(), False)
        selector.register(proc.stdout, selectors.EVENT_READ)
        result = bytearray()
        while selector.get_map():
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise ValueError('catalogue reader timed out')
            for key, _ in selector.select(min(remaining, .1)):
                try:
                    chunk = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    selector.unregister(key.fileobj)
                else:
                    result.extend(chunk)
                    if len(result) > reader['output_limit']:
                        raise ValueError('catalogue reader output exceeded limit')
        remaining = deadline-time.monotonic()
        if remaining <= 0 or proc.wait(timeout=remaining) != 0:
            raise ValueError('catalogue reader did not confirm a complete read')
        try:
            os.killpg(proc.pid, 0)
        except ProcessLookupError:
            pass
        else:
            raise ValueError('catalogue reader left descendants')
        return _decode(bytes(result))
    finally:
        selector.close()
        if proc is not None:
            failed = False
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            except OSError:
                failed = True
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                failed = True
            finally:
                proc.stdout.close()
            if failed:
                raise ValueError('catalogue reader cleanup unconfirmed') from None


def describe(projection, config_sha256):
    return dict(version=1, graphs=[projection.graph], attributes=ATTRIBUTES,
                types=[SCHEMA+x for x in ('Action', 'Event', 'Schedule')],
                configuration_sha256=config_sha256)


def changes(projection, reader, request):
    if type(request) is not dict:
        raise ValueError('incremental request required')
    now = request.get('now')
    if (type(now) not in (int, float) or not 0 <= now <= MAX_TIME or not math.isfinite(now)):
        raise ValueError('bounded trusted current time required')
    if set(request) == {'now', 'route'}:
        rows = request['route']
        if (type(rows) is not list or not 0 < len(rows) <= 256 or any(
                type(r) is not dict or r.get('graph') != projection.graph or
                r.get('attribute') not in ATTRIBUTES or type(r.get('entity')) is not str or
                not 0 < len(r['entity']) <= 2048 for r in rows)):
            raise ValueError('bounded matching graph changes required')
        return dict(version=1, items=[projection.scope.job])  # static routing; no read
    if set(request) == {'now', 'discover'} and request['discover'] is True:
        projection.evaluate(read_catalogue(projection, reader), now)
        return dict(version=1, items=[projection.scope.job])
    _keys(request, ('now', 'items', 'changes'))
    if request['items'] != [projection.scope.job] or request['changes'] != []:
        raise ValueError('only the fixed catalogue key may be evaluated')
    return projection.evaluate(read_catalogue(projection, reader), now)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    parser.add_argument('--config-sha256', required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--describe', action='store_true')
    mode.add_argument('--changes', action='store_true')
    args = parser.parse_args(argv)
    try:
        projection, reader = load_config(args.config, args.config_sha256)
        if args.describe:
            result = describe(projection, args.config_sha256)
        else:
            raw = sys.stdin.buffer.read(65537)
            if len(raw) > 65536:
                raise ValueError('incremental request exceeded bound')
            result = changes(projection, reader, _decode(raw))
    except (ValueError, TypeError, OSError, subprocess.SubprocessError):
        print('catalogue adapter UNKNOWN; no result', file=sys.stderr)
        return 2
    print(json.dumps(result, allow_nan=False))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
