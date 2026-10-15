"""Build an opt-in scheduled receiver from trusted local configuration only."""
from __future__ import annotations

from dataclasses import fields
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from async_scheduled_sink import AsyncScheduledSink
from fixed_outcome_executor import FixedOutcomeExecutor
from scheduled_event_sink import ScheduledEventSink
from scheduled_receiver import ExecutionScope
from shuttle_process_port import ShuttleProcessPort
from slot_receiver import SlotLedger


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise ValueError('complete supported scheduled sink configuration required')


def execution_manifest_sha256(recipe):
    """Hash the literal versioned execution recipe; not executable file bytes."""
    raw = json.dumps({'version': 1, 'execute': recipe}, sort_keys=True,
                     separators=(',', ':'), allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def validate(config):
    """Validate and copy fixed execution policy, without opening a ledger."""
    _keys(config, ('scope', 'ledger', 'shuttle', 'execute', 'workers', 'capacity'))
    _keys(config['scope'], (f.name for f in fields(ExecutionScope)))
    scope = ExecutionScope(**config['scope'])
    scope.validate()
    _keys(config['shuttle'], ('executable', 'config', 'config_sha256', 'timeout_s'))
    shuttle = dict(config['shuttle'])
    # Constructor validation has no subprocess, graph query or key operation.
    ShuttleProcessPort(shuttle['executable'], shuttle['config'], shuttle['config_sha256'],
                       scope, 0, timeout_s=shuttle['timeout_s'])
    _keys(config['execute'], ('argv', 'cwd', 'env', 'result_fd_env', 'timeout_s', 'output_limit'))
    execute = FixedOutcomeExecutor(**config['execute'])
    if execution_manifest_sha256(config['execute']) != scope.manifest_sha256:
        raise ValueError('execution recipe differs from the signed manifest digest')
    workers, capacity = config['workers'], config['capacity']
    if (type(workers) is not int or type(capacity) is not int or
            not 1 <= workers <= capacity <= 64):
        raise ValueError('require 1 <= workers <= capacity <= 64')
    ledger = config['ledger']
    if type(ledger) is not str or not Path(ledger).is_absolute():
        raise ValueError('absolute persistent ledger path required')
    return scope, ledger, shuttle, execute, workers, capacity


def _check_local_files(ledger, shuttle):
    """Early feedback only; the process port rechecks pinned config per phase."""
    parent = Path(ledger).parent
    info = parent.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise ValueError('provisioned owned ledger parent must not be writable by others')
    if os.path.lexists(ledger):
        info = Path(ledger).lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ValueError('existing ledger must be an owned regular file')
    fd = os.open(shuttle['config'], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, 'rb') as source:
        info = os.fstat(source.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or
                info.st_mode & 0o022 or info.st_size > 65536):
            raise ValueError('pinned Shuttle config custody unavailable')
        raw = source.read(65537)
        if len(raw) > 65536 or hashlib.sha256(raw).hexdigest() != shuttle['config_sha256']:
            raise ValueError('Shuttle config differs from pinned bytes')


def validate_emitter(emitter):
    if (emitter.event != 'scheduled' or not emitter.baseline_emits or
            emitter.generation_event_ids or emitter.resolve_recovery or
            emitter.missing_is_from or emitter.renew_interval_s or not emitter.change_driven or
            emitter.externally_triggered or emitter.key != 'item' or
            emitter.from_verdict != 'BLOCKED' or emitter.to_verdict != 'UNBLOCKED' or
            type(emitter.label) is not str or
            not re.fullmatch(r'[A-Za-z][A-Za-z0-9_.-]{0,127}', emitter.label) or
            type(emitter.owner) is not str or not 0 < len(emitter.owner) <= 256):
        raise ValueError('scheduled sink requires canonical non-recovering scheduled events')


def build(emitter, config):
    scope, ledger, shuttle, execute, workers, capacity = validate(config)
    validate_emitter(emitter)
    _check_local_files(ledger, shuttle)
    receiver = AsyncScheduledSink(SlotLedger(ledger), scope,
        lambda slot: ShuttleProcessPort(shuttle['executable'], shuttle['config'],
            shuttle['config_sha256'], scope, slot, timeout_s=shuttle['timeout_s']),
        execute, workers=workers, capacity=capacity)
    try:
        return ScheduledEventSink(receiver, emitter.label, emitter.owner)
    except BaseException:
        receiver.close()
        raise
