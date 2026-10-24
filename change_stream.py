"""Bounded change-stream delivery; resume advances only after durable inbox commit."""
from __future__ import annotations

import hashlib
import json
import logging
import queue
import shutil
import tempfile
import threading
import time
import urllib.request

from emitter import ProtocolError

LOG = logging.getLogger("chaski.changes.stream")
FRAME_BYTES = 1 << 20
MAX_CURSOR = (1 << 63) - 1
BURST_PAGES = 64
BURST_SECONDS = 0.05
MAX_TRANSACTION_BYTES = 1 << 30
MAX_TRANSACTION_PARTS = 4096
ASSEMBLY_SECONDS = 300
DISK_FLOOR_BYTES = 64 << 20


class StagedRecords:
    """Verified parts on a private temporary file; iteration holds one part.

    The network reader owns this file until the SQLite owner acknowledges the
    page. No partial transaction is exposed to that owner.
    """
    def __init__(self, file, count):
        self.file, self.count = file, count

    def __len__(self):
        return self.count

    def __iter__(self):
        self.file.seek(0)
        for line in self.file:
            yield from json.loads(line)['records']

    def close(self):
        self.file.close()


def _integer(value, minimum=0):
    return type(value) is int and minimum <= value <= MAX_CURSOR


class _Multipart:
    def __init__(self, body):
        self.tx = body.get('source_tx')
        self.parts = body.get('parts')
        self.records = body.get('records')
        self.bytes = body.get('bytes')
        self.digest = body.get('digest')
        self.source = body.get('source')
        if (not _integer(self.tx, 1) or not _integer(self.parts, 1)
                or self.parts > MAX_TRANSACTION_PARTS or not _integer(self.records, 1)
                or self.parts > self.records or not _integer(self.bytes, 1)
                or self.bytes > MAX_TRANSACTION_BYTES or not isinstance(self.digest, str)
                or len(self.digest) != 64 or any(c not in '0123456789abcdef' for c in self.digest)):
            raise ProtocolError('invalid multipart transaction totals')
        if self.source is not None and (not isinstance(self.source, dict)
                or any(not isinstance(self.source.get(k), str) or not self.source[k]
                       for k in ('store_id', 'log_epoch'))):
            raise ProtocolError('invalid multipart transaction source')
        required = self.bytes + self.parts + DISK_FLOOR_BYTES
        if shutil.disk_usage(tempfile.gettempdir()).free < required:
            raise ProtocolError('insufficient transaction staging disk')
        self.file = tempfile.TemporaryFile(mode='w+b')
        self.started = time.monotonic()
        self.hasher = hashlib.sha256()
        self.index = self.count = self.written = 0

    def check_time(self):
        if time.monotonic() - self.started > ASSEMBLY_SECONDS:
            raise ProtocolError('multipart transaction assembly timed out')

    def part(self, body, data):
        rows = body.get('records')
        if (body.get('source_tx') != self.tx or type(body.get('source_tx')) is not int
                or type(body.get('index')) is not int or body['index'] != self.index
                or self.index >= self.parts or body.get('digest_before') != self.hasher.hexdigest()
                or not isinstance(rows, list) or not rows
                or any(not isinstance(r, dict) or type(r.get('tx')) is not int
                       or r['tx'] != self.tx for r in rows)):
            raise ProtocolError('invalid multipart transaction part')
        encoded = data.encode('utf-8')
        self.written += len(encoded)
        self.count += len(rows)
        if self.written > self.bytes or self.count > self.records:
            raise ProtocolError('multipart transaction exceeds declared totals')
        self.file.write(encoded + b'\n')
        self.hasher.update(encoded)
        self.index += 1

    def commit(self, body, cursor):
        next_tx = body.get('next_tx', self.tx)
        watermark = body.get('watermark_tx', self.tx)
        if (cursor != self.tx or self.index != self.parts or self.count != self.records
                or type(next_tx) is not int or next_tx != self.tx
                or not _integer(watermark) or watermark < next_tx
                or self.written != self.bytes or self.hasher.hexdigest() != self.digest
                or any(type(body.get(k)) is not int or body[k] != value
                       for k, value in (('source_tx', self.tx), ('parts', self.parts), ('records', self.records)))
                or body.get('digest') != self.digest or body.get('source') != self.source):
            raise ProtocolError('invalid multipart transaction commit')
        self.file.flush()
        records = StagedRecords(self.file, self.records)
        self.file = None  # Ownership transfers only after the complete barrier.
        return {**body, 'records': records, 'next_tx': next_tx, 'watermark_tx': watermark}

    def close(self):
        if self.file is not None:
            self.file.close()


def _sse_frames(response):
    """Parse bounded SSE envelopes without changing transaction state."""
    fields, size = {}, 0
    while True:
        line = response.readline(FRAME_BYTES + 1)
        if not line:
            if fields:
                raise ProtocolError("truncated stream frame")
            return
        size += len(line)
        if size > FRAME_BYTES:
            raise ProtocolError("stream frame exceeds budget")
        line = line.rstrip(b'\r\n')
        if not line:
            if fields:
                yield fields
            else:
                yield None  # Heartbeats still enforce an open assembly's deadline.
            fields, size = {}, 0
            continue
        if line.startswith(b':'):
            continue
        key, sep, value = line.partition(b':')
        if not sep:
            raise ProtocolError("invalid stream field")
        key, value = key.decode('ascii'), value.removeprefix(b' ').decode('utf-8')
        if key not in {'event', 'id', 'data'} or key in fields:
            raise ProtocolError("duplicate or unsupported stream field")
        fields[key] = value


def _cursor(fields):
    raw = fields.get('id', '')
    if not raw.isascii() or not raw.isdecimal() or int(raw) > MAX_CURSOR:
        raise ProtocolError('invalid transaction stream ID')
    return int(raw)


def frames(response):
    """Yield single pages or fully verified multipart transactions, never parts."""
    stage = None
    try:
        for fields in _sse_frames(response):
            if stage is not None:
                stage.check_time()
            if fields is None:
                continue
            event = fields.get('event')
            keys = {'event', 'id', 'data'} if event in {'quipu.changes', 'quipu.changes.commit'} else {'event', 'data'}
            if set(fields) != keys:
                raise ProtocolError('unexpected change stream fields')
            body = json.loads(fields['data'])
            if not isinstance(body, dict):
                raise ProtocolError('invalid transaction stream page')
            if event == 'quipu.changes' and stage is None:
                yield _cursor(fields), body
            elif event == 'quipu.changes.begin' and stage is None:
                stage = _Multipart(body)
            elif event == 'quipu.changes.part' and stage is not None:
                stage.part(body, fields['data'])
            elif event == 'quipu.changes.commit' and stage is not None:
                cursor = _cursor(fields)
                page = stage.commit(body, cursor)
                stage.close()
                stage = None
                yield cursor, page
            else:
                raise ProtocolError('unexpected change stream event')
        if stage is not None:
            raise ProtocolError('truncated multipart transaction')
    finally:
        if stage is not None:
            stage.close()


class ChangeStream:
    """One network reader, one queued frame; SQLite stays on the reactor thread."""
    def __init__(self, quipu, cursor):
        self.quipu, self.cursor = quipu, cursor
        self.pending = queue.Queue(maxsize=1)
        self.errors = 0
        self.received = 0
        self.logged_first_commit = False
        self.stop = threading.Event()
        self.wakeup = threading.Event()
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        backoff = 1
        while not self.stop.is_set():
            try:
                req = urllib.request.Request(
                    self.quipu.base + '/changes/stream?since=' + str(self.cursor),
                    headers={'Accept': 'text/event-stream', 'X-Quipu-Client': 'chaski',
                             'Last-Event-ID': str(self.cursor)})
                # Heartbeats arrive every15s; a stalled connection reconnects boundedly.
                with urllib.request.urlopen(req, timeout=45) as response:
                    if response.headers.get_content_type() != 'text/event-stream':
                        raise ProtocolError('change stream has wrong content type')
                    for stream_id, page in frames(response):
                        try:
                            done = threading.Event()
                            self.pending.put((stream_id, page, done), timeout=45)
                            self.wakeup.set()
                            while not done.wait(1):
                                if self.stop.is_set():
                                    return
                            if stream_id != self.cursor:
                                raise ProtocolError('delivery was not durably applied')
                            backoff = 1
                        finally:
                            if isinstance(page.get('records'), StagedRecords):
                                page['records'].close()
                raise ProtocolError("change stream disconnected")
            except Exception as exc:
                self.errors += 1
                LOG.warning("change stream UNKNOWN; applied cursor retained: %s", exc)
                # No cursor advancement on transport, parser or consumer failure.
                if self.stop.wait(backoff):
                    return
                backoff = min(60, backoff * 2)

    def pump(self, feed, max_pages=BURST_PAGES, budget_s=BURST_SECONDS):
        """Drain a bounded burst, then give adapters/deadlines their reactor turn.

        One queued frame is retained. Briefly wait for the next acknowledged
        frame so a ready backlog does not consume one five-second tick per tx.
        """
        if max_pages <= 0 or budget_s <= 0:
            raise ValueError("stream burst budgets must be positive")
        self.wakeup.clear()
        deadline = time.monotonic() + budget_s
        applied = 0
        for index in range(max_pages):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                if index == 0:
                    stream_id, page, done = self.pending.get_nowait()
                else:
                    stream_id, page, done = self.pending.get(timeout=remaining)
            except queue.Empty:
                break
            # Receipt counts deliveries, including refusals and replay. It is
            # not an acknowledgement; only accept_page's commit advances cursor.
            self.received += 1
            try:
                feed.accept_page(page, self.cursor, stream_id=stream_id)
                # Successful return means SQLite committed both inbox and cursor.
                self.cursor = stream_id
                if not self.logged_first_commit:
                    LOG.info("stream path committed tx=%s", stream_id)
                    self.logged_first_commit = True
                applied += 1
            except Exception as exc:
                feed.errors += 1
                LOG.warning("change delivery UNKNOWN; applied cursor retained: %s", exc)
                return applied
            finally:
                done.set()
        return applied

    def wait(self, timeout):
        """Wake the owner thread when delivery arrives; no graph-read polling."""
        if not self.pending.empty():
            return True
        self.wakeup.clear()
        # A producer can enqueue between the first empty check and clear.
        if not self.pending.empty():
            return True
        return self.wakeup.wait(timeout)

    def close(self):
        self.stop.set()
        self.wakeup.set()
