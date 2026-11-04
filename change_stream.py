"""Bounded change-stream delivery; resume advances only after durable inbox commit."""
from __future__ import annotations

import logging
import queue
import threading
import urllib.request
# Compatibility imports remain available to callers and staging controls.
import shutil  # noqa: F401
import tempfile  # noqa: F401
import time

from emitter import ProtocolError
from change_frames import (  # noqa: F401
    ASSEMBLY_SECONDS, DISK_FLOOR_BYTES, FRAME_BYTES, MAX_CURSOR,
    MAX_TRANSACTION_BYTES, MAX_TRANSACTION_PARTS, StagedRecords,
)
from change_frames import frames as decoded_frames, ProtocolError as FrameError

LOG = logging.getLogger("chaski.changes.stream")
BURST_PAGES = 64
BURST_SECONDS = 0.05


def frames(response):
    try:
        yield from decoded_frames(response)
    except FrameError as exc:
        raise ProtocolError(str(exc)) from exc


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
