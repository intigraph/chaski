"""Bounded background delivery, with receipt only after signed publication.

An operator supplies one immutable scope and trusted callbacks. Incoming data
may select only its UTC slot. Threads remain in the owning service's cgroup;
this adapter cannot keep a child alive after that service is stopped.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import asdict
import json
import threading

from scheduled_receiver import ExecutionScope, receive, run_id
from slot_receiver import SlotLedger


class DeliveryPending(ConnectionError):
    """No terminal receipt yet; leave the durable emitter row undelivered."""


class AsyncScheduledSink:
    """A sink for complete admitted bindings, not generic emitter envelopes.

    deliver() never waits for a child: it raises DeliveryPending until receive()
    has persisted and published the outcome. A failed worker raises its actual
    error on the next delivery; another delivery then re-enters the durable
    receiver, which permits publication retries but never child replay.

    Capacity counts queued, running AND completed-but-unacknowledged futures.
    A thread pool's unbounded queue therefore never becomes an unbounded job
    queue. The owner must call close() during orderly shutdown. Abrupt death
    leaves claimed slots UNKNOWN; reconcile them, never erase the ledger.
    """

    def __init__(self, ledger: SlotLedger, scope: ExecutionScope, port_factory,
                 execute, *, workers: int = 1, capacity: int = 4):
        scope.validate()
        if (type(workers) is not int or type(capacity) is not int or
                not 1 <= workers <= capacity <= 64):
            raise ValueError("require 1 <= workers <= capacity <= 64")
        if not callable(port_factory) or not callable(execute):
            raise ValueError("trusted port factory and executor required")
        self.ledger, self.scope = ledger, scope
        self.port_factory, self.execute = port_factory, execute
        self.capacity = capacity
        self._lock = threading.Lock()
        self._pending = {}
        self._closed = False
        self._pool = ThreadPoolExecutor(max_workers=workers,
                                        thread_name_prefix="scheduled-receiver")

    def delivery_scope(self):
        return {"receiver": "async-scheduled-v1", **asdict(self.scope)}

    def deliver(self, delivery: dict) -> None:
        if type(delivery) is not dict:
            raise ValueError("complete admitted binding required")
        slot = delivery.get("scheduled_at")
        identity = run_id(self.scope, slot)
        binding = {**asdict(self.scope), "scheduled_at": slot}
        if (json.dumps(delivery, sort_keys=True, allow_nan=False) !=
                json.dumps(binding, sort_keys=True, allow_nan=False)):
            raise ValueError("delivery differs from pinned scope")
        with self._lock:
            if self._closed:
                raise ConnectionError("scheduled receiver is closed")
            future = self._pending.get(identity)
            if future is None:
                if len(self._pending) >= self.capacity:
                    raise DeliveryPending("scheduled receiver capacity reached")
                context = copy_context()
                future = self._pool.submit(context.run, self._receive, slot, binding)
                self._pending[identity] = future
                # Submission is not a durable receipt, even if a fast worker
                # happens to finish before this call exits.
                raise DeliveryPending("scheduled delivery submitted; receipt pending")
            if not future.done():
                raise DeliveryPending("scheduled delivery still pending")
            try:
                future.result()  # terminal publication verified, or real error
            finally:
                del self._pending[identity]

    def _receive(self, slot, binding):
        port = self.port_factory(slot)
        return receive(self.ledger, self.scope, slot, binding, port,
                       lambda: self.execute(slot))

    def close(self):
        """Stop admission and drain workers; call outside the reactor tick."""
        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=True)
