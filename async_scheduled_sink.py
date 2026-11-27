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
import time

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
                 execute, *, workers: int = 1, capacity: int = 4,
                 delivery_ttl_s: int | None = None, clock=time.time):
        scope.validate()
        if delivery_ttl_s is not None and (type(delivery_ttl_s) is not int or delivery_ttl_s <= 0):
            raise ValueError("positive integer delivery TTL required")
        if (type(workers) is not int or type(capacity) is not int or
                not 1 <= workers <= capacity <= 64):
            raise ValueError("require 1 <= workers <= capacity <= 64")
        if not callable(port_factory) or not callable(execute):
            raise ValueError("trusted port factory and executor required")
        self.ledger, self.scope = ledger, scope
        self.port_factory, self.execute = port_factory, execute
        self.capacity = capacity
        self.delivery_ttl_s, self._clock = delivery_ttl_s, clock
        self._slots = {}
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
            self._evict_expired(keep=identity)
            future = self._pending.get(identity)
            if future is None:
                if len(self._pending) >= self.capacity:
                    raise DeliveryPending("scheduled receiver capacity reached")
                context = copy_context()
                future = self._pool.submit(context.run, self._receive, slot, binding)
                self._pending[identity] = future
                self._slots[identity] = slot
                # Submission is not a durable receipt, even if a fast worker
                # happens to finish before this call exits.
                raise DeliveryPending("scheduled delivery submitted; receipt pending")
            if not future.done():
                raise DeliveryPending("scheduled delivery still pending")
            try:
                future.result()  # terminal publication verified, or real error
            finally:
                del self._pending[identity]
                self._slots.pop(identity, None)

    def _evict_expired(self, keep):
        """aegis-se8: release finished work whose slot is past the delivery TTL.

        The emitter never selects such a firing again, so its unacknowledged
        future would hold capacity forever. receive() persisted the terminal
        result before publication, so nothing durable is lost: a later
        delivery replays from the ledger and never re-runs the child.
        Unfinished work, and the identity being delivered, are never evicted.
        Caller holds the lock.
        """
        if self.delivery_ttl_s is None:
            return
        horizon = self._clock() - self.delivery_ttl_s
        for identity, future in list(self._pending.items()):
            if (identity != keep and future.done()
                    and self._slots.get(identity, horizon) < horizon):
                del self._pending[identity]
                del self._slots[identity]

    def _receive(self, slot, binding):
        port = self.port_factory(slot)
        return receive(self.ledger, self.scope, slot, binding, port,
                       lambda: self.execute(slot))

    def close(self):
        """Stop admission and drain workers; call outside the reactor tick."""
        with self._lock:
            self._closed = True
        self._pool.shutdown(wait=True)
