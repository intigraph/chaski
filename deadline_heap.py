"""Replaceable/cancellable deadlines; the owner thread alone mutates this heap."""
from __future__ import annotations

import heapq
import itertools
import math


class DeadlineHeap:
    def __init__(self):
        self._heap = []
        self._current = {}
        self._serial = itertools.count()

    def set(self, key, due):
        if type(due) not in (int, float) or not math.isfinite(due):
            raise ValueError('finite deadline required')
        entry = (due, next(self._serial), key)
        self._current[key] = entry
        heapq.heappush(self._heap, entry)
        self._compact()

    def cancel(self, key):
        self._current.pop(key, None)
        self._compact()

    def _compact(self):
        # Lazy cancellation keeps ordinary edits O(log n); bound obsolete entries
        # under repeated edits of a far-future job as well as at the heap head.
        if len(self._heap) > 2 * len(self._current) + 64:
            self._heap = list(self._current.values())
            heapq.heapify(self._heap)

    def peek(self):
        while self._heap and self._current.get(self._heap[0][2]) != self._heap[0]:
            heapq.heappop(self._heap)
        return self._heap[0][0] if self._heap else None

    def pop_due(self, now):
        while self.peek() is not None and self.peek() <= now:
            due, _, key = heapq.heappop(self._heap)
            del self._current[key]
            yield key, due

    def __len__(self):
        return len(self._current)
