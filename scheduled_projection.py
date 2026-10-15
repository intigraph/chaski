"""Pure projection of one pinned declaration; no reads, writes or execution.

The caller supplies a fresh, bounded catalogue from the declaration reader.
Errors must remain UNKNOWN in the caller, preserving its durable queue. This
module neither caches a failed catalogue nor grants executable admission.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from scheduled_receiver import ExecutionScope, run_id

MAX_TIME = 253402300799


def _text(value):
    return type(value) is str and 0 < len(value) <= 2048


@dataclass(frozen=True)
class ScheduledProjection:
    scope: ExecutionScope
    graph: str
    target: str
    owner: str
    anchor_s: int
    interval_s: int
    timezone: str
    effective_at: int

    def __post_init__(self):
        self.scope.validate()
        if not all(_text(v) for v in (self.graph, self.target, self.owner, self.timezone)):
            raise ValueError('bounded pinned graph, target, owner and timezone required')
        if len(self.owner) > 256:
            raise ValueError('owner must fit the pinned receiver recipient')
        for value in (self.anchor_s, self.effective_at):
            if type(value) is not int or not 0 <= value <= MAX_TIME:
                raise ValueError('bounded integer UTC anchors required')
        if type(self.interval_s) is not int or not 0 < self.interval_s <= MAX_TIME:
            raise ValueError('bounded positive fixed interval required')
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError('valid pinned IANA timezone required') from exc

    def evaluate(self, catalogue: dict, now: float) -> dict:
        """One current eligible slot, never a catch-up execution list.

        UTC arithmetic intentionally preserves fixed elapsed seconds through
        DST. Calendar recurrence is not supported. effective_at is trusted
        local activation policy, not a delivery timestamp or graph command.
        """
        if (type(now) not in (int, float) or not 0 <= now <= MAX_TIME or
                not math.isfinite(now)):
            raise ValueError('bounded current UTC time required')
        root = {'schema_version', 'source', 'graph', 'membership', 'jobs',
                'executable_admission', 'signed_run_status', 'firing_eligibility'}
        if (type(catalogue) is not dict or set(catalogue) != root or
                type(catalogue['schema_version']) is not int or
                catalogue['schema_version'] != 1 or catalogue['source'] != 'declarations_only' or
                catalogue['graph'] != self.graph or any(catalogue[k] != 'UNKNOWN' for k in
                ('executable_admission', 'signed_run_status', 'firing_eligibility'))):
            raise ValueError('pinned versioned declaration catalogue required')
        membership, jobs = catalogue['membership'], catalogue['jobs']
        if (type(membership) is not dict or set(membership) != {'status', 'required'} or
                membership['status'] != 'required_identities_verified' or
                type(membership['required']) is not list or
                not 0 < len(membership['required']) <= 256 or
                not all(_text(v) for v in membership['required']) or
                type(jobs) is not list or not 0 < len(jobs) <= 256):
            raise ValueError('bounded catalogue and positive membership controls required')
        required = membership['required']
        identities = [j.get('iri') if type(j) is dict else None for j in jobs]
        if (not all(_text(v) for v in identities) or len(set(identities)) != len(identities) or
                len(set(required)) != len(required) or self.scope.job not in required or
                not set(required) <= set(identities)):
            raise ValueError('missing or duplicate required job identity')
        job = jobs[identities.index(self.scope.job)]
        fields = {'iri', 'name', 'revision', 'owner', 'enabled', 'host', 'target',
                  'event', 'schedule', 'interval_s', 'anchor_s', 'timezone'}
        pins = {'revision': self.scope.revision, 'host': self.scope.host,
                'target': self.target, 'owner': self.owner, 'interval_s': self.interval_s,
                'anchor_s': self.anchor_s, 'timezone': self.timezone}
        if (set(job) != fields or type(job['enabled']) is not bool or
                type(job['anchor_s']) is not int or type(job['interval_s']) is not int or
                not all(_text(job[k]) for k in ('name', 'event', 'schedule')) or
                any(job[k] != v for k, v in pins.items())):
            raise ValueError('declaration differs from pinned schedule and execution scope')
        first = self.anchor_s + max(0, (self.effective_at - self.anchor_s +
                                       self.interval_s - 1) // self.interval_s) * self.interval_s
        record = {'item': self.scope.job, 'owner': self.owner, 'verdict': 'BLOCKED'}
        next_check = first if first <= MAX_TIME else None
        if not job['enabled']:
            next_check = None
        elif now >= first:
            slot = self.anchor_s + (math.floor(now) - self.anchor_s) // self.interval_s * self.interval_s
            record.update(verdict='UNBLOCKED', event_id=run_id(self.scope, slot),
                          evidence='Current eligible UTC slot; historical slots are not replayed.')
            next_check = slot + self.interval_s
            if next_check > MAX_TIME:
                next_check = None
        return {'version': 1, 'scope': [self.scope.job], 'records': [record],
                'next_checks': {self.scope.job: next_check}}
