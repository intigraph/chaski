"""Pinned weekly catalogue projection; existing emitter/receiver owns delivery."""
from dataclasses import dataclass
import math

from scheduled_projection import MAX_TIME, _text, validated_job, validate_pins
from scheduled_receiver import ExecutionScope, run_id
from wall_clock_schedule import WeeklyWallClock


@dataclass(frozen=True)
class WeeklyScheduledProjection:
    scope: ExecutionScope
    graph: str
    target: str
    owner: str
    anchor_s: int
    weekday: int
    local_time: str
    timezone: str
    effective_at: int

    def __post_init__(self):
        validate_pins(self.scope, self.graph, self.target, self.owner,
                      self.anchor_s, self.timezone, self.effective_at)
        WeeklyWallClock(self.weekday, self.local_time, self.timezone, 0).next_after(self.anchor_s)

    def evaluate(self, catalogue, now):
        if (type(now) not in (int, float) or not 0 <= now <= MAX_TIME or
                not math.isfinite(now)):
            raise ValueError('bounded trusted current UTC time required')
        job = validated_job(catalogue, self.graph, self.scope)
        fields = {'iri', 'name', 'revision', 'owner', 'enabled', 'host', 'target',
                  'event', 'schedule', 'interval_s', 'anchor_s', 'timezone',
                  'weekday', 'local_time'}
        pins = dict(revision=self.scope.revision, host=self.scope.host, target=self.target,
                    owner=self.owner, anchor_s=self.anchor_s, timezone=self.timezone,
                    weekday=self.weekday, local_time=self.local_time)
        if (set(job) != fields or type(job['enabled']) is not bool or
                job['interval_s'] is not None or type(job['weekday']) is not int or
                type(job['anchor_s']) is not int or
                not all(_text(job[k]) for k in ('name', 'event', 'schedule')) or
                any(job[k] != v for k, v in pins.items())):
            raise ValueError('declaration differs from pinned weekly schedule and scope')
        record = dict(item=self.scope.job, owner=self.owner, verdict='BLOCKED')
        deadline = None
        if job['enabled']:
            window = WeeklyWallClock(self.weekday, self.local_time, self.timezone,
                                    max(self.anchor_s, self.effective_at)).window(now)
            deadline = window.next_deadline
            if window.scheduled_at is not None:
                record.update(verdict='UNBLOCKED', event_id=run_id(self.scope, window.scheduled_at),
                              evidence='Current weekly wall-clock slot; no historical catch-up.')
        return dict(version=1, scope=[self.scope.job], records=[record],
                    next_checks={self.scope.job: deadline})
