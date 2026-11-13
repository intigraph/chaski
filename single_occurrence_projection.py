"""One pinned UTC occurrence; existing durable receiver owns execution/publication."""

from dataclasses import dataclass
import math

from scheduled_projection import MAX_TIME, _text, validated_job, validate_pins
from scheduled_receiver import ExecutionScope, run_id


@dataclass(frozen=True)
class SingleOccurrenceProjection:
    scope: ExecutionScope
    graph: str
    target: str
    owner: str
    anchor_s: int
    timezone: str
    effective_at: int
    repeat_count: int

    def __post_init__(self):
        validate_pins(self.scope, self.graph, self.target, self.owner, self.anchor_s, self.timezone, self.effective_at)
        if type(self.repeat_count) is not int or self.repeat_count != 1:
            raise ValueError("one explicit occurrence required")
        if self.effective_at > self.anchor_s:
            raise ValueError("activation cannot reinterpret a historical occurrence")

    def evaluate(self, catalogue, now):
        if type(now) not in (int, float) or not 0 <= now <= MAX_TIME or not math.isfinite(now):
            raise ValueError("bounded trusted current UTC time required")
        job = validated_job(catalogue, self.graph, self.scope)
        fields = {
            "iri",
            "name",
            "revision",
            "owner",
            "enabled",
            "host",
            "target",
            "event",
            "schedule",
            "interval_s",
            "anchor_s",
            "timezone",
            "repeat_count",
        }
        pins = dict(
            revision=self.scope.revision,
            host=self.scope.host,
            target=self.target,
            owner=self.owner,
            anchor_s=self.anchor_s,
            timezone=self.timezone,
        )
        if (
            set(job) != fields
            or type(job["enabled"]) is not bool
            or job["interval_s"] is not None
            or type(job["repeat_count"]) is not int
            or job["repeat_count"] != 1
            or type(job["anchor_s"]) is not int
            or not all(_text(job[k]) for k in ("name", "event", "schedule"))
            or any(job[k] != v for k, v in pins.items())
        ):
            raise ValueError("declaration differs from pinned single occurrence and scope")
        record = dict(item=self.scope.job, owner=self.owner, verdict="BLOCKED")
        deadline = None
        if job["enabled"]:
            if now < self.anchor_s:
                deadline = self.anchor_s
            else:
                # Never advance/rebase the slot. Late execution admission remains
                # the signed receiver's decision; no fresh catch-up identity.
                record.update(
                    verdict="UNBLOCKED",
                    event_id=run_id(self.scope, self.anchor_s),
                    evidence="One declared UTC occurrence; no recurrence or slot rebasing.",
                )
        return dict(version=1, scope=[self.scope.job], records=[record], next_checks={self.scope.job: deadline})
