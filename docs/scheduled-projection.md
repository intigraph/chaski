# Pinned fixed-interval projection

`scheduled_projection.ScheduledProjection` consumes the versioned declaration
catalogue produced by the graph reader. Its constructor pins one execution
scope, catalogue graph, manifest target IRI, owner, anchor, interval, timezone
and effective UTC time from trusted local admission. The manifest digest stays
inside `ExecutionScope`; a target IRI is not a command or a digest substitute.

Evaluation requires the declared identity to be an explicit verified membership
control. A missing, duplicate, malformed or changed declaration raises; it never
means an empty healthy catalogue. The caller must treat failure as UNKNOWN and
retain its durable deadlines, verdicts and delivery outbox. Disabled declarations
remain explicit BLOCKED records. Their next check is null, so change-feed routing
and bounded catalogue reconciliation must observe subsequent enablement.

The result uses the existing incremental adapter envelope: one scoped record
and its `next_checks` deadline. An already-due eligible slot gets the canonical
signed-run identifier, `UNBLOCKED` and the pinned owner. Configure its emitter
with `event: scheduled`, `baseline_emits: true`, and no generation-based identity
rewriting. Repeated observations deduplicate in the actual emitter. A later
slot advances even while earlier receipts remain pending; success acceptance
and missed-run alerts are separate responsibilities.

Only the current aligned slot is projected, never historical catch-up work.
The first permitted slot is the anchor-aligned occurrence at or after the
explicit effective time. UTC arithmetic preserves fixed elapsed seconds through
DST; it does not implement local calendar recurrence. The caller supplies a
trusted current clock and must preserve UNKNOWN when that clock is uncertain.
There is no persistent clock watermark or backwards-clock policy in this module.

This is a library, not a configured adapter process or service. It performs no
query, write, child execution, receipt verification, graph profile installation
or cron change. The configured catalogue transport, change subscription, sink
factory, bounded executor, deadline-success verifier and activation remain
separate integration work. It does not clear the child-survival or runtime gates.

`python scripts/demo-scheduled-projection.py` demonstrates actual emitter outbox
deduplication and refusal without executing anything. The fixture's runtime
admission fields remain UNKNOWN. `test_scheduled_projection.py` covers activation
boundaries, missing identity retention, pinned scope refusal, disabled state,
time bounds and a real DST transition.

Replay the output-only [recording](demos/scheduled-projection.log) with its
[timing](demos/scheduled-projection.timing) using `scriptreplay`. On base0b4a0eb5
the same demo reports the projection absent and its receiver control present.
