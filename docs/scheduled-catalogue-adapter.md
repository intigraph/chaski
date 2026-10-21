# Incremental schedule catalogue transport

`python -m scheduled_catalogue_adapter --config FILE --config-sha256 SHA`
implements the existing `--describe` / `--changes` protocol. The configuration
is an absolute, bounded, owned regular JSON file with no final symlink or group/
other write permission. Each invocation checks the exact bytes before use.

Version 1 configuration has exactly `version`, `projection`, and `reader`.
`projection` contains the documented `ScheduledProjection` fields, including
the six-field scope, graph, target, owner, UTC anchor/interval, IANA timezone
and effective activation time. `reader` contains exactly `executable`, `root`,
`env`, `timeout_s`, and `output_limit`. Paths are absolute; environment is explicit
and bounded; timeout is at most 60 seconds and stdout at most 8 MiB. Stderr is
discarded, never copied into an error or event. No caller-supplied reader argv
is accepted: the only constructed operation is:

```text
ST --root ROOT work jobs --graph GRAPH --expect-job JOB --json list
```

This uses the installed typed declaration reader without executing a TOML job,
opening its history, or using graph-provided command text. The executable and
its import tree must still come from the approved immutable deployment; hashing
adapter configuration does not prove their artifact identity.

Version 2 keeps the outer keys and reader contract, but selects
`WeeklyScheduledProjection`. Its projection pins `scope`, `graph`, `target`,
`owner`, `anchor_s`, `weekday` (Monday=0), `local_time` (`HH:MM`), `timezone`
and `effective_at`; there is no `interval_s` configuration field. The typed
reader's weekly job has `interval_s: null`, `weekday` and `local_time`. Mixing
fixed and weekly forms refuses. The anchor must match its pinned weekly time.
Both `byDay` and `startTime` changes participate in subscription routing.
See [wall-clock deadlines](wall-clock-schedule.md) for DST and refusal policy.

`--describe` returns the pinned graph and all declaration/schedule dependencies,
without invoking ST. Its configuration digest participates in the consumer's
subscription identity so a local policy refresh reconciles immediately.
Matching change routing statically queues the single pinned job, without a
reader call. A due evaluation reads and validates the current catalogue and
emits the current eligible slot plus the next deadline. Discovery also validates
the catalogue before returning membership. Idle ticks perform no reader call.

Input, output, time, JSON keys and JSON numbers are bounded/validated. A missing
identity, nonzero reader exit, timeout, changed scope, malformed result or output
overflow exits 2 with no protocol result. The existing incremental consumer then
retains due work and the previous outbox; it cannot interpret failure as an
empty healthy catalogue. Required membership and runtime admission remain
distinct. The declaration's runtime verdicts must still be explicitly UNKNOWN.

Set the outer emitter adapter timeout above reader timeout plus two seconds of
cleanup and interpreter startup. An external kill is not confirmed cleanup.
Owned process groups provide ordinary cleanup, not kernel containment; service
cgroup/supervisor custody remains required. Version 1 preserves fixed elapsed
intervals. Version 2 recomputes each weekly UTC deadline from its IANA local
date, refusing ambiguous or nonexistent wall times. Broader calendar syntax is
unsupported. No version enrolls a job or authorizes migration by itself.

Keep an emitter label bound to its admitted job identity. Changing the job key
requires a new label or the separately reviewed state-key migration protocol;
do not erase old deadlines, cursors, claims or pending outbox rows. Configuration
refresh is not authority to reinterpret earlier deliveries.

`python scripts/demo-scheduled-catalogue-adapter.py` runs the actual incremental
consumer and reader subprocess against a temporary typed-catalogue fixture.
It shows two initial reads, no extra idle reads, one new read on the next slot,
and retained deadline/outbox after deleting the required identity. The fake
reader asserts exact list-only argv. No graph, job execution, signing key,
profile or schedule is activated. The demo reuses development test fixtures.
