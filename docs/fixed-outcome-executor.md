# Fixed command outcome adapter

`FixedOutcomeExecutor` is a synchronous callback for the scheduled receiver. A
trusted caller constructs it with fixed absolute argv, an existing directory,
an explicit environment, a result descriptor variable and bounded runtime/output.
Delivery metadata cannot choose those values. The slot is validated as identity
metadata and is never interpolated into the command. The adapter does not inherit
the caller's ambient environment or invoke a shell.

The child receives a writable anonymous descriptor in the configured variable.
It must write one bounded JSON object with `schema_version: 1`, `outcome`, and
`exit_code`; a bounded optional `reason` is accepted and discarded. Supported
outcomes are `succeeded`, `failed`, and `skipped-by-hold`. Failure requires a
nonzero exit, and the other outcomes require zero. The declared exit must match
the actual process exit. Duplicate/extra keys, malformed JSON, missing results,
signals, output overflow, timeout and unconfirmed cleanup raise `ExecutionUnknown`.
Neither exit zero nor a log message is sufficient evidence of success.

The caller passes this callback to `scheduled_receiver.receive`, after the
receiver has durably claimed and admitted the slot. A valid hold remains distinct
from success. Lost publication replays the retained outcome without executing the
child again. Unknown execution preserves the claim for operator disposition;
it must never cause an automatic second execution.

Combined stdout/stderr is drained and discarded under a byte limit. Results have
a separate 4096-byte limit. A new process group permits cleanup of ordinary
descendants, but is not kernel containment: a process that creates a new session
can escape that group. Host execution still needs its approved cgroup/supervisor,
immutable artifact and manifest admission, credential policy and timeout policy.
The adapter does not provide parent-session survival or artifact custody.

Outcome metadata is unsigned. The surrounding signed receiver/port must retain
the documented distinction between signed lifecycle state and unsigned metadata.
This module neither enables a schedule nor installs a production executor.

Run `python scripts/demo-fixed-outcome-executor.py` for isolated child-process
examples. The retained recording shows a real descriptor result, an explicit
hold, a nonzero failure, and an exit-zero child whose missing descriptor result
is refused. Tests also check timeout cleanup and durable no-repeat behavior.
