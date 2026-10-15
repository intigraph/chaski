# Signed scheduled delivery bridge

## Bounded asynchronous handoff

`async_scheduled_sink.AsyncScheduledSink` runs the existing signed receiver
on a bounded thread pool. The operator supplies one immutable `ExecutionScope`,
the persistent ledger, a trusted `port_factory(scheduled_at)`, and a trusted
`execute(scheduled_at)` callback. Delivery data must repeat the exact scope and
integer UTC slot; it cannot supply commands or choose a different identity.
The callback must enforce the admitted command's runtime and output limits.

`deliver(binding)` raises `DeliveryPending` after submission and while work is
running. It returns only on a later call after the receiver verifies terminal
publication. Existing emitter delivery therefore keeps its durable outbox row
undelivered and retries with its normal backoff. Pending counts as a delivery
failure in existing metrics; it does not mean the child failed. A terminal hold
or failed child can have a successful delivery receipt for its actual outcome.
The emitter's retry cadence bounds receipt latency independently of completion.

Capacity includes queued, running, and completed work awaiting receipt. Duplicate
in-flight deliveries reuse one future. Worker exceptions reach the next delivery
unchanged; later retries use the persistent slot ledger, so publication may retry
but a claimed child cannot run again. Context variables are copied on submission,
including any strict-admission context set by the caller.

The owner calls `close()` outside the reactor tick to stop admission and drain
workers. This can wait for the full callback duration. Abrupt termination leaves
claims UNKNOWN. Threads and children remain inside the owning service's cgroup;
this adapter cannot provide survival across that service's exit.

The asynchronous receiver accepts complete bindings. `ScheduledEventSink` below
converts native emitter envelopes into that binding. Production configuration
still needs a validated catalogue projection and trusted fixed-command executor.
No sink factory or production configuration enables either class automatically.
Do not acknowledge an upstream delivery merely because a future was submitted.

Reproduce the blocked-child/receipt/capacity controls with
`python -m pytest -q test_async_scheduled_sink.py`. These use real threads,
SQLite claims and the actual emitter outbox, with synthetic signing callbacks.
They do not establish installed signing, schedule coverage or natural firings.

### Native emitter envelopes

Wrap the asynchronous receiver with
`scheduled_event_sink.ScheduledEventSink(receiver, emitter, recipient)`. The
operator pins the emitter label, human routing recipient and receiver scope.
The adapter accepts the existing emitter's `scheduled` envelope only when its
item and routing match those pins. It decodes the slot from the canonical
`scheduled-v1-` event identifier and re-encodes the complete expected scope to
verify exact equality. Alternate encodings, foreign scopes, duplicate or extra
JSON fields, missing fields, boolean slots and unbounded timestamps refuse
before the asynchronous receiver is called. Delivery timestamps and emitter
generation cannot replace the frozen scheduled slot.

The adapter exposes a stable delivery scope containing the emitter, recipient
and inner receiver scope. Existing outbox delivery freezes this scope before
its first call, preserving the original binding across retries. A scheduling
adapter must emit `run_id(scope, scheduled_at)` as its deterministic `event_id`
and use `event: scheduled`; it must not set `generation_event_ids`, which
replaces that identifier with a hash. Set `baseline_emits` when an already-due
slot should emit during bootstrap. These options require the reviewed schedule
projection; this envelope adapter cannot infer a schedule or admit execution.

`test_scheduled_event_sink.py` uses actual `observe` and `deliver_pending` with
the durable outbox, real worker threads and slot ledger. Signing callbacks are
synthetic. It verifies one blocked execution, no early receipt and terminal
publication before acknowledgment, alongside rejected scopes and malformed
events. All execution admission remains the trusted signed port's job.

Replay `python scripts/demo-scheduled-event.py`, or download the output-only
[terminal recording](demos/scheduled-event.log) and [timing](demos/scheduled-event.timing)
for `scriptreplay`. The demo prints actual outbox and receipt state with
synthetic signing callbacks; it does not contact a service or run a real job.

`scheduled_receiver.receive` connects an already admitted delivery to the
persistent `SlotLedger` and a trusted `SignedRunPort`. It is a library entry
point: importing it starts no listener, timer, scheduler or child process.

The port checks host, immutable manifest, declaration eligibility, existing
local key and registered workflow scope. Only after admission does the ledger
commit a unique slot claim. The port must durably persist signed `started`
before execution, then the actual signed terminal outcome. Success, failure
and hold are distinct outcomes; contradictory exit codes refuse. A crash or
exception after claiming leaves UNKNOWN and never authorizes a second child.

The complete job, revision, host, manifest digest, agent, workflow and scheduled
UTC slot are encoded reversibly into the run identifier covered by the
signature. Delivery must repeat that exact binding, including value types.
This avoids relying on an unsigned hash lookup to interpret signed scope.

The terminal result is committed before publication. If publication or its
acknowledgement fails, redelivery rechecks admission and retries publication
only. It never calls the child or signs a second execution. The ledger must
survive rollback. Changed manifest/scope for a claimed slot is a conflict.

A concrete port must export and independently read back the exact signed
started and terminal records against the admitted registered public key. A
successful callback return or local ledger result alone is not governed
acceptance. Registered-key integrity does not establish an isolated human or
agent identity on a shared Unix account. Transport authentication, manifest
compilation, schedule projection, maintenance policy and deadline monitoring
remain separate integration requirements. No port or runtime activation is
provided by this library module.

Tests use an explicitly synthetic port double to check sequencing, scope
refusal, crash/exception uncertainty and publication-only replay. They do not
claim cryptographic or live graph verification. A concrete adapter needs its
own actual-signature and export/read-back tests before it may execute a job.

The local receipt's exit code is metadata. Shuttle's current signature format
covers the transition's state and step, not arbitrary receipt payload. A port
must not describe an unsigned exit-code field as cryptographically bound.

Demo: [recording](demos/scheduled-receiver.log) and
[timing](demos/scheduled-receiver.timing), reproduced with
`python scripts/demo-scheduled-receiver.py`. On base707e0d9 this bridge module
is absent; its existing ledger control already suppresses a duplicate callback.
The proposal adds the displayed started/terminal/publication ordering and a
reversible run scope. The synthetic port is explicitly a double: no real keys,
signatures, child processes or graph writes occur. The one child callback on
two deliveries is observable; duplicate delivery invokes only admission and
publication the second time. The small recording is retained with this source
proposal, replayed using `scriptreplay --log-out scheduled-receiver.log
--log-timing scheduled-receiver.timing`. It requires downloading both files;
no inline video playback is claimed.

## Separate Shuttle environment

`shuttle_process_port.ShuttleProcessPort` implements the same four-phase port
through a fixed absolute Shuttle CLI path. Supply an absolute local config path,
its SHA256, the admitted `ExecutionScope` and UTC slot. Each call uses
`--require-existing-keys scheduled <phase>`, JSON on stdin and a bounded timeout;
a delivery cannot choose executable, arguments, config, graph query or key.
The config must be a regular file owned by the current Unix user, at most 64KiB,
and not writable by group or others. The final component must not be a symlink.
The hashed opened descriptor is passed to Shuttle through `/dev/fd`, so replacing
the config pathname does not substitute different bytes. The executable and
parent directories require operator custody; this is not isolation from another
process running as the same user or an executable hash admission mechanism.

The CLI must return its exact verified phase receipt. Nonzero exit, timeout,
oversized output, an empty success, wrong phase or malformed receipt is UNKNOWN
and cannot authorize execution. The receiver's existing claim/replay behavior
still applies: failure after claim does not rerun the child. A successful
`publish` means the Shuttle SDK accepted its exact registered-signature read-back,
not merely that this process returned zero.

This library does not launch a background worker or make a long child safe to
execute in a synchronous command sink. An asynchronous domain execution handoff
and installed child-survival acceptance are separate runtime prerequisites.

Run `python -m pytest -q test_shuttle_process_port.py` for real process and file
descriptor controls. Its CLI is explicitly a fixture that records packets and
returns phase receipts; it does not prove real keys, registration, graph export,
macOS acceptance or production scheduling. Those require the installed Shuttle
SDK and the scoped runtime smoke.

Process bridge demo: `python scripts/demo-shuttle-process.py` shows two deliveries,
one execution callback and config drift refusal using actual subprocesses and
a durable ledger. [Recording](demos/shuttle-process.log) and
[timing](demos/shuttle-process.timing) retain that fixture-only limitation.
