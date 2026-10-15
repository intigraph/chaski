# chaski

A post-commit and time-driven **reactor** for a [quipu](https://github.com/scbrown/quipu)
knowledge graph.

In the Inca empire, chaski were the relay runners who carried quipus between
stations. This one carries what the graph learned to the people and systems who
need to act on it.

## What it does

quipu records facts. chaski watches for facts that call for action, and acts:

- **Event rules** fire when a write changes something that matters. For example,
  "a credential was verified and is now dead". chaski follows quipu's durable
  event feed (`GET /events` with a committed consumer offset), so a restart
  resumes exactly where it stopped.
- **Schedule rules** fire when time makes something true. For example, "a
  credential expires within 14 days". quipu has no timers of its own; chaski is
  the scheduler.

Rules are data. Each rule is a SPARQL `SELECT ?focus` whose result is the set of
nodes the rule is firing for **right now**. A node leaving the set resolves the
alert, so alerts resolve on their own.

## Design constraints

- **Public API only.** chaski never reads quipu's storage. A reactor that reads
  a store's internals dies the day the store changes.
- **Liveness is measured by output, not process.** A canary rule must fire every
  cycle. A chaski that is running but not evaluating alerts on itself.
- **Unknown is not "all clear".** A query that times out is counted as
  `unknown`, never as "not firing".
- **Writes are transitions only.** Steady state writes nothing to the graph.
- **Emission reuses Prometheus and Alertmanager.** chaski exports metrics; it
  does not invent notification plumbing.

## Status

Committed-change stream progress is exported as three scalar metrics:
`chaski_changes_stream_events_received_total` counts received SSE pages,
including refused or replayed deliveries; it does not prove processing.
`chaski_changes_cursor_offset` reports the last committed **transaction ID**,
not the legacy `/events` offset, and is `-1` until a commit is observed.
`chaski_changes_poll_fallback_events_total` counts records admitted through
polling, including startup catch-up for an existing cursor. The initial
tail bootstrap discards records and adds nothing to that counter.
Scrapes read in-memory values without touching SQLite or querying the server.
Counters reset on restart. Compare two scrapes from the same process: received
pages plus an advancing committed cursor and no additional poll records prove
stream progress. One INFO line per process names the first committed stream
transaction; later transactions use counters rather than per-page log traffic.

Stage 1: a read-only skeleton, with the event tail, schedule rules, metrics and
the canary. No writes and no pages yet.

Stage 2 (in progress): **emitters** (`emitter.py`). An emitter runs an external
verdict adapter on a schedule and turns its answers into transition events. A
first sighting is a baseline, "unknown" keeps the last verdict, and only a
known blocked-to-unblocked transition emits. Verdict and outbox commit in one
transaction. Delivery is at least once to a receiver that deduplicates on the
adapter's deterministic event id. Configure with `--emitters emitters.yaml`:

```yaml
emitters:
  - label: workitem-unblocked
    command: [python3, /path/to/blocked_by.py]
    schedule: PT15M
    owner: someone
    event: unblocked
    sink: {jsonl: /var/lib/chaski/unblocked.jsonl}
```

## License

Apache-2.0. See [LICENSE](LICENSE).

### Incremental verdict adapters

An emitter can set `trigger: changes` and `schedule: PT24H`. The schedule then
means catalogue reconciliation, not a full verdict scan. The adapter must support
`--describe` (local subscription metadata) and `--changes` (JSON on stdin). Existing
scheduled adapters remain supported.

The version-1 protocol describes `attributes`, `types` and trusted `graphs`.
A request contains `now` plus one of:

- `discover: true`: return `items`, the complete catalogue of candidate keys.
- `route: [...]`: return `items` affected by the supplied fact changes. Retracted
  links carry `old_value`; adapters must include their former owners.
- `items: [...]`: return `scope`, `records` and `next_checks` (one future Unix
  deadline or null per record). Missing keys inside scope become untracked;
  keys outside scope retain their verdict. UNKNOWN preserves the last verdict.

Every response includes `version: 1`. The first discovery starts after a durable
feed-tail checkpoint; subsequent changes cannot fall through the bootstrap gap.
Chaski polls `/changes` with `old_and_new_values` at the existing 60-second floor,
committing the cursor and matching inbox records together. Routing atomically
moves inbox records into a persistent key queue. Two changed subjects and two
ready keys are processed per tick, with changed keys ahead of reconciliation.
Verdicts, future deadlines and transition outbox records commit together. A
failed operation leaves its work for retry, and restart does not repeat discovery.
The existing idempotent receiver and global write budget still govern delivery.

`chaski_changes_lag_transactions`, `chaski_changes_errors_total` and per-emitter
`changes_pending`, `deadlines`, `next_due_timestamp_seconds` expose feed and queue
health. The existing event lag/error signals also include the change feed. Idle
adapters make no queries until a matching change, deadline or reconciliation.
Catch-up is bounded to ten pages of 100 transactions per poll; sustained overload
is visible as lag rather than silently dropping work. Latency includes queueing
and adapter runtime; a large dependency fan-out can take multiple ticks.

Adapter state uses additive SQLite tables in the existing emitter database.
Rollback to a scheduled adapter preserves verdicts and delivered-event identities;
keep that database, and restore the previous code and emitter configuration.

### Authenticated external event sources

An authenticated webhook or local event source can queue an explicit event into
Chaski's existing emitter outbox with `external_event.py`. Authentication belongs
to that source; this local CLI exposes no network listener. Configure an external
emitter with `baseline_emits: true` so the first published event is delivered:

```yaml
emitters:
  - label: release-pin
    trigger: external
    schedule: PT24H # metadata only; no periodic verdict adapter is invoked
    owner: kit
    event: release.published
    from: ABSENT
    to: PUBLISHED
    baseline_emits: true
    sink: {jsonl: /var/lib/example/releases.jsonl}
```

```sh
printf '%s\n' '{"event_id":"repo-kit-1","type":"release.published","payload":{"tag":"v1.2.3"}}' |
  python external_event.py --emitters emitters.yaml --label release-pin --state emitter.db
```

Use the same emitter database and configuration as the running Chaski instance.
The existing runner delivers pending events on its next tick without running a
verdict adapter or scanning remote data. Existing graph, command, alert, retry,
write-budget and receiver-deduplication contracts remain in force. A command
receiver must return nonzero while held or on failed proof: exit zero is its
receipt and acknowledges delivery.

The receipt contains the original source ID only after a synchronous transaction
proves the exact outbox payload. A lost receipt may be retried with identical
bytes; restart and redelivery do not queue a second firing. Changed payload under
an existing source ID refuses. The outbox ID and graph focus use a hash scoped to
the emitter, so arbitrary source identifiers cannot become graph syntax and two
emitters can consume the same source event independently. Graph reaction metadata
records an event trigger for this mode. No webhook or installer is activated by
adding the source code.

### Kind-specific review signals

Verdict adapters may include a bounded `work_kind` token. The emitter freezes it
in the outbox with the event identity, so a retry cannot reclassify an event.
The Alertmanager sink accepts an optional `kind_overrides` map in deployment
configuration. Each exact kind may override only `severity` and `alertname`;
other labels and adapter-supplied severity are ignored. Missing and unknown kinds
retain the emitter's default policy. For example, a deployment can route
`DreamCycle` and `DreamLane` as informational signals while directives remain
warnings. Changing this map is a routing-policy change requiring review.


### Committed transaction stream (opt-in)

`--changes-stream` moves incremental emitter change delivery to the read-only
`/changes/stream` SSE endpoint. Existing configurations keep the poll path.
Bootstrap retains the existing tail-before-discovery behavior. Transaction IDs
are local durable change cursors; they are not `/events/commit` event offsets.
The reader queues one complete frame and waits for the reactor thread to commit
both routed inbox records and the cursor before accepting another. The reactor drains up to64 pages in a50ms burst
(budget checked between commits), then gives adapters/deadlines their turn.
New delivery wakes the reactor instead of waiting for its five-second idle
sleep; only one frame stays queued. Disconnects,
invalid frames and failed local commits retain the last applied cursor and
reconnect with 1–60 second backoff. A frame is bounded to 1 MiB; heartbeat
comments perform no graph read. The network thread never accesses SQLite.

Scheduled rules, incremental adapter deadlines and bounded catalogue
reconciliation retain their existing behavior. This flag does not move the
separate ROOT event-rule consumer to SSE. Closing a network reader can wait for
its bounded 45-second socket timeout. Source tests are not permission to enable
this flag, replay production history or claim natural scheduled acceptance.

### Repeated command failures

`failure_family_adapter.py` consumes the `dp failure-families` JSON report and a directly typed Quipu `ConfigSetting` containing a single JSON description. Policy fields are `default_threshold`, `window_seconds`, `default_owner`, optional `family_thresholds` (hashed family ID to threshold), and `tool_owners` (tool name to crew owner). Invalid/missing/ambiguous policy or failed aggregation exits nonzero, leaving the emitter UNKNOWN. Ownership comes from this graph policy, with an explicit crew fallback; human recipients are refused.

Configure the emitter with `from: BELOW`, `to: ABOVE`, `resolve_recovery: true`, `generation_event_ids: true`, and `missing_is_from: true`. Keep `baseline_emits: false`: first sighting above threshold is a baseline, not an observed crossing. A complete successful rolling-window inventory can resolve absent families; partial observations and failed runs cannot. Unsupported program shapes are a separate disclosed measurement population, not silently attributed to a known family. Each actual crossing gets a generation-bound identity, stable across retries and restarts. Recovery refers to the original frozen firing labels and cannot overtake its receipt. The existing Alertmanager sink retains its finite TTL; scheduled delivery and producer-liveness monitoring are still deployment requirements. This adapter is not scheduled or enabled by the repository itself.

### Exclusive consumer custody

The `chaski.py` CLI takes non-blocking process-lifetime locks before loading
rules or reading its persistent cursor and emitter inbox. A second consumer
using those paths refuses loudly with exit code 2, including `--once`. Use
separate private state paths for an independent diagnostic invocation.

The inbox lock follows the database inode, so symbolic-link and hard-link
aliases do not admit a second owner. The legacy cursor also has a stable
`.consumer-lease` sidecar beside its canonical path. Keep the sidecar in place;
unlinking or replacing a live lease or database can split ownership. Kernel
locks release on process exit, including crashes, and acquisition failures
release any earlier locks. These locks serialize cooperating CLI consumers;
they do not exclude arbitrary privileged tools or authorize stream activation.

Deployments must include `consumer_lease.py` with the matching `chaski.py` pin.

## Development

Python 3.11 or newer and [uv](https://docs.astral.sh/uv/) are required.
The distribution is named `chaski-reactor`; its commands are `chaski`,
`chaski-event` and `chaski-once`.

```sh
uv run --locked python -m pytest
uv run --locked ruff check .
uv build
```

An offline canary exercises the actual reaction engine with controlled query
answers. It fires, preserves its firing state on UNKNOWN, then resolves after a
successful empty answer. It does not connect to a graph or deliver notifications.
The receipt is separate from live consumer state:

```sh
uv run --locked chaski demo status
uv run --locked chaski demo run --focus example-canary
uv run --locked chaski demo status
```

CI builds the wheel and installs it into an isolated environment on Linux and
macOS. Its smoke check verifies module bytes against the wheel RECORD, checks
all console entry points, and reads the measured canary receipt back.

### Scheduled receiver admission (library)

`slot_receiver.SlotLedger` supplies durable, at-most-once admission for a
scheduled command receiver. `slot_id(job, revision, scheduled_at)` identifies a
UTC schedule slot independently of delivery retries. Bind each claim to the
admitted host and immutable manifest. A lost reply replays the stored command
outcome; a crash after claiming leaves `SlotUnknown` and requires reconciliation,
never automatic re-execution. Preserve the ledger across rollback.

This library does not schedule jobs, authenticate senders, admit executable
manifests, sign transitions, or export runs. Those remain the integrating
receiver's responsibilities. A stored outcome is not signed workflow acceptance.
No runtime configuration enables this library automatically.
