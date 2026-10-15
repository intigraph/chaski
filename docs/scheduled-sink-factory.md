# Opt-in scheduled sink configuration

An emitter may use the exclusive `sink: {scheduled: ...}` kind. It cannot be
combined with a graph, command, JSONL or alert sink. Shuttle's exact terminal
publication receipt is the acknowledgement boundary. Submission or a local
terminal result leaves the emitter's durable row pending.

The emitter requires `trigger: changes`, `event: scheduled`, `baseline_emits:
true`, the default `item` key and BLOCKED-to-UNBLOCKED transition. Generation
rewriting, recovery, missing-as-from and renewal are refused: they would change
the canonical signed slot identity or acknowledge a different protocol.

The scheduled mapping has exactly these fields:

| Field | Meaning |
| --- | --- |
| `scope` | The six immutable `ExecutionScope` fields. |
| `ledger` | Absolute SQLite path in a provisioned, owned persistent directory. |
| `shuttle` | `executable`, `config`, `config_sha256`, `timeout_s` for the existing process port. |
| `execute` | Fixed `argv`, `cwd`, `env`, `result_fd_env`, `timeout_s`, `output_limit`. |
| `workers`, `capacity` | Explicit bounds, with `1 <= workers <= capacity <= 64`. |

The signed scope's `manifest_sha256` must equal
`execution_manifest_sha256(execute)`: SHA256 of UTF-8 canonical JSON
`{"execute": RECIPE, "version": 1}` with sorted keys, compact separators and no
non-finite numbers. The literal recipe includes the complete explicit environment
and resource limits. Changing an argument, environment value or bound requires
a new digest and corresponding admitted declaration. The factory copies the
recipe so later mutation of the input mapping cannot change execution.

This digest binds the recipe, not executable file bytes. Runtime admission must
still verify immutable artifact paths/content, parent directory custody and the
exact declaration/profile/query. The factory does not invent an eligibility
query, register keys or supply a catalogue adapter. Credentials belong in the
approved local execution policy and must never be printed as configuration.

Before opening a ledger, the factory checks configuration shape, emitter policy,
recipe digest, ledger custody and the pinned Shuttle config's bytes/ownership.
The process port checks that config again through an opened descriptor on every
phase. Existing ledgers must be regular owned files, not symlinks. Directory
ancestry and shared-UID isolation remain operator responsibilities.

Orderly consumer exit drains every constructed closeable sink before closing the
emitter database and releasing its consumer lease. Partial startup failures do
the same. A completed worker still needs outbox redelivery before acknowledgement.
Abrupt termination leaves claims unknown; cleanup is not permission to replay
execution. Preserve both the slot ledger and emitter database across rollback.
No claim of survival through supervisor shutdown or host reboot is made.

`python scripts/demo-scheduled-sink-factory.py` uses actual temporary child
processes and SQLite outboxes, with an explicitly synthetic Shuttle executable.
The recorded result shows one hold outcome, one child invocation and four phase
receipts. This is source integration evidence, not live signing, profile loading,
schedule activation or deployment acceptance. The demo requires development test
dependencies because it reuses the isolated process fixture.
