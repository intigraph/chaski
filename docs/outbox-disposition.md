# Expire retained firings without delivery

`chaski-outbox-disposition` (also `python emitter.py`) supports an explicit,
reviewed, append-only `expired-not-delivered` disposition. Original outbox rows,
payloads, attempts, receiver bindings, delivery timestamps and cursors remain
unchanged. A disposition is not a receiver receipt or acknowledgement. Recoveries
require separate adjudication and cannot use this command.

Create a private plan from an existing database with `plan --db INBOX --plan PLAN
--emitter LABEL --created-before UNIX_SECONDS --expected-count COUNT --reason
REASON --ruling-ref REFERENCE`. This opens SQLite read-only and holds a consistent
read transaction. The plan records every exact event ID and full-row hash, scope,
cutoff, count, reason, ruling reference and source UTC. It contains no event
payloads, but its identities may still be private. The command writes an exclusive
0600 plan file and prints its SHA256. Review that exact file and hash through the
normal deployment process; a free-text ruling reference does not authenticate or
authorize the caller.

Only after the consumer is quiesced and the deployment's hold-release admission
is recorded, run `apply --db INBOX --cursor CURSOR --plan PLAN
--expected-plan-sha256 HASH`. This takes the same exclusive cursor/inbox lease as
the consumer and appends in one SQLite transaction. Every selected row must still
be undelivered, never attempted, unbound and not recovery-queued; the exact scoped
population and every full-row hash must match. Changes, duplicates, foreign scope,
recovery payloads and conflicting prior decisions refuse the whole append.
Replaying the identical plan is idempotent. No sink or network call occurs.

`list --db INBOX` lists retained receipts (up to 10,000; a larger population
refuses). `verify --db INBOX` checks original row hashes and eligibility read-only.
Startup verifies the same contract; missing or changed originals refuse startup.
The runtime fences by event identity even if a row drifts after startup, so a
changed fingerprint never silently releases it. Delivery, receiver setup,
receiver binding, write-budget scheduling, wakeups, renewal and recovery selection
honor this fence. Pending/oldest-pending metrics exclude disposed rows;
`chaski_emitter_outbox_disposed_total` reports them separately. Delivered metrics
continue to count only actual delivery receipts. Supported key migration refuses
to rename an identity with a disposition.

## Rollback and custody

An older consumer that does not understand this ledger can send retained rows
again. Neither installing the ledger alone nor restoring such a binary is a safe
rollback. Keep the delivery hold asserted while preparing the ledger and moving
to the expiry-aware consumer. Before releasing the hold, rehearse the same source
revision and name either an expiry-aware rollback target or the explicit operation
that re-asserts the hold before any older consumer starts. The CLI's lease prevents
an overlapping cooperating consumer; it cannot confer this operational admission
or control a legacy process that ignores custody.

Never erase originals, mark them delivered, reset a cursor, forge a historical
receiver binding, or disable the ledger to manufacture a clean queue. New events
outside the exact plan retain the normal delivery contract.
