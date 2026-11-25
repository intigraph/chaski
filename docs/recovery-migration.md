# One-time recovery scope migration

`chaski-recovery-migration` (also `python recovery_migration.py`) is the reviewed,
one-time answer for recovery rows that were frozen before chaski #79 added
`vocabulary` to the Quipu sink's delivery scope (aegis-ssvr). Those rows refuse
forever with "prior delivery receiver scope changed or unproven" even though
the receiver did not move. The scope check is NOT relaxed: there is no runtime
rule that treats a scope without `vocabulary` as equal to the current one.

Each undelivered, undisposed recovery is classified, and anything else refuses
the whole plan:

* `refreeze`: every Quipu sink in the frozen scope is exactly
  `{kind, base, namespace, reaction}`. The migration adds
  `vocabulary: aegis-legacy/v1` to those sinks in the recorded scope and changes
  nothing else. The normal delivery path then compares scopes exactly as before,
  so a receiver on another vocabulary still refuses.
* `null-scope`: no frozen scope (legacy rows from before scope binding).
* `missing-prior`: the last attempt could not prove the prior graph firing.

`null-scope` and `missing-prior` rows are expired, not delivered: an append-only
receipt in `recovery_dispositions` fences them by identity from delivery,
scheduling and the pending/oldest-pending metrics, and counts them in
`chaski_emitter_outbox_disposed_total`. Originals are retained. Startup verifies
that every expired recovery is still present, undelivered and unchanged.

1. `plan --db INBOX --plan PLAN --expect-refreeze N --expect-null-scope N
   --expect-missing-prior N --reason R --ruling-ref REF` is the dry run. It opens
   SQLite read-only, refuses if the counts differ, and writes a 0600 plan with
   each row's identity hash (immutable fields only, so retry counters may move)
   and, for `refreeze`, the expected hash after the change. It prints the plan
   SHA256.
2. Back up the inbox (record its SHA256) and hold the consumer.
3. `apply --db INBOX --cursor CURSOR --plan PLAN --expected-plan-sha256 HASH
   --log NEW_LOG` takes the consumer lease and applies the whole plan in one
   transaction. A changed row, a recovery outside the plan or a conflicting
   receipt refuses everything. The log gets one JSON line per row (class,
   action, before/after hash). Replaying the identical plan is a no-op.
4. `verify --db INBOX` reads only: verified receipts and the remaining
   undelivered recoveries by class.

Rollback: restore the backup with the consumer still held. Never roll back by
restoring an older chaski pin alone: a consumer without `recovery_dispositions`
ignores the expiry receipts and retries the expired rows, and the refrozen rows
keep their new scope. No sink or network
call is made by any action.
