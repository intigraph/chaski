# Expire a stale scheduled backlog

`chaski-scheduled-expiry` (also `python scheduled_expiry.py`) is the reviewed
answer for a scheduled emitter whose old firings can no longer be delivered
and should not be (aegis-se8). A heartbeat that is a day late has no delivery
value, and a slot whose child outcome was lost stays UNKNOWN forever: the slot
ledger refuses to run it again, which is correct, and retries never end.

[`chaski-outbox-disposition`](outbox-disposition.md) cannot expire these rows.
It accepts only firings that were never attempted and never bound to a
receiver, and a scheduled firing is bound before its first send. This tool
covers exactly the other case, for the async scheduled receiver only.

Each undelivered, undisposed firing of the emitter created at or before the
cutoff is classified, and anything else refuses the whole plan:

* `never-claimed`: the slot ledger has no claim for the firing's slot. The
  child was never started through this receiver.
* `claimed-unknown`: the slot ledger has a claim with no result. Whether the
  child ran is UNKNOWN, and stays UNKNOWN.

The plan refuses an unbound row, a receiver other than
`scheduled-event-v1`/`async-scheduled-v1`, an identifier that does not
re-encode exactly from its bound scope, and a slot that already holds a result
(that row is a publication retry, not stale work).

Both classes get an append-only `expired-not-delivered` receipt in
`scheduled_dispositions` (inbox). A `claimed-unknown` claim also gets an
append-only `retired-unknown` receipt in `slot_retirements` (slot ledger);
`SlotLedger.run` then refuses it with `SlotRetired`. A retirement is never a
result, so no replay and no publication follows from it. Originals (rows,
payloads, bindings, attempt counters, claims) are retained unchanged. Delivery,
receiver setup, pending metrics and renewal honour the identity fence, and
`chaski_emitter_outbox_disposed_total` counts the receipts. Startup verifies
that every expired firing is still present, bound, undelivered and unchanged.

1. `plan --db INBOX --slots SLOTS --plan PLAN --emitter LABEL
   --created-before UNIX_SECONDS --expect-never-claimed N
   --expect-claimed-unknown N --reason R --ruling-ref REF` is the dry run. It
   opens both databases read-only, refuses if the counts differ, writes a 0600
   plan with each row's identity hash (immutable fields only, so retry counters
   may move between review and apply) and its slot claim, and prints the plan
   SHA256.
2. Stop the consumer. Back up both databases and record their SHA256.
3. `apply --db INBOX --slots SLOTS --cursor CURSOR --plan PLAN
   --expected-plan-sha256 HASH` takes the consumer lease, re-checks the whole
   reviewed population, then commits the slot receipts and then the firing
   receipts. A changed row, an unplanned row in scope, a resolved claim or a
   conflicting receipt refuses everything. Replaying the identical plan is a
   no-op; a crash between the two commits completes on replay (a retired claim
   whose firing receipt is missing only keeps refusing delivery).
4. `verify --db INBOX [--slots SLOTS]` reads only: every receipt, and every
   retired claim still unresolved with its receipt.

Rollback: restore both backups with the consumer stopped. Never roll back by
restoring an older chaski alone: a consumer without `scheduled_dispositions`
ignores the receipts and retries the expired rows (they would refuse again in
the slot ledger, but would hold receiver capacity). No sink, child or network
call is made by any action.
