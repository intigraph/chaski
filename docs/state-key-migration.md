# Exact emitter state-key migration

A producer identity change can turn a real transition into a silent first
observation: the old short key is BLOCKED while the new IRI is baselined as
UNBLOCKED. Do not enable baseline emission globally or replay old deliveries.
This opt-in tool moves only explicitly reviewed mappings; it never queues an
event or calls a sender. No mapping is inferred from a label or arbitrary IRI.

First obtain a consistent backup of the live emitter database through its
owned operations lane. Retain a protected copy, integrity result and checksum.
Prove each mapping against the authoritative reader, not a guessed namespace.
Prepare a `chaski-state-key-map/v1` plan with `mappings`, each containing an
`emitter`, exact `from` short key, exact `to` IRI and `expected` logical-state
fingerprint. The fingerprint is SHA256 of sorted compact JSON containing
`from` and `to` verdict rows with only the volatile `updated` field removed;
absent rows are JSON null. Helper functions `row`, `stable` and `digest` implement
this format. Plans contain no credentials or event payloads.

```sh
chaski-state-key-migrate --db /path/to/emitter.db --plan reviewed-plan.json --dry-run
```

Record rename/merge/no-op/conflict counts and the plan checksum. A canonical-only
row is a no-op, never a fabricated BLOCKED baseline. A target with generation,
event or recovery history, a target deadline, an untracked row or changed
logical state refuses. All mappings are checked before any write. An unknown
or conflicting history needs adjudication, never automatic merging.

After the required independent review and operational approval, use the owned
normal service deployment lane with the exact reviewed script and plan:

```sh
chaski-state-key-migrate --db /path/to/emitter.db --plan reviewed-plan.json \
  --apply --backup /path/to/private/new-backup.db
```

Apply reserves the writer, rechecks state and creates a fresh exclusive0600
SQLite backup before changing any row. It renames verdict/join keys and archives
superseded silent baselines. Event IDs, original payload bytes, generations,
delivery attempts, receipt timestamps/scopes and durable change cursors are
retained. It never creates a transition. A future normal producer observation
may now match the retained historical state; verify the actual scheduled result,
receiver receipt and replay dedup separately. Detect recreated legacy keys and
leave acceptance open if old producers still use them.

An identical applied plan returns an audited no-op even after the next real
transition. A new plan must not overwrite a backup. `--plan-json` is available
for a normal configuration-management transport; it has identical validation.
There is no force flag, default migration, background schedule or implicit plan.

Rollback is an owned service operation: stop all writers, retain the post-apply
state for investigation, restore the consistent snapshot with matching ownership
and WAL/SHM custody, then restart and verify. Never replace the live main file
under an active WAL writer. Restoring a snapshot also rewinds later receipts and
cursors; reconcile that boundary before allowing delivery again. This script
does not perform an automatic restore or authorize a restart.
