# Resolving a verified recovery

An emitter can opt into explicit recovery delivery with `resolve_recovery: true`.
It is off by default. Only verdict emitters with graph, Alertmanager or JSONL
sinks support it. External events and command sinks refuse this setting; a
recovery must not execute domain work that expects a new firing.

A verified non-firing verdict or an authoritative removal records its first
recovery time. UNKNOWN does not create a recovery or invent a verdict. Only a
previous firing with a confirmed delivery receipt is eligible. Never-delivered
and indeterminate sends remain in their original outbox; if their exact original
receipt arrives later, the recorded recovery can then close that identity.

Enabling recovery forces one fresh complete key discovery. That verified discovery
can also recover a previously delivered identity retired before upgrade. Failed
discovery creates no recovery; the normal disabled subscription digest is unchanged.

Recovery has its own durable `recovery_outbox`. Original firing payloads and
receipts remain unchanged. A unique prior-event key and an atomic reservation
make repeated observations, outer cursor transactions and restart replay safe.
Queueing is limited to five records per tick, with indexes that exclude already
reserved original rows. Firing and recovery delivery share the existing global
budget and backoff. Existing outbox health metrics include recovery debt.

Before a new firing's first external send, the outbox durably binds its receiver
scope in a separate `delivery_scope` column, without rewriting the event payload.
It includes the Alertmanager endpoint and emitter, graph endpoint/namespace/reaction,
JSONL absolute resolved path, or the complete ordered chain of those bindings.
Retries refuse changes, including a same-length chain replacement or reorder.
Credentials are not part of the binding; credential rotation at the same endpoint
does not change receiver identity. A changed backing receiver at an unchanged URL
cannot be detected by this binding and requires operator provenance.

Recovery and every frozen-receipt retry require that original binding to match.
An empty newly configured receiver is never evidence that the original firing ended.
Pre-upgrade confirmed deliveries without a binding retain unresolved recovery debt;
there is no automatic historical-endpoint inference or migration attestation.
An unbound original row with previous attempts may still retry its original firing
under the legacy contract, but it remains unbound and cannot justify recovery.
Future migration needs an independently reviewed historical-receiver attestation.

Before the first close write, the complete sink chain reads and freezes the
original identities into the recovery row. Alertmanager uses the actual prior
label set, including its original start time, rather than today's routing
configuration. An already absent alert requires no fabricated labels or POST;
the received check still verifies absence. Missing, malformed or ambiguous
identity refuses delivery. The graph checks the exact prior firing and updates
only its existing typed `resolvedAt` predicate. The original start, event ID and
provenance remain. An earlier authoritative close is preserved.

A lost write response retains the row for retry with the same frozen body.
Successful HTTP submission alone is insufficient: both sinks check the receiver.
The graph reader requires the structured `xsd:dateTime` value returned by the
current API. A plain string or wrong datatype is UNKNOWN, not a successful close.

The separate table also protects rollback: an older firing-only binary cannot
read a pending recovery as a new alert. It will leave that queue undrained.
Operators must account for that debt before declaring rollback complete; retain
the database and never erase queued rows or manually clear alerts to make a
verification pass. Disabling the option also preserves pending recoveries.

Source tests do not activate emitters. Adoption still requires a reviewed pinned
deployment, followed by an actual scheduled firing-to-recovery observation at
both receivers and a positive still-firing control.
