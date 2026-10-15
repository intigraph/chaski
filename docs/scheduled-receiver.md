# Signed scheduled delivery bridge

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
