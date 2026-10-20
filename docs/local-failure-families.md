# Scheduled local failure-family collector

The adapter reads a directly typed ConfigSetting with one JSON threshold and
crew-ownership policy. Missing or ambiguous policy is UNKNOWN. Use a desire-path
release whose local failure-families command opens existing databases read-only.
The adapter isolates its child HOME so operator remote-store configuration cannot
redirect the explicit local measurement. It never copies the database or exports
raw commands, arguments or error messages.

The adapter requests only the configured current window, with a fixed
`--max-records 10000` cap. The DP release must support that flag and return
`since`, `until`, `max_records`, `input_records` and `complete` alongside its
existing family counts. A legacy release fails closed; there is no seven-day
scan fallback. Matching exact source instants (including UTC spelling
differences), complete inventory and consistent record accounting are required.
Overflow, stale/mismatched intervals and missing fields refuse the whole tick.
With unsupported normalized identities, known failures still prove a lower
bound: a known family at or above threshold can fire. Below-threshold families
become UNKNOWN, since unsupported records could belong to them. The adapter
returns the existing partial-scope observation contract so absent families
cannot recover. Whole-inventory health remains UNKNOWN (no success-time advance),
even when a conclusive crossing is delivered. A legacy runner that cannot read
the partial envelope refuses rather than treating it as an empty inventory.

A complete empty window permits the existing missing-family recovery path;
ordinary below-threshold counts also resolve through the original frozen alert
identity. The adapter changes no routing or sink and exports only hashed
identities, crew owners and numerical evidence. Its top-level measurement errors
are scrubbed, including child stderr. The row cap does not establish production
query latency, byte volume, normalization coverage or the appropriate threshold.

The once.py entry runs one scheduled emitter tick without a listener. Its state
and metrics parents must be provisioned beforehand. A process lock prevents
overlap; a persistent write budget retains its thirty-second floor across
restarts. Adapter counters and graph setup receipts also persist. Setup reuse
requires directly typed read-back; changed metadata, retraction or graph-delivery
failure requires repair. Startup failure publishes UNKNOWN when the metrics path
is writable.

Opt-in renewal requires a scheduled Alertmanager emitter with generation IDs and
recovery configured. Set renew_interval_s at least as high as the schedule
interval and strictly below the alert TTL. Each renewal requires fresh positive
source evidence and a received firing, and sends the original frozen alert only
to the lease-aware sink. It creates no additional graph, command or outbox event.
UNKNOWN, missing, recovered and old generations cannot renew. Failed renewals
retain backoff across restarts. Lease expiry is not evidence of recovery.
A partial inventory may renew a known ABOVE family's received alert using its
fresh positive evidence; it still cannot renew an UNKNOWN or absent family, and
the whole-inventory measurement status remains UNKNOWN.

Monitor both measurement status and freshness, along with pending outbox and
renewal failures. Source measurement success does not prove receiver delivery.
This source installs no schedule, changes no routing and authorizes no test page.
