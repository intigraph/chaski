# Scheduled local failure-family collector

The adapter reads a directly typed ConfigSetting with one JSON threshold and
crew-ownership policy. Missing or ambiguous policy is UNKNOWN. Use a desire-path
release whose local failure-families command opens existing databases read-only.
The adapter isolates its child HOME so operator remote-store configuration cannot
redirect the explicit local measurement. It never copies the database or exports
raw commands, arguments or error messages.

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

Monitor both measurement status and freshness, along with pending outbox and
renewal failures. Source measurement success does not prove receiver delivery.
This source installs no schedule, changes no routing and authorizes no test page.
