# Exact state-key migration recording

Watch the same logical item arrive first as a short BLOCKED key, then as an
UNBLOCKED canonical IRI. Before migration the IRI is a silent baseline and no
event is queued. After the reviewed exact mapping moves the retained historical
state, the same UNBLOCKED observation queues one stable event; replay queues
nothing. No sender is called.

This is an isolated SQLite fixture using the real emitter and migration code,
not a production database, notification receipt or installed-service claim.
Base behavior is from `ed83911f3e24479e671768d27c74d32a0503fe18`;
the candidate migration source is included with this recording. The subsequent
CLI input validation/docs changes do not change the demonstrated transition.

[Output recording and readable transcript](output.log) plus [timing](timing.log)
are retained in this repository. Download both and replay with:

```sh
scriptreplay --log-out output.log --log-timing timing.log
```

Reproduce from the checkout:

```sh
python3 scripts/demo-state-key-migration.py
```

The recording is terminal output, not an inline video. Private operational
mapping files, database snapshots and host identities are excluded.
