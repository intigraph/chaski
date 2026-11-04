# Incremental inbox routing: temporary-write control

Trigger: the existing routing query selects its two oldest entity groups from a
large retained inbox. Without the entity expression index, SQLite builds a disk
GROUP BY sorter even though the returned batch is small.

This recording uses 60,000 synthetic changes, an old-schema fixture and the
additive current-schema migration. The query and input rows are identical. The
output records the SHA-256 of `incremental.py`; the source query is unchanged
from base `6a56c26b69cb2207df3ff03177a5af1f2ab19e37`.

Watch `write_bytes`: the measured SELECT-only process writes fell from
69,849,088 to zero. Both sides returned 10 rows with the same result digest.
The index removes the GROUP BY temporary tree; the small top-batch ORDER BY
remains. This is a Linux synthetic control, not an installed-service result or
a claim that all application writes disappear. Non-Linux runs report unavailable
process-write counters as null. Index construction is a one-time migration cost
excluded from the SELECT measurement.

Reproduce:

```sh
uv run --locked python scripts/demo-inbox-entity-index.py
```

[Readable transcript](inbox-entity-index.txt),
[terminal output recording](inbox-entity-index.typescript),
[timing](inbox-entity-index.timing).
The small sanitized artifacts are retained here alongside existing repository
demos. Playback requires downloading the output/timing files and util-linux
`scriptreplay`; no inline player is assumed:

```sh
scriptreplay --log-out inbox-entity-index.typescript --log-timing inbox-entity-index.timing
```
