# Outbox expiry demonstration

This terminal recording uses disposable synthetic SQLite state and the real
`deliver_pending` implementation. An obsolete firing delivers on base
`64e484528d1b7f6955dbdff01e9d4e2c2a367464`; after its explicit disposition on
implementation `7650ae4`, it remains undelivered with zero attempts. A fresh event
still delivers on both revisions. No sink network, deployment or private state
participates; the sink is an in-process receipt fixture.

[Readable transcript](outbox-disposition.log) and
[timing file](outbox-disposition.timing) are retained with this source as a small
review artifact. Replay after downloading both files:

```sh
scriptreplay --log-out outbox-disposition.log --log-timing outbox-disposition.timing
```

Playback was verified with util-linux `scriptreplay`; no inline video embed is
claimed. Reproduce from the implementation checkout, with a separate export of
the exact base:

```sh
DEMO_BASE_SOURCE=/path/to/base python3 scripts/demo-outbox-disposition.py
```

Later commits adding this recording and caption do not change the demonstrated
implementation. This source fixture demonstrates delivery behavior, not installed
acceptance or representative backlog rehearsal.
