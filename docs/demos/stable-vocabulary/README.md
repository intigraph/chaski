# Builder demonstration

Base `64e484528d1b7f6955dbdff01e9d4e2c2a367464` versus this candidate,
same private Reaction and firing input with an explicitly supplied neutral
instance namespace. Watch vocabulary derived from the instance base disappear while the firing identity
stays identical; an unpublished term refuses. No HTTP, credentials or graph writes.

Reproduce: `python3 docs/demos/stable-vocabulary/demo.py`.
Replay the output-only terminal recording:
`scriptreplay --log-out output.log --log-timing timing.log`.
The small sanitized recording is retained in Git because this repository has no
established external demo destination. This proves source builders, not deployed
shape/native-reader compatibility or instance-allocation policy.
