# Single-occurrence adapter demo

Base `64e484528d1b7f6955dbdff01e9d4e2c2a367464` refuses adapter version3.
The candidate accepts the same pinned configuration, sets one deadline, and
retains the original run identity after that deadline without recurrence.
Candidate projection SHA256: `1fc27c0205159f6fe00f5b94ea2ae07cc876a29e29cf0eaa32f9e6a35606af1e`.

[Recording](single-occurrence.log), [timing](single-occurrence.timing).
Download both and replay with `scriptreplay --log-out single-occurrence.log --log-timing single-occurrence.timing`.
The log is a readable transcript too; no inline player is claimed.

```sh
git show 64e484528d1b7f6955dbdff01e9d4e2c2a367464:scheduled_catalogue_adapter.py > /tmp/catalogue-base.py
python scripts/demo-single-occurrence.py --before /tmp/catalogue-base.py
```

Uses the real config loader plus a pure typed-catalogue fixture. Before loader
uses current sibling imports; no reader process, graph, signing key or receiver
is invoked. This is source behavior, not runtime execution or admission.
The small sanitized recording follows the repository's existing demo storage.
