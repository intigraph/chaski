# Release rehearsal

Product candidate `c37b31f` adds the release lane after the packaging baseline
`c56140f`. The recording shows the missing baseline checker, refusal of an
incorrect tag, and a matching local build whose wheel and source archive pass
metadata and checksum checks. No release is published and no host is installed.

Replay the retained terminal capture:

```sh
scriptreplay --timing timing.txt --typescript output.txt --divisor 2
```

Run the probe from this revision with `bash docs/probes/release/run.sh`.
The `v0.1.0` argument is the version at recording time. The source checks also
have regression coverage for mismatched package/lock versions, incorrect
artifact metadata, and unexpected payloads.

CI and account configuration are separate gates. The recorded local result
does not prove hosted publication, trusted-publisher registration or automated
consumer acceptance.
