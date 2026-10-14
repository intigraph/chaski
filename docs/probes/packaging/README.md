# Packaging and installed canary demonstration

The same `uv build` operation fails on base `4f5e7914a6d0e31f6de7d54a50b73dc5e0ab3e55` because it has no packaging metadata. Candidate `3e6c7ba` builds both artifacts; an isolated wheel installation then verifies its module RECORD, three console scripts, and a measured canary round trip. Later commits add only this evidence.

This is a replayable terminal recording: [output/transcript](output.txt) and [timing](timing.txt). Download both and run `scriptreplay --log-out output.txt --log-timing timing.txt`; local playback was checked. No inline player is required. Temporary base/source paths are replaced by `<base>`/`<source>` in the captured command output. No test results are synthesized.

Reproduce with `bash docs/probes/packaging/run.sh` from a full checkout containing the base commit. It requires uv; its isolated wheel probe explicitly selects Python 3.11 and may download that test interpreter if absent. Dependency installation can use the network. The canary itself is offline: real reactor code with controlled query outcomes, no live graph or notification delivery. It proves firing, UNKNOWN preservation and resolution, then reads the stored receipt rather than echoing an input marker.

The small recording and reproducer are retained with source history here. They demonstrate candidate packaging, not a published release, managed host installation or live integration.
