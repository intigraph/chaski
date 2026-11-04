# Multipart delivery demonstration

Run `uv run --locked python docs/demos/multipart-stream.py` from the checkout.
This is an offline synthetic fixture, not an installed-service recovery claim.
The script loads base `d94e5f1711585c17261fc25e160b1783e061510b` from Git and
compares it with this revision's decoder using the same two-part transaction.

The base rejects the first multipart frame. The candidate refuses an incomplete
transaction without emitting a page; with the commit barrier it emits exactly
one page containing both records and cursor 11. The focused test suite separately
checks SQLite inbox/cursor atomicity and rollback on a bad late record.

Download [output](multipart-stream.txt) and [timing](multipart-stream.time), then
run `scriptreplay --log-timing multipart-stream.time --log-out multipart-stream.txt`.
These small sanitized artifacts are retained in Git. Playback requires the
terminal replay utility; the output file also provides a readable transcript.

Staging retains at most one bounded part in memory. An anonymous private file
holds up to 1 GiB of declared part data plus 4,096 newline delimiters. Begin
requires 64 MiB of additional free disk; assembly expires after 300 seconds
(checked on arriving frames/heartbeats, with the existing 45-second socket
timeout bounding a silent connection). Refusal preserves the prior cursor.
These limits are safety budgets, not measured throughput or recovery latency.
