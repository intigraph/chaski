# Adapter accounting source fixture

Run `python3 docs/demos/adapter-accounting/run.py` in a checkout containing
base `d238c52e5810315cd74487b728f3bae756a77063` and behavioral head `5adb838`.
The same ten-request reservation reports two actual requests and refunds eight.
Before, no adapter request metric exists. After, the reported-request metric is
two and the unknown metric is zero; an unknown failed call raises the latter.
Later documentation commits preserve these behavioral source files.

This is an in-memory cost-report fixture, not an adapter HTTP measurement or
installed-service test. A real bounded adapter call and independent server
counter reconciliation remain required before runtime acceptance.

The actual [output](output.txt) and [timing](timing.txt) are retained small text
artifacts. Download both and replay with
`scriptreplay -t timing.txt -O output.txt -m 2`. No browser embed is claimed.
