# Explicit graph writer vocabulary

The baseline selects canonical classes that a legacy receiver does not sanction.
The candidate defaults to `legacy-vocabulary`; canonical mode is an explicit
opt-in after receiver profile activation. Instance identities stay the same and
the two modes have different delivery-scope markers, so retained events cannot
silently cross the boundary.

Run `python3 docs/demos/graph-vocabulary-selection/demo.py`. This recording is a
source fixture with a representative legacy class inventory, without HTTP or
graph writes. Separate native validation used the exact served server binary
and complete served shape inventory: canonical setup returned HTTP 400 for its
unknown class; legacy setup and firing passed. No production write or restart
is demonstrated here.

Download `output.txt` and `timing.txt`, then replay with
`scriptreplay --timing timing.txt --log-out output.txt`. Local playback is
verified; no browser embedding is claimed.
