# Observed PR flow

This extends the existing GitHub routing adapter and work receiver. It does
not add a poller, a merge authority, or a second review verifier.

`stale_prs.enabled: true` on a repository enables conflict work for its declared
author and a `merge-stalled` request after an approved, green head waits 120
minutes. `approved_after_minutes` changes that interval. `review_after_minutes`
continues to govern the existing green/unreviewed deadline. Head changes and
closures retire candidates through the existing deadline protocol. Requests
reuse the existing deterministic request IDs and delivery ledger.

`crew_reviews.enabled: true` routes reviews to named crew instead of ephemeral
workers. Configure an explicit `reviewers` list, the admitted delivery SDK's
`sdk_sha256`, and optionally `st_bin`. The receiver
uses the bounded `st crew --review-routing --local --json` profile and refuses
incomplete, stale, held, wrong-SDK, busy or unchecked observations. It excludes
the declared author and reserves one recipient in its ledger before creating
the item. This queues work; ordinary crew dispatch still admits execution.
Other legacy job types retain their existing routing; disable their job routes
in a crew-only deployment until their own routing is admitted.

Run `chaski-github-routes --config CONFIG --quipu URL --brief` for the same
read-time state judgment as JSON. `--brief-output OWNED_PATH` atomically writes
a local, mode-0600 projection for an independently admitted static reader. It
does not publish a network endpoint. Missing repositories or absent PR facts
produce UNKNOWN, never a fabricated zero. Counts cover the configured activity
window, not a complete current-PR census.

Each record names the head, state, source observation, state age and responsible
role or declared agent. An observed-head-to-first-review duration is available
when a non-author review and declared author are known. It measures the bounded
observed head, not lifetime first review. Lifetime first-review and main-only
repush counts remain UNKNOWN: they require complete history and verified
unchanged-proposal refresh evidence. A head/base SHA change alone is insufficient.

All new behavior is opt-in. Live use also requires the additive Camayoc schema,
mergeability observations, trusted review/author producers, runner credentials,
reader registration and read-cost admission. Source tests do not prove those
runtime gates are satisfied. No timer, job or receiver is armed by this change.
