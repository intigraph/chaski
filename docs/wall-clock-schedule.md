# Weekly wall-clock deadlines

`wall_clock_schedule.WeeklyWallClock` pins a weekday (Monday=0), `HH:MM`
local time, IANA timezone and inclusive UTC activation time. `window(now)`
returns only the current eligible UTC slot (or `None` before activation) and
the next UTC deadline. `next_after(scheduled_at)` advances an exact run slot
using the next local calendar date. It refuses a timestamp that is not a slot.

For Monday 09:17 America/New_York, the October 26, 2026 slot is 13:17 UTC.
After the November 1 fallback, November 2 is 14:17 UTC: that week is 608400
elapsed seconds. The March 2 to March 9 spring transition is 601200 seconds.
Adding 604800 seconds changes local firing time and is not this schedule.

Ambiguous and nonexistent scheduled local times raise `ValueError`; no implicit
fold selection or gap shift is permitted. Bad zones, clocks, pins and unavailable
calendar bounds also refuse. The caller retains its previous deadlines, claims
and outbox as UNKNOWN on failure. This calculator cannot decide clock trust or
erase old runs after a backward clock change.

After validating the governed declaration and its immutable execution scope,
the catalogue adapter can use `window.scheduled_at` with the existing
`run_id(scope, scheduled_at)` and put `window.next_deadline` in `next_checks`.
The existing deadline heap accepts that UTC timestamp. Repeated observations
share one slot identity; the actual emitter outbox and receiver ledger own
deduplication and uncertainty. Historical missed slots are not a catch-up list.
Changing schedule pins requires the normal revision/scope migration; this
library does not reinterpret earlier pending deliveries.

This is a source library, not an enrolled adapter or catalogue schema. Existing
fixed-interval `ScheduledProjection` and its transport remain unchanged. The
domain profile/catalogue binding, schedule persistence, signer, fixed child,
per-run episode/findings and natural delivery acceptance remain separate.
Timezone rules come from the installed IANA database; deployment must bind and
verify that dependency along with the source artifact. No production schedule,
key, job, timer, service or legacy schedule is changed by the tests.

`python -m pytest -q test_wall_clock_schedule.py` checks the UTC golden values,
both occurrences of the November 1 fallback hour, spring gap/fold refusal,
activation and replay, plus the actual deadline heap and SQLite emitter outbox
with synthetic scope. These are source controls, not natural runtime firings.

## Source demonstration

Run `python scripts/demo-wall-clock.py`. Base
`fc3c017bdc91b3b08792577e5caf40b76f54904a` has the fixed projection but no
wall-clock module; the script checks both identities before demonstrating the
candidate calculator. It prints the exact module digest, the October/November
UTC pair, both November 1 fallback observations and an ambiguous-time refusal.
The candidate source head is recorded in the PR; this is not an installed test.

Download the [output recording](demos/wall-clock-output.log) and
[timing](demos/wall-clock-timing.log), then use
`scriptreplay --log-out wall-clock-output.log --log-timing wall-clock-timing.log`.
Playback was checked with `scriptreplay`; terminal download/playback is required,
with no inline-video claim. The small recording stays with the source.
