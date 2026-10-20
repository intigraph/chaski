# Deadline-driven reactor turns

The consumer blocks until its earliest heap deadline or an incoming SSE page.
The fixed five-second loop and full scheduled-rule scan are removed. Rule
replacement/cancellation invalidates the prior entry. Lazy deletion is bounded
by compaction; ordinary firing/set/cancel uses the priority queue. Empty rule
workloads can block indefinitely. A spurious wake evaluates no future rules.

This is an owner-thread in-memory scheduling index, not a new graph timer.
Quipu supplies facts and change events; the existing domain adapter computes
its schedule deadlines. Restart rebuilds this index from the existing retained
rule/runner state; no cursor, pending delivery or signed claim is cleared.
Calendar semantics remain with the admitted projection. This change does not
convert a weekly wall-clock schedule to a fixed elapsed interval.

The existing incremental adapter inbox and persisted deadlines remain in SQLite.
Their earliest due instant has an indexed query and becomes a runner heap entry.
Successful change-feed commits mark only matching runners dirty; irrelevant SSE
pages do not invoke every adapter. Reconciliation, source UNKNOWN retry and
outbox delivery backoff supply their next actual deadlines. Global sink-write
pacing remains in force. Pending work can require immediate bounded drain turns;
that is distinct from an idle population scan.

Legacy stage-one event rules retain their explicitly configured event polling
floor; this patch does not replace their /events transport with SSE. Legacy
local-only external emitters still need their existing five-second outbox check,
because another process can write that local database without a Quipu event.
That remaining transport is NOT zero-idle-wake acceptance. It does not scan all
scheduled rules, and must be replaced by a reviewed wake transport before
claiming the whole fleet is free of that polling. Failed sink setup also retains
bounded retries while it has actionable work. No new production flag is enabled
or service started by a source test.

Metrics add reactor turns, due rule evaluations and heap size. Existing per-rule
and emitter metrics remain. Runtime work must still measure CPU, natural firing
latency, graph reads and replay/rollback on the installed artifact. Long-running
synchronous queries/adapters can delay other due work; source fake-query timings
are not network or signed-job latency guarantees. Wall-clock changes during a
blocking relative wait require separate clock-change/runtime acceptance.

Agent jobs must create one stable seed per firing before assignment, and link
that seed to the firing; pure-code jobs mint a seed only on failure. This patch
does not implement that source boundary or authorize a job, enrollment, key,
profile, native expiry or legacy retirement. Reuse existing expiry facts and the
separate domain reactor rather than inventing Quipu timer behavior.

## Reproduce

`python3 scripts/demo-deadline-heap.py --source SOURCE --output RESULT.json`
uses the actual consumer loop with a controlled clock for a 60-second idle
window. Run the SAME script against baseline226aba02 and the candidate. The
source-only fake query counter is a positive instrument; the second workload
actually fires every rule after a real OS wait. No service/graph/key is used.

[Baseline results](demos/deadline-heap/before.json) show 12 turns (11 after
bootstrap),250/250000 rule visits for10/10000 jobs. [Candidate results](demos/deadline-heap/after.json)
show one bootstrap turn,zero later idle wakes,zero rule visits andzero graph
reads at both sizes. The real wait plus10000 sequential in-process fake queries
measured p500.02327s/p950.03608s;10 queries p500.000155s/p950.000170s. These are one
local synthetic observation, exclude network and graph execution, and are not
production percentile estimates. The baseline has no deadline-wait API, so its
physical latency field is explicitly unsupported. Source controls also exercise
update/cancel/stale-entry replay, UNKNOWN state retention and selective SSE wake.

A replayable [output recording](demos/deadline-heap/output.log) and
[timing](demos/deadline-heap/timing.log) retain the candidate run. Download both
and use `scriptreplay --log-out output.log --log-timing timing.log`; terminal
playback is required, with no inline video claim. The exact source head and
base are supplied in the PR.

## Review regression controls

The original heap proposal selected retained recovery rows even when recovery
was disabled, and omitted incremental sink setup retries after successful empty
source discovery. The correction preserves the rows while scheduling only
policy-eligible queues, bounds unresolved dependency checks to five seconds,
and shares setup receiver/policy eligibility between execution and wake planning.
Setup retry deadlines remain independent of source reconcile deadlines and use
the shared write budget.

The same synthetic actual-consumer probes before and after the correction report:

| 60-second fixture | Original proposal | Corrected |
| --- | --- | --- |
| Disabled recovery debt | 5,702 turns | 1 turn, row retained |
| Empty discovery, failed sink setup | 1 attempt, next wait 86,400s | 12 attempts, next wait 5s |

Both probes use a controlled clock and synthetic sinks, with zero external
requests. These are source regression measurements, not installed CPU or network
latency claims. `test_wakeup_feedback.py` exercises these consumer paths plus
blocked dependency pacing, preserved debt, setup policy, bound receiver scope,
and global write-budget controls. The reusable probes are in
`scripts/probe-recovery-wakeups.py` and `scripts/probe-setup-wakeups.py`.
