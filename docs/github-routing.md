# GitHub routing: activity facts to work requests

**Status: source only (aegis-c01c.4).** Nothing here runs until an emitter
names it. See [Enabling](#enabling).

This is step 2 of the pipeline GitHub -> Quipu -> chaski -> st (aegis-c01c).
camayoc's GitHub ingress writes each GitHub change as an append-only
`aegis:GitHubActivity` record into one `github` graph (camayoc
`docs/design/github-ingress.md`). Chaski reads those facts and files one work
request per piece of work. shantytown's `st agent spawn --ephemeral`
(shantytown #406) runs the job.

```text
quipu github graph --/changes--> chaski ChangeRunner --> github_routes.py (adapter, read-only)
                                        |  WAIT -> READY, one event id per item
                                        v
                         command sink: github_work.py deliver (receiver)
                           ledger + merge lane -> seeds item -> st agent spawn --ephemeral
```

## Rules

All of them are declared in one YAML file (`github-routing.example.yaml`):
defaults, then per-repository overrides.

| kind | READY when | job |
|---|---|---|
| `review` | PR open, not draft, CI green on its head, and no non-author review of that head for `review_after_minutes` (timer starts at the later of CI green and the head's first sighting) | `st … --job review` |
| `merge` | An approving verdict from an agent other than the PR's author agent, and CI green, both on the **same head**; the author is a declared member; the PR holds the head of its repo's merge lane | `st … --job merge` |
| `outside-contribution` | Same condition, but the author is not in `members` | none: a seeds item assigned to the administrator |
| `triage` | The latest run of a workflow or check on the default branch is red (a newer green run of the same workflow, or a passing re-run, supersedes it) | `st … --job triage` |
| `install`, `announce` | A release was published inside `window_hours` (prereleases only if `include_prereleases`) | `st … --job task` |

CI green means every workflow run and check run reported for that commit has
completed with success, skipped or neutral, at least one exists, and every
name in `required_checks` is among them. A run's latest attempt counts.

Anything the facts do not show is WAIT, never READY. A Quipu read that fails
or comes back truncated makes the whole adapter call fail, so chaski records
UNKNOWN and keeps every previous verdict.

## Approval: agent verdicts, not GitHub reviews

Every PR and every review comes from one shared GitHub account. A GitHub
"APPROVE" by a non-author therefore never exists, and the login cannot tell
the author from the reviewer. The merge rule keys on **verdict records**
instead (`approval_sources: [verdict]`, the default).

A verdict record is a node in Quipu, in any graph, typed
`aegis:GateApproval`. During quipu's Decision-to-GateApproval migration
(camayoc `docs/design/agent-memory-entities.md` 1.3), `aegis:Decision` is
read too. Its fields:

| predicate | value |
|---|---|
| `aegis:aboutGitHubEntity` | the PR node, `aegis:github.<PR node_id>` (`gh pr view N --json id`) |
| `aegis:commitSha` | the exact head the reviewer read (40 hex) |
| `aegis:outcome` | `approve`, or `changes` / `reject` |
| `aegis:by` | the reviewing agent (IRI or name) |
| `aegis:recordedAt` | RFC 3339 |
| `aegis:authorAgent` | optional: the PR's author agent, as the reviewer job's item states it |

The PR's author agent is the fact `?pr aegis:authoredByAgent ?agent` when one
exists. Otherwise it is the verdict's `authorAgent`. A PR with two declared
author agents has none. Then:

- A verdict by the author agent never counts.
- For each other agent, its latest verdict on the head counts. An approval
  with no later `changes` verdict from anyone approves.
- A verdict on another head counts for nothing. A new push is a new head, so
  the old approval falls away by itself.
- If the author agent is unknown, the PR counts as reviewed (no second review
  job) but is never approved. Fail closed.

`github_review` can be added to `approval_sources` for repositories where
reviewers have their own GitHub logins. Then a GitHub APPROVE by a login
other than the PR author's counts the same way.

**Writers needed (not in this change).** The review job must record its
verdict as above. Tonight's reviewers post a PR comment headed "APPROVE" or
"REQUEST CHANGES". The adapter never parses comments: they are unsigned and
unattributable. The st side is listed under [Handoff contract](#handoff-contract).

## Idempotency

The item key is `github:<kind>:<owner/name>:<subject>:<revision>`, where:

- for a PR, the subject is `pr/<n>` and the revision is the head sha;
- for the default branch, `branch/<name>` and the commit sha;
- for a release, `release/<tag>` and the release node id.

The event id, which is also the request id, is
`sha256("chaski.github.work-request/v1\n" + key)`.

So there is one request per (repository, PR, head sha, kind), and the
duplicates are refused at three layers:

1. **Emitter outbox.** The outbox is keyed on the event id. A replay, a
   restart or a WAIT/READY flap on the same head re-derives the same id, and
   `INSERT OR IGNORE` keeps one row. This is `baseline_emits: true`, so work
   that is already due when chaski first meets it is not lost.
2. **Receiver ledger.** A request that has been dispatched or routed is
   acknowledged and does nothing else.
3. **Tracker.** Each seeds item carries the label
   `chaski-req:<first 16 hex of the id>`. On a replay the receiver looks the
   item up before it creates one, so a crash between create and ledger write,
   or even a lost ledger, files no second item.

A request that already got its job is never re-requested on the same head. A
retry is the administrator's call: `st agent spawn --retry`.

## The merge lane

There is ONE lane per repository. For `intigraph/quipu`, `spacing_minutes:
10` replaces hammond's scratchpad `quipu-merge.sh`.

- **Lock.** The receiver takes the lane in one `BEGIN IMMEDIATE`
  transaction on its ledger. It refuses while another merge for the repo is
  in flight, or while less than `spacing_minutes` has passed since the last
  merge. A refusal exits 75, and chaski re-delivers later with backoff.
- **Order.** The adapter reads the lane table read-only. It marks READY only
  the head of line: the oldest approval first, then the lowest PR number.
  Even then, only when the lane is free and spacing has elapsed. A waiting
  item gets a `next_checks` deadline: the spacing end, the in-flight lease,
  or a 5-minute recheck. Nothing polls GitHub.
- **Completion from facts.** When the PR in flight shows up closed (merged
  or not) in the activity facts, the adapter attaches
  `lane.completed: [{request_id, outcome, at}]` to the next merge request.
  The receiver releases that turn in the same transaction that takes the
  next one. Spacing counts every merge the facts show, including hand
  merges, as well as the lane's own.
- **Endings the facts cannot show.** A merge job that reports `blocked` or
  `failed` leaves the PR open, and the lane cannot see that. The turn is
  released by
  `chaski-github-work --config C lane release --request <id> --outcome blocked`
  (the job's report hook, or the administrator), or by `lease_minutes`
  (default 75, above st's 60-minute job deadline). A head that had its turn
  never gets a second one, so the lane moves on to the next PR.
- **A refused spawn** (cap, governor) drops the lane turn it took, so the
  lane never waits behind a job that did not start.

st #406 also holds a per-repo lane at spawn admission
(`merge_spacing_minutes`), and its guarded merge re-checks everything
against fresh forge reads. The two lanes are deliberately redundant. Chaski
owns the queue and its order. st refuses anything that slipped through, and
st's refusal is a retry here.

Not modelled: wu's quipu rule "merge only after the previous merge's deploy
is served" (aegis-6ye comment 11). Spacing stands in for it. A served-SHA
check belongs in the merge job's preflight or a later rule input.

## Handoff contract

**Work request.** The seeds item body carries this JSON, and so does the
`evidence` of the chaski event:

```json
{"schema": "chaski.github.work-request/v1", "request_id": "<64 hex>", "item": "github:merge:intigraph/quipu:pr/812:<sha>",
 "kind": "merge", "repo": "intigraph/quipu", "subject": "pr/812", "revision": "<head sha>",
 "route": "spawn", "administrator": "hammond",
 "facts": {"pr": 812, "url": "...", "head_sha": "<sha>", "author": "scbrown", "base_ref": "main",
           "ci": {"state": "green", "green_at": "...", "checks": [...]},
           "approval": {"approvals": [{"by": "sattler", "source": "verdict", "record": "<iri>", "at": "..."}],
                        "blocking": [], "author_agent": "lex", "approved_at": "..."},
           "lane": {"spacing_minutes": 10, "last_merge_at": "...", "completed": [], "in_flight": [], "position": 0}}}
```

The item is titled `[<kind>] <repo>#<n> @ <sha12> (chaski github routing)`.
It is assigned to the administrator and labelled `chaski-github`,
`job:<kind>` and `chaski-req:<id16>`. Its external ref is the PR or release
URL.

**Spawn.** The receiver runs exactly the flags shantytown #406 defines:

```sh
SHANTY_AGENT=<administrator> st agent spawn --ephemeral --job <review|merge|triage|task> --item <seed id> --repo <owner/name> [--pr <n>]
```

| exit code | meaning here |
|---|---|
| 0 | spawned: `dispatched` |
| 1 | refused (cap, governor, lane) |
| 2 | could not tell |

Exit codes 1 and 2 leave the request `refused`, release any lane turn and
exit 75, so chaski retries. `install` and `announce` run as `task` jobs.

Until #406 merges, `spawner: {kind: stub}` records the exact argv in a JSONL
file and starts nothing. `spawner: {kind: st}` is the real call.

**What the st side must do (follow-ups for shantytown #406):**

1. **Guarded merge accepts the verdict source.** `st agent spawn --merge
   --head SHA` counts GitHub APPROVE reviews by login today, and with one
   shared account that can never pass. It must accept a verdict record as
   defined above:
   - it is on exactly `--head`;
   - its `by` is not the PR's author agent;
   - no later `changes` verdict exists on that head.

   It reads the record from Quipu: the same `verdict_query` shape as
   `github_routes.py`.
2. **The review job writes that verdict record** (GateApproval fields above)
   as well as its PR comment, naming the PR's author agent when the item or
   the PR's authorship fact gives it.
3. **The merge job uses `revision` from the item as `--head`.** On ending
   without a merge, its report calls `lane release` (or leaves it to the
   lease).
4. **Whoever opens a crew PR records `aegis:authoredByAgent`** for it. Until
   then, the author agent comes only from verdicts.

## Enabling

Not done here; each step is a separate, reviewed change.

1. camayoc ingress running and its Quipu schema proposal accepted
   (aegis-c01c.6), so `GitHubActivity` facts exist.
2. A verdict writer (above) for the review job, and shantytown #406 merged
   with the verdict-source change.
3. Copy `github-routing.example.yaml`. Set `members`, `repos`, `ledger`,
   `work_items` and the `spawner` (`stub` first).
4. Add the emitter to chaski's emitters file:

   ```yaml
   emitters:
     - label: github-routing
       trigger: changes
       schedule: PT24H            # catalogue reconciliation only
       owner: hammond
       command: [chaski-github-routes, --config, /etc/chaski/github-routing.yaml, --quipu, http://quipu.svc:3030]
       key: item
       event: work_requested
       from: WAIT
       to: READY
       baseline_emits: true
       timeout_s: 60
       sink:
         graph:
           severity: info
           action: [bead]
           condition: "SELECT ?focus WHERE { ?focus a <http://aegis.gastown.local/ontology/GitHubActivity> } LIMIT 0"
           comment: GitHub work requests (aegis-c01c.4); one firing per request.
           token_file: /etc/chaski/quipu-token
         command:
           argv: [env, SHANTY_AGENT=hammond, chaski-github-work, --config, /etc/chaski/github-routing.yaml, deliver]
           timeout_s: 900
   ```

   A command sink is only accepted beside a graph sink, which records each
   firing in Quipu. The receiver must run where `st` and `sd` are configured
   for the administrator.
5. Dry run first: `spawner: {kind: stub}` plus `work_items: {kind: jsonl}`.
   Then compare the recorded requests with what hammond would have spawned
   by hand. After that, switch `work_items` to `seeds`, then `spawner` to
   `st`.
6. Retire `quipu-merge.sh` once the quipu lane has merged on its own.

Offline check against the recorded facts:

```sh
chaski-github-routes --config github-routing.example.yaml --fixture facts.json --describe
echo '{"now": 1793124689, "discover": true}' | chaski-github-routes --config github-routing.example.yaml --fixture facts.json --changes
```

`facts.json` is `{"episodes": [{"episode": <camayoc /episode body>}], "verdicts": [...], "authors": {...}}`;
see `fixtures/github/` and the tests.

## Operating

```sh
chaski-github-work --config C lane status [--repo owner/name] [--json]
chaski-github-work --config C lane release --request <id> --outcome blocked|failed|stopped|closed|merged
```

The ledger (`ledger:`, mode 0600) holds two tables:

- `requests`: state `filing`, `filed`, `waiting-lane`, `refused`, `retry`,
  `dispatched`, `routed` or `spent`, plus the attempt count and the last
  detail;
- `lane`: one row per merge turn.

Chaski's emitter metrics (`chaski_emitter_*{emitter="github-routing"}`)
count the queued, pending and delivered requests.
