# PR lifecycle: chaski recruits review, fix and merge jobs

**Status: source (aegis-za4f.21, half A).** Nothing runs until the emitter
below is added to a deployed rules file. Half B, the consumer that turns each
line into `st agent spawn --ephemeral`, lives in shantytown
(`st agent spawn --drain-events <jsonl>`).

```text
chaski emitter pr-lifecycle (PT2M)
  -> chaski-pr-lifecycle --config C        (verdict adapter, read-only GitHub GraphQL)
  -> sink jsonl {path, project: action}    (one contract line per event_id, ever)
  -> st agent spawn --drain-events <jsonl> (half B, shantytown)
```

## Event contract

One JSONL line per needed action:

```json
{"event_id": "<sha256 of repo|pr|head|job>", "repo": "intigraph/quipu", "pr": 1354,
 "head": "<40-hex>", "job": "review|task|merge", "item": "<seed id or null>",
 "reason": "new-head-unreviewed|changes-requested|green-and-approved|ci-red",
 "observed_at": "<UTC>"}
```

`item` is the first seed id (`item_prefixes`, default `aegis`) in the PR
title, then the branch, then the body.

## Rules, per open PR and its current head

Drafts, bot authors (`[bot]` or a GitHub Bot), `skip_authors` and
`release-please--` branches are skipped. In order, the first that applies:

| state of the current head | job | reason |
|---|---|---|
| latest verdict is REQUEST CHANGES | `task` | `changes-requested` |
| a required check failed | `task` | `ci-red` |
| no verdict | `review` | `new-head-unreviewed` |
| latest verdict is APPROVE and required checks green | `merge` | `green-and-approved` |

A red head gets a fix task rather than a review, so a review and a fix do not
race on the same head; the fix's push re-arms the review. Approved with
pending checks waits; the run after the last required check goes green
emits the merge (within the 2 minute schedule).

**Verdicts.** A GitHub review (APPROVED / CHANGES_REQUESTED) on the head
commit by a login other than the PR author, or a comment by a
`verdict_logins` login whose first line is `APPROVE at head <40-hex>` or
`REQUEST CHANGES at head <40-hex>`, which is how crew review jobs record their
verdicts under the shared operator login. A verdict for an older head does
not count. The merge job re-verifies head, checks and independence itself.

**Checks.** The newest run of each check name counts (a superseded CANCELLED
or FAILURE run does not linger). Required = contexts GitHub marks required
for the PR. A repo with no required checks falls back to all reported
checks. FAILURE, ERROR, TIMED_OUT, ACTION_REQUIRED and STARTUP_FAILURE are
red. CANCELLED and STALE are neither red nor green.

## Once, never twice; unknown never emits

- The emitter key is `repo#pr@head:job` and the event id is the contract's
  sha256, so a pair is one event ever and a new head re-arms. The outbox
  (`INSERT OR IGNORE`), the projecting JSONL sink (skips an id already in the
  file) and half B's ledger each deduplicate on it.
- `baseline_emits: true`: a needed action already true at first sight is an
  event (chaski did not watch it become needed, but it is still needed).
- A failed run exits non-zero, which leaves every item unchanged. A repo or
  PR the API could not read, a head that moved between the two reads,
  truncated reviews/comments (over 100) or checks, or a head with no checks
  produce no record for the step that needed them.

## Cost

Two GraphQL calls per run for up to 20 non-draft PRs (one more per further
20): an open-PR list across all repos, then head checks, reviews and comments.
At PT2M that is about 60 calls an hour, far below the 5000 an hour limit.
It uses the `gh` login of the chaski runner.

## Enabling (separate, reviewed steps)

1. Install this chaski version (goldblum `automation_servers` chaski role pin
   bump; named rollback = previous pin) with an authenticated `gh` for the
   runner user.
2. Install `pr-lifecycle.example.yaml`'s top half as
   `/etc/chaski/pr-lifecycle.yaml` and its `emitters:` block into the rules
   file.
3. Point half B at `/var/lib/chaski/pr-lifecycle.jsonl`.
4. Live acceptance (source does not show it): push a commit to an open PR
   and a `review` line for the new head appears within 2 minutes, then a
   review job, with no crew action.

`chaski-pr-lifecycle --config C --fixture prs.json` runs offline against a
JSON list of normalized PRs (see `test_pr_lifecycle.py`).
