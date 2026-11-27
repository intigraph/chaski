# GitHub watch: one draft job per unanswered outside thread

**Status: source only (aegis-za4f.7, item 3).** Nothing here runs until an
emitter names it. The notifier (`docs/github-notify.md`) pushes each outside
activity to Stiwi's phone. This adapter recruits the follow-up: an ephemeral
job that drafts a reply or disposition as one comment on a work item, for
Stiwi or hammond to act on.

```text
camayoc ingress -> quipu github graph --/changes--> chaski ChangeRunner
   -> chaski-github-watch --changes        (adapter, read-only, same config as the notifier)
   -> jsonl sink, projected `action`       -> st agent spawn --drain-events
```

The adapter reads the same `aegis:GitHubActivity` rows as the notifier and
needs no new poller.

## What recruits a job

The adapter groups notifiable activity by thread (repo plus number: issues,
PRs and discussions share one number space). It then judges the newest
activity on the thread whose actor is not a bot:

| newest non-bot activity on the thread | result |
|---|---|
| by an outside actor (the notifier's `push` route) | one READY item for that activity |
| by crew | nothing: the thread is answered |

- Bot activity never answers a thread.
- Several outside activities in one window make one job, for the newest.
- A later outside comment re-arms the thread as a new item, so it gets one new job.
- The notifier's `max_age_minutes` bound applies, so a first start cannot recruit jobs for old threads.
- Outside PRs are left out, because they already reach hammond through the pr-lifecycle review jobs. That covers `pull_request*` kinds, PR entities, and an `issue_comment` whose link is a `/pull/` URL.

## The job contract

The item key is `github-watch:<owner/name>#<n>:github.activity.<key>`. The
event id is `sha256("chaski.github.watch/v1\n" + item)`, so a replay is one
outbox row. Each record's `action` is one line:

```json
{"event_id": "...", "repo": "intigraph/quipu", "pr": null, "branch": null, "head": null,
 "job": "triage", "item": "aegis-za4f.7", "reason": "external-thread",
 "thread": "discussion", "number": 1211, "kind": "discussion_comment.created",
 "actor": "outside-person", "url": "https://github.com/...", "activity_at": "...",
 "observed_at": "...", "instructions": "Draft a reply ... Never post ... on GitHub ..."}
```

The job drafts. It never posts, reacts, labels or closes anything on GitHub,
and crew post nothing on outside-contributor threads.

## Config

Use the notifier's file with one more section:

```yaml
watch:
  item: aegis-za4f.7   # the work item the drafts are posted on
  job: triage          # triage | task
```

## Enabling (separate, reviewed steps)

1. Before enabling, qualify that `st agent spawn --drain-events` accepts a
   thread event, that is, `pr`, `head` and `branch` set to null with job
   `triage`. This source does not show that it does.
2. Add the emitter:

   ```yaml
   emitters:
     - label: github-watch
       trigger: changes
       schedule: PT24H
       owner: stiwi
       command: [chaski-github-watch, --config, /etc/chaski/github-notify.yaml, --quipu, http://quipu.svc:3030, --changes]
       key: item
       event: thread_observed
       from: WAIT
       to: READY
       baseline_emits: true
       timeout_s: 60
       sink:
         jsonl: {path: /var/lib/chaski/github-watch.jsonl, project: action}
   ```

3. Live check (this source does not show it): an outside test comment
   produces one job, whose draft lands as a comment on the item, and nothing
   is posted on GitHub. A crew reply after it produces no job.
