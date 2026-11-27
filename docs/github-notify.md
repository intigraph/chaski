# GitHub notify: a push for each outside activity on a public repo

**Status: source only (aegis-za4f.7).** Nothing here runs until an emitter
names it. This is the notifier half of the public-repo watcher. The ingress
half (the discussion and discussion_comment mappings) is camayoc #272. The
webhook events are owned by goldblum.

```text
camayoc ingress -> quipu github graph --/changes--> chaski ChangeRunner
   -> chaski-github-notify --changes            (adapter, read-only)
   -> command sink: chaski-github-notify deliver (ledger -> ntfy push | digest)
```

## What notifies

There is one READY item for each `aegis:GitHubActivity` record of these kinds
in a configured repo: `issues.opened|reopened|closed`,
`pull_request.opened|reopened|closed|ready_for_review`, `issue_comment.created`,
`pull_request_review.submitted`, `pull_request_review_comment.created`,
`discussion.created` and `discussion_comment.created`. `kinds:` narrows the
list. Edits and CI activity never notify.

| actor | route |
|---|---|
| in `crew`, in `bots`, or a login ending in `[bot]` | `digest`: recorded, never pushed |
| anyone else, including an actor the facts do not name | `push`: one ntfy message |

The receiver recomputes the route from its own crew list. A stale adapter
config therefore cannot page about crew activity.

A push's title is `<repo>#<n> <kind label> by <actor>`. Its body holds the
first line, the event kind and time, and the link, and its ntfy `Click`
header is the link. Camayoc never stores text: it keeps only @-mentions. So
the first line comes from an optional `first_line.argv` reader, which is given
the URL. Without a reader, the push says the text is not stored.

## Once, never twice

- **Item key** `github-notify:<owner/name>:github.activity.<key>`. The
  activity name is camayoc's per-delivery activity key, so a re-sent GitHub
  delivery or a replay is the same item. The event id is
  `sha256("chaski.github.notify/v1\n" + item)`. The emitter outbox keeps one
  row per event id.
- **Ledger** (`ledger:`, SQLite, mode 0600). The delivery is claimed before
  the HTTP call. Once an earlier delivery of the same id reached `sent`,
  `digest` or `ambiguous`, any later one is a receipted duplicate.
- **Retry versus ambiguous.** A delivery that fails for sure is retried
  (exit 75): a refused connection, an unresolvable host or an HTTP error
  status. When the push may have gone out, the delivery is recorded
  `ambiguous` and is never re-sent. That covers a timeout or reset after the
  request was sent, and a delivery that died mid-send (its claim lapsed while
  in `sending`). `chaski-github-notify --config C status` lists these rows.
- **Age bound.** An activity older than `max_age_minutes` (default 180) is not
  a candidate. A first start with `baseline_emits: true`, or a long outage,
  therefore cannot replay weeks of history to the phone.

## Digest

`chaski-github-notify --config C digest` prints the crew and bot rows not yet
digested. `digest --send` pushes them as one message and marks them. A
schedule for `--send` is a deployment choice. It is not set here.

## Enabling (separate, reviewed steps)

1. Deploy camayoc #272. Add the `discussion` and `discussion_comment` events
   to every public repo's ingress webhook through goldblum, never by hand.
2. Copy `github-notify.example.yaml` and set `repos`, `crew`, the ntfy topic
   and the token file.
3. Add the emitter:

   ```yaml
   emitters:
     - label: github-notify
       trigger: changes
       schedule: PT24H
       owner: stiwi
       command: [chaski-github-notify, --config, /etc/chaski/github-notify.yaml, --quipu, http://quipu.svc:3030, --changes]
       key: item
       event: activity_observed
       from: WAIT
       to: READY
       baseline_emits: true
       timeout_s: 60
       sink:
         graph:
           severity: info
           action: [notify]
           comment: Public repo activity (aegis-za4f.7)
           condition: "SELECT ?focus WHERE { ?focus a <http://aegis.gastown.local/ontology/GitHubActivity> } LIMIT 0"
           token_file: /etc/chaski/quipu-token
         command:
           argv: [chaski-github-notify, --config, /etc/chaski/github-notify.yaml, deliver]
           timeout_s: 60
   ```

4. Live acceptance (this source does not show it): a test comment on a
   scratch discussion and a test issue comment each reach the phone within
   2 minutes as exactly one push, and a crew comment produces no push.

The adapter runs offline against `{"episodes": [{"episode": ...}]}` with
`--fixture`. See `test_cli_adapter_runs_offline_against_the_fixture`.
