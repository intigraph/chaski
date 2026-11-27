# Main red: chaski recruits one triage task per red default-branch sha

**Status: source (aegis-dja).** Nothing runs until the `main-red` emitter in
`pr-lifecycle.example.yaml` is added to a deployed rules file. It is the
"main red -> triage" half of aegis-dja; "CI green -> merge job" is already the
PR-lifecycle drain ([pr-lifecycle.md](pr-lifecycle.md), aegis-za4f.21). The
stub spawner in `github_work.py` was removed: GitHub-event recruitment goes
through this emitter -> `st agent spawn --drain-events` path.

```text
chaski emitter main-red (PT2M)
  -> chaski-main-red --config C           (read-only GitHub GraphQL, one call for all repos)
  -> sink jsonl {path, project: action}   (one contract line per event_id, ever)
  -> st agent spawn --drain-events <jsonl> (shantytown; ONE task job per (repo, sha))
```

## Event contract

The PR-lifecycle line, with the branch in the PR slot:

```json
{"event_id": "<sha256 of repo|branch|head|task>", "repo": "intigraph/chaski",
 "pr": null, "branch": "main", "head": "<40-hex>", "job": "task",
 "item": "<seed id from the commit headline, or null>", "reason": "main-red",
 "failing": ["<check name>", "..."], "red_at": "<newest failed check completion, UTC>",
 "observed_at": "<UTC>"}
```

`red_at` is the natural event time: event-to-spawn latency is the spawn time
minus `red_at`.

## Rules

Per configured repo, only the current head of the default branch is judged.
It is red when the newest run of any check name on it (check runs and
commit statuses, as `pr_lifecycle.latest_per_name` picks them) is FAILURE,
ERROR, TIMED_OUT, ACTION_REQUIRED or STARTUP_FAILURE. A red head emits one
`task` line.

- **Idempotent on the sha.** The emitter key is `repo@branch@head:task` and
  the event id is the sha256 above: one red sha is one event ever, through
  the outbox, the projecting JSONL sink and st's drain ledger.
- **Green or superseded never emits.** A head whose failure was re-run green
  is not red. A sha that a newer push superseded is never judged, so a red
  sha fixed by the next commit before the run reads it gets no job.
- **Unknown never emits.** An unreadable repo, a head with no checks read,
  or a truncated check list (over 100) that shows no failure produce no line.
  A failed run exits non-zero and every item stays unchanged.
- `baseline_emits: false`: a main already red when the emitter is first
  enabled is not a new event, so enabling it does not recruit a backlog of
  triage jobs for old reds. The next red sha recruits.

## What the st side must do (shantytown, before pointing the drain here)

`st agent spawn --drain-events` (`shantytown/pr_events.py`, #776) today
accepts only PR lines: `pr` must be a positive integer and `reason` one of
the four PR reasons. A main-red line fed to it now is reported as malformed
once and then ignored forever, so this emitter writes its own file
(`/var/lib/chaski/main-red.jsonl`) and the drain must not be pointed at it
until shantytown:

1. accepts `pr: null` with a `branch` (`event_id` = sha256 of
   `repo|branch|head|task`) and `reason: main-red` for `job: task`;
2. keys supersession on `(repo, branch)`, so a newer red sha on main settles
   an older pending one as superseded;
3. spawns `--job task --repo <repo> --head <sha>` with the failing checks as
   evidence, and the item when one is given;
4. records `event_to_spawn_s` from `red_at`.

## Enabling (separate, reviewed steps)

1. Install this chaski version (goldblum `automation_servers` chaski role pin
   bump; named rollback = previous pin).
2. Add the `main-red` emitter block from `pr-lifecycle.example.yaml` to the
   rules file. It reads the same `/etc/chaski/pr-lifecycle.yaml`.
3. After the shantytown change above, run `st agent spawn --drain-events
   /var/lib/chaski/main-red.jsonl` on the same schedule as the PR drain.
4. Live acceptance (source does not show it): a pushed red main commit on a
   configured repo produces a `main-red` line within 2 minutes, then one task
   job with no crew action; the job posts its verdict; `event_to_spawn_s` is
   recorded.

`chaski-main-red --config C --fixture heads.json` runs offline against a
JSON list of normalized heads (see `test_main_red.py`).
