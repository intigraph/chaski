#!/usr/bin/env python3
"""External-thread watch: one draft job per outside thread activity (aegis-za4f.7 item 3).

docs/github-watch.md. The notifier (github_notify.py) pushes each outside
activity to Stiwi's phone. This adapter recruits the follow-up: an ephemeral
job that DRAFTS a reply or disposition as a comment on a work item, for
Stiwi or hammond to act on. The job never posts on GitHub, and crew post
nothing on outside-contributor threads.

`chaski-github-watch --config C --quipu URL --changes` speaks chaski's
incremental adapter protocol v1, reading the same `aegis:GitHubActivity`
rows as the notifier with the same config file. It needs no new poller.

Per thread (repo + number; issues, PRs and discussions share one number
space), it judges the newest notifiable activity:

    an outside actor's (the notifier's `push` route)   -> one READY item
    a crew member's                                    -> nothing: answered
    only bot activity after the outside one            -> still READY

Outside PRs are left out: they already go to hammond through the
pr-lifecycle review jobs. An issue_comment whose link is a /pull/ URL is a
PR comment and is left out too.

The item key is `github-watch:<owner/name>#<n>:github.activity.<key>`, so a
new outside comment on the same thread re-arms one new job, and a replay of
the same delivery is the same item and event id. Each record's `action` is
one contract line for `st agent spawn --drain-events`:

    {"event_id", "repo", "pr": null, "branch": null, "head": null,
     "job": "triage", "item": <watch.item>, "reason": "external-thread",
     "thread", "number", "kind", "actor", "url", "activity_at",
     "observed_at", "instructions"}

The adapter is stateless and read-only. A failed or truncated Quipu read
raises, so chaski records UNKNOWN and keeps every prior verdict.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

import github_notify as gn
import github_routes as gr
from chaski_version import add_version_argument

SCHEMA = "chaski.github.watch/v1"
PREFIX = "github-watch"
JOB = "triage"
REASON = "external-thread"
JOBS = ("triage", "task")
ITEM = re.compile(r"^[a-z][a-z0-9]{0,15}-[a-z0-9]{2,12}(?:\.[0-9]+)*$")
KEY = re.compile(r"^github-watch:([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)#([0-9]{1,9}):(github\.activity\.[0-9a-f]{8,64})$")
INSTRUCTIONS = ("Draft a reply or a disposition for this outside GitHub thread as ONE comment on the work "
                "item, for Stiwi or hammond to act on. Never post, react, label or close on GitHub. If the "
                "thread is a pull request, say so and leave it to hammond.")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class WatchConfig:
    notify: gn.Config
    item: str
    job: str


def parse_config(doc) -> WatchConfig:
    """The notifier's config plus a `watch:` section: {item, job?}."""
    try:
        notify = gn.parse_config(doc)
    except gn.ConfigError as exc:
        raise ConfigError(str(exc)) from exc
    watch = doc.get("watch")
    if not isinstance(watch, dict):
        raise ConfigError("watch: {item} is required: the work item the drafts are posted on")
    item = watch.get("item")
    if not isinstance(item, str) or not ITEM.match(item):
        raise ConfigError("watch.item must be a seed item id, e.g. aegis-za4f.7")
    job = watch.get("job", JOB)
    if job not in JOBS:
        raise ConfigError(f"watch.job must be one of {list(JOBS)}")
    return WatchConfig(notify=notify, item=item, job=job)


def load_config(path: Path | str) -> WatchConfig:
    return parse_config(yaml.safe_load(Path(path).read_text()))


# --------------------------------------------------------------------------- judgement

def item_key(repo: str, number: int, activity: str) -> str:
    key = f"{PREFIX}:{repo}#{number}:{activity}"
    if not gr.REPO.match(repo) or not KEY.match(key):
        raise ValueError("invalid watch item")
    return key


def parse_item(item: str) -> tuple[str, int, str]:
    m = KEY.match(str(item))
    if not m or not gr.REPO.match(m.group(1)):
        raise ValueError(f"not a watch item: {item!r}")
    return m.group(1), int(m.group(2)), m.group(3)


def event_id(item: str) -> str:
    return hashlib.sha256(f"{SCHEMA}\n{item}".encode()).hexdigest()


def is_pr(notice: dict) -> bool:
    return (notice["thread"] == "pr" or notice["kind"].startswith("pull_request")
            or "/pull/" in (notice.get("url") or ""))


def _order(notice: dict) -> tuple[str, str]:
    # occurredAt is ISO-8601 Z; the activity key breaks a same-second tie deterministically.
    return (notice.get("at") or "", notice["activity"])


def threads(cfg: WatchConfig, repo: str, rows: list[dict], now: float) -> dict[str, dict]:
    """item -> notice for each thread whose newest non-bot activity is an
    outside one. Pure. The notifier's age bound applies, so a first start
    cannot recruit jobs for weeks-old threads."""
    by_thread: dict[int, list[dict]] = {}
    for notice in gn.candidates(cfg.notify, repo, rows, now).values():
        if isinstance(notice.get("number"), int) and not is_pr(notice):
            by_thread.setdefault(notice["number"], []).append(notice)
    out = {}
    for number, notices in by_thread.items():
        humans = [n for n in notices if not _is_bot(cfg, n.get("actor"))]
        if not humans:
            continue
        newest = max(humans, key=_order)
        if newest["route"] != "push":
            continue  # a crew member spoke last: the thread is answered
        out[item_key(repo, number, newest["activity"])] = newest
    return out


def _is_bot(cfg: WatchConfig, actor: str | None) -> bool:
    a = (actor or "").lower()
    return bool(a) and (a in cfg.notify.bots or a.endswith("[bot]"))


class Adapter:
    def __init__(self, cfg: WatchConfig, facts):
        self.cfg, self.facts = cfg, facts
        self._cache: dict[str, dict[str, dict]] = {}

    def describe(self) -> dict:
        return {"version": 1, "graphs": [self.cfg.notify.graph], "types": [gr.AEGIS + "GitHubActivity"],
                "attributes": [gr.AEGIS + "inGitHubRepository", gr.AEGIS + "githubEventKind"]}

    def watched(self, repo: str, now: float) -> dict[str, dict]:
        if repo not in self._cache:
            self._cache[repo] = threads(self.cfg, repo, self.facts.snapshot(repo, now).rows, now)
        return self._cache[repo]

    def discover(self, now: float, repos=None) -> list[str]:
        items: set[str] = set()
        for repo in sorted(repos if repos is not None else self.cfg.notify.repos):
            items |= set(self.watched(repo, now))
        return sorted(items)

    def route(self, changes: list[dict], now: float) -> list[str]:
        refs = set()
        for c in changes:
            if not isinstance(c, dict):
                continue
            for k in ("value", "old_value"):
                v = c.get(k)
                if c.get("attribute") == gr.AEGIS + "inGitHubRepository" and isinstance(v, dict) and v.get("ref"):
                    refs.add(str(v["ref"]))
            if c.get("entity"):
                refs.add(str(c["entity"]))
        names = self.facts.repos_of(refs)
        return self.discover(now, [n for n in names if n in self.cfg.notify.repos])

    def evaluate(self, items: list[str], now: float) -> dict:
        observed = dt.datetime.fromtimestamp(now, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        records, next_checks = [], {}
        for item in items:
            repo, number, _ = parse_item(item)
            if repo not in self.cfg.notify.repos:
                continue
            notice = self.watched(repo, now).get(item)
            if notice is None:
                continue  # answered, superseded by a newer outside activity, or aged out
            eid = event_id(item)
            action = {"event_id": eid, "repo": repo, "pr": None, "branch": None, "head": None,
                      "job": self.cfg.job, "item": self.cfg.item, "reason": REASON,
                      "thread": notice["thread"], "number": number, "kind": notice["kind"],
                      "actor": notice["actor"], "url": notice["url"], "activity_at": notice["at"],
                      "observed_at": observed, "instructions": INSTRUCTIONS}
            records.append({"item": item, "verdict": gr.READY, "event_id": eid, "work_kind": self.cfg.job,
                            "assignee": None,
                            "evidence": f"{REASON}: {repo}#{number} {notice['label']} by {notice['actor']}"[:300],
                            "action": action})
            next_checks[item] = None
        return {"version": 1, "scope": list(items), "records": records, "next_checks": next_checks}

    def handle(self, request: dict) -> dict:
        now = float(request.get("now"))
        if request.get("discover"):
            return {"version": 1, "items": self.discover(now)}
        if "route" in request:
            return {"version": 1, "items": self.route(request["route"], now)}
        if isinstance(request.get("items"), list):
            return self.evaluate(request["items"], now)
        raise ValueError("request needs discover, route or items")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="chaski-github-watch", description=__doc__.splitlines()[0])
    add_version_argument(ap, "chaski-github-watch")
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--quipu", help="quipu base URL (live reads)")
    ap.add_argument("--fixture", type=Path, help="episodes JSON instead of quipu (offline)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--describe", action="store_true")
    mode.add_argument("--changes", action="store_true", help="one adapter request as JSON on stdin")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
    except (ConfigError, OSError, yaml.YAMLError) as exc:
        print(f"github-watch: {type(exc).__name__}: {exc}"[:500], file=sys.stderr)
        return 2
    if args.fixture:
        doc = json.loads(args.fixture.read_text())
        facts = gr.FixtureFacts([e["episode"] for e in doc.get("episodes", [])])
    elif args.quipu:
        facts = gr.QuipuFacts(args.quipu, cfg.notify.graph, cfg.notify.window_days)
    else:
        ap.error("--quipu or --fixture is required")
    adapter = Adapter(cfg, facts)
    try:
        out = adapter.describe() if args.describe else adapter.handle(json.load(sys.stdin))
    except gr.FactsUnknown as exc:
        print(f"UNKNOWN: {exc}", file=sys.stderr)
        return 3
    json.dump(out, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
