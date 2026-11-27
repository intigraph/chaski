"""Main-red triage adapter (aegis-dja, docs/main-red.md).

The PR-lifecycle emitter (pr_lifecycle.py, aegis-za4f.21) already recruits
review, fix and merge jobs for open PRs. A red default branch has no PR, so
nothing recruited a triage job for it: a crew member had to notice. This
adapter closes that gap with the same drain contract.

Each run reads the head commit of each configured repo's default branch
(one bounded GraphQL call for all repos) and prints one emitter record when
that head is red:

    task    the newest run of some check on the branch head failed

The record's `action` is one contract line for `st agent spawn
--drain-events`, the same consumer the PR-lifecycle lines feed:

    {"event_id": sha256("repo|branch|head|task"), "repo", "pr": null,
     "branch", "head", "job": "task", "item", "reason": "main-red",
     "failing": [check names], "red_at", "observed_at"}

The emitter item key is `repo@branch@head:task`, so one red sha is one event
ever (idempotent on the sha). Only the current head is judged: a sha that a
later push superseded is never emitted, and a sha whose failed check was
re-run green is not red. Unknown never emits: an unreadable repo, a head with
no checks, or a truncated check list that shows no failure produces no
record, and a failed run exits non-zero (UNKNOWN for every item).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

import pr_lifecycle as pl
from chaski_version import add_version_argument

JOB = "task"
REASON = "main-red"
BRANCH_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,199}")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    repos: tuple[str, ...]
    item_prefixes: tuple[str, ...] = ("aegis",)
    gh: tuple[str, ...] = ("gh",)
    timeout_s: int = 60
    item_re: re.Pattern = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        prefixes = "|".join(re.escape(p) for p in self.item_prefixes)
        object.__setattr__(self, "item_re",
                           re.compile(rf"(?<![A-Za-z0-9_-])((?:{prefixes})-[a-z0-9]{{2,12}}(?:\.[0-9]+)*)(?![A-Za-z0-9_])"))


def load_config(path: Path | str) -> Config:
    """The PR-lifecycle config file serves as is: only `repos`, `item_prefixes`,
    `gh` and `timeout_s` are read here, so one file can configure both."""
    doc = yaml.safe_load(Path(path).read_text()) or {}
    repos = doc.get("repos")
    if not isinstance(repos, list) or not repos or not all(isinstance(r, str) and pl.REPO_RE.fullmatch(r)
                                                           for r in repos):
        raise ConfigError("repos must be a non-empty list of owner/name")
    if len(set(repos)) != len(repos):
        raise ConfigError("repos lists a repo twice")
    prefixes = doc.get("item_prefixes", ["aegis"])
    if (not isinstance(prefixes, list) or not prefixes
            or not all(isinstance(p, str) and re.fullmatch(r"[a-z][a-z0-9]{0,15}", p) for p in prefixes)):
        raise ConfigError("item_prefixes must be lowercase seed prefixes")
    t = doc.get("timeout_s", 60)
    if type(t) is not int or not 1 <= t <= 600:
        raise ConfigError("timeout_s must be an integer 1..600")
    gh = doc.get("gh", ["gh"])
    gh = [gh] if isinstance(gh, str) else gh
    if not isinstance(gh, list) or not gh or not all(isinstance(s, str) and s for s in gh):
        raise ConfigError("gh must be a command (string or argv list)")
    return Config(repos=tuple(repos), item_prefixes=tuple(prefixes), gh=tuple(gh), timeout_s=t)


# ---------------------------------------------------------------- judgement

def event_id(repo: str, branch: str, head: str) -> str:
    """sha256 of `repo|branch|head|task`: the PR contract's id with the branch
    in the PR slot, so a main event can never collide with a PR event."""
    return hashlib.sha256(f"{repo}|{branch}|{head}|{JOB}".encode()).hexdigest()


def failing(head: dict) -> list[dict] | None:
    """The failed newest-per-name checks on the head, [] when not red, or None
    when unknown (no checks read)."""
    checks = head.get("checks")
    if checks is None:
        return None
    return [c for c in checks if c.get("state") in pl.FAIL]


def judge(head: dict) -> tuple[list[str], str | None] | None:
    """(failing check names, newest failure time) when the head is red, else None.

    The failure time is when the check turned red (CheckRun completedAt), not
    when it started, so event-to-spawn latency excludes CI run time; a check
    with no completion time (a commit status) falls back to its `at`.

    Red needs only one failure, so a truncated check list that already shows
    one is still red; a truncated list with none is unknown, never green."""
    sha, branch = head.get("head"), head.get("branch")
    if not isinstance(sha, str) or not pl.SHA_RE.fullmatch(sha):
        return None
    if not isinstance(branch, str) or not BRANCH_RE.fullmatch(branch):
        return None
    bad = failing(head)
    if not bad:
        return None
    names = sorted({c.get("name") or "?" for c in bad})
    stamps = [c.get("red_at") or c["at"] for c in bad if c.get("red_at") or c.get("at")]
    return names, (max(stamps) if stamps else None)


def seed_item(cfg: Config, head: dict) -> str | None:
    m = cfg.item_re.search(head.get("message") or "")
    return m.group(1) if m else None


def records(cfg: Config, heads: list[dict], now: dt.datetime) -> list[dict]:
    observed = now.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []
    for h in heads:
        verdict = judge(h)
        if verdict is None:
            continue
        names, red_at = verdict
        eid = event_id(h["repo"], h["branch"], h["head"])
        action = {"event_id": eid, "repo": h["repo"], "pr": None, "branch": h["branch"], "head": h["head"],
                  "job": JOB, "item": seed_item(cfg, h), "reason": REASON, "failing": names,
                  "red_at": red_at, "observed_at": observed}
        out.append({"item": f"{h['repo']}@{h['branch']}@{h['head']}:{JOB}", "verdict": "READY",
                    "event_id": eid, "work_kind": JOB, "evidence": f"{REASON}: {', '.join(names)}"[:300],
                    "action": action})
    return out


# ---------------------------------------------------------------- GitHub read

_HEAD = """
 defaultBranchRef { name target { ... on Commit { oid messageHeadline statusCheckRollup { contexts(first: 100) {
   pageInfo { hasNextPage }
   nodes { __typename
     ... on CheckRun { name status conclusion startedAt completedAt }
     ... on StatusContext { context state createdAt } } } } } } }
"""


def read_heads(cfg: Config) -> list[dict]:
    parts = []
    for i, repo in enumerate(cfg.repos):
        owner, name = pl._split(repo)
        parts.append(f"r{i}: repository(owner: {owner}, name: {name}) {{ {_HEAD} }}")
    data = pl._graphql(cfg, "query { " + " ".join(parts) + " }")
    heads = []
    for i, repo in enumerate(cfg.repos):
        ref = (data.get(f"r{i}") or {}).get("defaultBranchRef")
        commit = (ref or {}).get("target") or {}
        if not ref or not commit.get("oid"):
            print(f"main-red: {repo}: default branch unreadable, skipped (unknown)", file=sys.stderr)
            continue
        rollup = commit.get("statusCheckRollup")
        checks = None
        if rollup is not None:
            ctx = rollup["contexts"]
            checks = pl.latest_per_name([{**pl._context(c), "red_at": c.get("completedAt")}
                                         for c in ctx["nodes"]])
            if ctx["pageInfo"]["hasNextPage"]:
                print(f"main-red: {repo}: more than 100 checks on {commit['oid'][:12]}; "
                      "only a failure among the first 100 counts (truncated)", file=sys.stderr)
        heads.append({"repo": repo, "branch": ref["name"], "head": commit["oid"],
                      "message": commit.get("messageHeadline") or "", "checks": checks})
    return heads


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="chaski-main-red", description=__doc__.splitlines()[0])
    add_version_argument(ap, "chaski-main-red")
    ap.add_argument("--config", required=True)
    ap.add_argument("--fixture", help="JSON list of normalized branch heads; no network")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
        heads = json.loads(Path(args.fixture).read_text()) if args.fixture else read_heads(cfg)
        out = records(cfg, heads, dt.datetime.now(dt.timezone.utc))
    except (ConfigError, RuntimeError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as exc:
        print(f"main-red: {type(exc).__name__}: {exc}"[:500], file=sys.stderr)
        return 1
    json.dump(out, sys.stdout, sort_keys=True)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
