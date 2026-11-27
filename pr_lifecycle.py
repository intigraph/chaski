"""PR lifecycle verdict adapter (aegis-za4f.21, docs/pr-lifecycle.md).

Chaski recruits the next PR step from PR state, so no crew member has to
notice it. Each run reads the open PRs of the configured repos from the GitHub
GraphQL API (two bounded calls per batch) and prints one emitter record per
action the current head needs:

    review  open, non-draft PR whose current head has no review verdict
    task    the latest verdict on the head is REQUEST CHANGES, or the head's
            required checks are red (fix and push to the PR branch)
    merge   required checks green AND an APPROVE on the current head, with no
            later REQUEST CHANGES

Each record carries the fixed event-contract line as `action`; an emitter
with `sink: {jsonl: {path: ..., project: action}}` writes exactly that line:

    {"event_id": sha256("repo|pr|head|job"), "repo", "pr", "head", "job",
     "item", "reason", "observed_at"}

The emitter item key is `repo#pr@head:job`, so a pair is one event ever and a
new head re-arms every job. Unknown never emits: a PR whose head, checks or
reviews could not be read completely produces no record for the step that
needed them, and a failed run exits non-zero (UNKNOWN for every item).

A verdict is a GitHub review (APPROVED / CHANGES_REQUESTED) by a login other
than the PR author on the head commit, or a crew verdict comment from a
configured `verdict_logins` login whose first line is
`APPROVE at head <40-hex>` or `REQUEST CHANGES at head <40-hex>`. Crew share
one operator login, so the comment form is how crew reviews are recorded;
the merge job re-verifies independence and the head before merging.
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

REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
SHA_RE = re.compile(r"[0-9a-f]{40}")
VERDICT_LINE = re.compile(r"\s*(APPROVE|REQUEST[ _-]CHANGES)\s+at\s+head\s+([0-9a-f]{40})\b", re.I)
PASS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
# CANCELLED and STALE are neither: a superseded or abandoned run never recruits
# a fix job and never counts as green.
FAIL = {"FAILURE", "ERROR", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE"}
JOBS = ("review", "task", "merge")
MAX_BATCH = 20


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    repos: tuple[str, ...]
    verdict_logins: frozenset[str] = frozenset()
    item_prefixes: tuple[str, ...] = ("aegis",)
    skip_authors: frozenset[str] = frozenset()
    skip_branch_prefixes: tuple[str, ...] = ("release-please--",)
    max_prs_per_repo: int = 30
    gh: tuple[str, ...] = ("gh",)
    timeout_s: int = 60
    item_re: re.Pattern = field(init=False, repr=False, compare=False)

    def __post_init__(self):
        prefixes = "|".join(re.escape(p) for p in self.item_prefixes)
        object.__setattr__(self, "item_re",
                           re.compile(rf"(?<![A-Za-z0-9_-])((?:{prefixes})-[a-z0-9]{{2,12}}(?:\.[0-9]+)*)(?![A-Za-z0-9_])"))


def load_config(path: Path | str) -> Config:
    doc = yaml.safe_load(Path(path).read_text()) or {}
    repos = doc.get("repos")
    if not isinstance(repos, list) or not repos or not all(isinstance(r, str) and REPO_RE.fullmatch(r) for r in repos):
        raise ConfigError("repos must be a non-empty list of owner/name")
    if len(set(repos)) != len(repos):
        raise ConfigError("repos lists a repo twice")

    def strings(key, default=()):
        v = doc.get(key, list(default))
        if not isinstance(v, list) or not all(isinstance(s, str) and s for s in v):
            raise ConfigError(f"{key} must be a list of non-empty strings")
        return v

    n = doc.get("max_prs_per_repo", 30)
    if type(n) is not int or not 1 <= n <= 100:
        raise ConfigError("max_prs_per_repo must be an integer 1..100")
    t = doc.get("timeout_s", 60)
    if type(t) is not int or not 1 <= t <= 600:
        raise ConfigError("timeout_s must be an integer 1..600")
    prefixes = strings("item_prefixes", ("aegis",))
    if not prefixes or not all(re.fullmatch(r"[a-z][a-z0-9]{0,15}", p) for p in prefixes):
        raise ConfigError("item_prefixes must be lowercase seed prefixes")
    gh = doc.get("gh", ["gh"])
    gh = [gh] if isinstance(gh, str) else gh
    if not isinstance(gh, list) or not gh or not all(isinstance(s, str) and s for s in gh):
        raise ConfigError("gh must be a command (string or argv list)")
    return Config(repos=tuple(repos), verdict_logins=frozenset(strings("verdict_logins")),
                  item_prefixes=tuple(prefixes), skip_authors=frozenset(strings("skip_authors")),
                  skip_branch_prefixes=tuple(strings("skip_branch_prefixes", ("release-please--",))),
                  max_prs_per_repo=n, gh=tuple(gh), timeout_s=t)


# ---------------------------------------------------------------- judgement

def event_id(repo: str, pr: int, head: str, job: str) -> str:
    return hashlib.sha256(f"{repo}|{pr}|{head}|{job}".encode()).hexdigest()


def seed_item(cfg: Config, pr: dict) -> str | None:
    """The seed id from the PR title, then branch, then body, or None."""
    for text in (pr.get("title"), pr.get("head_ref"), (pr.get("body") or "")[:20000]):
        m = cfg.item_re.search(text or "")
        if m:
            return m.group(1)
    return None


def skipped(cfg: Config, pr: dict) -> bool:
    author = pr.get("author") or ""
    return bool(pr.get("draft") or pr.get("author_is_bot") or author.endswith("[bot]")
                or author in cfg.skip_authors
                or any((pr.get("head_ref") or "").startswith(p) for p in cfg.skip_branch_prefixes))


def head_verdicts(cfg: Config, pr: dict) -> list[str] | None:
    """Chronological verdicts ('approve'/'changes') on the current head, or None
    when the review/comment lists were truncated (unknown)."""
    if not pr.get("reviews_complete") or not pr.get("comments_complete"):
        return None
    head, author = pr["head"], pr.get("author")
    timed = []
    for r in pr.get("reviews", []):
        state = r.get("state")
        if (r.get("commit") == head and r.get("author") and r["author"] != author
                and state in ("APPROVED", "CHANGES_REQUESTED")):
            timed.append((r.get("at") or "", "approve" if state == "APPROVED" else "changes"))
    for c in pr.get("comments", []):
        if c.get("author") not in cfg.verdict_logins:
            continue
        m = VERDICT_LINE.match(c.get("body") or "")
        if m and m.group(2).lower() == head:
            timed.append((c.get("at") or "", "approve" if m.group(1).upper() == "APPROVE" else "changes"))
    return [v for _, v in sorted(timed, key=lambda t: t[0])]


def check_state(pr: dict) -> str:
    """'green' | 'red' | 'pending' | 'unknown' for the head's required checks.

    Required = contexts GitHub marks required for this PR. A repo with no
    required checks falls back to every reported check. No checks at all, or a
    truncated list that shows no failure, is unknown.
    """
    contexts = pr.get("checks")
    if contexts is None:
        return "unknown"
    required = [c for c in contexts if c.get("required")]
    pool = required or contexts
    states = [c.get("state") for c in pool]
    if any(s in FAIL for s in states):
        return "red"
    if not pr.get("checks_complete") or not pool:
        return "unknown"
    if all(s in PASS for s in states):
        return "green"
    return "pending"


def judge(cfg: Config, pr: dict) -> list[tuple[str, str]]:
    """[(job, reason)] the current head needs; [] when nothing or unknown."""
    head = pr.get("head")
    if skipped(cfg, pr) or not isinstance(head, str) or not SHA_RE.fullmatch(head):
        return []
    if pr.get("head_ref_oid") and pr["head_ref_oid"] != head:
        return []  # head moved between reads: next run decides
    verdicts = head_verdicts(cfg, pr)
    checks = check_state(pr)
    out: list[tuple[str, str]] = []
    if verdicts is not None and verdicts and verdicts[-1] == "changes":
        out.append(("task", "changes-requested"))
    elif checks == "red":
        out.append(("task", "ci-red"))
    elif verdicts is not None and not verdicts:
        out.append(("review", "new-head-unreviewed"))
    elif verdicts and verdicts[-1] == "approve" and checks == "green":
        out.append(("merge", "green-and-approved"))
    return out


def records(cfg: Config, prs: list[dict], now: dt.datetime) -> list[dict]:
    observed = now.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out = []
    for pr in prs:
        for job, reason in judge(cfg, pr):
            eid = event_id(pr["repo"], pr["number"], pr["head"], job)
            action = {"event_id": eid, "repo": pr["repo"], "pr": pr["number"], "head": pr["head"],
                      "job": job, "item": seed_item(cfg, pr), "reason": reason, "observed_at": observed}
            out.append({"item": f"{pr['repo']}#{pr['number']}@{pr['head']}:{job}", "verdict": "READY",
                        "event_id": eid, "work_kind": job, "evidence": reason, "action": action})
    return out


# ---------------------------------------------------------------- GitHub read

def _graphql(cfg: Config, query: str) -> dict:
    proc = subprocess.run([*cfg.gh, "api", "graphql", "-f", f"query={query}"], capture_output=True,
                          text=True, timeout=cfg.timeout_s, check=False)
    try:
        doc = json.loads(proc.stdout or "null")
    except ValueError:
        doc = None
    if not isinstance(doc, dict) or not isinstance(doc.get("data"), dict):
        raise RuntimeError(f"github graphql failed ({proc.returncode}): {(proc.stderr or proc.stdout)[:300]}")
    return doc["data"]


def _split(repo: str) -> tuple[str, str]:
    owner, name = repo.split("/", 1)
    return json.dumps(owner), json.dumps(name)


def list_open(cfg: Config) -> list[dict]:
    parts = []
    for i, repo in enumerate(cfg.repos):
        owner, name = _split(repo)
        parts.append(f"r{i}: repository(owner: {owner}, name: {name}) {{ pullRequests(states: OPEN, "
                     f"first: {cfg.max_prs_per_repo}, orderBy: {{field: UPDATED_AT, direction: DESC}}) "
                     "{ nodes { number isDraft headRefOid headRefName title body "
                     "author { login __typename } } } }")
    data = _graphql(cfg, "query { " + " ".join(parts) + " }")
    prs = []
    for i, repo in enumerate(cfg.repos):
        node = data.get(f"r{i}")
        if not isinstance(node, dict):
            print(f"pr-lifecycle: {repo}: unreadable, skipped (unknown)", file=sys.stderr)
            continue
        for p in node["pullRequests"]["nodes"]:
            author = p.get("author") or {}
            prs.append({"repo": repo, "number": p["number"], "draft": bool(p["isDraft"]),
                        "head_ref_oid": p["headRefOid"], "head_ref": p["headRefName"],
                        "title": p.get("title") or "", "body": p.get("body") or "",
                        "author": author.get("login"), "author_is_bot": author.get("__typename") == "Bot"})
    return prs


_DETAIL = """
 commits(last: 1) { nodes { commit { oid statusCheckRollup { contexts(first: 100) {
   pageInfo { hasNextPage }
   nodes { __typename
     ... on CheckRun { name status conclusion startedAt isRequired(pullRequestNumber: %(n)d) }
     ... on StatusContext { context state createdAt isRequired(pullRequestNumber: %(n)d) } } } } } } }
 reviews(last: 100) { pageInfo { hasPreviousPage }
   nodes { state submittedAt author { login } commit { oid } } }
 comments(last: 100) { pageInfo { hasPreviousPage }
   nodes { createdAt author { login } body } }
"""


def _context(c: dict) -> dict:
    if c.get("__typename") == "CheckRun":
        state = c.get("conclusion") if c.get("status") == "COMPLETED" else "PENDING"
        return {"name": c.get("name"), "state": state or "PENDING", "required": bool(c.get("isRequired")),
                "at": c.get("startedAt")}
    return {"name": c.get("context"), "state": c.get("state") or "PENDING", "required": bool(c.get("isRequired")),
            "at": c.get("createdAt")}


def latest_per_name(contexts: list[dict]) -> list[dict]:
    """The newest run of each check name, as the GitHub checks view shows it: a
    re-run or superseded run must not leave an old CANCELLED or FAILURE behind.
    A run not yet started counts as the newest. Requiredness sticks to the name."""
    best: dict[str, dict] = {}
    for c in contexts:
        name = c.get("name") or ""
        prior = best.get(name)
        if prior is None or (c.get("at") or "9999") >= (prior.get("at") or "9999"):
            best[name] = {**c, "required": c.get("required") or bool(prior and prior.get("required"))}
        elif c.get("required"):
            prior["required"] = True
    return list(best.values())


def add_details(cfg: Config, prs: list[dict]) -> list[dict]:
    todo = [p for p in prs if not skipped(cfg, p)]
    out = []
    for start in range(0, len(todo), MAX_BATCH):
        batch = todo[start:start + MAX_BATCH]
        parts = []
        for i, p in enumerate(batch):
            owner, name = _split(p["repo"])
            parts.append(f"p{i}: repository(owner: {owner}, name: {name}) "
                         f"{{ pullRequest(number: {int(p['number'])}) {{ {_DETAIL % {'n': int(p['number'])}} }} }}")
        data = _graphql(cfg, "query { " + " ".join(parts) + " }")
        for i, p in enumerate(batch):
            node = ((data.get(f"p{i}") or {}).get("pullRequest")) or None
            if not node:
                continue  # unreadable PR: unknown, no record
            commits = node["commits"]["nodes"]
            commit = commits[0]["commit"] if commits else {}
            rollup = commit.get("statusCheckRollup")
            checks, complete = None, False
            if rollup is not None:
                ctx = rollup["contexts"]
                checks = latest_per_name([_context(c) for c in ctx["nodes"]])
                complete = not ctx["pageInfo"]["hasNextPage"]
            out.append({**p, "head": commit.get("oid"), "checks": checks, "checks_complete": complete,
                        "reviews": [{"state": r["state"], "at": r.get("submittedAt"),
                                     "author": (r.get("author") or {}).get("login"),
                                     "commit": (r.get("commit") or {}).get("oid")}
                                    for r in node["reviews"]["nodes"]],
                        "reviews_complete": not node["reviews"]["pageInfo"]["hasPreviousPage"],
                        "comments": [{"author": (c.get("author") or {}).get("login"), "at": c.get("createdAt"),
                                      "body": (c.get("body") or "")[:400]} for c in node["comments"]["nodes"]],
                        "comments_complete": not node["comments"]["pageInfo"]["hasPreviousPage"]})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="chaski-pr-lifecycle", description=__doc__.splitlines()[0])
    ap.add_argument("--config", required=True)
    ap.add_argument("--fixture", help="JSON list of normalized PRs; no network")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.fixture:
            prs = json.loads(Path(args.fixture).read_text())
        else:
            prs = add_details(cfg, list_open(cfg))
        out = records(cfg, prs, dt.datetime.now(dt.timezone.utc))
    except (ConfigError, RuntimeError, OSError, ValueError, KeyError, TypeError,
            subprocess.TimeoutExpired) as exc:
        print(f"pr-lifecycle: {type(exc).__name__}: {exc}"[:500], file=sys.stderr)
        return 1
    json.dump(out, sys.stdout, sort_keys=True)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
