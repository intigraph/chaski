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

Only trusted input recruits: a PR is judged only when its author is trusted,
and a GitHub review counts only from a trusted reviewer. Trusted = GitHub
authorAssociation in `trusted_associations` (default OWNER, MEMBER,
COLLABORATOR) or a login in `trusted_logins` / `verdict_logins`. A missing
association is untrusted. A drive-by review or an outside author's PR on a
public repo therefore recruits nothing.

A verdict is a GitHub review (APPROVED / CHANGES_REQUESTED) by a trusted login
other than the PR author on the head commit, or a crew verdict comment from a
configured `verdict_logins` login whose first line is
`APPROVE at head <40-hex>` or `REQUEST CHANGES at head <40-hex>`. Crew share
one operator login, so the comment form is how crew reviews are recorded;
the merge job re-verifies independence and the head before merging.

Main moving does not re-arm review (Stiwi 2026-11-28, "Stop re reviewing
things, even when main change[s]"): a verdict on an earlier PR commit still
counts for the head when every commit from it to the head is a merge commit
that only brings in base-branch commits ("Update branch"). That is judged
from the PR's complete commit list. A REQUEST CHANGES carries on that shape
alone (carrying a block is the safe direction). An APPROVE carries only
across a merge proved clean: its tree equals `git merge-tree --write-tree` of
its two parents with no conflict, so a conflict resolution or any extra edit
in the merge is new code and needs a review. A truncated list, a rebase, a
real commit after the verdict or an unprovable merge needs a fresh review.

Review policy comes from Quipu (Stiwi 2026-11-28): the stored query
`repo_review_mode`, the same one st's merge gate reads, is asked once per run
per repo. A repo whose mode is "none" never gets a review: a green non-draft
head merges (reason `green-and-approved`, the approval being waived by
policy, which st's merge gate re-applies) and a red head or a standing
REQUEST CHANGES gets a fix task. Any other answer, or an unreadable one, keeps
the review-required behaviour.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from chaski_version import add_version_argument

REPO_RE = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+")
SHA_RE = re.compile(r"[0-9a-f]{40}")
VERDICT_LINE = re.compile(r"\s*(APPROVE|REQUEST[ _-]CHANGES)\s+at\s+head\s+([0-9a-f]{40})\b", re.I)
PASS = {"SUCCESS", "NEUTRAL", "SKIPPED"}
# CANCELLED and STALE are neither: a superseded or abandoned run never recruits
# a fix job and never counts as green.
FAIL = {"FAILURE", "ERROR", "TIMED_OUT", "ACTION_REQUIRED", "STARTUP_FAILURE"}
JOBS = ("review", "task", "merge")
TRUSTED_ASSOCIATIONS = ("OWNER", "MEMBER", "COLLABORATOR")
ASSOCIATIONS = {"OWNER", "MEMBER", "COLLABORATOR", "CONTRIBUTOR", "FIRST_TIME_CONTRIBUTOR",
                "FIRST_TIMER", "MANNEQUIN", "NONE"}
MAX_BATCH = 20
REVIEW_MODE_QUERY = "repo_review_mode"
REVIEW_MODE_LIMIT = 8
URL_RE = re.compile(r"https?://[^\s]+")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    repos: tuple[str, ...]
    verdict_logins: frozenset[str] = frozenset()
    trusted_associations: frozenset[str] = frozenset(TRUSTED_ASSOCIATIONS)
    trusted_logins: frozenset[str] = frozenset()
    item_prefixes: tuple[str, ...] = ("aegis",)
    skip_authors: frozenset[str] = frozenset()
    skip_branch_prefixes: tuple[str, ...] = ("release-please--",)
    max_prs_per_repo: int = 30
    gh: tuple[str, ...] = ("gh",)
    timeout_s: int = 60
    carry_verdicts_over_base_merges: bool = True
    quipu_url: str | None = None
    git_cache: str | None = None
    git: tuple[str, ...] = ("git",)
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
    assoc = strings("trusted_associations", TRUSTED_ASSOCIATIONS)
    if not all(a in ASSOCIATIONS for a in assoc):
        raise ConfigError(f"trusted_associations must be GitHub author associations: {sorted(ASSOCIATIONS)}")
    carry = doc.get("carry_verdicts_over_base_merges", True)
    if type(carry) is not bool:
        raise ConfigError("carry_verdicts_over_base_merges must be true or false")
    def command(key, default):
        v = doc.get(key, [default])
        v = [v] if isinstance(v, str) else v
        if not isinstance(v, list) or not v or not all(isinstance(s, str) and s for s in v):
            raise ConfigError(f"{key} must be a command (string or argv list)")
        return tuple(v)

    gh, git = command("gh", "gh"), command("git", "git")
    quipu = doc.get("quipu_url")
    if quipu is not None and not (isinstance(quipu, str) and URL_RE.fullmatch(quipu)):
        raise ConfigError("quipu_url must be an http(s) URL")
    cache = doc.get("git_cache")
    if cache is not None and not (isinstance(cache, str) and cache.startswith("/")):
        raise ConfigError("git_cache must be an absolute directory path")
    return Config(repos=tuple(repos), verdict_logins=frozenset(strings("verdict_logins")),
                  trusted_associations=frozenset(assoc), trusted_logins=frozenset(strings("trusted_logins")),
                  item_prefixes=tuple(prefixes), skip_authors=frozenset(strings("skip_authors")),
                  skip_branch_prefixes=tuple(strings("skip_branch_prefixes", ("release-please--",))),
                  max_prs_per_repo=n, gh=gh, timeout_s=t,
                  carry_verdicts_over_base_merges=carry, quipu_url=quipu, git_cache=cache, git=git)


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


def trusted(cfg: Config, login: str | None, association: str | None) -> bool:
    """A login whose GitHub input may recruit a job: a configured login, or a
    trusted repo association. Missing association is untrusted."""
    if not login:
        return False
    return (login in cfg.trusted_logins or login in cfg.verdict_logins
            or association in cfg.trusted_associations)


def skipped(cfg: Config, pr: dict) -> bool:
    author = pr.get("author") or ""
    return bool(pr.get("draft") or pr.get("author_is_bot") or author.endswith("[bot]")
                or author in cfg.skip_authors
                or not trusted(cfg, author, pr.get("author_association"))
                or any((pr.get("head_ref") or "").startswith(p) for p in cfg.skip_branch_prefixes))


def base_merges(cfg: Config, pr: dict) -> list[tuple[str, list[str]]]:
    """The merge commits on the head's first-parent line, newest first, that
    only bring base-branch commits in: [(merge, parents)].

    Needs the PR's complete commit list (`commits`, oldest first, each with
    `oid` and `parents`; `commits_complete`). A merge parent outside that list
    is on the base branch, because GitHub lists every commit reachable from the
    head and not from the base. A merge parent inside the list (another branch
    merged into the PR) is new code and stops the walk, as does any ordinary
    commit, a list that does not end at the head, or a truncated list.
    """
    head = pr["head"]
    commits = pr.get("commits")
    if (not cfg.carry_verdicts_over_base_merges or not pr.get("commits_complete")
            or not isinstance(commits, list) or not commits or commits[-1].get("oid") != head):
        return []
    parents = {c.get("oid"): c.get("parents") for c in commits}
    out, cur, seen = [], head, set()
    while cur not in seen:
        seen.add(cur)
        ps = parents.get(cur)
        if not isinstance(ps, list) or len(ps) < 2:
            break  # an ordinary commit (or unknown parents): the PR changed here
        first, others = ps[0], ps[1:]
        if first not in parents or any(o in parents for o in others):
            break  # not a base-only merge on the PR's first-parent line
        out.append((cur, list(ps)))
        cur = first
    return out


def verdict_commits(cfg: Config, pr: dict, *, proven: bool = True) -> frozenset[str]:
    """The commits whose verdicts count for the head: the head, plus each
    earlier PR commit reached from it through base-only merges (base_merges).

    proven=True (what an APPROVE needs) also requires each merge on the way to
    be proved clean (`pr["clean_merges"][merge] is True`, see merge_is_clean):
    a merge whose tree is not the clean automatic merge of its parents carries
    a conflict resolution or an extra edit, which nobody reviewed. proven=False
    (what a REQUEST CHANGES needs: carrying a block is fail-safe) takes the
    commit-list shape alone.
    """
    proofs = pr.get("clean_merges") or {}
    out = {pr["head"]}
    for merge, ps in base_merges(cfg, pr):
        if proven and proofs.get(merge) is not True:
            break
        out.add(ps[0])
    return frozenset(out)


def head_verdicts(cfg: Config, pr: dict) -> list[str] | None:
    """Chronological verdicts ('approve'/'changes') that count for the current
    head (see verdict_commits), or None when the review/comment lists were
    truncated (unknown)."""
    if not pr.get("reviews_complete") or not pr.get("comments_complete"):
        return None
    author = pr.get("author")
    counted = {"approve": verdict_commits(cfg, pr), "changes": verdict_commits(cfg, pr, proven=False)}
    timed = []
    for r in pr.get("reviews", []):
        state = r.get("state")
        word = {"APPROVED": "approve", "CHANGES_REQUESTED": "changes"}.get(state)
        if (word and r.get("commit") in counted[word] and r.get("author") and r["author"] != author
                and trusted(cfg, r["author"], r.get("association"))):
            timed.append((r.get("at") or "", word))
    for c in pr.get("comments", []):
        if c.get("author") not in cfg.verdict_logins:
            continue
        m = VERDICT_LINE.match(c.get("body") or "")
        word = m and ("approve" if m.group(1).upper() == "APPROVE" else "changes")
        if m and m.group(2).lower() in counted[word]:
            timed.append((c.get("at") or "", word))
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
    elif pr.get("review_mode") == "none":
        # Quipu says this repo needs no review: never recruit one. Green merges;
        # unknown verdicts (a standing REQUEST CHANGES may be hidden) wait.
        if verdicts is not None and checks == "green":
            out.append(("merge", "green-and-approved"))
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


# ---------------------------------------------------------------- review policy

def review_mode_from_rows(rows) -> str:
    """st's rule (shantytown ephemeral.review_mode_from_rows): only a single,
    unanimous "none" waives review; no row, an unknown value or conflicting
    rows mean "non-author"."""
    if not isinstance(rows, list):
        return "non-author"
    modes = set()
    for row in rows:
        value = row.get("mode") if isinstance(row, dict) else None
        if isinstance(value, dict):
            value = value.get("value")
        modes.add(str(value).rsplit("/", 1)[-1].rsplit("#", 1)[-1] if value else "")
    return "none" if modes == {"none"} else "non-author"


def _ask(cfg: Config, name: str, params: dict) -> list:
    req = urllib.request.Request(
        cfg.quipu_url.rstrip("/") + "/ask", data=json.dumps({"name": name, "params": params}).encode(),
        method="POST", headers={"Content-Type": "application/json", "User-Agent": "chaski-pr-lifecycle/1",
                                "X-Quipu-Client": "chaski"})
    with urllib.request.urlopen(req, timeout=min(cfg.timeout_s, 10)) as resp:
        result = json.loads(resp.read(1_000_001))
    if (not isinstance(result, dict) or result.get("error") or result.get("truncated") is not False
            or not isinstance(result.get("rows"), list) or type(result.get("count")) is not int
            or result["count"] != len(result["rows"]) or result["count"] >= REVIEW_MODE_LIMIT):
        raise ValueError("named query completeness UNKNOWN")
    return result["rows"]


def review_modes(cfg: Config, repos, ask=_ask) -> dict[str, str | None]:
    """{repo: "none" | "non-author" | None}, one Quipu read per repo. None is
    unreadable (no quipu_url, timeout, refusal, truncation): review stays
    required, as it is for "non-author"."""
    out: dict[str, str | None] = {}
    for repo in sorted(set(repos)):
        if not cfg.quipu_url:
            out[repo] = None
            continue
        try:
            out[repo] = review_mode_from_rows(ask(cfg, REVIEW_MODE_QUERY, {"repo": repo}))
        except Exception as exc:  # noqa: BLE001 - unreadable is UNKNOWN, never "none"
            print(f"pr-lifecycle: {repo}: review mode unreadable ({type(exc).__name__}); "
                  "review required", file=sys.stderr)
            out[repo] = None
    return out


# ---------------------------------------------------------------- merge proofs

def _git(cfg: Config, gitdir: Path | None, *args: str) -> subprocess.CompletedProcess:
    helper = "!" + shlex.join([*cfg.gh, "auth", "git-credential"])
    pre = [*cfg.git, "-c", "credential.helper=", "-c", f"credential.helper={helper}"]
    if gitdir is not None:
        pre += ["--git-dir", str(gitdir)]
    return subprocess.run([*pre, *args], capture_output=True, text=True, timeout=cfg.timeout_s,
                          check=False, env={**os.environ, "GIT_TERMINAL_PROMPT": "0"})


def merge_is_clean(cfg: Config, gitdir: Path, merge: str, parents: list[str]) -> bool:
    """True only when `merge` has exactly these two parents and its tree is the
    clean automatic merge of them (`git merge-tree --write-tree`, no conflict).
    Anything else (an evil merge, a conflict resolution, an octopus, a git
    failure) is False: the approval does not carry."""
    if len(parents) != 2 or not all(SHA_RE.fullmatch(x or "") for x in [merge, *parents]):
        return False
    got = _git(cfg, gitdir, "rev-list", "--parents", "-n", "1", merge)
    if got.returncode != 0 or got.stdout.split() != [merge, *parents]:
        return False
    tree = _git(cfg, gitdir, "rev-parse", "--verify", f"{merge}^{{tree}}")
    clean = _git(cfg, gitdir, "merge-tree", "--write-tree", "--no-messages", *parents)
    lines = clean.stdout.split()
    return (tree.returncode == 0 and clean.returncode == 0 and len(lines) >= 1
            and SHA_RE.fullmatch(lines[0]) is not None and lines[0] == tree.stdout.strip())


def _repo_git(cfg: Config, repo: str, oids: list[str]) -> Path:
    """A blobless bare mirror of `repo` under git_cache holding `oids`
    (fetched only when missing). Raises on any failure."""
    gitdir = Path(cfg.git_cache) / (repo.replace("/", "__") + ".git")
    url = f"https://github.com/{repo}.git"
    if not (gitdir / "HEAD").exists():
        gitdir.parent.mkdir(parents=True, exist_ok=True)
        r = _git(cfg, None, "clone", "--bare", "--quiet", "--filter=blob:none", "--no-tags", url, str(gitdir))
        if r.returncode != 0:
            raise RuntimeError(f"git clone {repo} failed: {r.stderr[:200]}")
    missing = [o for o in oids if _git(cfg, gitdir, "cat-file", "-e", f"{o}^{{commit}}").returncode != 0]
    if missing:
        r = _git(cfg, gitdir, "fetch", "--quiet", "--no-tags", "origin", *missing)
        if r.returncode != 0:
            raise RuntimeError(f"git fetch {repo} failed: {r.stderr[:200]}")
    return gitdir


def _verdict_targets(cfg: Config, pr: dict) -> set[str]:
    """Commits an APPROVE names (trusted review or crew comment), unfiltered by
    head; used only to decide whether a merge proof is worth running."""
    out = {r.get("commit") for r in pr.get("reviews", []) if r.get("state") == "APPROVED"}
    for c in pr.get("comments", []):
        m = c.get("author") in cfg.verdict_logins and VERDICT_LINE.match(c.get("body") or "")
        if m and m.group(1).upper() == "APPROVE":
            out.add(m.group(2).lower())
    return {o for o in out if o}


def add_merge_proofs(cfg: Config, prs: list[dict], prove=None) -> list[dict]:
    """Set pr["clean_merges"] = {merge: bool} for the base-only merges an
    APPROVE would have to carry across, newest first, stopping at the first
    merge that is not clean. Only PRs whose APPROVE sits on an earlier commit
    of that line are proved, so a tick with no main sync runs no git at all.
    No git_cache means no proof, so an APPROVE does not carry."""
    for pr in prs:
        if "clean_merges" in pr or skipped(cfg, pr) or not SHA_RE.fullmatch(pr.get("head") or ""):
            continue
        chain = base_merges(cfg, pr)
        targets = _verdict_targets(cfg, pr)
        reach = [i for i, (_, ps) in enumerate(chain) if ps[0] in targets]
        if not reach:
            continue
        chain = chain[:reach[-1] + 1]
        proofs: dict[str, bool] = {}
        if cfg.git_cache or prove:
            try:
                if prove is None:
                    gitdir = _repo_git(cfg, pr["repo"], [m for m, _ in chain])
                    check = lambda m, ps: merge_is_clean(cfg, gitdir, m, ps)  # noqa: E731
                else:
                    check = prove
                for merge, ps in chain:
                    proofs[merge] = bool(check(merge, ps))
                    if not proofs[merge]:
                        print(f"pr-lifecycle: {pr['repo']}#{pr['number']}: merge {merge[:12]} is not a "
                              "clean merge of its parents; approval does not carry", file=sys.stderr)
                        break
            except Exception as exc:  # noqa: BLE001 - unproved is not clean
                print(f"pr-lifecycle: {pr['repo']}#{pr['number']}: merge proof unknown "
                      f"({type(exc).__name__}: {str(exc)[:200]}); approval does not carry", file=sys.stderr)
        pr["clean_merges"] = proofs
    return prs


def add_review_modes(cfg: Config, prs: list[dict], ask=_ask) -> list[dict]:
    judged = [p for p in prs if not skipped(cfg, p) and "review_mode" not in p]
    modes = review_modes(cfg, [p["repo"] for p in judged], ask=ask) if judged else {}
    for p in judged:
        p["review_mode"] = modes.get(p["repo"])
    return prs


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
                     "{ nodes { number isDraft headRefOid headRefName title body authorAssociation "
                     "author { login __typename } } pageInfo { hasNextPage } } }")
    data = _graphql(cfg, "query { " + " ".join(parts) + " }")
    prs = []
    for i, repo in enumerate(cfg.repos):
        node = data.get(f"r{i}")
        if not isinstance(node, dict):
            print(f"pr-lifecycle: {repo}: unreadable, skipped (unknown)", file=sys.stderr)
            continue
        if node["pullRequests"].get("pageInfo", {}).get("hasNextPage"):
            print(f"pr-lifecycle: {repo}: more than {cfg.max_prs_per_repo} open PRs; least recently "
                  "updated ones not read this run (truncated)", file=sys.stderr)
        for p in node["pullRequests"]["nodes"]:
            author = p.get("author") or {}
            prs.append({"repo": repo, "number": p["number"], "draft": bool(p["isDraft"]),
                        "head_ref_oid": p["headRefOid"], "head_ref": p["headRefName"],
                        "title": p.get("title") or "", "body": p.get("body") or "",
                        "author": author.get("login"),
                        "author_association": p.get("authorAssociation"),
                        "author_is_bot": author.get("__typename") == "Bot"})
    return prs


_DETAIL = """
 commits(last: 1) { nodes { commit { oid statusCheckRollup { contexts(first: 100) {
   pageInfo { hasNextPage }
   nodes { __typename
     ... on CheckRun { name status conclusion startedAt isRequired(pullRequestNumber: %(n)d) }
     ... on StatusContext { context state createdAt isRequired(pullRequestNumber: %(n)d) } } } } } } }
 chain: commits(last: 100) { pageInfo { hasPreviousPage }
   nodes { commit { oid parents(first: 5) { totalCount nodes { oid } } } } }
 reviews(last: 100) { pageInfo { hasPreviousPage }
   nodes { state submittedAt authorAssociation author { login } commit { oid } } }
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


def _chain(chain: dict | None) -> dict:
    """The PR's commit list, oldest first, for verdict_commits. Incomplete when
    GitHub truncated it or a commit's parent list was cut off."""
    if not isinstance(chain, dict):
        return {"commits": None, "commits_complete": False}
    commits, complete = [], not chain["pageInfo"]["hasPreviousPage"]
    for n in chain["nodes"]:
        c = n.get("commit") or {}
        ps = c.get("parents") or {}
        oids = [p.get("oid") for p in ps.get("nodes") or []]
        if ps.get("totalCount") != len(oids):
            complete = False
        commits.append({"oid": c.get("oid"), "parents": oids})
    return {"commits": commits, "commits_complete": complete}


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
            if node["reviews"]["pageInfo"]["hasPreviousPage"] or node["comments"]["pageInfo"]["hasPreviousPage"]:
                print(f"pr-lifecycle: {p['repo']}#{p['number']}: more than 100 reviews or comments; "
                      "verdicts unknown, no event (stalled)", file=sys.stderr)
            out.append({**p, "head": commit.get("oid"), "checks": checks, "checks_complete": complete,
                        "reviews": [{"state": r["state"], "at": r.get("submittedAt"),
                                     "author": (r.get("author") or {}).get("login"),
                                     "association": r.get("authorAssociation"),
                                     "commit": (r.get("commit") or {}).get("oid")}
                                    for r in node["reviews"]["nodes"]],
                        "reviews_complete": not node["reviews"]["pageInfo"]["hasPreviousPage"],
                        "comments": [{"author": (c.get("author") or {}).get("login"), "at": c.get("createdAt"),
                                      "body": (c.get("body") or "")[:400]} for c in node["comments"]["nodes"]],
                        "comments_complete": not node["comments"]["pageInfo"]["hasPreviousPage"],
                        **_chain(node.get("chain"))})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="chaski-pr-lifecycle", description=__doc__.splitlines()[0])
    add_version_argument(ap, "chaski-pr-lifecycle")
    ap.add_argument("--config", required=True)
    ap.add_argument("--fixture", help="JSON list of normalized PRs; no network")
    args = ap.parse_args(argv)
    try:
        cfg = load_config(args.config)
        if args.fixture:
            prs = json.loads(Path(args.fixture).read_text())
        else:
            prs = add_details(cfg, list_open(cfg))
        prs = add_merge_proofs(cfg, add_review_modes(cfg, prs))
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
