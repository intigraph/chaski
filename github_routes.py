#!/usr/bin/env python3
"""GitHub routing rules: GitHub activity facts in Quipu -> work requests.

An incremental verdict adapter (`trigger: changes`, docs/github-routing.md).
It reads the append-only `aegis:GitHubActivity` records that camayoc's GitHub
ingress writes into ONE `github` graph (camayoc docs/design/github-ingress.md),
plus agent review verdict records, and decides per work item:

    WAIT   not yet (CI pending, review timer running, merge lane busy)
    READY  emit ONE work request for this (repo, subject, revision, kind)

The rules, all declared in one YAML file (github-routing.example.yaml):

  review   PR open, not draft, CI green on its head, and no non-author review
           of that head for `review_after_minutes`.
  merge    An approving verdict from an agent other than the PR's author agent
           and CI green, both on the SAME head. Goes through ONE serialized
           merge lane per repository: one merge in flight, `spacing_minutes`
           between merges, head-of-line by approval time.
  outside-contribution
           The same condition when the PR author is not a declared member.
           Never a merge job: routed to the administrator.
  triage   The latest run of a workflow on the default branch is red.
  install / announce
           A release was published (per-repo job list).

Idempotency: the item key names (kind, repo, subject, revision) and the event
id is a hash of the key, so one request exists per (repo, PR, head sha, kind)
whatever the replays. The adapter is stateless and writes nothing: it reads
Quipu and, read-only, the work ledger's merge-lane table.

A failed or truncated read raises, so chaski records UNKNOWN and keeps every
previous verdict. "Could not look" never rounds to "nothing to do".
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import yaml

AEGIS = "http://aegis.gastown.local/ontology/"
SCHEMA = "chaski.github.work-request/v1"
WAIT, READY = "WAIT", "READY"
KINDS = ("review", "merge", "outside-contribution", "triage", "install", "announce")
RELEASE_JOBS = ("install", "announce")
GREEN = {"success", "skipped", "neutral"}
RED = {"failure", "timed_out", "startup_failure", "cancelled_by_failure"}
TERMINAL_RUN = "completed"
REPO = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
LOGIN = re.compile(r"^[A-Za-z0-9_.\[\]-]{1,64}$")
IRI = re.compile(r"^https?://[^<>\s\"{}|\\^`]+$")
SHA = re.compile(r"^[0-9a-f]{40}$")
APPROVE_WORDS = {"approve", "approved", "accept", "accepted"}
CHANGES_WORDS = {"changes", "request-changes", "changes_requested", "reject", "rejected"}
# Verdict records: the GateApproval split (camayoc agent-memory design 1.3)
# plus the pre-migration aegis:Decision class, read through one release.
VERDICT_CLASSES = ("GateApproval", "Decision")
LANE_RECHECK_S = 300


class ConfigError(ValueError):
    pass


class FactsUnknown(RuntimeError):
    """The facts could not be read completely. The caller reports UNKNOWN."""


# --------------------------------------------------------------------------- config

DEFAULTS = {
    "default_branch": "main",
    "review_after_minutes": 15,
    "required_checks": [],
    "approval_sources": ["verdict"],
    "merge": {"enabled": True, "spacing_minutes": 10, "lease_minutes": 75},
    "triage": {"enabled": True},
    "release": {"jobs": list(RELEASE_JOBS), "include_prereleases": False, "window_hours": 24},
}


@dataclass
class RepoConfig:
    name: str
    default_branch: str
    review_after_s: int
    required_checks: tuple[str, ...]
    approval_sources: tuple[str, ...]
    merge_enabled: bool
    spacing_s: int
    lease_s: int
    triage_enabled: bool
    release_jobs: tuple[str, ...]
    include_prereleases: bool
    release_window_s: int


@dataclass
class Config:
    graph: str
    administrator: str
    members: frozenset[str]
    ledger: Path | None
    window_days: int
    repos: dict[str, RepoConfig]
    raw: dict = field(default_factory=dict)

    def is_member(self, login: str | None) -> bool:
        return bool(login) and login.lower() in self.members


def _section(base: dict, override: dict | None, key: str) -> dict:
    if override is None:
        return dict(base)
    if not isinstance(override, dict):
        raise ConfigError(f"{key} must be a mapping")
    unknown = set(override) - set(base)
    if unknown:
        raise ConfigError(f"{key}: unknown field(s) {sorted(unknown)}")
    return {**base, **override}


def _int(value, key, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ConfigError(f"{key} must be an integer in [{low}, {high}]")
    return value


def load_config(path: Path | str) -> Config:
    doc = yaml.safe_load(Path(path).read_text()) or {}
    return parse_config(doc)


def parse_config(doc: dict) -> Config:
    if not isinstance(doc, dict) or doc.get("version") != 1:
        raise ConfigError("config needs version: 1")
    known = {"version", "graph", "administrator", "members", "ledger", "window_days", "defaults", "repos",
             "work_items", "spawner"}
    if set(doc) - known:
        raise ConfigError(f"unknown top-level field(s) {sorted(set(doc) - known)}")
    graph = doc.get("graph")
    if not isinstance(graph, str) or not IRI.match(graph):
        raise ConfigError("graph must be the github graph IRI")
    admin = doc.get("administrator")
    if not isinstance(admin, str) or not LOGIN.match(admin):
        raise ConfigError("administrator must name the administrator agent")
    members = doc.get("members")
    if not isinstance(members, list) or not members or any(
            not isinstance(m, str) or not LOGIN.match(m) for m in members):
        raise ConfigError("members must list the GitHub logins whose PRs may be merged by the lane;"
                          " every other author is an outside contribution")
    defaults = doc.get("defaults") or {}
    if not isinstance(defaults, dict):
        raise ConfigError("defaults must be a mapping")
    base = {k: (_section(v, defaults.get(k), f"defaults.{k}") if isinstance(v, dict) else defaults.get(k, v))
            for k, v in DEFAULTS.items()}
    if set(defaults) - set(DEFAULTS):
        raise ConfigError(f"defaults: unknown field(s) {sorted(set(defaults) - set(DEFAULTS))}")
    repos_doc = doc.get("repos")
    if not isinstance(repos_doc, dict) or not repos_doc:
        raise ConfigError("repos must name at least one owner/name")
    repos = {}
    for name, override in repos_doc.items():
        if not isinstance(name, str) or not REPO.match(name):
            raise ConfigError(f"repo {name!r} is not owner/name")
        override = override or {}
        if not isinstance(override, dict) or set(override) - set(DEFAULTS):
            raise ConfigError(f"repos.{name}: unknown field(s)")
        merged = {k: (_section(base[k], override.get(k), f"repos.{name}.{k}") if isinstance(base[k], dict)
                      else override.get(k, base[k])) for k in DEFAULTS}
        repos[name] = _repo_config(name, merged)
    ledger = doc.get("ledger")
    if ledger is not None and not isinstance(ledger, str):
        raise ConfigError("ledger must be a path")
    return Config(graph=graph, administrator=admin, members=frozenset(m.lower() for m in members),
                  ledger=Path(ledger).expanduser() if ledger else None,
                  window_days=_int(doc.get("window_days", 30), "window_days", 1, 365),
                  repos=repos, raw=doc)


def _repo_config(name: str, d: dict) -> RepoConfig:
    key = f"repos.{name}"
    branch = d["default_branch"]
    if not isinstance(branch, str) or not branch:
        raise ConfigError(f"{key}.default_branch must be a branch name")
    checks = d["required_checks"]
    if not isinstance(checks, list) or any(not isinstance(c, str) or not c for c in checks):
        raise ConfigError(f"{key}.required_checks must be a list of check or workflow names")
    sources = d["approval_sources"]
    if (not isinstance(sources, list) or not sources
            or any(s not in ("verdict", "github_review") for s in sources)):
        raise ConfigError(f"{key}.approval_sources must be a non-empty subset of [verdict, github_review]")
    jobs = d["release"]["jobs"]
    if not isinstance(jobs, list) or any(j not in RELEASE_JOBS for j in jobs):
        raise ConfigError(f"{key}.release.jobs must be a subset of {list(RELEASE_JOBS)}")
    for flag, value in (("merge.enabled", d["merge"]["enabled"]), ("triage.enabled", d["triage"]["enabled"]),
                        ("release.include_prereleases", d["release"]["include_prereleases"])):
        if type(value) is not bool:
            raise ConfigError(f"{key}.{flag} must be true or false")
    spacing = _int(d["merge"]["spacing_minutes"], f"{key}.merge.spacing_minutes", 0, 1440)
    lease = _int(d["merge"]["lease_minutes"], f"{key}.merge.lease_minutes", 1, 1440)
    return RepoConfig(
        name=name, default_branch=branch,
        review_after_s=60 * _int(d["review_after_minutes"], f"{key}.review_after_minutes", 0, 10080),
        required_checks=tuple(checks), approval_sources=tuple(sources),
        merge_enabled=d["merge"]["enabled"], spacing_s=60 * spacing, lease_s=60 * lease,
        triage_enabled=d["triage"]["enabled"], release_jobs=tuple(jobs),
        include_prereleases=d["release"]["include_prereleases"],
        release_window_s=3600 * _int(d["release"]["window_hours"], f"{key}.release.window_hours", 1, 720))


# --------------------------------------------------------------------------- keys

def item_key(kind: str, repo: str, subject: str, revision: str) -> str:
    if kind not in KINDS:
        raise ValueError(f"unknown work kind {kind!r}")
    return f"github:{kind}:{repo}:{subject}:{revision}"


def parse_item(item: str) -> tuple[str, str, str, str]:
    parts = item.split(":")
    if len(parts) != 5 or parts[0] != "github" or parts[1] not in KINDS or not REPO.match(parts[2]):
        raise ValueError(f"not a github work item key: {item!r}")
    return parts[1], parts[2], parts[3], parts[4]


def request_id(item: str) -> str:
    """Deterministic from the key alone: replays and restarts reproduce it."""
    return hashlib.sha256(f"{SCHEMA}\n{item}".encode()).hexdigest()


# --------------------------------------------------------------------------- facts

def epoch(text: str | None) -> float | None:
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(str(text).replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        return None
    return dt.timestamp()


def iso(ts: float | None) -> str | None:
    return None if ts is None else datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def local(value) -> str | None:
    """The local name of an IRI (or the literal itself)."""
    if value is None:
        return None
    text = str(value)
    for sep in ("#", "/"):
        if sep in text and text.startswith("http"):
            text = text.rsplit(sep, 1)[1]
    return text or None


def _bool(value):
    if isinstance(value, bool) or value is None:
        return value
    text = str(value).strip().lower()
    return True if text == "true" else False if text == "false" else None


def _intv(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value))
    except ValueError:
        return None


ROW_FIELDS = ("a", "kind", "at", "actor", "sha", "state", "merged", "draft", "base", "status", "conclusion",
              "attempt", "review", "pre", "e", "etype", "num", "author", "name", "tag", "url", "thread", "tnum")


def normalize_row(raw: dict) -> dict:
    """One activity joined with the entity it is about, typed."""
    row = {k: raw.get(k) for k in ROW_FIELDS}
    for k in ("merged", "draft", "pre"):
        row[k] = _bool(row[k])
    for k in ("attempt", "num", "tnum"):
        row[k] = _intv(row[k])
    row["etype"] = local(row["etype"])
    for k in ("a", "e", "thread"):
        row[k] = None if row[k] is None else str(row[k])
    for k in ("kind", "at", "actor", "sha", "state", "base", "status", "conclusion", "review", "author", "name",
              "tag", "url"):
        row[k] = None if row[k] is None else str(row[k])
    if row["review"]:
        row["review"] = row["review"].lower()
    row["ts"] = epoch(row["at"])
    return row


def normalize_verdict(raw: dict) -> dict | None:
    outcome = str(raw.get("outcome") or "").strip().lower()
    verdict = "approve" if outcome in APPROVE_WORDS else "changes" if outcome in CHANGES_WORDS else None
    sha = str(raw.get("sha") or "").lower()
    ts = epoch(raw.get("at"))
    by = local(raw.get("by"))
    if verdict is None or not SHA.match(sha) or ts is None or not by or not raw.get("pr"):
        return None  # malformed records never count, either way
    return {"v": str(raw.get("v") or ""), "pr": str(raw["pr"]), "sha": sha, "verdict": verdict,
            "by": by.lower(), "ts": ts, "at": raw.get("at"),
            "author": (local(raw.get("author")) or "").lower() or None}


@dataclass
class Snapshot:
    """Everything one repository's rules read, for one evaluation."""
    rows: list[dict]
    verdicts: list[dict] = field(default_factory=list)
    authors: dict[str, str] = field(default_factory=dict)  # PR IRI -> author agent (declared fact)


def _q(text: str) -> str:
    return json.dumps(text)  # a SPARQL string literal; inputs are validated first


def activity_query(graph: str, repo: str, since: str) -> str:
    if not IRI.match(graph) or not REPO.match(repo):
        raise ValueError("invalid graph or repo")
    opt = "".join(f" OPTIONAL {{ ?a aegis:{p} ?{v} }}" for p, v in (
        ("githubActorLogin", "actor"), ("commitSha", "sha"), ("githubState", "state"),
        ("githubMerged", "merged"), ("githubDraft", "draft"), ("githubBaseRef", "base"),
        ("githubStatus", "status"), ("githubConclusion", "conclusion"), ("githubRunAttempt", "attempt"),
        ("reviewState", "review"), ("githubPrerelease", "pre")))
    eopt = "".join(f" OPTIONAL {{ ?e aegis:{p} ?{v} }}" for p, v in (
        ("githubNumber", "num"), ("githubAuthorLogin", "author"), ("githubName", "name"),
        ("githubTagName", "tag"), ("githubUrl", "url")))
    return ("PREFIX aegis: <%s> SELECT ?a ?kind ?at ?actor ?sha ?state ?merged ?draft ?base ?status "
            "?conclusion ?attempt ?review ?pre ?e ?etype ?num ?author ?name ?tag ?url ?thread ?tnum "
            "WHERE { GRAPH <%s> { ?repo aegis:githubFullName %s . "
            "?a a aegis:GitHubActivity ; aegis:inGitHubRepository ?repo ; aegis:githubEventKind ?kind ; "
            "aegis:occurredAt ?at ; aegis:aboutGitHubEntity ?e . FILTER(STR(?at) >= %s) "
            "?e a ?etype . FILTER(STRSTARTS(STR(?etype), \"%sGitHub\"))%s%s "
            "OPTIONAL { ?e aegis:onGitHubThread ?thread . ?thread aegis:githubNumber ?tnum } } }"
            % (AEGIS, graph, _q(repo), _q(since), AEGIS, opt, eopt))


def verdict_query(graph: str, repo: str) -> str:
    """Approving / changes verdicts about this repo's PRs, in ANY graph: the
    reviewer job records them where its writer lives, not in the github graph."""
    classes = ", ".join(f"aegis:{c}" for c in VERDICT_CLASSES)
    return ("PREFIX aegis: <%s> SELECT ?v ?pr ?sha ?outcome ?by ?at ?author WHERE { "
            "GRAPH <%s> { ?repo aegis:githubFullName %s . ?pr a aegis:GitHubPullRequest ; "
            "aegis:inGitHubRepository ?repo . } "
            "GRAPH ?g { ?v a ?t ; aegis:aboutGitHubEntity ?pr ; aegis:commitSha ?sha ; aegis:outcome ?outcome ; "
            "aegis:by ?by ; aegis:recordedAt ?at . OPTIONAL { ?v aegis:authorAgent ?author } "
            "FILTER(?t IN (%s)) } }" % (AEGIS, graph, _q(repo), classes))


def author_query(graph: str, repo: str) -> str:
    return ("PREFIX aegis: <%s> SELECT ?pr ?agent WHERE { GRAPH <%s> { ?repo aegis:githubFullName %s . "
            "?pr a aegis:GitHubPullRequest ; aegis:inGitHubRepository ?repo . } "
            "GRAPH ?g { ?pr aegis:authoredByAgent ?agent } }" % (AEGIS, graph, _q(repo)))


class QuipuFacts:
    """Reads over Quipu's public POST /query only."""

    def __init__(self, base: str, graph: str, window_days: int, timeout: float = 15.0):
        if not IRI.match(base):
            raise ValueError("quipu base must be an http(s) URL")
        self.base, self.graph, self.window_days, self.timeout = base.rstrip("/"), graph, window_days, timeout

    def _rows(self, sparql: str) -> list[dict]:
        req = urllib.request.Request(
            self.base + "/query", data=json.dumps({"query": sparql}).encode(), method="POST",
            headers={"Content-Type": "application/json", "User-Agent": "chaski-github-routes/1",
                     "X-Quipu-Client": "chaski"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                out = json.load(resp)
        except Exception as exc:  # noqa: BLE001 — every failure is UNKNOWN
            raise FactsUnknown(f"quipu /query failed: {exc}") from exc
        if not isinstance(out, dict) or "error" in out or out.get("truncated") or not isinstance(out.get("rows"), list):
            raise FactsUnknown(f"quipu /query incomplete: {str(out)[:200]}")
        return out["rows"]

    def snapshot(self, repo: str, now: float) -> Snapshot:
        since = iso(now - 86400 * self.window_days)
        rows = [normalize_row(r) for r in self._rows(activity_query(self.graph, repo, since))]
        verdicts = [v for v in (normalize_verdict(r) for r in self._rows(verdict_query(self.graph, repo))) if v]
        authors = {}
        for r in self._rows(author_query(self.graph, repo)):
            agent = local(r.get("agent"))
            if r.get("pr") and agent:
                authors.setdefault(str(r["pr"]), set()).add(agent.lower())
        # Two declared author agents is ambiguous: no author agent, so no merge.
        return Snapshot(rows, verdicts, {pr: next(iter(a)) for pr, a in authors.items() if len(a) == 1})

    def repos_of(self, iris: set[str]) -> set[str]:
        if not iris:
            return set()
        values = " ".join(f"<{i}>" for i in sorted(iris) if IRI.match(i))
        sparql = ("PREFIX aegis: <%s> SELECT ?name WHERE { GRAPH <%s> { VALUES ?x { %s } "
                  "{ ?x aegis:githubFullName ?name } UNION { ?x aegis:inGitHubRepository ?r . "
                  "?r aegis:githubFullName ?name } } }" % (AEGIS, self.graph, values))
        return {str(r["name"]) for r in self._rows(sparql) if r.get("name")}


def rows_from_episodes(episodes: list[dict]) -> list[dict]:
    """Flatten camayoc /episode bodies (the facts Quipu stores) into rows.

    The fixture path: the same row shape QuipuFacts reads, built from the
    recorded GitHub deliveries mapped by camayoc's own mapper."""
    out = []
    for body in episodes:
        nodes = {n["name"]: n for n in body["nodes"]}
        edges = {}
        for e in body["edges"]:
            edges.setdefault((e["source"], e["relation"]), e["target"])
        for n in body["nodes"]:
            if n["type"] != "GitHubActivity":
                continue
            p = n["properties"]
            subject = nodes[edges[(n["name"], "aboutGitHubEntity")]]
            sp = subject.get("properties", {})
            thread_name = edges.get((subject["name"], "onGitHubThread"))
            thread = nodes.get(thread_name) if thread_name else None
            out.append(normalize_row({
                "a": AEGIS + n["name"], "kind": p.get("githubEventKind"), "at": p.get("occurredAt"),
                "actor": p.get("githubActorLogin"), "sha": p.get("commitSha"), "state": p.get("githubState"),
                "merged": p.get("githubMerged"), "draft": p.get("githubDraft"), "base": p.get("githubBaseRef"),
                "status": p.get("githubStatus"), "conclusion": p.get("githubConclusion"),
                "attempt": p.get("githubRunAttempt"), "review": p.get("reviewState"),
                "pre": p.get("githubPrerelease"), "e": AEGIS + subject["name"], "etype": AEGIS + subject["type"],
                "num": sp.get("githubNumber"), "author": sp.get("githubAuthorLogin"), "name": sp.get("githubName"),
                "tag": sp.get("githubTagName"), "url": sp.get("githubUrl"),
                "thread": AEGIS + thread["name"] if thread else None,
                "tnum": (thread or {}).get("properties", {}).get("githubNumber")}))
    return out


# --------------------------------------------------------------------------- lane

@dataclass
class LaneRow:
    request_id: str
    pr: int
    head: str
    acquired_at: float
    lease_until: float
    released_at: float | None
    outcome: str | None


def read_lane(ledger: Path | None, repo: str) -> list[LaneRow]:
    """The merge lane's rows for one repo, READ-ONLY. No ledger = empty lane."""
    if ledger is None or not Path(ledger).exists():
        return []
    try:
        conn = sqlite3.connect(f"file:{Path(ledger).resolve()}?mode=ro", uri=True)
    except sqlite3.Error as exc:
        raise FactsUnknown(f"merge lane unreadable: {exc}") from exc
    try:
        rows = conn.execute("SELECT request_id, pr, head, acquired_at, lease_until, released_at, outcome "
                            "FROM lane WHERE repo=?", (repo,)).fetchall()
    except sqlite3.OperationalError as exc:
        if "no such table" in str(exc):
            return []
        raise FactsUnknown(f"merge lane unreadable: {exc}") from exc
    finally:
        conn.close()
    return [LaneRow(*r) for r in rows]


# --------------------------------------------------------------------------- rules

@dataclass
class Decision:
    item: str
    kind: str
    verdict: str
    next_check: float | None
    facts: dict


def _latest(rows: list[dict], key=None) -> dict | None:
    key = key or (lambda r: (r["ts"] or 0, r["a"] or ""))
    return max(rows, key=key) if rows else None


def ci_state(rows: list[dict], sha: str, required: tuple[str, ...], branch: str | None = None) -> dict:
    """green / pending / red / none for one commit, from the latest run of each
    workflow and check (re-runs: the latest attempt wins)."""
    runs: dict[str, list[dict]] = {}
    for r in rows:
        if r["sha"] != sha or r["etype"] not in ("GitHubWorkflowRun", "GitHubCheckRun"):
            continue
        if not (r["kind"] or "").startswith(("workflow_run.", "check_run.")):
            continue
        if branch is not None and r["base"] != branch:
            continue
        runs.setdefault(r["e"], []).append(r)
    latest_per_entity = [_latest(v, key=lambda r: (r["attempt"] or 0, r["ts"] or 0, r["a"] or ""))
                         for v in runs.values()]
    per_name: dict[tuple, dict] = {}
    for r in latest_per_entity:
        k = (r["etype"], r["name"] or r["e"])
        if k not in per_name or (r["ts"] or 0, r["a"]) > (per_name[k]["ts"] or 0, per_name[k]["a"]):
            per_name[k] = r
    checks = sorted(per_name.values(), key=lambda r: (r["etype"], r["name"] or ""))
    detail = [{"name": r["name"], "type": r["etype"], "status": r["status"], "conclusion": r["conclusion"],
               "at": r["at"], "url": r["url"]} for r in checks]
    names = {r["name"] for r in checks}
    if not checks:
        return {"state": "none", "checks": detail, "green_at": None}
    if any(r["status"] == TERMINAL_RUN and (r["conclusion"] or "") in RED for r in checks):
        return {"state": "red", "checks": detail, "green_at": None}
    if any(r["status"] != TERMINAL_RUN or (r["conclusion"] or "") not in GREEN for r in checks):
        return {"state": "pending", "checks": detail, "green_at": None}
    missing = [c for c in required if c not in names]
    if missing:
        return {"state": "pending", "checks": detail, "green_at": None, "missing": missing}
    return {"state": "green", "checks": detail, "green_at": max(r["ts"] for r in checks)}


def _prs(rows: list[dict]) -> dict[str, dict]:
    by_pr: dict[str, list[dict]] = {}
    for r in rows:
        if r["etype"] == "GitHubPullRequest" and (r["kind"] or "").startswith("pull_request."):
            by_pr.setdefault(r["e"], []).append(r)
    out = {}
    for iri, acts in by_pr.items():
        last = _latest(acts)
        if last is None or last["num"] is None or last["sha"] is None or last["ts"] is None:
            continue  # incomplete facts: not a candidate, never guessed
        head = last["sha"]
        draft = last["draft"]
        if draft is None:  # poller events drop draft: take the latest activity that carried it
            carried = [a for a in acts if a["draft"] is not None]
            draft = _latest(carried)["draft"] if carried else False
        head_since = min(a["ts"] for a in acts if a["sha"] == head)
        out[iri] = {"iri": iri, "number": last["num"], "author": last["author"], "url": last["url"],
                    "state": last["state"], "merged": bool(last["merged"]), "draft": bool(draft), "head": head,
                    "base": last["base"], "head_since": head_since, "last_at": last["ts"], "last_kind": last["kind"]}
    return out


def _github_reviews(rows: list[dict]) -> dict[int, list[dict]]:
    by_review: dict[str, list[dict]] = {}
    for r in rows:
        if r["etype"] == "GitHubReview" and (r["kind"] or "").startswith("pull_request_review."):
            by_review.setdefault(r["e"], []).append(r)
    out: dict[int, list[dict]] = {}
    for acts in by_review.values():
        last = _latest(acts)
        if last["kind"] == "pull_request_review.dismissed" or last["tnum"] is None:
            continue
        state = (last["review"] or "").lower()
        verdict = ("approve" if state == "approved" else "changes" if state == "changes_requested"
                   else "comment")
        out.setdefault(last["tnum"], []).append({"by": (last["actor"] or last["author"] or "").lower(),
                                                 "sha": last["sha"], "verdict": verdict, "ts": last["ts"],
                                                 "at": last["at"], "source": "github_review", "v": last["e"]})
    return out


def approval(pr: dict, snap: Snapshot, gh_reviews: list[dict], sources: tuple[str, ...]) -> dict:
    """Approval state of the PR's CURRENT head.

    verdict source: agent verdict records. The reviewer must be a different
    AGENT from the PR's author agent: every PR and review comes from one shared
    GitHub account, so logins cannot tell author from reviewer. The author
    agent is the declared `authoredByAgent` fact, else what the verdict states;
    unknown means not approved.
    github_review source: GitHub reviews by a login other than the PR author.
    The latest word of each reviewer on this head counts; a later CHANGES
    from anyone but the author blocks."""
    head = pr["head"]
    author_agent = snap.authors.get(pr["iri"])
    author_login = (pr["author"] or "").lower()
    words: dict[tuple, dict] = {}
    reviewed = []
    unknown_author = False
    # A verdict record always counts as a review for the review rule, so a
    # reviewed head never gets a second review job; it approves only when the
    # verdict source is enabled.
    for v in snap.verdicts:
        if v["pr"] != pr["iri"] or v["sha"] != head:
            continue
        agent_author = author_agent or v["author"]
        if agent_author is None:
            unknown_author = True
            reviewed.append(v)  # someone reviewed; who wrote the PR is unknown
            continue
        if v["by"] == agent_author:
            continue  # self-review never counts
        reviewed.append(v)
        if "verdict" in sources:
            k = ("verdict", v["by"])
            if k not in words or (v["ts"], v["v"]) > (words[k]["ts"], words[k]["v"]):
                words[k] = {**v, "source": "verdict"}
    for g in gh_reviews:
        if g["sha"] != head or not g["by"] or g["by"] == author_login:
            continue
        reviewed.append(g)
        if "github_review" in sources and g["verdict"] in ("approve", "changes"):
            k = ("github_review", g["by"])
            if k not in words or (g["ts"], g["v"]) > (words[k]["ts"], words[k]["v"]):
                words[k] = g
    approvals = [w for w in words.values() if w["verdict"] == "approve"]
    blocking = [w for w in words.values() if w["verdict"] == "changes"]
    approved = bool(approvals) and not blocking
    return {"approved": approved, "reviewed": bool(reviewed),
            "approved_at": max((w["ts"] for w in approvals), default=None) if approved else None,
            "approvals": [{"by": w["by"], "source": w["source"], "at": w["at"], "record": w["v"]} for w in approvals],
            "blocking": [{"by": w["by"], "source": w["source"], "at": w["at"], "record": w["v"]} for w in blocking],
            "author_agent": author_agent, "author_agent_unknown": unknown_author and not approvals}


def _pr_facts(repo: str, pr: dict, ci: dict, appr: dict | None = None) -> dict:
    facts = {"repo": repo, "pr": pr["number"], "url": pr["url"], "head_sha": pr["head"], "base_ref": pr["base"],
             "author": pr["author"], "draft": pr["draft"], "ci": {"state": ci["state"], "green_at": iso(ci["green_at"]),
                                                                   "checks": ci["checks"]}}
    if appr is not None:
        facts["approval"] = {k: appr[k] for k in ("approvals", "blocking", "author_agent")}
        facts["approval"]["approved_at"] = iso(appr["approved_at"])
    return facts


def evaluate_repo(cfg: Config, repo_cfg: RepoConfig, snap: Snapshot, lane: list[LaneRow],
                  now: float) -> dict[str, Decision]:
    """Every current candidate item of one repo with its verdict. Pure."""
    repo = repo_cfg.name
    rows = snap.rows
    out: dict[str, Decision] = {}
    prs = _prs(rows)
    gh = _github_reviews(rows)
    open_prs = [p for p in prs.values() if p["state"] == "open" and not p["draft"]]

    def put(kind, subject, revision, verdict, next_check, facts):
        item = item_key(kind, repo, subject, revision)
        out[item] = Decision(item, kind, verdict, next_check, facts)

    merge_candidates = []
    for pr in open_prs:
        ci = ci_state(rows, pr["head"], repo_cfg.required_checks)
        appr = approval(pr, snap, gh.get(pr["number"], []), repo_cfg.approval_sources)
        subject = f"pr/{pr['number']}"
        # (a) review
        if not appr["reviewed"]:
            facts = _pr_facts(repo, pr, ci)
            if ci["state"] == "green":
                start = max(ci["green_at"], pr["head_since"])
                due = start + repo_cfg.review_after_s
                facts["unreviewed_since"] = iso(start)
                put("review", subject, pr["head"], READY if now >= due else WAIT, None if now >= due else due, facts)
            else:
                put("review", subject, pr["head"], WAIT, None, facts)
        # (b) merge / outside contribution
        if appr["approved"]:
            facts = _pr_facts(repo, pr, ci, appr)
            if not cfg.is_member(pr["author"]):
                facts["outside"] = True
                put("outside-contribution", subject, pr["head"], READY if ci["state"] == "green" else WAIT, None,
                    facts)
            elif repo_cfg.merge_enabled:
                merge_candidates.append((pr, ci, appr, facts))

    if merge_candidates:
        _merge_lane(cfg, repo_cfg, prs, merge_candidates, lane, rows, now, put)

    # (c) triage: latest run per workflow/check on the default branch is red
    if repo_cfg.triage_enabled:
        branch_rows = [r for r in rows if r["base"] == repo_cfg.default_branch
                       and r["etype"] in ("GitHubWorkflowRun", "GitHubCheckRun")
                       and (r["kind"] or "").startswith(("workflow_run.", "check_run."))]
        per_entity: dict[str, list[dict]] = {}
        for r in branch_rows:
            per_entity.setdefault(r["e"], []).append(r)
        latest_runs = [_latest(v, key=lambda r: (r["attempt"] or 0, r["ts"] or 0, r["a"] or ""))
                       for v in per_entity.values()]
        per_name: dict[tuple, dict] = {}
        for r in latest_runs:
            k = (r["etype"], r["name"] or r["e"])
            if k not in per_name or (r["ts"] or 0, r["a"]) > (per_name[k]["ts"] or 0, per_name[k]["a"]):
                per_name[k] = r
        red: dict[str, list[dict]] = {}
        for r in per_name.values():
            if r["status"] == TERMINAL_RUN and (r["conclusion"] or "") in RED and r["sha"]:
                red.setdefault(r["sha"], []).append(r)
        for sha, failed in red.items():
            put("triage", f"branch/{repo_cfg.default_branch}", sha, READY, None, {
                "repo": repo, "branch": repo_cfg.default_branch, "head_sha": sha,
                "failed": [{"name": r["name"], "type": r["etype"], "conclusion": r["conclusion"], "at": r["at"],
                            "url": r["url"], "attempt": r["attempt"]}
                           for r in sorted(failed, key=lambda r: r["name"] or "")]})

    # (d) release published -> install / announce
    for r in rows:
        if r["etype"] != "GitHubRelease" or r["kind"] not in ("release.published", "release.released"):
            continue
        if r["pre"] and not repo_cfg.include_prereleases:
            continue
        if r["ts"] is None or now - r["ts"] > repo_cfg.release_window_s or not r["tag"]:
            continue
        for job in repo_cfg.release_jobs:
            put(job, f"release/{r['tag']}", local(r["e"]), READY, None, {
                "repo": repo, "tag": r["tag"], "release": local(r["e"]), "url": r["url"],
                "prerelease": bool(r["pre"]), "published_at": r["at"]})
    return out


def _merge_lane(cfg, repo_cfg, prs, candidates, lane, rows, now, put):
    """ONE serialized lane per repo: at most one merge in flight, spacing
    between merges, head-of-line by approval time then PR number."""
    by_number = {p["number"]: p for p in prs.values()}
    completed, inflight, terminal = [], [], set()
    for row in lane:
        if row.released_at is not None:
            if row.outcome != "merged":
                terminal.add((row.pr, row.head))
            continue
        pr = by_number.get(row.pr)
        if pr is not None and pr["state"] == "closed":
            closed_at = pr["last_at"]
            completed.append({"request_id": row.request_id, "pr": row.pr, "head": row.head,
                              "outcome": "merged" if pr["merged"] else "closed", "at": iso(closed_at),
                              "ts": closed_at})
        elif row.lease_until <= now:
            terminal.add((row.pr, row.head))  # lease expired: the job ended without a merge fact
        else:
            inflight.append(row)
    merged_times = [p["last_at"] for p in prs.values() if p["state"] == "closed" and p["merged"]]
    merged_times += [c["ts"] for c in completed if c["outcome"] == "merged"]
    merged_times += [r.released_at for r in lane if r.released_at is not None and r.outcome == "merged"]
    last_merge = max(merged_times, default=None)
    spacing_end = (last_merge + repo_cfg.spacing_s) if last_merge is not None else None
    eligible = sorted(
        [c for c in candidates if c[1]["state"] == "green" and (c[0]["number"], c[0]["head"]) not in terminal],
        key=lambda c: (c[2]["approved_at"], c[0]["number"]))
    head_of_line = eligible[0][0]["number"] if eligible else None
    for pr, ci, appr, facts in candidates:
        if (pr["number"], pr["head"]) in terminal:
            continue  # this head already had its merge job; the administrator has the result
        lane_facts = {"spacing_minutes": repo_cfg.spacing_s // 60, "lease_minutes": repo_cfg.lease_s // 60,
                      "last_merge_at": iso(last_merge),
                      "completed": [{k: v for k, v in c.items() if k != "ts"} for c in completed],
                      "in_flight": [{"request_id": r.request_id, "pr": r.pr, "head": r.head} for r in inflight],
                      "position": next((i for i, c in enumerate(eligible) if c[0]["number"] == pr["number"]), None)}
        facts = {**facts, "lane": lane_facts}
        subject, rev = f"pr/{pr['number']}", pr["head"]
        mine = [r for r in inflight if r.pr == pr["number"] and r.head == pr["head"]]
        if ci["state"] != "green":
            put("merge", subject, rev, WAIT, None, facts)
        elif mine:
            put("merge", subject, rev, READY, None, facts)  # already dispatched: stays READY, same id
        elif inflight:
            nxt = min(r.lease_until for r in inflight)
            put("merge", subject, rev, WAIT, min(nxt, now + LANE_RECHECK_S), facts)
        elif pr["number"] != head_of_line:
            put("merge", subject, rev, WAIT, now + LANE_RECHECK_S, facts)
        elif spacing_end is not None and now < spacing_end:
            put("merge", subject, rev, WAIT, spacing_end, facts)
        else:
            put("merge", subject, rev, READY, None, facts)


# --------------------------------------------------------------------------- adapter protocol

def record_for(cfg: Config, d: Decision) -> dict:
    rid = request_id(d.item)
    kind, repo, subject, revision = parse_item(d.item)
    request = {"schema": SCHEMA, "request_id": rid, "item": d.item, "kind": kind, "repo": repo,
               "subject": subject, "revision": revision,
               "route": "administrator" if kind == "outside-contribution" else "spawn",
               "administrator": cfg.administrator, "facts": d.facts}
    rec = {"item": d.item, "verdict": d.verdict, "work_kind": kind, "assignee": cfg.administrator,
           "evidence": json.dumps(request, sort_keys=True, separators=(",", ":"))}
    if d.verdict == READY:
        rec["event_id"] = rid
    return rec


class Adapter:
    def __init__(self, cfg: Config, facts):
        self.cfg, self.facts = cfg, facts
        self._cache: dict[str, dict[str, Decision]] = {}

    def describe(self) -> dict:
        return {"version": 1, "graphs": [self.cfg.graph], "types": [AEGIS + "GitHubActivity"],
                "attributes": [AEGIS + "inGitHubRepository", AEGIS + "githubEventKind"]}

    def decisions(self, repo: str, now: float) -> dict[str, Decision]:
        if repo not in self._cache:
            rc = self.cfg.repos[repo]
            snap = self.facts.snapshot(repo, now)
            self._cache[repo] = evaluate_repo(self.cfg, rc, snap, read_lane(self.cfg.ledger, repo), now)
        return self._cache[repo]

    def discover(self, now: float, repos=None) -> list[str]:
        items = set()
        for repo in sorted(repos if repos is not None else self.cfg.repos):
            items |= set(self.decisions(repo, now))
        return sorted(items)

    def route(self, changes: list[dict], now: float) -> list[str]:
        refs, subjects = set(), set()
        for c in changes:
            if not isinstance(c, dict):
                continue
            for k in ("value", "old_value"):
                v = c.get(k)
                if c.get("attribute") == AEGIS + "inGitHubRepository" and isinstance(v, dict) and v.get("ref"):
                    refs.add(str(v["ref"]))
            if c.get("entity"):
                subjects.add(str(c["entity"]))
        names = self.facts.repos_of(refs | subjects)
        return self.discover(now, [n for n in names if n in self.cfg.repos])

    def evaluate(self, items: list[str], now: float) -> dict:
        records, next_checks = [], {}
        for item in items:
            _, repo, _, _ = parse_item(item)
            if repo not in self.cfg.repos:
                continue  # a repo removed from config: untracked, never guessed
            d = self.decisions(repo, now).get(item)
            if d is None:
                continue  # no longer a candidate: chaski untracks it
            records.append(record_for(self.cfg, d))
            nc = d.next_check
            next_checks[item] = None if nc is None else (nc if nc > now else now + 1)
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


class FixtureFacts:
    """Facts from recorded episodes (tests, offline dry runs)."""

    def __init__(self, episodes: list[dict], verdicts: list[dict] | None = None,
                 authors: dict[str, str] | None = None):
        self.rows = rows_from_episodes(episodes)
        self.verdicts = [v for v in (normalize_verdict(r) for r in (verdicts or [])) if v]
        self.authors = {k: v.lower() for k, v in (authors or {}).items()}
        self.repo_iris = {}
        for body in episodes:
            for n in body["nodes"]:
                if n["type"] == "GitHubRepository":
                    self.repo_iris[AEGIS + n["name"]] = n["properties"]["githubFullName"]
        self.activity_repo = {}
        for body in episodes:
            for e in body["edges"]:
                if e["relation"] == "inGitHubRepository":
                    self.activity_repo[AEGIS + e["source"]] = self.repo_iris.get(AEGIS + e["target"])

    def snapshot(self, repo: str, now: float) -> Snapshot:
        iris = {i for i, n in self.repo_iris.items() if n == repo}
        mine = [r for r in self.rows if self.activity_repo.get(r["a"]) in (repo,) or r["e"] in iris]
        return Snapshot(mine, list(self.verdicts), dict(self.authors))

    def repos_of(self, iris: set[str]) -> set[str]:
        out = set()
        for i in iris:
            if i in self.repo_iris:
                out.add(self.repo_iris[i])
            elif self.activity_repo.get(i):
                out.add(self.activity_repo[i])
        return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="chaski GitHub routing adapter (incremental protocol v1)")
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--quipu", help="quipu base URL (live reads)")
    ap.add_argument("--fixture", type=Path, help="episodes JSON instead of quipu (offline dry run)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--describe", action="store_true")
    mode.add_argument("--changes", action="store_true", help="read one request as JSON on stdin")
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    if args.fixture:
        doc = json.loads(args.fixture.read_text())
        facts = FixtureFacts([e["episode"] for e in doc.get("episodes", [])], doc.get("verdicts"), doc.get("authors"))
    elif args.quipu:
        facts = QuipuFacts(args.quipu, cfg.graph, cfg.window_days)
    else:
        ap.error("--quipu or --fixture is required")
    adapter = Adapter(cfg, facts)
    if args.describe:
        out = adapter.describe()
    else:
        try:
            out = adapter.handle(json.load(sys.stdin))
        except FactsUnknown as exc:
            print(f"UNKNOWN: {exc}", file=sys.stderr)
            return 3
    json.dump(out, sys.stdout, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
