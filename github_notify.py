#!/usr/bin/env python3
"""Public-repo activity notifier: GitHub activity facts -> one push per external event.

Two halves, one file (docs/github-notify.md):

  adapter   `chaski-github-notify --config C --quipu URL --changes` speaks
            chaski's incremental adapter protocol v1 (`trigger: changes`). It
            reads the append-only `aegis:GitHubActivity` records camayoc's
            ingress writes, the same rows github_routes reads, and makes ONE
            READY item per notifiable activity: an issue, PR, comment, review,
            discussion or discussion comment. The item key names the activity
            record, whose name is camayoc's per-delivery activity key, so a
            replayed or re-sent GitHub delivery is the same item and the same
            event id.
  receiver  `chaski-github-notify --config C deliver` is the command sink.
            A NON-crew, non-bot actor -> ONE ntfy push (repo#n, actor, kind,
            first line, link). Crew and bot activity -> the digest table only,
            never a push. `digest` prints, or with --send pushes once, the
            undigested crew/bot rows.

Delivery is at most once per event id, never twice. The ledger claims the
event before the HTTP call; a definite refusal (connection refused, HTTP
error status) is retried (exit 75); an answer that cannot be told apart from
"sent" (a timeout after the request left, a crash mid-send) is recorded
`ambiguous` and is NOT re-sent. `status` lists those for a person.

The adapter is stateless and read-only. A failed or truncated Quipu read
raises, so chaski records UNKNOWN and keeps every prior verdict.

Text is never stored in Quipu (camayoc keeps only @-mentions), so the "first
line" comes from an optional `first_line.argv` reader at delivery time. Without
one, the push says the text is not stored and carries the link.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import socket
import sqlite3
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml

import github_routes as gr
from chaski_version import add_version_argument

SCHEMA = "chaski.github.notify/v1"
PREFIX = "github-notify"
EX_TEMPFAIL = 75
CLAIM_TTL_S = 300
# event kind -> (what Stiwi reads, thread type). Only these are notifiable.
DEFAULT_KINDS = {
    "issues.opened": "issue opened", "issues.reopened": "issue reopened", "issues.closed": "issue closed",
    "pull_request.opened": "PR opened", "pull_request.reopened": "PR reopened",
    "pull_request.closed": "PR closed", "pull_request.ready_for_review": "PR ready for review",
    "issue_comment.created": "comment", "pull_request_review.submitted": "review",
    "pull_request_review_comment.created": "review comment",
    "discussion.created": "discussion opened",
    "discussion_comment.created": "discussion comment",
}
THREAD_TYPES = {"GitHubIssue": "issue", "GitHubPullRequest": "pr", "GitHubDiscussion": "discussion"}
ENTITY_TYPES = set(THREAD_TYPES) | {"GitHubComment", "GitHubReview"}
ACTIVITY = re.compile(r"^github\.activity\.[0-9a-f]{8,64}$")
TOPIC = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
ROUTES = ("push", "digest")


class ConfigError(ValueError):
    pass


class RequestError(ValueError):
    """Not a valid notification request. Never retried into success."""


# --------------------------------------------------------------------------- config

@dataclass(frozen=True)
class Config:
    graph: str
    repos: tuple[str, ...]
    crew: frozenset[str]
    bots: frozenset[str]
    kinds: dict
    max_age_s: float
    window_days: int
    ledger: Path | None
    ntfy: dict
    first_line: list[str] | None
    raw: dict

    def route_for(self, actor: str | None) -> str:
        """Crew and bots get the digest. Anyone else, including an actor the
        facts do not name, is not provably crew, so it is pushed."""
        if not actor:
            return "push"
        a = actor.lower()
        if a in self.crew or a in self.bots or a.endswith("[bot]"):
            return "digest"
        return "push"


def load_config(path: Path | str) -> Config:
    doc = yaml.safe_load(Path(path).read_text())
    return parse_config(doc)


def _logins(value, key) -> frozenset[str]:
    if value is None:
        return frozenset()
    if not isinstance(value, list) or any(not isinstance(v, str) or not gr.LOGIN.fullmatch(v) for v in value):
        raise ConfigError(f"{key} must be a list of GitHub logins")
    return frozenset(v.lower() for v in value)


def parse_config(doc) -> Config:
    if not isinstance(doc, dict) or doc.get("version") != 1:
        raise ConfigError("notify config needs version: 1")
    graph = doc.get("graph")
    if not isinstance(graph, str) or not gr.IRI.match(graph):
        raise ConfigError("graph must be an http(s) IRI")
    repos = doc.get("repos")
    if (not isinstance(repos, list) or not repos or len(set(repos)) != len(repos)
            or any(not isinstance(r, str) or not gr.REPO.match(r) for r in repos)):
        raise ConfigError("repos must be a non-empty, duplicate-free list of owner/name")
    crew = _logins(doc.get("crew"), "crew")
    if not crew:
        raise ConfigError("crew must name at least one login: an empty crew pushes every crew comment")
    kinds = doc.get("kinds", list(DEFAULT_KINDS))
    if (not isinstance(kinds, list) or len(set(kinds)) != len(kinds)
            or any(k not in DEFAULT_KINDS for k in kinds)):
        raise ConfigError(f"kinds must be a duplicate-free subset of {sorted(DEFAULT_KINDS)}")
    max_age = doc.get("max_age_minutes", 180)
    if isinstance(max_age, bool) or not isinstance(max_age, int) or not 1 <= max_age <= 10080:
        raise ConfigError("max_age_minutes must be an integer in [1, 10080]")
    ntfy = doc.get("ntfy")
    if not isinstance(ntfy, dict):
        raise ConfigError("ntfy: {url, topic} is required")
    if not isinstance(ntfy.get("url"), str) or not gr.IRI.match(ntfy["url"]):
        raise ConfigError("ntfy.url must be an http(s) URL")
    if not isinstance(ntfy.get("topic"), str) or not TOPIC.match(ntfy["topic"]):
        raise ConfigError("ntfy.topic must be [A-Za-z0-9_-]{1,64}")
    if "token_file" in ntfy and not isinstance(ntfy["token_file"], str):
        raise ConfigError("ntfy.token_file must be a path")
    timeout = ntfy.get("timeout_s", 10)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 1 <= timeout <= 60:
        raise ConfigError("ntfy.timeout_s must be in [1, 60]")
    first = doc.get("first_line")
    argv = None
    if first is not None:
        argv = (first or {}).get("argv") if isinstance(first, dict) else None
        if not isinstance(argv, list) or not argv or any(not isinstance(a, str) or not a for a in argv):
            raise ConfigError("first_line.argv must be a non-empty list of strings")
    ledger = doc.get("ledger")
    return Config(graph=graph, repos=tuple(repos), crew=crew, bots=_logins(doc.get("bots"), "bots"),
                  kinds={k: DEFAULT_KINDS[k] for k in kinds}, max_age_s=max_age * 60.0,
                  window_days=max(1, -(-max_age // 1440)),
                  ledger=Path(ledger).expanduser() if isinstance(ledger, str) else None,
                  ntfy={**ntfy, "timeout_s": float(timeout)}, first_line=argv, raw=doc)


# --------------------------------------------------------------------------- items

def item_key(repo: str, activity: str) -> str:
    if not gr.REPO.match(repo) or not ACTIVITY.match(activity):
        raise ValueError("invalid notify item")
    return f"{PREFIX}:{repo}:{activity}"


def parse_item(item: str) -> tuple[str, str]:
    parts = str(item).split(":")
    if len(parts) != 3 or parts[0] != PREFIX or not gr.REPO.match(parts[1]) or not ACTIVITY.match(parts[2]):
        raise ValueError(f"not a notify item: {item!r}")
    return parts[1], parts[2]


def event_id(item: str) -> str:
    return hashlib.sha256(f"{SCHEMA}\n{item}".encode()).hexdigest()


def notice_for(cfg: Config, repo: str, row: dict) -> dict | None:
    """The notification an activity row makes, or None when it makes none."""
    label = cfg.kinds.get(row.get("kind") or "")
    if label is None or row.get("etype") not in ENTITY_TYPES or not row.get("a"):
        return None
    activity = gr.local(row["a"])
    if not activity or not ACTIVITY.match(activity):
        return None
    on_thread = row["etype"] in THREAD_TYPES
    number = row["num"] if on_thread else row.get("tnum")
    if row["kind"].startswith("discussion"):
        thread = "discussion"
    elif row["etype"] in THREAD_TYPES:
        thread = THREAD_TYPES[row["etype"]]
    elif row["kind"].startswith("pull_request"):
        thread = "pr"
    else:
        thread = "issue-or-pr"  # issue_comment fires on both; the link says which
    url = row.get("url")
    if not url and number:
        path = {"discussion": "discussions", "pr": "pull"}.get(thread, "issues")
        url = f"https://github.com/{repo}/{path}/{number}"
    return {"repo": repo, "activity": activity, "number": number, "thread": thread, "kind": row["kind"],
            "label": label, "actor": row.get("actor"), "author": row.get("author"), "at": row.get("at"),
            "url": url, "route": cfg.route_for(row.get("actor"))}


def candidates(cfg: Config, repo: str, rows: list[dict], now: float) -> dict[str, dict]:
    """item -> notice for every notifiable activity young enough to notify. Pure.

    The age bound keeps a first start (`baseline_emits`) and a long outage
    from replaying weeks of history to a phone: anything older than
    max_age_minutes is no candidate at all."""
    out = {}
    for row in rows:
        if row.get("ts") is None or now - row["ts"] > cfg.max_age_s:
            continue
        notice = notice_for(cfg, repo, row)
        if notice is not None:
            out[item_key(repo, notice["activity"])] = notice
    return out


class Adapter:
    def __init__(self, cfg: Config, facts):
        self.cfg, self.facts = cfg, facts
        self._cache: dict[str, dict[str, dict]] = {}

    def describe(self) -> dict:
        return {"version": 1, "graphs": [self.cfg.graph], "types": [gr.AEGIS + "GitHubActivity"],
                "attributes": [gr.AEGIS + "inGitHubRepository", gr.AEGIS + "githubEventKind"]}

    def notices(self, repo: str, now: float) -> dict[str, dict]:
        if repo not in self._cache:
            self._cache[repo] = candidates(self.cfg, repo, self.facts.snapshot(repo, now).rows, now)
        return self._cache[repo]

    def discover(self, now: float, repos=None) -> list[str]:
        items: set[str] = set()
        for repo in sorted(repos if repos is not None else self.cfg.repos):
            items |= set(self.notices(repo, now))
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
        return self.discover(now, [n for n in names if n in self.cfg.repos])

    def evaluate(self, items: list[str], now: float) -> dict:
        records, next_checks = [], {}
        for item in items:
            repo, _ = parse_item(item)
            if repo not in self.cfg.repos:
                continue
            notice = self.notices(repo, now).get(item)
            if notice is None:
                continue  # aged out or no longer notifiable: chaski untracks it
            eid = event_id(item)
            request = {"schema": SCHEMA, "event_id": eid, "item": item, **notice}
            records.append({"item": item, "verdict": gr.READY, "event_id": eid, "work_kind": "notify",
                            "assignee": None,
                            "evidence": json.dumps(request, sort_keys=True, separators=(",", ":"))})
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


# --------------------------------------------------------------------------- receiver

def parse_request(event: dict, cfg: Config) -> dict:
    if not isinstance(event, dict) or not isinstance(event.get("evidence"), str):
        raise RequestError("event carries no notification evidence")
    try:
        req = json.loads(event["evidence"])
    except ValueError as exc:
        raise RequestError(f"evidence is not JSON: {exc}") from None
    if not isinstance(req, dict) or req.get("schema") != SCHEMA:
        raise RequestError(f"evidence schema is not {SCHEMA}")
    try:
        repo, activity = parse_item(req.get("item"))
    except ValueError as exc:
        raise RequestError(str(exc)) from None
    eid = event_id(req["item"])
    if req.get("event_id") != eid or event.get("event_id") != eid or event.get("item") != req["item"]:
        raise RequestError("event id does not match its item key")
    if (req.get("repo"), req.get("activity")) != (repo, activity):
        raise RequestError("request fields disagree with its item key")
    if repo not in cfg.repos:
        raise RequestError(f"{repo} is not a configured repository")
    if req.get("kind") not in cfg.kinds:
        raise RequestError("notification kind is not enabled")
    url = req.get("url")
    if url is not None and (not isinstance(url, str) or not url.startswith("https://github.com/")):
        raise RequestError("link is not a github.com URL")
    actor = req.get("actor")
    if actor is not None and (not isinstance(actor, str) or not gr.LOGIN.fullmatch(actor)):
        raise RequestError("actor is not a GitHub login")
    # The route is recomputed from the receiver's own crew list, never trusted:
    # a stale adapter config cannot page Stiwi about crew activity.
    req["route"] = cfg.route_for(actor)
    return req


LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS notices (
  event_id TEXT PRIMARY KEY, item TEXT NOT NULL, repo TEXT NOT NULL, kind TEXT NOT NULL,
  actor TEXT, route TEXT NOT NULL, state TEXT NOT NULL, request TEXT NOT NULL,
  attempts INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, updated REAL NOT NULL,
  claimed_until REAL, detail TEXT, digested_at REAL);
"""
# sent / ambiguous / digest are final: none of them is ever pushed again.
FINAL = {"sent", "ambiguous", "digest"}


class Ledger:
    def __init__(self, path: Path):
        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.conn = sqlite3.connect(str(path), timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(LEDGER_SCHEMA)
        path.chmod(0o600)

    def close(self):
        self.conn.close()

    def get(self, eid: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM notices WHERE event_id=?", (eid,)).fetchone()
        return dict(row) if row else None

    def claim(self, req: dict, now: float) -> tuple[str, dict | None]:
        """('final', row) | ('busy', row) | ('crashed', row) | ('claimed', row).

        A live claim is another delivery in progress. A lapsed claim on a
        `sending` row is a delivery that died after it may have published:
        that is ambiguous, never a second push."""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.get(req["event_id"])
            if row and row["state"] in FINAL:
                self.conn.execute("COMMIT")
                return "final", row
            if row and row["claimed_until"] and row["claimed_until"] > now:
                self.conn.execute("COMMIT")
                return "busy", row
            if row and row["state"] == "sending":
                self.conn.execute("UPDATE notices SET state='ambiguous', updated=?, claimed_until=NULL, "
                                  "detail='a previous delivery died mid-send; not re-sent' WHERE event_id=?",
                                  (now, req["event_id"]))
                self.conn.execute("COMMIT")
                return "crashed", self.get(req["event_id"])
            body = json.dumps(req, sort_keys=True)
            if row is None:
                self.conn.execute(
                    "INSERT INTO notices (event_id,item,repo,kind,actor,route,state,request,created,updated,"
                    "claimed_until) VALUES (?,?,?,?,?,?,'claimed',?,?,?,?)",
                    (req["event_id"], req["item"], req["repo"], req["kind"], req.get("actor"), req["route"],
                     body, now, now, now + CLAIM_TTL_S))
            else:
                self.conn.execute("UPDATE notices SET state='claimed', route=?, request=?, updated=?, "
                                  "claimed_until=? WHERE event_id=?",
                                  (req["route"], body, now, now + CLAIM_TTL_S, req["event_id"]))
            self.conn.execute("COMMIT")
            return "claimed", self.get(req["event_id"])
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    def mark(self, eid: str, state: str, now: float, detail: str | None = None, attempt: bool = False):
        self.conn.execute(
            "UPDATE notices SET state=?, updated=?, detail=?, attempts=attempts+?, "
            "claimed_until=CASE WHEN ?='sending' THEN claimed_until ELSE NULL END WHERE event_id=?",
            (state, now, (detail or "")[:300] or None, 1 if attempt else 0, state, eid))

    def undigested(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT * FROM notices WHERE state='digest' AND digested_at IS NULL ORDER BY created, event_id")]

    def digested(self, eids: list[str], now: float):
        self.conn.executemany("UPDATE notices SET digested_at=? WHERE event_id=?", [(now, e) for e in eids])

    def status(self) -> list[dict]:
        return [dict(r) for r in self.conn.execute(
            "SELECT event_id,item,kind,actor,route,state,attempts,detail,updated FROM notices "
            "WHERE state NOT IN ('sent','digest') ORDER BY updated")]


class Ambiguous(RuntimeError):
    """The request may have been published; it must not be sent again."""


class NtfyPublisher:
    """POST {url}/{topic}. Raises ConnectionError for a definite non-delivery,
    Ambiguous when the request may have reached the server."""

    def __init__(self, conf: dict):
        self.url = conf["url"].rstrip("/") + "/" + conf["topic"]
        self.timeout = conf["timeout_s"]
        self.token_file = conf.get("token_file")
        self.priority = str(conf.get("priority", "default"))

    def publish(self, title: str, message: str, click: str | None, tags: str) -> None:
        headers = {"Title": title.encode("ascii", "replace").decode(), "Tags": tags, "Priority": self.priority,
                   "User-Agent": "chaski-github-notify/1"}
        if click:
            headers["Click"] = click
        if self.token_file:
            token = Path(self.token_file).expanduser().read_text().strip()
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(self.url, data=message.encode(), method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp.read(4096)
        except urllib.error.HTTPError as exc:
            raise ConnectionError(f"ntfy answered HTTP {exc.code}") from None
        except urllib.error.URLError as exc:
            # Refused and unresolvable fail before a byte is sent: definite.
            # Anything later (timeout, reset) may have published: ambiguous.
            if isinstance(exc.reason, (ConnectionRefusedError, socket.gaierror)):
                raise ConnectionError(f"ntfy unreachable: {exc.reason}") from None
            raise Ambiguous(f"ntfy outcome unknown: {exc.reason}") from None
        except OSError as exc:  # a timeout or reset while reading the answer
            raise Ambiguous(f"ntfy outcome unknown: {exc}") from None


def first_line(cfg: Config, req: dict) -> str:
    """The event's first line, from the configured reader, else a fixed note."""
    if cfg.first_line and req.get("url"):
        try:
            done = subprocess.run([*cfg.first_line, req["url"]], capture_output=True, text=True, timeout=20,
                                  check=False)
        except (OSError, subprocess.TimeoutExpired):
            done = None
        if done is not None and done.returncode == 0:
            for line in done.stdout.splitlines():
                if line.strip():
                    return line.strip()[:200]
    return "(text is not stored in Quipu; open the link)"


def message_for(cfg: Config, req: dict) -> tuple[str, str]:
    ref = f"{req['repo']}#{req['number']}" if req.get("number") else req["repo"]
    title = f"{ref} {req['label']} by {req.get('actor') or 'unknown actor'}"
    body = "\n".join([first_line(cfg, req), f"{req['kind']} at {req.get('at') or 'unknown time'}",
                      req.get("url") or "(no link in the facts)"])
    return title, body


def deliver(event: dict, cfg: Config, ledger: Ledger, publisher, now: float) -> tuple[int, str]:
    """(exit code, message). 0 = received (incl. duplicates, digest, ambiguous), 75 = retry."""
    req = parse_request(event, cfg)
    eid = req["event_id"]
    state, row = ledger.claim(req, now)
    if state == "final":
        return 0, f"duplicate {eid[:12]}: already {row['state']}"
    if state == "busy":
        return EX_TEMPFAIL, f"{eid[:12]} is being delivered"
    if state == "crashed":
        return 0, f"ambiguous {eid[:12]}: {row['detail']}"
    if req["route"] == "digest":
        ledger.mark(eid, "digest", now, f"crew/bot actor {req.get('actor')}: digest only")
        return 0, f"digest {eid[:12]} {req['repo']} {req['kind']} by {req.get('actor')}"
    title, body = message_for(cfg, req)
    ledger.mark(eid, "sending", now, attempt=True)
    try:
        publisher.publish(title, body, req.get("url"), "speech_balloon")
    except Ambiguous as exc:
        ledger.mark(eid, "ambiguous", now, str(exc))
        return 0, f"ambiguous {eid[:12]}: {exc}; not re-sent"
    except (ConnectionError, OSError) as exc:
        ledger.mark(eid, "retry", now, str(exc))
        return EX_TEMPFAIL, f"retry {eid[:12]}: {exc}"
    ledger.mark(eid, "sent", now, title)
    return 0, f"sent {eid[:12]}: {title}"


def digest_text(rows: list[dict]) -> str:
    lines = []
    for r in rows:
        req = json.loads(r["request"])
        ref = f"{req['repo']}#{req['number']}" if req.get("number") else req["repo"]
        lines.append(f"{ref} {req['label']} by {req.get('actor')} {req.get('url') or ''}".rstrip())
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="chaski public-repo activity notifier (adapter and receiver)")
    add_version_argument(ap, "chaski-github-notify")
    ap.add_argument("--config", required=True, type=Path)
    ap.add_argument("--quipu", help="quipu base URL (adapter, live reads)")
    ap.add_argument("--fixture", type=Path, help="episodes JSON instead of quipu (adapter, offline)")
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--describe", action="store_true")
    mode.add_argument("--changes", action="store_true", help="adapter: one request as JSON on stdin")
    mode.add_argument("cmd", nargs="?", choices=["deliver", "digest", "status"])
    ap.add_argument("--send", action="store_true", help="digest: push the digest once and mark it")
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    if args.describe or args.changes:
        if args.fixture:
            doc = json.loads(args.fixture.read_text())
            facts = gr.FixtureFacts([e["episode"] for e in doc.get("episodes", [])])
        elif args.quipu:
            facts = gr.QuipuFacts(args.quipu, cfg.graph, cfg.window_days)
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
    if cfg.ledger is None:
        ap.error("config needs ledger: <path>")
    ledger = Ledger(cfg.ledger)
    try:
        if args.cmd == "deliver":
            try:
                code, msg = deliver(json.load(sys.stdin), cfg, ledger, NtfyPublisher(cfg.ntfy), time.time())
            except (RequestError, ValueError) as exc:
                print(f"refused request: {exc}", file=sys.stderr)
                return 2  # stays in chaski's outbox for a person; exit 0 would checkpoint it
            print(msg, file=sys.stderr if code else sys.stdout)
            return code
        if args.cmd == "digest":
            rows = ledger.undigested()
            text = digest_text(rows)
            if not args.send:
                print(text or "(no undigested crew/bot activity)")
                return 0
            if rows:
                try:
                    NtfyPublisher(cfg.ntfy).publish(f"GitHub crew/bot digest: {len(rows)} events", text, None,
                                                    "memo")
                except Ambiguous as exc:
                    print(f"digest outcome unknown, marked anyway: {exc}", file=sys.stderr)
                except (ConnectionError, OSError) as exc:
                    print(f"digest not sent: {exc}", file=sys.stderr)
                    return EX_TEMPFAIL
                ledger.digested([r["event_id"] for r in rows], time.time())
            print(f"digested {len(rows)}")
            return 0
        for r in ledger.status():
            print(f"{r['state']:9} {r['event_id'][:12]} {r['kind']} {r['actor']} {r['detail'] or ''}")
        return 0
    finally:
        ledger.close()


if __name__ == "__main__":
    raise SystemExit(main())
