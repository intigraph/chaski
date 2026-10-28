#!/usr/bin/env python3
"""GitHub work-request receiver: chaski command sink -> work item -> st job.

chaski's command sink runs `chaski-github-work deliver --config C` with one
emitter event as JSON on stdin; exit 0 is the receipt, anything else is a
retry with backoff (command_sink.py). Every step is idempotent on the
request id, which is a hash of (kind, repo, subject, revision), so a re-sent
event never files a second item or spawns a second job.

    1. Validate the request carried in the event (github_routes.SCHEMA).
    2. Merge requests: take the repo's merge lane (ONE in flight, spacing).
       Busy -> exit 75 (temporary); chaski re-delivers later.
    3. File the work item (seeds) once, found again by its label on replay.
    4. route=administrator (outside contributions): the item is assigned to
       the administrator and nothing is spawned. Never a merge job.
    5. route=spawn: `st agent spawn --ephemeral --job <job> --item <id>`
       through a SpawnPort. Refused / cannot tell -> exit 75, lane released.

The ledger (SQLite) is the receiver's own state. Its `lane` table is the
merge lane, read read-only by the adapter to order the queue.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import github_routes as gr

EX_TEMPFAIL = 75
JOB_FOR_KIND = {"review": "review", "merge": "merge", "triage": "triage", "install": "task", "announce": "task"}
TERMINAL = {"dispatched", "routed", "spent"}
OUTCOMES = {"merged", "blocked", "failed", "closed", "stopped"}

LEDGER_SCHEMA = """
CREATE TABLE IF NOT EXISTS requests (
  request_id TEXT PRIMARY KEY, item TEXT NOT NULL, kind TEXT NOT NULL, repo TEXT NOT NULL,
  subject TEXT NOT NULL, revision TEXT NOT NULL, state TEXT NOT NULL, work_item TEXT,
  attempts INTEGER NOT NULL DEFAULT 0, created REAL NOT NULL, updated REAL NOT NULL, detail TEXT);
CREATE TABLE IF NOT EXISTS lane (
  request_id TEXT PRIMARY KEY, repo TEXT NOT NULL, pr INTEGER NOT NULL, head TEXT NOT NULL,
  acquired_at REAL NOT NULL, lease_until REAL NOT NULL, released_at REAL, outcome TEXT);
CREATE INDEX IF NOT EXISTS lane_open ON lane(repo) WHERE released_at IS NULL;
"""


class RequestError(ValueError):
    """The event is not a valid work request. Never retried into success."""


class LaneBusy(RuntimeError):
    pass


class LaneSpent(RuntimeError):
    """This request already had its merge turn; it never gets a second."""


class Refused(RuntimeError):
    pass


# --------------------------------------------------------------------------- request

def parse_request(event: dict, cfg: gr.Config) -> dict:
    if not isinstance(event, dict) or not isinstance(event.get("evidence"), str):
        raise RequestError("event carries no request evidence")
    try:
        req = json.loads(event["evidence"])
    except ValueError as exc:
        raise RequestError(f"evidence is not JSON: {exc}") from None
    if not isinstance(req, dict) or req.get("schema") != gr.SCHEMA:
        raise RequestError(f"evidence schema is not {gr.SCHEMA}")
    item = req.get("item")
    kind, repo, subject, revision = gr.parse_item(item)
    rid = gr.request_id(item)
    # The id is recomputed, never trusted: a request cannot claim another's slot.
    if req.get("request_id") != rid or event.get("event_id") != rid or event.get("item") != item:
        raise RequestError("request id does not match its item key")
    if (req.get("kind"), req.get("repo"), req.get("subject"), req.get("revision")) != (kind, repo, subject, revision):
        raise RequestError("request fields disagree with its item key")
    if repo not in cfg.repos:
        raise RequestError(f"{repo} is not a configured repository")
    facts = req.get("facts")
    if not isinstance(facts, dict):
        raise RequestError("request has no facts")
    route = "administrator" if kind == "outside-contribution" else "spawn"
    if kind == "merge":
        # Defence in depth: the adapter never emits a merge for an outside
        # author, and the receiver refuses to be the one that does.
        if not cfg.is_member(facts.get("author")) or facts.get("outside"):
            route = "administrator"
            req["note"] = "outside author: routed to the administrator, never merged"
        if not gr.SHA.match(str(facts.get("head_sha", ""))) or facts.get("head_sha") != revision:
            raise RequestError("merge request without its exact head sha")
    if req.get("route") not in ("spawn", "administrator"):
        raise RequestError("request route is not spawn or administrator")
    req["route"] = "administrator" if "administrator" in (route, req["route"]) else "spawn"
    return req


def pr_number(req: dict) -> int | None:
    m = re.fullmatch(r"pr/(\d+)", req["subject"])
    return int(m.group(1)) if m else None


# --------------------------------------------------------------------------- ledger

class Ledger:
    def __init__(self, path: Path):
        path = Path(path).expanduser()
        path.parent.mkdir(parents=True, exist_ok=True)
        fresh = not path.exists()
        self.conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)
        if fresh:
            os.chmod(path, 0o600)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(LEDGER_SCHEMA)

    def close(self):
        self.conn.close()

    def get(self, rid: str) -> dict | None:
        row = self.conn.execute("SELECT request_id,item,kind,repo,state,work_item,attempts,detail FROM requests "
                                "WHERE request_id=?", (rid,)).fetchone()
        if row is None:
            return None
        return dict(zip(("request_id", "item", "kind", "repo", "state", "work_item", "attempts", "detail"), row))

    def upsert(self, req: dict, state: str, now: float, work_item: str | None = None, detail: str | None = None,
               attempt: bool = False):
        self.conn.execute(
            "INSERT INTO requests (request_id,item,kind,repo,subject,revision,state,work_item,attempts,created,"
            "updated,detail) VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(request_id) DO UPDATE SET "
            "state=excluded.state, work_item=COALESCE(excluded.work_item, requests.work_item), "
            "attempts=requests.attempts+?, updated=excluded.updated, detail=excluded.detail",
            (req["request_id"], req["item"], req["kind"], req["repo"], req["subject"], req["revision"], state,
             work_item, 1 if attempt else 0, now, now, detail, 1 if attempt else 0))

    # ---- merge lane

    def lane_acquire(self, req: dict, repo_cfg: gr.RepoConfig, now: float) -> None:
        """The lock. One transaction: apply completions the facts proved, then
        refuse while another merge is in flight or spacing has not elapsed."""
        facts = req["facts"]
        lane = facts.get("lane") or {}
        rid, pr, head = req["request_id"], pr_number(req), facts["head_sha"]
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            for done in lane.get("completed") or []:
                at = gr.epoch(done.get("at")) or now
                if done.get("outcome") not in OUTCOMES:
                    continue
                self.conn.execute("UPDATE lane SET released_at=?, outcome=? WHERE request_id=? AND repo=? "
                                  "AND released_at IS NULL", (at, done["outcome"], done.get("request_id"),
                                                              req["repo"]))
            self.conn.execute("UPDATE lane SET released_at=lease_until, outcome='expired' "
                              "WHERE repo=? AND released_at IS NULL AND lease_until<=?", (req["repo"], now))
            mine = self.conn.execute("SELECT released_at FROM lane WHERE request_id=?", (rid,)).fetchone()
            if mine is not None and mine[0] is None:
                self.conn.execute("COMMIT")
                return  # re-delivery of the request already holding the lane
            if mine is not None:
                raise LaneSpent("this request already had its merge turn")
            other = self.conn.execute("SELECT request_id, pr FROM lane WHERE repo=? AND released_at IS NULL",
                                      (req["repo"],)).fetchone()
            if other:
                raise LaneBusy(f"merge lane {req['repo']} busy with PR #{other[1]}")
            last = self.conn.execute("SELECT MAX(released_at) FROM lane WHERE repo=? AND outcome='merged'",
                                     (req["repo"],)).fetchone()[0]
            facts_last = gr.epoch(lane.get("last_merge_at"))
            last = max([t for t in (last, facts_last) if t is not None], default=None)
            if last is not None and now < last + repo_cfg.spacing_s:
                nxt = gr.iso(last + repo_cfg.spacing_s)
                raise LaneBusy(f"merge lane {req['repo']} spacing: next merge after {nxt}")
            self.conn.execute("INSERT INTO lane (request_id,repo,pr,head,acquired_at,lease_until) VALUES (?,?,?,?,?,?)",
                              (rid, req["repo"], pr, head, now, now + repo_cfg.lease_s))
            self.conn.execute("COMMIT")
        except BaseException:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK")
            raise

    def lane_drop(self, rid: str) -> None:
        """Undo an acquire whose spawn was refused: it never got its turn."""
        self.conn.execute("DELETE FROM lane WHERE request_id=? AND released_at IS NULL", (rid,))

    def lane_release(self, rid: str, outcome: str, now: float) -> bool:
        if outcome not in OUTCOMES:
            raise ValueError(f"outcome must be one of {sorted(OUTCOMES)}")
        cur = self.conn.execute("UPDATE lane SET released_at=?, outcome=? WHERE request_id=? AND released_at IS NULL",
                                (now, outcome, rid))
        return cur.rowcount == 1

    def lane_status(self, repo: str | None = None) -> list[dict]:
        q = "SELECT request_id,repo,pr,head,acquired_at,lease_until,released_at,outcome FROM lane"
        rows = self.conn.execute(q + (" WHERE repo=?" if repo else "") + " ORDER BY acquired_at",
                                 (repo,) if repo else ()).fetchall()
        keys = ("request_id", "repo", "pr", "head", "acquired_at", "lease_until", "released_at", "outcome")
        return [dict(zip(keys, r)) for r in rows]


# --------------------------------------------------------------------------- ports

def _run(argv: list[str], env: dict | None = None, timeout: float = 120.0) -> subprocess.CompletedProcess:
    return subprocess.run(argv, capture_output=True, text=True, timeout=timeout, check=False,
                          env={**os.environ, **(env or {})})


def item_title(req: dict) -> str:
    f = req["facts"]
    if req["subject"].startswith("pr/"):
        what = f"{req['repo']}#{f.get('pr')} @ {req['revision'][:12]}"
    elif req["subject"].startswith("branch/"):
        what = f"{req['repo']} {f.get('branch')} @ {req['revision'][:12]}"
    else:
        what = f"{req['repo']} {f.get('tag')}"
    return f"[{req['kind']}] {what} (chaski github routing)"


def item_body(req: dict) -> str:
    return ("Work request filed by chaski's GitHub routing rules (aegis-c01c.4).\n"
            "The JSON below is the handoff contract (chaski docs/github-routing.md): the job works on\n"
            "exactly `revision` (the head sha for PR jobs) and reports through `st agent spawn --report`.\n\n"
            "```json\n" + json.dumps(req, indent=1, sort_keys=True) + "\n```\n")


def dedupe_label(rid: str) -> str:
    return f"chaski-req:{rid[:16]}"


class SeedsItems:
    """Work items as seeds, found again by a per-request label on replay."""

    def __init__(self, conf: dict, administrator: str):
        self.prefix = list(conf.get("argv_prefix") or ["sd"])
        self.labels = list(conf.get("labels") or ["chaski-github"])
        self.priority = str(conf.get("priority", 1))
        self.parent = conf.get("parent")
        self.env = dict(conf.get("env") or {})
        self.administrator = administrator

    def find(self, req: dict) -> str | None:
        p = _run([*self.prefix, "list", "-l", dedupe_label(req["request_id"]), "--all", "--json", "--limit", "0"],
                 self.env)
        if p.returncode != 0:
            raise Refused(f"sd list failed: {(p.stderr or p.stdout).strip()[:200]}")
        issues = json.loads(p.stdout or "{}").get("issues") or []
        return issues[0]["id"] if issues else None

    def create(self, req: dict) -> str:
        labels = [*self.labels, dedupe_label(req["request_id"]), f"job:{req['kind']}"]
        url = req["facts"].get("url")
        argv = [*self.prefix, "create", "--title", item_title(req), "-d", item_body(req), "-l", ",".join(labels),
                "-p", self.priority, "-a", self.administrator, "--silent"]
        if url:
            argv += ["--external-ref", url]
        if self.parent:
            argv += ["--parent", self.parent]
        p = _run(argv, self.env)
        if p.returncode != 0 or not p.stdout.strip():
            raise Refused(f"sd create failed: {(p.stderr or p.stdout).strip()[:200]}")
        return p.stdout.strip().split()[-1]


class JsonlItems:
    """A stand-in tracker for dry runs and tests: one line per request."""

    def __init__(self, conf: dict, administrator: str):
        self.path = Path(conf["path"]).expanduser()

    def _lines(self):
        if not self.path.exists():
            return []
        return [json.loads(x) for x in self.path.read_text().splitlines() if x]

    def find(self, req: dict) -> str | None:
        return next((x["work_item"] for x in self._lines() if x["request_id"] == req["request_id"]), None)

    def create(self, req: dict) -> str:
        wid = "stub-" + req["request_id"][:12]
        with self.path.open("a") as f:
            f.write(json.dumps({"request_id": req["request_id"], "work_item": wid, "title": item_title(req),
                                "request": req}, sort_keys=True) + "\n")
        return wid


def spawn_argv(prefix: list[str], req: dict, work_item: str) -> list[str]:
    """The handoff to shantytown #406. Only the flags #406 defines."""
    argv = [*prefix, "agent", "spawn", "--ephemeral", "--job", JOB_FOR_KIND[req["kind"]], "--item", work_item,
            "--repo", req["repo"]]
    pr = pr_number(req)
    if pr is not None:
        argv += ["--pr", str(pr)]
    return argv


class StSpawner:
    """`st agent spawn --ephemeral` (shantytown #406). Exit 0 spawned,
    1 refused (cap, governor, lane), 2 could not tell: both retried."""

    def __init__(self, conf: dict, administrator: str):
        self.prefix = list(conf.get("argv_prefix") or ["st"])
        # The spawner acts as the administrator: #406 lets only it (or a human
        # shell) spawn. The receiver runs under the administrator's identity.
        self.env = {"SHANTY_AGENT": administrator, **dict(conf.get("env") or {})}
        self.timeout = float(conf.get("timeout_s", 600))

    def spawn(self, req: dict, work_item: str) -> tuple[bool, str]:
        p = _run(spawn_argv(self.prefix, req, work_item), self.env, self.timeout)
        out = (p.stdout + p.stderr).strip()[-300:]
        if p.returncode == 0:
            return True, out
        return False, f"st exited {p.returncode}: {out}"


class StubSpawner:
    """Records the exact `st agent spawn` call instead of making it. The
    default until shantytown #406 is merged and the pipeline is enabled."""

    def __init__(self, conf: dict, administrator: str):
        self.path = Path(conf["path"]).expanduser()
        self.prefix = list(conf.get("argv_prefix") or ["st"])

    def spawn(self, req: dict, work_item: str) -> tuple[bool, str]:
        seen = set()
        if self.path.exists():
            seen = {json.loads(x)["request_id"] for x in self.path.read_text().splitlines() if x}
        if req["request_id"] not in seen:
            with self.path.open("a") as f:
                f.write(json.dumps({"request_id": req["request_id"], "argv": spawn_argv(self.prefix, req, work_item)},
                                   sort_keys=True) + "\n")
        return True, "stub: recorded, not spawned"


def make_ports(cfg: gr.Config):
    items_conf = cfg.raw.get("work_items") or {}
    spawn_conf = cfg.raw.get("spawner") or {}
    items = {"seeds": SeedsItems, "jsonl": JsonlItems}.get(items_conf.get("kind"))
    spawner = {"st": StSpawner, "stub": StubSpawner}.get(spawn_conf.get("kind"))
    if items is None:
        raise gr.ConfigError("work_items.kind must be seeds or jsonl")
    if spawner is None:
        raise gr.ConfigError("spawner.kind must be st or stub")
    for name, conf, cls in (("work_items", items_conf, items), ("spawner", spawn_conf, spawner)):
        if cls in (JsonlItems, StubSpawner) and not conf.get("path"):
            raise gr.ConfigError(f"{name}.path is required for kind {conf.get('kind')}")
    return items(items_conf, cfg.administrator), spawner(spawn_conf, cfg.administrator)


# --------------------------------------------------------------------------- deliver

def deliver(event: dict, cfg: gr.Config, ledger: Ledger, items, spawner, now: float) -> tuple[int, str]:
    """(exit code, message). 0 = received (incl. duplicates), 75 = retry later."""
    req = parse_request(event, cfg)
    rid = req["request_id"]
    row = ledger.get(rid)
    if row and row["state"] in TERMINAL:
        return 0, f"duplicate {rid[:12]}: already {row['state']} ({row['work_item']})"
    holds_lane = False
    if req["kind"] == "merge" and req["route"] == "spawn":
        try:
            ledger.lane_acquire(req, cfg.repos[req["repo"]], now)
            holds_lane = True
        except LaneBusy as exc:
            ledger.upsert(req, "waiting-lane", now, detail=str(exc), attempt=True)
            return EX_TEMPFAIL, str(exc)
        except LaneSpent as exc:
            ledger.upsert(req, "spent", now, detail=str(exc))
            return 0, str(exc)
    try:
        work_item = (row or {}).get("work_item") or items.find(req)
        if not work_item:
            ledger.upsert(req, "filing", now)
            work_item = items.create(req)
        ledger.upsert(req, "filed", now, work_item=work_item)
        if req["route"] == "administrator":
            ledger.upsert(req, "routed", now, work_item=work_item, detail=f"assigned to {cfg.administrator}")
            return 0, f"routed {rid[:12]} to {cfg.administrator} as {work_item}"
        ok, detail = spawner.spawn(req, work_item)
    except (Refused, OSError, subprocess.TimeoutExpired, ValueError) as exc:
        if holds_lane:
            ledger.lane_drop(rid)
        ledger.upsert(req, "retry", now, detail=str(exc)[:300], attempt=True)
        return EX_TEMPFAIL, str(exc)
    if not ok:
        if holds_lane:
            ledger.lane_drop(rid)
        ledger.upsert(req, "refused", now, work_item=work_item, detail=detail, attempt=True)
        return EX_TEMPFAIL, detail
    ledger.upsert(req, "dispatched", now, work_item=work_item, detail=detail)
    return 0, f"dispatched {req['kind']} {rid[:12]} as {work_item}: {detail}"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="chaski GitHub work-request receiver")
    ap.add_argument("--config", required=True, type=Path)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("deliver", help="one chaski event on stdin (the command sink)")
    ln = sub.add_parser("lane", help="the per-repo merge lane")
    ln_sub = ln.add_subparsers(dest="lane_cmd", required=True)
    st = ln_sub.add_parser("status")
    st.add_argument("--repo")
    st.add_argument("--json", action="store_true")
    rl = ln_sub.add_parser("release", help="end a merge turn the facts cannot show (blocked, failed, stopped)")
    rl.add_argument("--request", required=True)
    rl.add_argument("--outcome", required=True, choices=sorted(OUTCOMES))
    args = ap.parse_args(argv)
    cfg = gr.load_config(args.config)
    if cfg.ledger is None:
        ap.error("config needs ledger: <path>")
    ledger = Ledger(cfg.ledger)
    try:
        if args.cmd == "deliver":
            items, spawner = make_ports(cfg)
            try:
                event = json.load(sys.stdin)
                code, msg = deliver(event, cfg, ledger, items, spawner, time.time())
            except (RequestError, ValueError) as exc:
                # A malformed request is not a transient failure, but exit 0
                # would checkpoint it as received. Fail loudly; it stays in
                # the outbox for a person to look at.
                print(f"refused request: {exc}", file=sys.stderr)
                return 2
            print(msg, file=sys.stderr if code else sys.stdout)
            return code
        if args.lane_cmd == "status":
            rows = ledger.lane_status(args.repo)
            if args.json:
                print(json.dumps(rows, indent=1))
            else:
                for r in rows:
                    state = r["outcome"] or ("in flight until " + gr.iso(r["lease_until"]))
                    print(f"{r['repo']} PR #{r['pr']} {r['head'][:12]} {r['request_id'][:12]} {state}")
            return 0
        ok = ledger.lane_release(args.request, args.outcome, time.time())
        print("released" if ok else "no open lane turn with that request id")
        return 0 if ok else 1
    finally:
        ledger.close()


if __name__ == "__main__":
    raise SystemExit(main())
