"""Main-red triage adapter: one task event per red default-branch head, ever."""
from __future__ import annotations

import datetime as dt
import hashlib
import json

import pytest

import github_routes as gr
import github_work as gw
import main_red as mr

SHA = "a" * 40
NEW = "b" * 40
NOW = dt.datetime(2026, 11, 27, 15, 0, tzinfo=dt.timezone.utc)


def cfg(tmp_path, **extra):
    p = tmp_path / "c.yaml"
    p.write_text(json.dumps({"repos": ["intigraph/chaski", "intigraph/quipu"], **extra}))
    return mr.load_config(p)


def head(sha=SHA, checks=None, repo="intigraph/chaski", message="fix: thing (aegis-dja)", branch="main"):
    return {"repo": repo, "branch": branch, "head": sha, "message": message,
            "checks": [{"name": "test", "state": "FAILURE", "at": "2026-11-27T14:58:00Z"},
                       {"name": "lint", "state": "SUCCESS", "at": "2026-11-27T14:57:00Z"}]
            if checks is None else checks}


def test_red_head_emits_one_task_event_in_the_drain_contract(tmp_path):
    out = mr.records(cfg(tmp_path), [head()], NOW)
    assert len(out) == 1
    rec = out[0]
    assert rec["item"] == f"intigraph/chaski@main@{SHA}:task" and rec["verdict"] == "READY"
    a = rec["action"]
    assert a["event_id"] == hashlib.sha256(f"intigraph/chaski|main|{SHA}|task".encode()).hexdigest()
    assert a == {"event_id": a["event_id"], "repo": "intigraph/chaski", "pr": None, "branch": "main",
                 "head": SHA, "job": "task", "item": "aegis-dja", "reason": "main-red", "failing": ["test"],
                 "red_at": "2026-11-27T14:58:00Z", "observed_at": "2026-11-27T15:00:00Z"}
    assert rec["event_id"] == a["event_id"]


def test_the_event_id_is_the_sha_so_a_repeat_run_is_the_same_event(tmp_path):
    c = cfg(tmp_path)
    first, again = mr.records(c, [head()], NOW), mr.records(c, [head()], NOW + dt.timedelta(minutes=2))
    assert first[0]["event_id"] == again[0]["event_id"] and first[0]["item"] == again[0]["item"]
    assert mr.records(c, [head(NEW)], NOW)[0]["event_id"] != first[0]["event_id"]


@pytest.mark.parametrize("checks", [
    [{"name": "test", "state": "SUCCESS"}, {"name": "lint", "state": "NEUTRAL"}],  # green
    [{"name": "test", "state": "PENDING"}],                                           # running
    [{"name": "test", "state": "CANCELLED"}],                                         # neither
])
def test_a_head_that_is_not_red_emits_nothing(tmp_path, checks):
    assert mr.records(cfg(tmp_path), [head(checks=checks)], NOW) == []


def test_no_checks_read_is_unknown_and_emits_nothing(tmp_path):
    h = head()
    h["checks"] = None
    assert mr.records(cfg(tmp_path), [h], NOW) == []


def test_a_rerun_green_after_a_failure_is_not_red():
    contexts = [{"name": "test", "state": "FAILURE", "at": "2026-11-27T14:00:00Z"},
                {"name": "test", "state": "SUCCESS", "at": "2026-11-27T14:30:00Z"}]
    assert mr.judge(head(checks=mr.pl.latest_per_name(contexts))) is None


def test_malformed_heads_emit_nothing(tmp_path):
    c = cfg(tmp_path)
    assert mr.records(c, [head(sha="nope")], NOW) == []
    assert mr.records(c, [head(branch="")], NOW) == []


def test_no_seed_id_in_the_commit_is_item_null(tmp_path):
    assert mr.records(cfg(tmp_path), [head(message="chore: bump")], NOW)[0]["action"]["item"] is None


def test_config_refuses_bad_repos(tmp_path):
    p = tmp_path / "c.yaml"
    for doc in ({}, {"repos": []}, {"repos": ["nope"]}, {"repos": ["a/b", "a/b"]}):
        p.write_text(json.dumps(doc))
        with pytest.raises(mr.ConfigError):
            mr.load_config(p)


def test_the_pr_lifecycle_config_serves_as_is():
    c = mr.load_config(mr.Path(__file__).parent / "pr-lifecycle.example.yaml")
    assert "intigraph/chaski" in c.repos


def test_read_heads_parses_the_graphql_shape(tmp_path, monkeypatch):
    rollup = {"contexts": {"pageInfo": {"hasNextPage": False}, "nodes": [
        {"__typename": "CheckRun", "name": "test", "status": "COMPLETED", "conclusion": "FAILURE",
         "startedAt": "2026-11-27T14:58:00Z"},
        {"__typename": "StatusContext", "context": "ci/x", "state": "SUCCESS", "createdAt": "2026-11-27T14:50:00Z"}]}}
    data = {"r0": {"defaultBranchRef": {"name": "main", "target": {"oid": SHA, "messageHeadline": "x aegis-dja",
                                                                   "statusCheckRollup": rollup}}},
            "r1": None}
    monkeypatch.setattr(mr.pl, "_graphql", lambda c, q: data)
    heads = mr.read_heads(cfg(tmp_path))
    assert len(heads) == 1  # quipu unreadable: skipped, not judged
    assert heads[0]["head"] == SHA and {c["name"] for c in heads[0]["checks"]} == {"test", "ci/x"}
    assert mr.records(cfg(tmp_path), heads, NOW)[0]["action"]["failing"] == ["test"]


def test_main_fixture_round_trip(tmp_path, capsys):
    fx = tmp_path / "heads.json"
    fx.write_text(json.dumps([head(), head(NEW, checks=[{"name": "t", "state": "SUCCESS"}], repo="intigraph/quipu")]))
    c = tmp_path / "c.yaml"
    c.write_text(json.dumps({"repos": ["intigraph/chaski", "intigraph/quipu"]}))
    assert mr.main(["--config", str(c), "--fixture", str(fx)]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["action"]["repo"] for r in out] == ["intigraph/chaski"]


def test_main_failed_read_is_nonzero(tmp_path, monkeypatch, capsys):
    c = tmp_path / "c.yaml"
    c.write_text(json.dumps({"repos": ["intigraph/chaski"]}))

    def boom(*a):
        raise RuntimeError("github graphql failed (1): rate limited")
    monkeypatch.setattr(mr.pl, "_graphql", boom)
    assert mr.main(["--config", str(c)]) == 1
    assert capsys.readouterr().out == ""


def test_the_stub_spawner_is_gone():
    assert not hasattr(gw, "StubSpawner")
    doc = {"version": 1, "graph": "https://camayoc.local/operational/github", "administrator": "hammond",
           "members": ["scbrown"], "ledger": ":memory:", "repos": {"intigraph/chaski": {}},
           "work_items": {"kind": "jsonl", "path": "/dev/null"}, "spawner": {"kind": "stub", "path": "/dev/null"}}
    with pytest.raises(gr.ConfigError, match="stub spawner was removed"):
        gw.make_ports(gr.parse_config(doc))
