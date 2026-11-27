"""External-thread watch over the notifier's camayoc-mapped activity fixture.

fixtures/github/notify-episodes.json (quipu): discussion #1211 opened and
commented by an outside person, then answered by crew (scbrown); issue #1242
commented by an outside person, then by a bot; issue #1250 opened by an
outside person.
"""
from __future__ import annotations

import copy
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest
import yaml

import github_routes as gr
import github_watch as gw
from emitter import Emitter, connect, observe

HERE = Path(__file__).parent
DOC = json.loads((HERE / "fixtures/github/notify-episodes.json").read_text())
EPISODES = [e["episode"] for e in DOC["episodes"]]
REPO = "intigraph/quipu"
NOW = datetime.fromisoformat("2026-11-21T22:11:00+00:00").timestamp()


def doc(**over):
    d = {"version": 1, "graph": "https://camayoc.local/operational/github", "repos": [REPO],
         "crew": ["scbrown"], "ntfy": {"url": "https://ntfy.example.test", "topic": "stiwi-github"},
         "max_age_minutes": 180, "watch": {"item": "aegis-za4f.7"}}
    d.update(over)
    return d


def evaluate(cfg=None, episodes=EPISODES, now=NOW):
    a = gw.Adapter(cfg or gw.parse_config(doc()), gr.FixtureFacts(episodes))
    return a.evaluate(a.discover(now), now)


def actions(result):
    return {(r["action"]["number"], r["action"]["kind"]): r["action"] for r in result["records"]}


def test_one_job_per_unanswered_outside_thread_and_none_for_answered_ones():
    got = actions(evaluate())
    # #1211: crew answered after the outside comment -> no job.
    # #1242: a bot commented after the outside comment -> still a job, for the outside comment.
    assert set(got) == {(1242, "issue_comment.created"), (1250, "issues.opened")}
    a = got[(1242, "issue_comment.created")]
    assert (a["actor"], a["job"], a["item"], a["reason"]) == ("outside-person", "triage", "aegis-za4f.7",
                                                              "external-thread")
    assert (a["pr"], a["head"], a["branch"]) == (None, None, None)
    assert a["url"] == "https://github.com/intigraph/quipu/issues/1242#issuecomment-6003"
    assert "Never post" in a["instructions"]
    assert a["observed_at"] == "2026-11-21T22:11:00Z"


def _without_crew_reply():
    return [e for e in EPISODES
            if not any(n.get("properties", {}).get("githubActorLogin") == "scbrown" for n in e["nodes"])]


def test_an_unanswered_thread_recruits_once_for_its_newest_outside_activity():
    got = actions(evaluate(episodes=_without_crew_reply()))
    # discussion.created and discussion_comment.created collapse to the newer comment.
    assert (1211, "discussion_comment.created") in got
    assert (1211, "discussion.created") not in got
    assert got[(1211, "discussion_comment.created")]["thread"] == "discussion"


def test_item_and_event_id_are_stable_and_replays_queue_one_outbox_row(tmp_path):
    first, second = evaluate(), evaluate()
    assert [(r["item"], r["event_id"]) for r in first["records"]] == \
           [(r["item"], r["event_id"]) for r in second["records"]]
    for r in first["records"]:
        repo, number, activity = gw.parse_item(r["item"])
        assert r["event_id"] == gw.event_id(r["item"]) == r["action"]["event_id"]
        assert r["item"].startswith(f"github-watch:{repo}#{number}:github.activity.")
    conn = connect(tmp_path / "e.db")
    e = Emitter("github-watch", ["unused"], 86400, "stiwi", event="thread_observed",
                from_verdict=gr.WAIT, to_verdict=gr.READY, baseline_emits=True, change_driven=True)
    assert len(observe(conn, e, first["records"], NOW, scope=set(first["scope"]))) == 2
    assert observe(conn, e, second["records"], NOW + 1, scope=set(second["scope"])) == []
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 2


def test_outside_pull_requests_are_left_to_hammond():
    eps = copy.deepcopy(_without_crew_reply())
    for e in eps:
        for n in e["nodes"]:
            url = n.get("properties", {}).get("githubUrl") or ""
            if "/issues/1242" in url:
                n["properties"]["githubUrl"] = url.replace("/issues/", "/pull/")
    got = actions(evaluate(episodes=eps))
    assert not any(num == 1242 for num, _ in got)
    assert (1250, "issues.opened") in got


def test_activities_past_the_age_bound_recruit_nothing():
    assert evaluate(now=NOW + 4 * 3600)["records"] == []


def test_an_item_that_was_answered_since_is_untracked_not_emitted():
    a = gw.Adapter(gw.parse_config(doc()), gr.FixtureFacts(_without_crew_reply()))
    stale = [i for i in a.discover(NOW) if "#1211:" in i]
    assert stale
    answered = gw.Adapter(gw.parse_config(doc()), gr.FixtureFacts(EPISODES))
    assert answered.evaluate(stale, NOW)["records"] == []


def test_config_needs_a_watch_item_and_a_known_job():
    with pytest.raises(gw.ConfigError, match="watch"):
        gw.parse_config({k: v for k, v in doc().items() if k != "watch"})
    with pytest.raises(gw.ConfigError, match="watch.item"):
        gw.parse_config(doc(watch={"item": "not an item"}))
    with pytest.raises(gw.ConfigError, match="watch.job"):
        gw.parse_config(doc(watch={"item": "aegis-za4f.7", "job": "merge"}))
    with pytest.raises(gw.ConfigError, match="crew"):
        gw.parse_config(doc(crew=[]))


def test_parse_item_refuses_foreign_keys():
    for bad in ("github-notify:intigraph/quipu:github.activity.abcdef12", "github-watch:intigraph/quipu#x:y", ""):
        with pytest.raises(ValueError):
            gw.parse_item(bad)


def test_cli_runs_offline_against_the_fixture(tmp_path):
    conf = tmp_path / "watch.yaml"
    conf.write_text(yaml.safe_dump(doc(max_age_minutes=10080)))
    fixture = tmp_path / "facts.json"
    fixture.write_text(json.dumps({"episodes": [{"episode": e} for e in EPISODES]}))
    base = [sys.executable, str(HERE / "github_watch.py"), "--config", str(conf), "--fixture", str(fixture),
            "--changes"]
    found = subprocess.run(base, input=json.dumps({"now": NOW, "discover": True}),
                           capture_output=True, text=True, check=True)
    items = json.loads(found.stdout)["items"]
    assert len(items) == 2
    run = subprocess.run(base, input=json.dumps({"now": NOW, "items": items}),
                         capture_output=True, text=True, check=True)
    assert {r["action"]["job"] for r in json.loads(run.stdout)["records"]} == {"triage"}
