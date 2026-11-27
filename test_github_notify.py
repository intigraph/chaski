"""Public-repo activity notifier over camayoc-mapped activity facts.

fixtures/github/notify-episodes.json holds synthetic deliveries mapped by
camayoc's own ingress mapper (fixtures/github/README.md): an outside
discussion and discussion comment, a crew discussion comment, outside and bot
issue comments, an edited outside comment and an outside issue.
"""
from __future__ import annotations

import json
import socket
import urllib.error
from datetime import datetime
from pathlib import Path

import pytest

import github_notify as gn
import github_routes as gr
from emitter import Emitter, connect, observe

HERE = Path(__file__).parent
DOC = json.loads((HERE / "fixtures/github/notify-episodes.json").read_text())
EPISODES = [e["episode"] for e in DOC["episodes"]]
REPO = "intigraph/quipu"
NOW = datetime.fromisoformat("2026-11-21T22:11:00+00:00").timestamp()


def config(**over):
    doc = {"version": 1, "graph": "https://camayoc.local/operational/github", "repos": [REPO],
           "crew": ["scbrown"], "ntfy": {"url": "https://ntfy.example.test", "topic": "stiwi-github"},
           "max_age_minutes": 180}
    doc.update(over)
    return gn.parse_config(doc)


def adapter(cfg=None, episodes=EPISODES):
    return gn.Adapter(cfg or config(), gr.FixtureFacts(episodes))


def evaluate(cfg=None, now=NOW):
    a = adapter(cfg)
    items = a.discover(now)
    return a.evaluate(items, now)


def requests(result):
    return [json.loads(r["evidence"]) for r in result["records"]]


class FakePublisher:
    def __init__(self, fail=None):
        self.sent, self.fail = [], fail

    def publish(self, title, message, click, tags):
        if self.fail:
            raise self.fail
        self.sent.append({"title": title, "message": message, "click": click})


def event_of(record):
    return {"event_id": record["event_id"], "item": record["item"], "evidence": record["evidence"]}


# ------------------------------------------------------------------ adapter

def test_every_notifiable_activity_is_one_ready_item_with_its_route():
    reqs = {(r["kind"], r["actor"]): r for r in requests(evaluate())}
    assert set(reqs) == {
        ("discussion.created", "outside-person"), ("discussion_comment.created", "outside-person"),
        ("discussion_comment.created", "scbrown"), ("issue_comment.created", "outside-person"),
        ("issue_comment.created", "github-actions[bot]"), ("issues.opened", "outside-person")}
    # issue_comment.edited is not notifiable by default
    assert reqs[("discussion.created", "outside-person")]["route"] == "push"
    assert reqs[("discussion_comment.created", "scbrown")]["route"] == "digest"
    assert reqs[("issue_comment.created", "github-actions[bot]")]["route"] == "digest"
    comment = reqs[("discussion_comment.created", "outside-person")]
    assert (comment["number"], comment["thread"]) == (1211, "discussion")
    assert comment["url"] == "https://github.com/intigraph/quipu/discussions/1211#issuecomment-6001"
    issue = reqs[("issue_comment.created", "outside-person")]
    assert issue["number"] == 1242 and issue["label"] == "comment"


def test_item_and_event_id_are_stable_per_activity_record():
    first, second = evaluate(), evaluate()
    assert [r["event_id"] for r in first["records"]] == [r["event_id"] for r in second["records"]]
    for r in first["records"]:
        repo, activity = gn.parse_item(r["item"])
        assert repo == REPO and activity.startswith("github.activity.")
        assert r["event_id"] == gn.event_id(r["item"]) and r["verdict"] == gr.READY
    assert set(first["next_checks"].values()) == {None}


def test_activities_older_than_max_age_are_no_candidates():
    later = NOW + 3 * 3600  # every fixture activity is older than 180 minutes now
    assert adapter().discover(later) == []
    assert evaluate(config(max_age_minutes=600), later)["records"]


def test_replays_of_the_same_facts_queue_one_outbox_row_per_activity(tmp_path):
    conn = connect(tmp_path / "e.db")
    e = Emitter("github-notify", ["unused"], 86400, "stiwi", event="activity_observed",
                from_verdict=gr.WAIT, to_verdict=gr.READY, baseline_emits=True, change_driven=True)
    first = evaluate()
    queued = observe(conn, e, first["records"], NOW, scope=set(first["scope"]))
    assert len(queued) == 6
    for _ in range(3):
        again = evaluate()
        assert observe(conn, e, again["records"], NOW + 1, scope=set(again["scope"])) == []
    assert conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0] == 6


def test_change_routing_reads_only_configured_repos():
    a = adapter()
    repo_iri = next(gr.AEGIS + n["name"] for n in EPISODES[0]["nodes"] if n["type"] == "GitHubRepository")
    change = {"attribute": gr.AEGIS + "inGitHubRepository", "value": {"ref": repo_iri}}
    assert a.handle({"now": NOW, "route": [change]})["items"] == a.discover(NOW)
    other = adapter(config(repos=["intigraph/chaski"]))
    assert other.handle({"now": NOW, "route": [change]})["items"] == []


def test_config_refuses_an_empty_crew_and_unknown_kinds():
    with pytest.raises(gn.ConfigError):
        config(crew=[])
    with pytest.raises(gn.ConfigError):
        config(kinds=["discussion.answered"])
    with pytest.raises(gn.ConfigError):
        config(ntfy={"url": "https://ntfy.example.test", "topic": "bad topic"})


# ------------------------------------------------------------------ receiver

def records_by(result):
    return {(json.loads(r["evidence"])["kind"], json.loads(r["evidence"])["actor"]): r for r in result["records"]}


def test_external_comment_is_pushed_once_and_replays_are_duplicates(tmp_path):
    cfg = config()
    ledger = gn.Ledger(tmp_path / "n.db")
    pub = FakePublisher()
    rec = records_by(evaluate(cfg))[("discussion_comment.created", "outside-person")]
    code, msg = gn.deliver(event_of(rec), cfg, ledger, pub, NOW)
    assert code == 0 and msg.startswith("sent")
    for i in range(3):
        assert gn.deliver(event_of(rec), cfg, ledger, pub, NOW + i)[0] == 0
    assert len(pub.sent) == 1
    push = pub.sent[0]
    assert push["title"] == "intigraph/quipu#1211 discussion comment by outside-person"
    assert push["click"].endswith("/discussions/1211#issuecomment-6001")
    assert "discussion_comment.created" in push["message"] and push["click"] in push["message"]


def test_crew_and_bot_activity_never_pushes_and_lands_in_the_digest(tmp_path):
    cfg = config()
    ledger = gn.Ledger(tmp_path / "n.db")
    pub = FakePublisher()
    recs = records_by(evaluate(cfg))
    for key in (("discussion_comment.created", "scbrown"), ("issue_comment.created", "github-actions[bot]")):
        code, msg = gn.deliver(event_of(recs[key]), cfg, ledger, pub, NOW)
        assert code == 0 and msg.startswith("digest")
    assert pub.sent == []
    text = gn.digest_text(ledger.undigested())
    assert "intigraph/quipu#1211 discussion comment by scbrown" in text
    assert "intigraph/quipu#1242 comment by github-actions[bot]" in text
    ledger.digested([r["event_id"] for r in ledger.undigested()], NOW)
    assert ledger.undigested() == []


def test_receiver_recomputes_the_route_from_its_own_crew_list(tmp_path):
    # An adapter running with a stale crew list says "push" for scbrown; the
    # receiver's crew list still makes it digest-only.
    stale = config(crew=["someone-else"])
    rec = records_by(evaluate(stale))[("discussion_comment.created", "scbrown")]
    assert json.loads(rec["evidence"])["route"] == "push"
    pub = FakePublisher()
    gn.deliver(event_of(rec), config(), gn.Ledger(tmp_path / "n.db"), pub, NOW)
    assert pub.sent == []


def test_definite_failure_retries_and_then_sends_once(tmp_path):
    cfg = config()
    ledger = gn.Ledger(tmp_path / "n.db")
    rec = records_by(evaluate(cfg))[("issue_comment.created", "outside-person")]
    code, _ = gn.deliver(event_of(rec), cfg, ledger, FakePublisher(ConnectionError("HTTP 503")), NOW)
    assert code == gn.EX_TEMPFAIL and ledger.get(rec["event_id"])["state"] == "retry"
    pub = FakePublisher()
    assert gn.deliver(event_of(rec), cfg, ledger, pub, NOW + 60)[0] == 0
    assert len(pub.sent) == 1 and ledger.get(rec["event_id"])["attempts"] == 2


def test_ambiguous_publication_is_never_resent(tmp_path):
    cfg = config()
    ledger = gn.Ledger(tmp_path / "n.db")
    rec = records_by(evaluate(cfg))[("issues.opened", "outside-person")]
    code, msg = gn.deliver(event_of(rec), cfg, ledger, FakePublisher(gn.Ambiguous("timed out")), NOW)
    assert code == 0 and "not re-sent" in msg
    pub = FakePublisher()
    assert gn.deliver(event_of(rec), cfg, ledger, pub, NOW + 60)[0] == 0
    assert pub.sent == [] and ledger.status()[0]["state"] == "ambiguous"


def test_a_delivery_that_died_mid_send_is_ambiguous_not_resent(tmp_path):
    cfg = config()
    ledger = gn.Ledger(tmp_path / "n.db")
    rec = records_by(evaluate(cfg))[("discussion.created", "outside-person")]
    req = gn.parse_request(event_of(rec), cfg)
    assert ledger.claim(req, NOW)[0] == "claimed"
    ledger.mark(req["event_id"], "sending", NOW, attempt=True)  # then the process dies
    pub = FakePublisher()
    assert gn.deliver(event_of(rec), cfg, ledger, pub, NOW + 10)[0] == gn.EX_TEMPFAIL  # claim still live
    assert gn.deliver(event_of(rec), cfg, ledger, pub, NOW + gn.CLAIM_TTL_S + 1)[0] == 0
    assert pub.sent == [] and ledger.get(req["event_id"])["state"] == "ambiguous"


def test_tampered_requests_are_refused(tmp_path):
    cfg = config()
    rec = records_by(evaluate(cfg))[("issues.opened", "outside-person")]
    ledger = gn.Ledger(tmp_path / "n.db")
    bad = dict(event_of(rec), event_id="0" * 64)
    with pytest.raises(gn.RequestError):
        gn.deliver(bad, cfg, ledger, FakePublisher(), NOW)
    req = json.loads(rec["evidence"])
    req["url"] = "https://evil.example/x"
    with pytest.raises(gn.RequestError):
        gn.deliver(dict(event_of(rec), evidence=json.dumps(req)), cfg, ledger, FakePublisher(), NOW)
    with pytest.raises(gn.RequestError):
        gn.deliver(event_of(rec), config(repos=["intigraph/chaski"]), ledger, FakePublisher(), NOW)


def test_first_line_reader_is_used_when_configured(tmp_path):
    reader = tmp_path / "reader.sh"
    reader.write_text("#!/bin/sh\nprintf '\\n  How can I think of this?\\nsecond\\n'\n")
    reader.chmod(0o755)
    cfg = config(first_line={"argv": [str(reader)]})
    rec = records_by(evaluate(cfg))[("discussion.created", "outside-person")]
    pub = FakePublisher()
    gn.deliver(event_of(rec), cfg, gn.Ledger(tmp_path / "n.db"), pub, NOW)
    assert pub.sent[0]["message"].splitlines()[0] == "How can I think of this?"
    title, body = gn.message_for(config(), gn.parse_request(event_of(rec), config()))
    assert body.splitlines()[0].startswith("(text is not stored in Quipu")


def test_ntfy_publisher_classifies_failures(monkeypatch):
    pub = gn.NtfyPublisher(config().ntfy)

    def raising(exc):
        def fake(*a, **k):
            raise exc
        return fake
    monkeypatch.setattr(gn.urllib.request, "urlopen", raising(urllib.error.URLError(ConnectionRefusedError())))
    with pytest.raises(ConnectionError):
        pub.publish("t", "m", None, "x")
    monkeypatch.setattr(gn.urllib.request, "urlopen", raising(urllib.error.URLError(socket.gaierror())))
    with pytest.raises(ConnectionError):
        pub.publish("t", "m", None, "x")
    monkeypatch.setattr(gn.urllib.request, "urlopen", raising(urllib.error.URLError(TimeoutError())))
    with pytest.raises(gn.Ambiguous):
        pub.publish("t", "m", None, "x")
    monkeypatch.setattr(gn.urllib.request, "urlopen", raising(TimeoutError()))
    with pytest.raises(gn.Ambiguous):
        pub.publish("t", "m", None, "x")
    monkeypatch.setattr(gn.urllib.request, "urlopen",
                        raising(urllib.error.HTTPError("u", 429, "slow down", {}, None)))
    with pytest.raises(ConnectionError):
        pub.publish("t", "m", None, "x")


def test_cli_adapter_runs_offline_against_the_fixture(tmp_path):
    import subprocess
    import sys
    import yaml
    conf = tmp_path / "notify.yaml"
    conf.write_text(yaml.safe_dump({"version": 1, "graph": "https://camayoc.local/operational/github",
                                    "repos": [REPO], "crew": ["scbrown"], "max_age_minutes": 10080,
                                    "ntfy": {"url": "https://ntfy.example.test", "topic": "t"}}))
    fixture = tmp_path / "facts.json"
    fixture.write_text(json.dumps({"episodes": [{"episode": e} for e in EPISODES]}))
    run = subprocess.run([sys.executable, str(HERE / "github_notify.py"), "--config", str(conf), "--fixture",
                          str(fixture), "--changes"], input=json.dumps({"now": NOW, "discover": True}),
                         capture_output=True, text=True, check=True)
    assert len(json.loads(run.stdout)["items"]) == 6
