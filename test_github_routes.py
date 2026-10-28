"""GitHub routing rules over recorded activity facts.

The facts are camayoc's recorded GitHub deliveries (intigraph/camayoc, PR 177,
its lint check run, a main workflow run, release v0.1.3), mapped by camayoc's
own ingress mapper into the /episode bodies Quipu stores
(fixtures/github/episodes.json). A scenario selects recorded episodes and,
where a rule needs a second state, derives a variant of a recorded one with
named field changes. Verdict records are synthetic: no verdict writer exists
yet, and their shape is the contract docs/github-routing.md defines.
"""
from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

import github_routes as gr
from emitter import Emitter, connect, observe

HERE = Path(__file__).parent
EPISODES = {e["delivery"].removesuffix(".json"): e["episode"]
            for e in json.loads((HERE / "fixtures/github/episodes.json").read_text())["episodes"]}
REPO = "intigraph/camayoc"
PR177 = gr.AEGIS + "github.PR_kwDOTvaTnM8AAAABLF8ZkA"
HEAD = "256a7e1433679ba0ccbff647d46738a65fdfc6af"   # PR 177's recorded head
GREEN_AT = "2026-10-27T18:51:29Z"                    # its recorded lint check completion
MAIN_SHA = "24091efeed09250206b3f3295bc7a31867c5b2c2"  # recorded main workflow run


def ts(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()


def ep(name):
    return copy.deepcopy(EPISODES[name])


def variant(name, tag, props=None, subject=None, subject_props=None):
    """A recorded episode with named changes. The activity (and, with
    `subject`, the entity) gets a new node name so it is a distinct record."""
    body = ep(name)
    act = body["nodes"][0]
    old_act, old_subj = act["name"], body["nodes"][1]["name"]
    act["name"] = f"{old_act}.{tag}"
    act["properties"].update(props or {})
    if subject:
        body["nodes"][1]["name"] = subject
    body["nodes"][1].setdefault("properties", {}).update(subject_props or {})
    for e in body["edges"]:
        if e["source"] == old_act:
            e["source"] = act["name"]
        if subject and e["source"] == old_subj:
            e["source"] = subject
        if subject and e["target"] == old_subj:
            e["target"] = subject
    return body


def verdict(by, outcome="approve", sha=HEAD, at="2026-10-27T18:55:00Z", number=177, trusted=True, v=None):
    """A stored signed review verdict as the fixture source sees it after
    /verdict/verify: `trusted` and `at` (the recorded instant) come from quipu."""
    return {"v": v or f"urn:verdict:{by}:{outcome}:{at}", "target": gr.review_target(REPO, number, sha),
            "outcome": {"approve": "satisfied", "changes": "unsatisfied"}[outcome],
            "verifier": gr.AEGIS + "verifier/" + by, "at": at, "trusted": trusted}


def config(**repo):
    return gr.parse_config({
        "version": 1, "graph": "https://camayoc.local/operational/github", "administrator": "hammond",
        "members": ["scbrown"], "window_days": 30,
        "repos": {REPO: {"review_after_minutes": 15, **repo}}})


def decide(episodes, now, verdicts=(), authors=None, lane=(), cfg=None, **repo):
    cfg = cfg or config(**repo)
    facts = gr.FixtureFacts(episodes, list(verdicts), authors)
    return gr.evaluate_repo(cfg, cfg.repos[REPO], facts.snapshot(REPO, now), list(lane), now)


def only(decisions, kind):
    return {k: d for k, d in decisions.items() if d.kind == kind}


BASE = ["pull_request.opened", "check_run.completed"]


# ---------------------------------------------------------------- (a) review

def test_review_waits_for_its_timer_then_requests_once():
    episodes = [ep(n) for n in BASE]
    item = gr.item_key("review", REPO, "pr/177", HEAD)
    early = decide(episodes, ts(GREEN_AT) + 600)
    assert early[item].verdict == gr.WAIT
    assert early[item].next_check == ts(GREEN_AT) + 900
    late = decide(episodes, ts(GREEN_AT) + 901)
    assert late[item].verdict == gr.READY
    rec = gr.record_for(config(), late[item])
    assert rec["event_id"] == gr.request_id(item) == hashlib.sha256(f"{gr.SCHEMA}\n{item}".encode()).hexdigest()
    facts = json.loads(rec["evidence"])["facts"]
    assert facts["head_sha"] == HEAD and facts["ci"]["state"] == "green" and facts["pr"] == 177


def test_review_needs_green_ci():
    d = decide([ep("pull_request.opened")], ts(GREEN_AT) + 3600)
    item = gr.item_key("review", REPO, "pr/177", HEAD)
    assert d[item].verdict == gr.WAIT and d[item].next_check is None
    red = variant("check_run.completed", "red", {"githubConclusion": "failure"})
    assert decide([ep("pull_request.opened"), red], ts(GREEN_AT) + 3600)[item].verdict == gr.WAIT


def test_required_check_missing_is_not_green():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 3600, required_checks=["ci"])
    assert d[gr.item_key("review", REPO, "pr/177", HEAD)].verdict == gr.WAIT


def test_a_non_author_verdict_on_the_head_ends_the_review_rule():
    now = ts(GREEN_AT) + 3600
    d = decide([ep(n) for n in BASE], now, [verdict("sattler")], {PR177: "lex"})
    assert not only(d, "review")


def test_self_review_and_other_head_verdicts_do_not_count():
    now = ts(GREEN_AT) + 3600
    d = decide([ep(n) for n in BASE], now, [verdict("lex"), verdict("sattler", sha="0" * 40)], {PR177: "lex"})
    assert only(d, "review")[gr.item_key("review", REPO, "pr/177", HEAD)].verdict == gr.READY
    assert not only(d, "merge")


def test_recorded_github_review_by_the_pr_author_does_not_approve():
    # The recorded review is an APPROVE by scbrown on scbrown's PR: the shared
    # account. Retargeted onto PR 177's head, it is still a self-review.
    review = variant("pull_request_review.submitted", "on177", {"commitSha": HEAD}, subject_props={})
    for e in review["edges"]:
        if e["relation"] == "onGitHubThread":
            e["target"] = "github.PR_kwDOTvaTnM8AAAABLF8ZkA"
    review["nodes"] = [n for n in review["nodes"] if n["name"] != "github.PR_kwDOTvaTnM8AAAABKlkOYg"]
    review["nodes"].append(ep("pull_request.opened")["nodes"][1])
    d = decide([ep(n) for n in BASE] + [review], ts(GREEN_AT) + 3600, approval_sources=["verdict", "github_review"])
    assert not only(d, "merge")
    assert only(d, "review")  # a self-review is not a review


# ---------------------------------------------------------------- (b) merge

def test_approve_and_green_on_the_same_head_requests_a_merge():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})
    item = gr.item_key("merge", REPO, "pr/177", HEAD)
    assert d[item].verdict == gr.READY
    approval = d[item].facts["approval"]
    assert approval["approvals"][0]["by"] == "sattler" and approval["author_agent"] == "lex"


def test_untrusted_verdicts_count_as_nothing():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 3600, [verdict("sattler", trusted=False)], {PR177: "lex"})
    assert not only(d, "merge")
    assert only(d, "review")  # not even a review: an unverified record is no verdict at all


def test_ambiguous_author_agents_never_merge_and_nothing_falls_back():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("malcolm")], {PR177: ["hammond", "malcolm"]})
    assert not only(d, "merge")


def test_unknown_author_agent_never_merges():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")])
    assert not only(d, "merge") and not only(d, "review")


def test_merge_waits_for_green_and_a_later_changes_verdict_blocks():
    item = gr.item_key("merge", REPO, "pr/177", HEAD)
    d = decide([ep("pull_request.opened")], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})
    assert d[item].verdict == gr.WAIT
    later = verdict("sattler", "changes", at="2026-10-27T18:58:00Z")
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler"), later], {PR177: "lex"})
    assert not only(d, "merge")
    earlier = verdict("sattler", "changes", at="2026-10-27T18:50:00Z")
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler"), earlier], {PR177: "lex"})
    assert d[item].verdict == gr.READY


def test_new_push_is_a_new_head_and_drops_the_old_approval():
    sync = variant("pull_request.opened", "sync", {"githubEventKind": "pull_request.synchronize",
                                                    "commitSha": "1" * 40, "occurredAt": "2026-10-27T19:00:00Z"})
    d = decide([ep(n) for n in BASE] + [sync], ts(GREEN_AT) + 3600, [verdict("sattler")], {PR177: "lex"})
    assert not only(d, "merge")
    assert set(only(d, "review")) == {gr.item_key("review", REPO, "pr/177", "1" * 40)}


def test_draft_and_closed_prs_get_nothing():
    draft = variant("pull_request.opened", "draft", {"githubDraft": True})
    assert decide([draft, ep("check_run.completed")], ts(GREEN_AT) + 3600, [verdict("sattler")], {PR177: "lex"}) == {}
    closed = [ep(n) for n in BASE] + [ep("pull_request.closed")]
    assert decide(closed, ts(GREEN_AT) + 3600, [verdict("sattler")], {PR177: "lex"}) == {}


def test_outside_contribution_goes_to_the_administrator_never_the_lane():
    outside = variant("pull_request.opened", "alice", subject_props={"githubAuthorLogin": "alice"})
    d = decide([outside, ep("check_run.completed")], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})
    assert not only(d, "merge")
    item = gr.item_key("outside-contribution", REPO, "pr/177", HEAD)
    assert d[item].verdict == gr.READY
    rec = json.loads(gr.record_for(config(), d[item])["evidence"])
    assert rec["route"] == "administrator" and rec["facts"]["outside"] is True


def second_pr(number=178, head="2" * 40, opened="2026-10-27T18:52:00Z", green="2026-10-27T18:53:00Z"):
    node = f"github.PR_second{number}"
    pr = variant("pull_request.opened", f"pr{number}", {"commitSha": head, "occurredAt": opened}, subject=node,
                 subject_props={"githubNumber": number, "githubNodeId": node[7:],
                                "githubUrl": f"https://github.com/{REPO}/pull/{number}"})
    check = variant("check_run.completed", f"cr{number}", {"commitSha": head, "occurredAt": green},
                    subject=f"github.CR_second{number}", subject_props={"commitSha": head})
    return gr.AEGIS + node, head, [pr, check]


def lane_row(rid, pr, head, acquired, lease=4500, released=None, outcome=None):
    return gr.LaneRow(rid, pr, head, acquired, acquired + lease, released, outcome)


def test_one_lane_head_of_line_by_approval_time():
    iri178, head178, extra = second_pr()
    verdicts = [verdict("sattler", at="2026-10-27T18:55:00Z"),
                verdict("sattler", sha=head178, number=178, at="2026-10-27T18:54:00Z")]
    d = decide([ep(n) for n in BASE] + extra, ts(GREEN_AT) + 600, verdicts, {PR177: "lex", iri178: "lex"})
    m = only(d, "merge")
    assert m[gr.item_key("merge", REPO, "pr/178", head178)].verdict == gr.READY
    assert m[gr.item_key("merge", REPO, "pr/177", HEAD)].verdict == gr.WAIT


def test_lane_in_flight_then_merged_fact_then_spacing():
    iri178, head178, extra = second_pr()
    verdicts = [verdict("sattler"), verdict("sattler", sha=head178, number=178, at="2026-10-27T18:56:00Z")]
    authors = {PR177: "lex", iri178: "lex"}
    rid177 = gr.request_id(gr.item_key("merge", REPO, "pr/177", HEAD))
    now = ts("2026-10-27T18:56:10Z")
    inflight = [lane_row(rid177, 177, HEAD, now - 60)]
    d = decide([ep(n) for n in BASE] + extra, now, verdicts, authors, inflight)
    item178 = gr.item_key("merge", REPO, "pr/178", head178)
    assert d[gr.item_key("merge", REPO, "pr/177", HEAD)].verdict == gr.READY  # holds the lane, same id
    assert d[item178].verdict == gr.WAIT and d[item178].next_check <= now + gr.LANE_RECHECK_S

    # The recorded merge of PR 177 (18:56:18Z) frees the lane; spacing holds PR 178 for 10 min.
    merged = ep("pull_request.closed")
    at_merge = ts("2026-10-27T18:56:18Z")
    d = decide([ep(n) for n in BASE] + extra + [merged], at_merge + 60, verdicts, authors, inflight)
    assert d[item178].verdict == gr.WAIT and d[item178].next_check == at_merge + 600
    lane = d[item178].facts["lane"]
    assert lane["completed"] == [{"request_id": rid177, "pr": 177, "head": HEAD, "outcome": "merged",
                                  "at": "2026-10-27T18:56:18Z"}]
    d = decide([ep(n) for n in BASE] + extra + [merged], at_merge + 601, verdicts, authors, inflight)
    assert d[item178].verdict == gr.READY


def test_spacing_counts_hand_merges_and_quipu_spacing_is_configurable():
    iri178, head178, extra = second_pr()
    merged = ep("pull_request.closed")  # PR 177 merged by hand, no lane row
    v = [verdict("sattler", sha=head178, number=178, at="2026-10-27T18:56:00Z")]
    at_merge = ts("2026-10-27T18:56:18Z")
    item = gr.item_key("merge", REPO, "pr/178", head178)
    assert decide(extra + [merged], at_merge + 300, v, {iri178: "lex"})[item].verdict == gr.WAIT
    assert decide(extra + [merged], at_merge + 300, v, {iri178: "lex"},
                  merge={"spacing_minutes": 4})[item].verdict == gr.READY


def test_a_failed_merge_turn_does_not_block_the_lane_or_repeat():
    iri178, head178, extra = second_pr()
    verdicts = [verdict("sattler"), verdict("sattler", sha=head178, number=178, at="2026-10-27T18:56:00Z")]
    rid177 = gr.request_id(gr.item_key("merge", REPO, "pr/177", HEAD))
    now = ts("2026-10-27T19:30:00Z")
    blocked = [lane_row(rid177, 177, HEAD, now - 900, released=now - 300, outcome="blocked")]
    d = decide([ep(n) for n in BASE] + extra, now, verdicts, {PR177: "lex", iri178: "lex"}, blocked)
    assert gr.item_key("merge", REPO, "pr/177", HEAD) not in d
    assert d[gr.item_key("merge", REPO, "pr/178", head178)].verdict == gr.READY
    expired = [lane_row(rid177, 177, HEAD, now - 9000, lease=4500)]
    d = decide([ep(n) for n in BASE] + extra, now, verdicts, {PR177: "lex", iri178: "lex"}, expired)
    assert d[gr.item_key("merge", REPO, "pr/178", head178)].verdict == gr.READY


def test_merge_disabled_per_repo():
    d = decide([ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"},
               merge={"enabled": False})
    assert not only(d, "merge")


# ---------------------------------------------------------------- (c) triage

def test_main_ci_red_requests_triage_and_a_later_green_supersedes_it():
    red = variant("workflow_run.completed", "red", {"githubConclusion": "failure"})
    d = decide([red], ts("2026-10-27T19:10:00Z"))
    item = gr.item_key("triage", REPO, "branch/main", MAIN_SHA)
    assert d[item].verdict == gr.READY
    assert d[item].facts["failed"][0]["name"] == "ci"
    assert decide([ep("workflow_run.completed")], ts("2026-10-27T19:10:00Z")) == {}
    fixed = variant("workflow_run.completed", "fixed", {"commitSha": "3" * 40, "occurredAt": "2026-10-27T19:20:00Z"},
                    subject="github.WFR_next", subject_props={"commitSha": "3" * 40})
    assert not only(decide([red, fixed], ts("2026-10-27T19:30:00Z")), "triage")


def test_a_rerun_attempt_that_passes_clears_triage():
    red = variant("workflow_run.completed", "red", {"githubConclusion": "failure"})
    rerun = variant("workflow_run.completed", "rerun", {"githubRunAttempt": 2, "occurredAt": "2026-10-27T19:12:00Z"})
    assert not only(decide([red, rerun], ts("2026-10-27T19:30:00Z")), "triage")


def test_pr_branch_runs_never_trigger_triage():
    red = variant("check_run.completed", "red", {"githubConclusion": "failure"})
    assert not only(decide([red], ts(GREEN_AT) + 60), "triage")


# ---------------------------------------------------------------- (d) release

def test_release_published_requests_install_and_announce_inside_its_window():
    published = ts("2026-10-20T21:41:34Z")
    d = decide([ep("release.published")], published + 3600)
    kinds = {x.kind for x in d.values()}
    assert kinds == {"install", "announce"}
    assert all(x.verdict == gr.READY and x.facts["tag"] == "v0.1.3" for x in d.values())
    assert decide([ep("release.published")], published + 2 * 86400) == {}
    assert {x.kind for x in decide([ep("release.published")], published + 60,
                                    release={"jobs": ["install"]}).values()} == {"install"}
    pre = variant("release.published", "pre", {"githubPrerelease": True})
    assert decide([pre], published + 60) == {}


# ---------------------------------------------------------------- idempotency through chaski's emitter

def emitter_():
    return Emitter("github-routing", ["unused"], 86400, "hammond", event="work_requested",
                   from_verdict=gr.WAIT, to_verdict=gr.READY, baseline_emits=True, change_driven=True)


def test_replays_and_flaps_queue_one_request_per_item(tmp_path):
    conn = connect(tmp_path / "e.db")
    e = emitter_()
    cfg = config()
    facts = gr.FixtureFacts([ep(n) for n in BASE], [verdict("sattler")], {PR177: "lex"})
    adapter = gr.Adapter(cfg, facts)
    now = ts(GREEN_AT) + 600
    items = adapter.discover(now)
    first = adapter.evaluate(items, now)
    queued = observe(conn, e, first["records"], now, scope=set(first["scope"]))
    assert len(queued) == 1  # the merge; the review is not a candidate once reviewed
    for _ in range(3):  # replays of the same facts
        assert observe(conn, e, gr.Adapter(cfg, facts).evaluate(items, now)["records"], now,
                       scope=set(items)) == []
    # flap: CI goes pending (WAIT), then green again on the SAME head: same id, no new row
    pending = gr.Adapter(cfg, gr.FixtureFacts([ep("pull_request.opened")], [verdict("sattler")], {PR177: "lex"}))
    observe(conn, e, pending.evaluate(items, now)["records"], now + 1, scope=set(items))
    observe(conn, e, gr.Adapter(cfg, facts).evaluate(items, now)["records"], now + 2, scope=set(items))
    rows = conn.execute("SELECT event_id, payload FROM outbox").fetchall()
    assert len(rows) == 1
    payload = json.loads(rows[0][1])
    assert payload["work_kind"] == "merge" and payload["recipient"] == "hammond"
    assert payload["event_id"] == gr.request_id(gr.item_key("merge", REPO, "pr/177", HEAD))


def test_adapter_protocol_shapes():
    cfg = config()
    adapter = gr.Adapter(cfg, gr.FixtureFacts([ep(n) for n in BASE]))
    desc = adapter.describe()
    assert desc["graphs"] == [cfg.graph] and desc["types"] == [gr.AEGIS + "GitHubActivity"]
    now = ts(GREEN_AT) + 600
    routed = adapter.handle({"now": now, "route": [{"entity": gr.AEGIS + "github.activity.x",
                                                    "attribute": gr.AEGIS + "inGitHubRepository",
                                                    "value": {"ref": gr.AEGIS + "github.R_kgDOTvaTnA"}}]})
    assert routed == {"version": 1, "items": [gr.item_key("review", REPO, "pr/177", HEAD)]}
    stale = gr.item_key("review", REPO, "pr/177", "9" * 40)
    out = adapter.handle({"now": now, "items": [routed["items"][0], stale], "changes": []})
    assert out["scope"] == [routed["items"][0], stale]
    assert [r["item"] for r in out["records"]] == [routed["items"][0]]  # stale -> untracked
    assert set(out["next_checks"]) == {routed["items"][0]}
    assert out["next_checks"][routed["items"][0]] > now


def test_cli_fixture_dry_run(tmp_path):
    cfg = tmp_path / "routing.yaml"
    cfg.write_text(json.dumps({"version": 1, "graph": "https://camayoc.local/operational/github",
                               "administrator": "hammond", "members": ["scbrown"], "repos": {REPO: {}}}))
    fixture = tmp_path / "facts.json"
    fixture.write_text(json.dumps({"episodes": [{"episode": ep(n)} for n in BASE]}))
    base = [sys.executable, str(HERE / "github_routes.py"), "--config", str(cfg), "--fixture", str(fixture)]
    desc = json.loads(subprocess.run(base + ["--describe"], capture_output=True, text=True, check=True).stdout)
    assert desc["version"] == 1
    out = subprocess.run(base + ["--changes"], input=json.dumps({"now": ts(GREEN_AT) + 3600, "discover": True}),
                         capture_output=True, text=True, check=True)
    assert json.loads(out.stdout)["items"] == [gr.item_key("review", REPO, "pr/177", HEAD)]


def test_quipu_read_failure_is_unknown_not_empty():
    class Broken(gr.QuipuFacts):
        def _rows(self, sparql):
            raise gr.FactsUnknown("down")
    adapter = gr.Adapter(config(), Broken("http://127.0.0.1:9", config().graph, 30))
    with pytest.raises(gr.FactsUnknown):
        adapter.handle({"now": 1.0e9, "discover": True})


def test_queries_are_bounded_to_the_one_graph_and_validated():
    q = gr.activity_query("https://camayoc.local/operational/github", REPO, "2026-10-01T00:00:00Z")
    assert "GRAPH <https://camayoc.local/operational/github>" in q and '"intigraph/camayoc"' in q
    with pytest.raises(ValueError):
        gr.activity_query("https://camayoc.local/operational/github", 'x/y" } DROP', "2026")
    vq = gr.verdict_query(REPO)
    assert "a aegis:Verdict" in vq and "aegis:signature ?sig" in vq and '"github:pr:intigraph/camayoc#"' in vq


def test_config_refuses_unknown_fields_and_needs_members():
    with pytest.raises(gr.ConfigError):
        gr.parse_config({"version": 1, "graph": "https://g.example/x", "administrator": "hammond",
                         "members": [], "repos": {REPO: {}}})
    with pytest.raises(gr.ConfigError):
        config(merge={"spacing": 10})
    assert config().repos[REPO].spacing_s == 600
    example = gr.load_config(HERE / "github-routing.example.yaml")
    assert example.repos["intigraph/quipu"].spacing_s == 600


def test_documented_emitter_definition_loads(tmp_path):
    import re as _re
    import textwrap

    import chaski
    doc = (HERE / "docs/github-routing.md").read_text()
    block = _re.search(r"```yaml\n(\s*emitters:.*?)```", doc, _re.S).group(1)
    path = tmp_path / "emitters.yaml"
    path.write_text(textwrap.dedent(block))
    [(e, sink)] = chaski.load_emitters(path)
    assert e.change_driven and e.from_verdict == gr.WAIT and e.to_verdict == gr.READY and e.baseline_emits
    assert set(sink) == {"graph", "command"}


# ---------------------------------------------------------------- the real quipu read path (HTTP)

class FakeQuipu:
    """A local HTTP quipu answering /query by query shape and /verdict/verify
    from a trust table: QuipuFacts runs unmodified against it."""

    def __init__(self, activity_rows, verdict_rows, author_rows, trusted):
        import threading
        from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
        fake = self
        self.verify_calls = []

        class H(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/verdict/verify":
                    fake.verify_calls.append(body)
                    ok = trusted.get(body["verdict"], False)
                    out = {"trusted": ok, "signature_valid": ok,
                           "as_of": {"tx": 7, "at": "2026-10-27T18:55:00Z", "basis": "recorded"}}
                else:
                    q = body["query"]
                    rows = (author_rows if "authoredByAgent" in q else verdict_rows if "aegis:Verdict" in q
                            else activity_rows)
                    out = {"rows": rows}
                data = json.dumps(out).encode()
                self.send_response(200)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *_):
                pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def close(self):
        self.server.shutdown()


def stored_verdict(name, by, outcome="satisfied", sha=HEAD, sig="ab" * 32, **extra):
    return {"v": f"{gr.AEGIS}verdict.{name}", "target": gr.review_target(REPO, 177, sha), "outcome": outcome,
            "hash": "sha256:" + "0" * 64, "tier": "committed", "verifier": f"{gr.AEGIS}verifier/{by}",
            "sig": sig, **extra}


def quipu_decide(verdict_rows, author_rows, trusted):
    activity = [{k: v for k, v in r.items() if k != "ts"}
                for r in gr.rows_from_episodes([ep(n) for n in BASE])]
    fake = FakeQuipu(activity, verdict_rows, author_rows, trusted)
    try:
        cfg = config()
        facts = gr.QuipuFacts(fake.url, cfg.graph, 30)
        now = ts(GREEN_AT) + 600
        snap = facts.snapshot(REPO, now)
        return gr.evaluate_repo(cfg, cfg.repos[REPO], snap, [], now), snap, fake
    finally:
        fake.close()


def test_quipu_path_ambiguous_author_is_unknown_even_when_the_verdict_names_another():
    # Review repro: authors {hammond, malcolm}; malcolm's (trusted) verdict claims author sattler.
    v = stored_verdict("m1", "malcolm", authorAgent=gr.AEGIS + "agent/sattler")
    authors = [{"pr": PR177, "agent": gr.AEGIS + "agent/hammond"}, {"pr": PR177, "agent": gr.AEGIS + "agent/malcolm"}]
    d, snap, _ = quipu_decide([v], authors, {v["v"]: True})
    assert snap.authors[PR177] is gr.AMBIGUOUS
    assert not only(d, "merge")
    assert not only(d, "review")  # reviewed, by someone; never approved


def test_quipu_path_only_trusted_signed_verdicts_count():
    authors = [{"pr": PR177, "agent": gr.AEGIS + "agent/lex"}]
    good = stored_verdict("ok", "malcolm")
    d, _, fake = quipu_decide([good], authors, {good["v"]: True})
    item = gr.item_key("merge", REPO, "pr/177", HEAD)
    assert d[item].verdict == gr.READY
    assert fake.verify_calls[0]["verdict"] == good["v"] and fake.verify_calls[0]["predicate_id"] == gr.REVIEW_PREDICATE
    forged = stored_verdict("forged", "malcolm")
    d, _, _ = quipu_decide([forged], authors, {})  # signature does not verify against the registry
    assert item not in d
    unsigned = stored_verdict("unsigned", "malcolm", sig="")
    d, _, fake = quipu_decide([unsigned], authors, {unsigned["v"]: True})
    assert item not in d and fake.verify_calls == []
    stale = stored_verdict("stale", "malcolm", sha="4" * 40)
    d, _, fake = quipu_decide([stale], authors, {stale["v"]: True})
    assert item not in d and fake.verify_calls == []  # a verdict on another head is never even verified
    self_review = stored_verdict("self", "lex")
    d, _, _ = quipu_decide([self_review], authors, {self_review["v"]: True})
    assert item not in d
