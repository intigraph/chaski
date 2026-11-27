"""pr-lifecycle adapter (aegis-za4f.21): PR state -> review/task/merge events."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import sys
from pathlib import Path

import pytest

import chaski
import emitter as em
import pr_lifecycle as pl

H1, H2 = "a" * 40, "b" * 40
NOW = dt.datetime(2026, 11, 27, 8, 0, tzinfo=dt.timezone.utc)
CFG = pl.Config(repos=("intigraph/quipu",), verdict_logins=frozenset({"scbrown"}))


def pr(**kw):
    base = {"repo": "intigraph/quipu", "number": 1354, "draft": False, "author": "scbrown",
            "author_association": "OWNER", "author_is_bot": False,
            "head_ref": "design/aegis-m5s.3.2-identity", "head_ref_oid": H1,
            "title": "docs(design): identity (aegis-m5s.3.2)", "body": "", "head": H1,
            "checks": [{"name": "Build", "state": "SUCCESS", "required": True},
                       {"name": "Lint", "state": "PENDING", "required": False}],
            "checks_complete": True, "reviews": [], "reviews_complete": True,
            "comments": [], "comments_complete": True}
    base.update(kw)
    return base


def verdict(word, head, at="2026-11-27T07:00:00Z", login="scbrown"):
    return {"author": login, "at": at, "body": f"{word} at head {head}\n\nbody"}


def jobs(p, cfg=CFG):
    return pl.judge(cfg, p)


def test_unreviewed_head_needs_review():
    assert jobs(pr()) == [("review", "new-head-unreviewed")]


def test_a_new_head_re_arms_review_after_a_verdict_on_the_old_head():
    p = pr(head=H2, head_ref_oid=H2, comments=[verdict("REQUEST CHANGES", H1)])
    assert jobs(p) == [("review", "new-head-unreviewed")]


def test_request_changes_on_the_head_needs_a_fix_task():
    assert jobs(pr(comments=[verdict("REQUEST CHANGES", H1)])) == [("task", "changes-requested")]


def test_red_required_check_needs_a_fix_task_not_a_review():
    p = pr(checks=[{"name": "Build", "state": "FAILURE", "required": True}])
    assert jobs(p) == [("task", "ci-red")]


def test_green_and_approved_on_the_head_needs_merge():
    p = pr(comments=[verdict("APPROVE", H1)])  # Lint is pending but not required
    assert jobs(p) == [("merge", "green-and-approved")]


def test_approved_but_pending_or_stale_approval_waits():
    pending = pr(checks=[{"name": "Build", "state": "PENDING", "required": True}],
                 comments=[verdict("APPROVE", H1)])
    assert jobs(pending) == []
    stale = pr(comments=[verdict("APPROVE", H2)])  # approval for another head
    assert jobs(stale) == [("review", "new-head-unreviewed")]


def test_latest_verdict_on_the_head_decides():
    later_changes = [verdict("APPROVE", H1, "2026-11-27T07:00:00Z"),
                     verdict("REQUEST CHANGES", H1, "2026-11-27T07:30:00Z")]
    assert jobs(pr(comments=later_changes)) == [("task", "changes-requested")]
    later_approve = list(reversed([dict(c, at=t) for c, t in
                                   zip(later_changes, ["2026-11-27T07:30:00Z", "2026-11-27T07:00:00Z"])]))
    assert jobs(pr(comments=later_approve)) == [("merge", "green-and-approved")]


def test_github_reviews_count_only_from_non_authors_on_the_head():
    own = pr(reviews=[{"author": "scbrown", "state": "APPROVED", "commit": H1, "at": "t"}])
    assert jobs(pr(**{k: own[k] for k in ("reviews",)}, comments=[])) == [("review", "new-head-unreviewed")]
    other = pr(reviews=[{"author": "reviewer", "association": "MEMBER", "state": "APPROVED",
                         "commit": H1, "at": "t"}])
    assert jobs(other) == [("merge", "green-and-approved")]


def test_drive_by_reviews_recruit_nothing():
    for assoc in ("NONE", "CONTRIBUTOR", "FIRST_TIME_CONTRIBUTOR", None):
        changes = pr(reviews=[{"author": "drive-by", "association": assoc, "state": "CHANGES_REQUESTED",
                               "commit": H1, "at": "t"}])
        assert jobs(changes) == [("review", "new-head-unreviewed")]  # no fix task on attacker text
        approve = pr(reviews=[{"author": "drive-by", "association": assoc, "state": "APPROVED",
                               "commit": H1, "at": "t"}])
        assert jobs(approve) == [("review", "new-head-unreviewed")]  # independent review still runs
    allowed = pl.Config(repos=CFG.repos, verdict_logins=CFG.verdict_logins, trusted_logins=frozenset({"drive-by"}))
    changes = pr(reviews=[{"author": "drive-by", "association": "NONE", "state": "CHANGES_REQUESTED",
                           "commit": H1, "at": "t"}])
    assert jobs(changes, allowed) == [("task", "changes-requested")]


def test_outside_author_prs_are_not_judged():
    for assoc in ("NONE", "CONTRIBUTOR", "FIRST_TIME_CONTRIBUTOR", None):
        outside = pr(author="outsider", author_association=assoc)
        assert jobs(outside) == []
        assert jobs(pr(author="outsider", author_association=assoc,
                       checks=[{"name": "Build", "state": "FAILURE", "required": True}])) == []
    assert jobs(pr(author="teammate", author_association="COLLABORATOR")) == [("review", "new-head-unreviewed")]


def test_verdict_comments_from_untrusted_logins_are_ignored():
    p = pr(comments=[verdict("APPROVE", H1, login="drive-by")])
    assert jobs(p) == [("review", "new-head-unreviewed")]


def test_drafts_bots_and_release_prs_are_skipped():
    assert jobs(pr(draft=True)) == []
    assert jobs(pr(author="release-please[bot]")) == []
    assert jobs(pr(author_is_bot=True)) == []
    assert jobs(pr(head_ref="release-please--branches--main")) == []


def test_unknown_never_emits():
    assert jobs(pr(head=None)) == []
    assert jobs(pr(head=H2)) == []  # head moved between the two reads
    assert jobs(pr(reviews_complete=False)) == []  # truncated reviews: cannot say unreviewed
    no_checks = pr(checks=None, comments=[verdict("APPROVE", H1)])
    assert jobs(no_checks) == []
    truncated = pr(checks_complete=False, comments=[verdict("APPROVE", H1)])
    assert jobs(truncated) == []


def test_cancelled_superseded_runs_do_not_turn_a_green_head_red():
    contexts = pl.latest_per_name([
        {"name": "Release correctness", "state": "CANCELLED", "required": True, "at": "2026-11-27T06:00:00Z"},
        {"name": "Release correctness", "state": "SUCCESS", "required": False, "at": "2026-11-27T06:05:00Z"},
    ])
    assert contexts == [{"name": "Release correctness", "state": "SUCCESS", "required": True,
                         "at": "2026-11-27T06:05:00Z"}]
    assert pl.check_state(pr(checks=[{"name": "x", "state": "CANCELLED", "required": True}])) == "pending"


def test_record_carries_the_fixed_event_contract():
    [r] = pl.records(CFG, [pr()], NOW)
    eid = hashlib.sha256(f"intigraph/quipu|1354|{H1}|review".encode()).hexdigest()
    assert r["item"] == f"intigraph/quipu#1354@{H1}:review" and r["verdict"] == "READY"
    assert r["event_id"] == eid
    assert r["action"] == {"event_id": eid, "repo": "intigraph/quipu", "pr": 1354, "head": H1,
                           "job": "review", "item": "aegis-m5s.3.2", "reason": "new-head-unreviewed",
                           "observed_at": "2026-11-27T08:00:00Z"}


def test_seed_item_prefers_title_then_branch_then_body_and_may_be_null():
    assert pl.seed_item(CFG, pr(title="fix (aegis-za4f.21)")) == "aegis-za4f.21"
    assert pl.seed_item(CFG, pr(title="fix", head_ref="wt/aegis-cj5-x")) == "aegis-cj5"
    assert pl.seed_item(CFG, pr(title="fix", head_ref="wt/eph", body="Closes aegis-xxoa.5")) == "aegis-xxoa.5"
    assert pl.seed_item(CFG, pr(title="fix", head_ref="wt/eph", body="")) is None


def test_emitter_and_projecting_sink_write_one_contract_line_per_pair_ever(tmp_path):
    e = em.Emitter(label="pr-lifecycle", owner="sattler", interval_s=120, command=["true"],
                   event="pr_action", from_verdict="WAIT", to_verdict="READY", baseline_emits=True)
    conn = em.connect(tmp_path / "state.db")
    sink = em.JsonlSink(tmp_path / "actions.jsonl", project="action")
    first = pl.records(CFG, [pr()], NOW)
    assert len(em.observe(conn, e, first, 1.0)) == 1
    assert em.observe(conn, e, pl.records(CFG, [pr()], NOW), 2.0) == []  # same head: nothing new
    em.observe(conn, e, [], 3.0)  # PR vanished from a run
    em.observe(conn, e, pl.records(CFG, [pr()], NOW), 4.0)  # returns: same event id
    em.deliver_pending(conn, sink, now=5.0, emitter="pr-lifecycle")
    sink.deliver({"event_id": first[0]["event_id"], "action": first[0]["action"]})  # lost-ack resend
    lines = [json.loads(x) for x in (tmp_path / "actions.jsonl").read_text().splitlines()]
    assert lines == [first[0]["action"]]
    moved = pr(head=H2, head_ref_oid=H2)
    assert len(em.observe(conn, e, pl.records(CFG, [moved], NOW), 6.0)) == 1  # new head re-arms
    em.deliver_pending(conn, sink, now=7.0, emitter="pr-lifecycle")
    lines = [json.loads(x) for x in (tmp_path / "actions.jsonl").read_text().splitlines()]
    assert [x["head"] for x in lines] == [H1, H2]


def test_projecting_sink_refuses_an_event_without_its_line(tmp_path):
    with pytest.raises(ValueError):
        em.JsonlSink(tmp_path / "a.jsonl", project="action").deliver({"event_id": "x"})
    assert not (tmp_path / "a.jsonl").exists()


def test_emitter_refuses_an_action_whose_id_differs():
    e = em.Emitter(label="pr-lifecycle", owner="o", interval_s=1, command=["true"],
                   from_verdict="WAIT", to_verdict="READY", baseline_emits=True)
    bad = {"item": "i", "verdict": "READY", "event_id": "x", "action": {"event_id": "y"}}
    with pytest.raises(em.ProtocolError):
        em._validate(e, [bad])


EXAMPLE = Path(__file__).parent / "pr-lifecycle.example.yaml"


def test_rules_file_accepts_the_projecting_jsonl_sink(tmp_path):
    rules = tmp_path / "rules.yaml"
    rules.write_text(EXAMPLE.read_text().split("# --- emitter ---\n", 1)[1])
    [(e, sink), (red, red_sink)] = chaski.load_emitters(rules)
    assert (e.label, e.interval_s, e.baseline_emits) == ("pr-lifecycle", 120, True)
    made = chaski.make_sink(e, sink, "http://unused")
    assert isinstance(made, em.JsonlSink) and made.project == "action"
    # main red -> triage (aegis-dja): its own file, no baseline backlog.
    assert (red.label, red.interval_s, red.baseline_emits) == ("main-red", 120, False)
    made = chaski.make_sink(red, red_sink, "http://unused")
    assert isinstance(made, em.JsonlSink) and made.project == "action" and str(made.path).endswith("main-red.jsonl")
    rules.write_text("emitters:\n- {label: x, schedule: PT2M, owner: o, command: [true],"
                     " sink: {jsonl: {path: /x, extra: 1}}}\n")
    with pytest.raises(chaski.RuleError):
        chaski.load_emitters(rules)


def test_config_validation(tmp_path):
    c = tmp_path / "c.yaml"
    c.write_text(EXAMPLE.read_text().split("# --- emitter ---\n", 1)[0])
    cfg = pl.load_config(c)
    assert "intigraph/quipu" in cfg.repos and "scbrown" in cfg.verdict_logins
    assert cfg.trusted_associations == {"OWNER", "MEMBER", "COLLABORATOR"}
    c.write_text("repos: [not a repo]\n")
    with pytest.raises(pl.ConfigError):
        pl.load_config(c)
    c.write_text("repos: [intigraph/quipu]\ntrusted_associations: [EVERYONE]\n")
    with pytest.raises(pl.ConfigError):
        pl.load_config(c)


def test_cli_runs_offline_against_a_fixture(tmp_path, capsys):
    c = tmp_path / "c.yaml"
    c.write_text("repos: [intigraph/quipu]\nverdict_logins: [scbrown]\n")
    f = tmp_path / "prs.json"
    f.write_text(json.dumps([pr(comments=[verdict("APPROVE", H1)]), pr(number=7, draft=True)]))
    assert pl.main(["--config", str(c), "--fixture", str(f)]) == 0
    [r] = json.loads(capsys.readouterr().out)
    assert r["action"]["job"] == "merge"


def test_cli_failure_is_nonzero_so_the_run_is_unknown(tmp_path):
    c = tmp_path / "c.yaml"
    c.write_text(f"repos: [intigraph/quipu]\ngh: [{sys.executable}, -c, 'import sys; sys.exit(1)']\n")
    assert pl.main(["--config", str(c)]) == 1


def test_version_exits_without_config(capsys):
    with pytest.raises(SystemExit) as exc:
        pl.main(["--version"])
    assert exc.value.code == 0
    out = capsys.readouterr()
    assert out.out.startswith("chaski-pr-lifecycle ") and out.err == ""


def test_list_open_reports_truncation(monkeypatch, capsys):
    data = {"r0": {"pullRequests": {"pageInfo": {"hasNextPage": True}, "nodes": [
        {"number": 1, "isDraft": False, "headRefOid": H1, "headRefName": "x", "title": "t", "body": "",
         "authorAssociation": "OWNER", "author": {"login": "scbrown", "__typename": "User"}}]}}}
    monkeypatch.setattr(pl, "_graphql", lambda cfg, q: data)
    [p] = pl.list_open(CFG)
    assert p["number"] == 1 and p["author_association"] == "OWNER"
    assert "intigraph/quipu: more than 30 open PRs" in capsys.readouterr().err


def test_add_details_reads_review_association(monkeypatch):
    node = {"commits": {"nodes": [{"commit": {"oid": H1, "statusCheckRollup": None}}]},
            "reviews": {"pageInfo": {"hasPreviousPage": False}, "nodes": [
                {"state": "CHANGES_REQUESTED", "submittedAt": "t", "authorAssociation": "NONE",
                 "author": {"login": "drive-by"}, "commit": {"oid": H1}}]},
            "comments": {"pageInfo": {"hasPreviousPage": False}, "nodes": []}}
    monkeypatch.setattr(pl, "_graphql", lambda cfg, q: {"p0": {"pullRequest": node}})
    [out] = pl.add_details(CFG, [pr()])
    assert out["reviews"][0]["association"] == "NONE"
    assert pl.head_verdicts(CFG, out) == []


def test_add_details_reports_stalled_pr(monkeypatch, capsys):
    node = {"commits": {"nodes": [{"commit": {"oid": H1, "statusCheckRollup": None}}]},
            "reviews": {"pageInfo": {"hasPreviousPage": True}, "nodes": []},
            "comments": {"pageInfo": {"hasPreviousPage": False}, "nodes": []}}
    monkeypatch.setattr(pl, "_graphql", lambda cfg, q: {"p0": {"pullRequest": node}})
    out = pl.add_details(CFG, [pr()])
    assert out[0]["reviews_complete"] is False and jobs(out[0]) == []
    assert "intigraph/quipu#1354: more than 100 reviews or comments" in capsys.readouterr().err
