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
    assert cfg.carry_verdicts_over_base_merges is True
    c.write_text("repos: [intigraph/quipu]\ncarry_verdicts_over_base_merges: \"yes\"\n")
    with pytest.raises(pl.ConfigError):
        pl.load_config(c)
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


# --- main moving does not re-arm review (Stiwi 2026-11-28) -----------------

C1, M1, M2, X1 = "c" * 40, "d" * 40, "e" * 40, "f" * 40
MAIN1, MAIN2 = "1" * 40, "2" * 40


def synced(head, commits, **kw):
    return pr(head=head, head_ref_oid=head, commits=commits, commits_complete=True, **kw)


def test_update_branch_merge_keeps_the_approval_and_merges():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN2]}]
    p = synced(M1, commits, comments=[verdict("APPROVE", C1)], clean_merges={M1: True})
    assert pl.verdict_commits(CFG, p) == {M1, C1}
    assert jobs(p) == [("merge", "green-and-approved")]


def test_an_approval_does_not_carry_across_an_unproved_or_evil_merge():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN2]}]
    for proofs in ({}, {M1: False}):
        p = synced(M1, commits, comments=[verdict("APPROVE", C1)], clean_merges=proofs)
        assert pl.verdict_commits(CFG, p) == {M1}
        assert jobs(p) == [("review", "new-head-unreviewed")]
    # A REQUEST CHANGES still carries without a proof: carrying a block is safe.
    p = synced(M1, commits, comments=[verdict("REQUEST CHANGES", C1)])
    assert jobs(p) == [("task", "changes-requested")]


def test_repeated_base_merges_keep_request_changes_too():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN1]},
               {"oid": M2, "parents": [M1, MAIN2]}]
    p = synced(M2, commits, comments=[verdict("REQUEST CHANGES", C1)])
    assert jobs(p) == [("task", "changes-requested")]


def test_a_real_commit_after_the_verdict_still_needs_review():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN2]},
               {"oid": X1, "parents": [M1]}]
    p = synced(X1, commits, comments=[verdict("APPROVE", C1)])
    assert pl.verdict_commits(CFG, p) == {X1}
    assert jobs(p) == [("review", "new-head-unreviewed")]


def test_merging_another_pr_branch_is_new_code():
    # X1 is in the PR's own commit list, so M1 brings in non-base commits.
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": X1, "parents": [MAIN1]},
               {"oid": M1, "parents": [C1, X1]}]
    p = synced(M1, commits, comments=[verdict("APPROVE", C1)])
    assert jobs(p) == [("review", "new-head-unreviewed")]


def test_no_carry_without_the_complete_commit_list_or_when_disabled():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN2]}]
    truncated = synced(M1, commits, comments=[verdict("APPROVE", C1)])
    truncated["commits_complete"] = False
    assert jobs(truncated) == [("review", "new-head-unreviewed")]
    off = pl.Config(repos=CFG.repos, verdict_logins=CFG.verdict_logins,
                    carry_verdicts_over_base_merges=False)
    assert jobs(synced(M1, commits, comments=[verdict("APPROVE", C1)]), off) == [
        ("review", "new-head-unreviewed")]
    # A rebase replaces the reviewed commit: it is not in the list, no carry.
    rebased = synced(X1, [{"oid": X1, "parents": [MAIN2]}], comments=[verdict("APPROVE", C1)])
    assert jobs(rebased) == [("review", "new-head-unreviewed")]


def test_github_review_on_the_pre_merge_commit_carries():
    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN2]}]
    review = {"state": "APPROVED", "at": "t", "author": "alice", "association": "MEMBER", "commit": C1}
    assert jobs(synced(M1, commits, reviews=[review], clean_merges={M1: True})) == [
        ("merge", "green-and-approved")]


def test_add_details_reads_the_commit_chain(monkeypatch):
    def commit(oid, parents, total=None):
        return {"commit": {"oid": oid, "parents": {"totalCount": len(parents) if total is None else total,
                                                    "nodes": [{"oid": o} for o in parents]}}}
    node = {"commits": {"nodes": [{"commit": {"oid": M1, "statusCheckRollup": None}}]},
            "chain": {"pageInfo": {"hasPreviousPage": False},
                      "nodes": [commit(C1, [MAIN1]), commit(M1, [C1, MAIN2])]},
            "reviews": {"pageInfo": {"hasPreviousPage": False}, "nodes": []},
            "comments": {"pageInfo": {"hasPreviousPage": False}, "nodes": [
                {"createdAt": "t", "author": {"login": "scbrown"}, "body": f"APPROVE at head {C1}"}]}}
    monkeypatch.setattr(pl, "_graphql", lambda cfg, q: {"p0": {"pullRequest": node}})
    [out] = pl.add_details(CFG, [pr(head_ref_oid=M1)])
    assert out["commits_complete"] is True and pl.head_verdicts(CFG, out) == []
    [out] = pl.add_merge_proofs(CFG, [out], prove=lambda m, ps: True)
    assert out["clean_merges"] == {M1: True} and pl.head_verdicts(CFG, out) == ["approve"]
    node["chain"]["nodes"][1] = commit(M1, [C1, MAIN2], total=6)  # parent list cut off
    [out] = pl.add_details(CFG, [pr(head_ref_oid=M1)])
    assert out["commits_complete"] is False and pl.head_verdicts(CFG, out) == []
    [out] = pl.add_merge_proofs(CFG, [out], prove=lambda m, ps: True)
    assert "clean_merges" not in out and pl.head_verdicts(CFG, out) == []


def test_merge_proofs_run_only_when_an_approval_would_carry():
    calls = []

    def prove(m, ps):
        calls.append(m)
        return m != M2

    commits = [{"oid": C1, "parents": [MAIN1]}, {"oid": M1, "parents": [C1, MAIN1]},
               {"oid": M2, "parents": [M1, MAIN2]}]
    # No approval on an earlier commit: no git at all.
    [p] = pl.add_merge_proofs(CFG, [synced(M2, commits)], prove=prove)
    assert calls == [] and "clean_merges" not in p
    # The approval sits under two merges; the newest is evil, so the walk stops there.
    [p] = pl.add_merge_proofs(CFG, [synced(M2, commits, comments=[verdict("APPROVE", C1)])], prove=prove)
    assert calls == [M2] and p["clean_merges"] == {M2: False}
    assert jobs(p) == [("review", "new-head-unreviewed")]
    # A failing proof is unknown, never clean.
    [p] = pl.add_merge_proofs(CFG, [synced(M2, commits, comments=[verdict("APPROVE", C1)])],
                              prove=lambda m, ps: 1 / 0)
    assert p["clean_merges"] == {} and jobs(p) == [("review", "new-head-unreviewed")]
    # No git_cache and no prover: nothing is proved, the approval does not carry.
    [p] = pl.add_merge_proofs(CFG, [synced(M2, commits, comments=[verdict("APPROVE", C1)])])
    assert p["clean_merges"] == {}


# --- merge_is_clean against a real repository (sattler 2026-11-28 ruling) ----

def _git(d, *a, env=None):
    import subprocess
    e = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x", "GIT_COMMITTER_NAME": "t",
         "GIT_COMMITTER_EMAIL": "t@x", "GIT_AUTHOR_DATE": "2026-11-28T00:00:00Z",
         "GIT_COMMITTER_DATE": "2026-11-28T00:00:00Z", "HOME": str(d), "PATH": "/usr/bin:/bin"}
    return subprocess.run(["git", "-C", str(d), *a], check=True, capture_output=True, text=True,
                          env=e).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    d = tmp_path / "r"
    d.mkdir()
    _git(d, "init", "-q", "-b", "main")
    (d / "a.txt").write_text("one\ntwo\nthree\n")
    (d / "b.txt").write_text("base\n")
    _git(d, "add", ".")
    _git(d, "commit", "-qm", "root")
    _git(d, "checkout", "-qb", "pr")
    (d / "a.txt").write_text("ONE\ntwo\nthree\n")
    _git(d, "commit", "-qam", "pr change")
    reviewed = _git(d, "rev-parse", "HEAD")
    _git(d, "checkout", "-q", "main")
    (d / "b.txt").write_text("base moved\n")
    _git(d, "commit", "-qam", "main moves")
    main = _git(d, "rev-parse", "HEAD")
    _git(d, "checkout", "-q", "pr")
    return d, reviewed, main


def _cfg_git(d):
    return pl.Config(repos=CFG.repos, git=("git",), timeout_s=30)


def test_a_clean_update_branch_merge_carries(repo):
    d, reviewed, main = repo
    _git(d, "merge", "-q", "--no-edit", main)
    merge = _git(d, "rev-parse", "HEAD")
    assert pl.merge_is_clean(_cfg_git(d), d / ".git", merge, [reviewed, main]) is True


def test_an_evil_merge_with_an_extra_edit_does_not_carry(repo):
    d, reviewed, main = repo
    _git(d, "merge", "-q", "--no-commit", main)
    (d / "c.txt").write_text("smuggled\n")
    _git(d, "add", "c.txt")
    _git(d, "commit", "-qm", "Merge branch main")
    merge = _git(d, "rev-parse", "HEAD")
    assert _git(d, "rev-list", "--parents", "-n", "1", merge).split()[1:] == [reviewed, main]
    assert pl.merge_is_clean(_cfg_git(d), d / ".git", merge, [reviewed, main]) is False


def test_a_conflict_resolution_merge_does_not_carry(repo):
    d, reviewed, _ = repo
    _git(d, "checkout", "-q", "main")
    (d / "a.txt").write_text("uno\ntwo\nthree\n")
    _git(d, "commit", "-qam", "main edits the same line")
    main = _git(d, "rev-parse", "HEAD")
    _git(d, "checkout", "-q", "pr")
    import subprocess
    with pytest.raises(subprocess.CalledProcessError):
        _git(d, "merge", "-q", "--no-edit", main)
    (d / "a.txt").write_text("ONE uno\ntwo\nthree\nplus an unreviewed line\n")
    _git(d, "commit", "-qam", "Merge branch main")
    merge = _git(d, "rev-parse", "HEAD")
    assert pl.merge_is_clean(_cfg_git(d), d / ".git", merge, [reviewed, main]) is False


def test_merge_is_clean_refuses_wrong_parents_and_unknown_commits(repo):
    d, reviewed, main = repo
    _git(d, "merge", "-q", "--no-edit", main)
    merge = _git(d, "rev-parse", "HEAD")
    cfg = _cfg_git(d)
    assert pl.merge_is_clean(cfg, d / ".git", merge, [main, reviewed]) is False
    assert pl.merge_is_clean(cfg, d / ".git", merge, [reviewed]) is False
    assert pl.merge_is_clean(cfg, d / ".git", "9" * 40, [reviewed, main]) is False


# --- review policy from Quipu (Stiwi 2026-11-28, sattler part 1) -------------

def test_none_mode_never_reviews_and_merges_green():
    assert jobs(pr(review_mode="none")) == [("merge", "green-and-approved")]
    # A new head after a verdict on the old one: still no review.
    assert jobs(pr(head=H2, head_ref_oid=H2, comments=[verdict("APPROVE", H1)],
                   review_mode="none")) == [("merge", "green-and-approved")]


def test_none_mode_red_or_changes_requested_needs_a_fix_and_pending_waits():
    red = [{"name": "Build", "state": "FAILURE", "required": True}]
    assert jobs(pr(review_mode="none", checks=red)) == [("task", "ci-red")]
    assert jobs(pr(review_mode="none", comments=[verdict("REQUEST CHANGES", H1)])) == [
        ("task", "changes-requested")]
    pending = [{"name": "Build", "state": "PENDING", "required": True}]
    assert jobs(pr(review_mode="none", checks=pending)) == []
    # Unknown verdicts may hide a standing REQUEST CHANGES: no merge.
    assert jobs(pr(review_mode="none", comments_complete=False)) == []
    assert jobs(pr(review_mode="none", draft=True)) == []


def test_required_or_unreadable_mode_keeps_review():
    for mode in ("non-author", None):
        assert jobs(pr(review_mode=mode)) == [("review", "new-head-unreviewed")]
    assert jobs(pr()) == [("review", "new-head-unreviewed")]


def test_review_mode_rows_fail_closed_like_st():
    assert pl.review_mode_from_rows([{"mode": "none", "node": "n"}]) == "none"
    assert pl.review_mode_from_rows([{"mode": {"value": "http://x/none"}}]) == "none"
    assert pl.review_mode_from_rows([]) == "non-author"
    assert pl.review_mode_from_rows([{"mode": "none"}, {"mode": "non-author"}]) == "non-author"
    assert pl.review_mode_from_rows([{"mode": "nnone"}]) == "non-author"
    assert pl.review_mode_from_rows(None) == "non-author"


def test_review_modes_read_once_per_repo_and_unreadable_is_none(capsys):
    cfg = pl.Config(repos=("intigraph/quipu", "intigraph/chaski"), quipu_url="http://quipu.svc")
    asked = []

    def ask(c, name, params):
        asked.append((name, params["repo"]))
        if params["repo"] == "intigraph/quipu":
            raise TimeoutError("slow")
        return [{"mode": "none"}]

    prs = [pr(repo="intigraph/chaski", number=1), pr(repo="intigraph/chaski", number=2),
           pr(repo="intigraph/quipu", number=3), pr(repo="intigraph/quipu", number=4, draft=True)]
    out = pl.add_review_modes(cfg, prs, ask=ask)
    assert sorted(asked) == [("repo_review_mode", "intigraph/chaski"), ("repo_review_mode", "intigraph/quipu")]
    assert [p.get("review_mode") for p in out] == ["none", "none", None, None]
    assert "review mode unreadable" in capsys.readouterr().err
    assert [pl.judge(cfg, p) for p in out] == [[("merge", "green-and-approved")]] * 2 + [
        [("review", "new-head-unreviewed")], []]
    # No quipu_url configured: no read, review required.
    assert pl.review_modes(CFG, ["intigraph/quipu"], ask=ask) == {"intigraph/quipu": None}


def test_ask_rejects_incomplete_answers(monkeypatch):
    import io

    cfg = pl.Config(repos=CFG.repos, quipu_url="http://quipu.svc")

    def answer(doc):
        class R(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False
        monkeypatch.setattr(pl.urllib.request, "urlopen", lambda req, timeout: R(json.dumps(doc).encode()))

    answer({"rows": [{"mode": "none"}], "count": 1, "truncated": False})
    assert pl._ask(cfg, "repo_review_mode", {"repo": "intigraph/quipu"}) == [{"mode": "none"}]
    for bad in ({"rows": [{"mode": "none"}], "count": 1, "truncated": True},
                {"rows": [{"mode": "none"}], "count": 2, "truncated": False},
                {"error": "no such query"}):
        answer(bad)
        with pytest.raises(ValueError):
            pl._ask(cfg, "repo_review_mode", {"repo": "intigraph/quipu"})


def test_config_reads_quipu_url_git_cache_and_git(tmp_path):
    f = tmp_path / "c.yaml"
    f.write_text("repos: [intigraph/quipu]\nquipu_url: http://quipu.svc\n"
                 "git_cache: /var/lib/chaski/git\ngit: [git, -c, core.x=1]\n")
    cfg = pl.load_config(f)
    assert (cfg.quipu_url, cfg.git_cache, cfg.git) == ("http://quipu.svc", "/var/lib/chaski/git",
                                                       ("git", "-c", "core.x=1"))
    assert pl.load_config(_write(tmp_path, "repos: [intigraph/quipu]\n")).quipu_url is None
    for bad in ("quipu_url: quipu.svc", "git_cache: relative/dir", "git: []"):
        with pytest.raises(pl.ConfigError):
            pl.load_config(_write(tmp_path, f"repos: [intigraph/quipu]\n{bad}\n"))


def _write(tmp_path, text):
    f = tmp_path / "x.yaml"
    f.write_text(text)
    return f
