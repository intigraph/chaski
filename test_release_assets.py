"""release-assets route (aegis-7zkl): one judgment per release, page only on incomplete.

Facts are camayoc's recorded release v0.1.3 plus derived variants: the tag
push (refs/tags/v0.1.3) and the trigger check run on the tag's commit.
Required by the 7zkl ruling: one fact and at most one fix job per release id;
the check run and the published+backstop arm never both file; a replay
never files either; the kind is off by default.
"""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest

import github_routes as gr
import github_work as gw
from test_github_routes import REPO, ep, ts, variant

TAG = "v0.1.3"
RELEASE = "github.RE_kwDOTvaTnM4Y-A2D"
PUBLISHED = "2026-10-20T21:41:34Z"
TAG_SHA = "3" * 40
TRIGGER = "Trusted repository share"
ITEM = gr.item_key("release-assets", REPO, f"release/{TAG}", RELEASE)
BACKSTOP = ts(PUBLISHED) + 180 * 60


def tag_push(sha=TAG_SHA, ref=f"refs/tags/{TAG}"):
    return variant("push", "tag", {"commitSha": sha, "occurredAt": PUBLISHED}, subject="github.push.tag",
                   subject_props={"githubRef": ref, "githubAfterSha": sha})


def trigger_run(at="2026-10-20T22:21:34Z", sha=TAG_SHA, name=TRIGGER, conclusion="failure", tag="trigger"):
    return variant("check_run.completed", tag, {"commitSha": sha, "occurredAt": at, "githubConclusion": conclusion,
                                                "githubBaseRef": TAG},
                   subject=f"github.CR_{tag}", subject_props={"commitSha": sha, "githubName": name})


CHECKER = {"checker_argv": ["python3", "release_asset_check.py"], "release_argv": ["gh", "api"]}


def doc(enabled=True, **repo):
    r = {"enabled_kinds": ["release-assets"], "release_assets": {"trigger_check_run": TRIGGER}} if enabled else {}
    return {"version": 1, "graph": "https://camayoc.local/operational/github", "administrator": "hammond",
            "members": ["scbrown"], "window_days": 30, "repos": {REPO: {**r, **repo}},
            **({"release_assets": CHECKER} if enabled else {})}


def decide(episodes, now, cfg=None):
    cfg = cfg or gr.parse_config(doc())
    facts = gr.FixtureFacts(episodes, [], None)
    return {k: d for k, d in gr.evaluate_repo(cfg, cfg.repos[REPO], facts.snapshot(REPO, now), [], now).items()
            if d.kind == "release-assets"}


ALL = lambda: [ep("release.published"), tag_push(), trigger_run()]  # noqa: E731


# ---------------------------------------------------------------- off by default / config

def test_off_by_default_even_with_every_fact_present():
    cfg = gr.parse_config(doc(enabled=False))
    assert "release-assets" not in cfg.repos[REPO].enabled_kinds
    assert decide(ALL(), BACKSTOP + 1, cfg) == {}
    # the existing release jobs are unchanged by the new kind
    every = gr.evaluate_repo(cfg, cfg.repos[REPO], gr.FixtureFacts(ALL(), [], None).snapshot(REPO, ts(PUBLISHED) + 60),
                             [], ts(PUBLISHED) + 60)
    assert {d.kind for d in every.values()} == {"install", "announce"}


def test_enabling_requires_a_trigger_name_and_the_receiver_checker():
    with pytest.raises(gr.ConfigError, match="trigger_check_run"):
        gr.parse_config({**doc(), "repos": {REPO: {"enabled_kinds": ["release-assets"]}}})
    without_checker = doc()
    del without_checker["release_assets"]
    with pytest.raises(gr.ConfigError, match="release_assets"):
        gr.parse_config(without_checker)
    with pytest.raises(gr.ConfigError, match="argv"):
        gr.parse_config({**doc(), "release_assets": {"checker_argv": [], "release_argv": ["gh", "api"]}})


# ---------------------------------------------------------------- adapter

def test_waits_for_the_trigger_with_the_backstop_as_its_only_time_arm():
    d = decide([ep("release.published"), tag_push()], ts(PUBLISHED) + 600)[ITEM]
    assert d.verdict == gr.WAIT and d.next_check == BACKSTOP and d.facts["trigger"] is None


def test_trigger_check_run_on_the_tag_commit_makes_it_ready():
    d = decide(ALL(), ts(PUBLISHED) + 2401)[ITEM]
    assert d.verdict == gr.READY
    assert (d.facts["trigger"], d.facts["as_of"], d.facts["head_sha"]) == ("check-run", "2026-10-20T22:21:34Z",
                                                                           TAG_SHA)


@pytest.mark.parametrize("run", [
    trigger_run(sha="4" * 40, tag="othersha"),          # same name, not the tag's commit
    trigger_run(name="Verify repository pendant", tag="wrongname"),  # a name quipu never emits
])
def test_other_check_runs_never_trigger(run):
    d = decide([ep("release.published"), tag_push(), run], ts(PUBLISHED) + 3600)[ITEM]
    assert d.verdict == gr.WAIT


def test_a_branch_push_is_not_the_tag_commit():
    d = decide([ep("release.published"), tag_push(ref="refs/heads/main"), trigger_run()], ts(PUBLISHED) + 3600)
    assert d[ITEM].verdict == gr.WAIT


def test_backstop_without_a_trigger_is_ready():
    d = decide([ep("release.published")], BACKSTOP)[ITEM]
    assert d.verdict == gr.READY and d.facts["trigger"] == "backstop" and d.facts["as_of"] == gr.iso(BACKSTOP)


def test_check_run_backstop_and_both_are_one_request_id():
    by_run = decide(ALL(), ts(PUBLISHED) + 2401)
    by_backstop = decide([ep("release.published")], BACKSTOP + 1)
    late_both = decide(ALL() + [trigger_run(at="2026-10-21T01:00:00Z", conclusion="success", tag="rerun")],
                       BACKSTOP + 3600)
    assert set(by_run) == set(by_backstop) == set(late_both) == {ITEM}
    # the first observed trigger is the one recorded, whatever arrives later
    assert late_both[ITEM].facts["as_of"] == "2026-10-20T22:21:34Z"
    rids = {gr.record_for(gr.parse_config(doc()), d[ITEM])["event_id"] for d in (by_run, by_backstop, late_both)}
    assert rids == {gr.request_id(ITEM)}


def test_published_and_released_activities_are_one_release():
    released = variant("release.published", "released", {"githubEventKind": "release.released",
                                                         "occurredAt": "2026-10-20T21:41:40Z"})
    d = decide([ep("release.published"), released], ts(PUBLISHED) + 60)
    assert list(d) == [ITEM] and d[ITEM].next_check == BACKSTOP


def test_enabling_never_rejudges_releases_outside_the_window():
    assert decide(ALL(), ts(PUBLISHED) + 25 * 3600) == {}


# ---------------------------------------------------------------- receiver

class FakeChecker:
    def __init__(self, *codes):
        self.codes, self.calls = list(codes), []

    def check(self, req, now):
        self.calls.append((req["facts"]["trigger"], req["facts"]["as_of"]))
        code = self.codes.pop(0) if len(self.codes) > 1 else self.codes[0]
        if isinstance(code, Exception):
            raise code
        return code, f"fake exit {code}"


@pytest.fixture
def env(tmp_path):
    d = {**doc(), "ledger": str(tmp_path / "work.db"),
         "work_items": {"kind": "jsonl", "path": str(tmp_path / "items.jsonl")},
         "spawner": {"kind": "stub", "path": str(tmp_path / "spawns.jsonl")}}
    cfg = gr.parse_config(d)
    ledger = gw.Ledger(cfg.ledger)
    items, spawner = gw.make_ports(cfg)
    yield cfg, ledger, items, spawner, tmp_path
    ledger.close()


def event(cfg, episodes, now):
    adapter = gr.Adapter(cfg, gr.FixtureFacts(episodes, [], None))
    recs = [r for r in adapter.evaluate(adapter.discover(now), now)["records"]
            if r["verdict"] == gr.READY and r["work_kind"] == "release-assets"]
    assert len(recs) == 1
    r = recs[0]
    return {"event_id": r["event_id"], "item": r["item"], "evidence": r["evidence"]}


def lines(path):
    return [json.loads(x) for x in Path(path).read_text().splitlines() if x] if Path(path).exists() else []


def test_complete_files_nothing_and_a_replay_does_not_recheck(env):
    cfg, ledger, items, spawner, tmp = env
    ev = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    checker = FakeChecker(0)
    assert gw.deliver(ev, cfg, ledger, items, spawner, 1.0, checker=checker)[0] == 0
    code, msg = gw.deliver(ev, cfg, ledger, items, spawner, 2.0, checker=checker)
    assert code == 0 and "duplicate" in msg
    assert len(checker.calls) == 1
    assert lines(tmp / "items.jsonl") == [] and lines(tmp / "spawns.jsonl") == []


def test_incomplete_files_one_item_and_one_job_across_trigger_backstop_and_replay(env):
    cfg, ledger, items, spawner, tmp = env
    by_run = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    by_backstop = event(cfg, [ep("release.published")], BACKSTOP + 1)
    assert by_run["event_id"] == by_backstop["event_id"] and by_run["evidence"] != by_backstop["evidence"]
    checker = FakeChecker(3)
    for i, ev in enumerate((by_run, by_run, by_backstop, by_backstop)):
        assert gw.deliver(ev, cfg, ledger, items, spawner, 10.0 + i, checker=checker)[0] == 0
    assert checker.calls == [("check-run", "2026-10-20T22:21:34Z")]  # ONE judgment, ONE fact
    filed, spawned = lines(tmp / "items.jsonl"), lines(tmp / "spawns.jsonl")
    assert len(filed) == 1 and len(spawned) == 1
    argv = spawned[0]["argv"]
    assert argv[argv.index("--job") + 1] == "task" and argv[argv.index("--repo") + 1] == REPO
    assert "[release-assets]" in filed[0]["title"] and TAG in filed[0]["title"]


def test_a_refused_spawn_retries_the_job_without_rejudging(env):
    cfg, ledger, items, _, tmp = env

    class Refusing:
        def spawn(self, req, work_item):
            return False, "cap"

    ev = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    checker = FakeChecker(3, 0)  # a re-run checker would now say complete: it must not be asked
    assert gw.deliver(ev, cfg, ledger, items, Refusing(), 1.0, checker=checker)[0] == gw.EX_TEMPFAIL
    _, spawner = gw.make_ports(cfg)
    assert gw.deliver(ev, cfg, ledger, items, spawner, 2.0, checker=checker)[0] == 0
    assert len(checker.calls) == 1
    assert len(lines(tmp / "items.jsonl")) == 1 and len(lines(tmp / "spawns.jsonl")) == 1


def test_quarantined_is_unknown_and_never_pages(env):
    cfg, ledger, items, spawner, tmp = env
    ev = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    checker = FakeChecker(2)
    code, msg = gw.deliver(ev, cfg, ledger, items, spawner, 1.0, checker=checker)
    assert code == 0 and "UNKNOWN" in msg
    assert gw.deliver(ev, cfg, ledger, items, spawner, 2.0, checker=checker)[0] == 0
    assert len(checker.calls) == 1 and lines(tmp / "spawns.jsonl") == []
    assert ledger.get(gr.request_id(ITEM))["state"] == "unknown"


@pytest.mark.parametrize("first", [4, 1, OSError("gh down"), gw.Refused("release read failed")])
def test_too_early_or_failed_checks_retry_then_judge_once(env, first):
    cfg, ledger, items, spawner, tmp = env
    ev = event(cfg, [ep("release.published")], BACKSTOP + 1)
    checker = FakeChecker(first, 3)
    assert gw.deliver(ev, cfg, ledger, items, spawner, 1.0, checker=checker)[0] == gw.EX_TEMPFAIL
    assert lines(tmp / "items.jsonl") == []
    assert gw.deliver(ev, cfg, ledger, items, spawner, 2.0, checker=checker)[0] == 0
    assert len(checker.calls) == 2 and len(lines(tmp / "spawns.jsonl")) == 1


def test_no_checker_configured_never_files(env):
    cfg, ledger, items, spawner, tmp = env
    ev = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    assert gw.deliver(ev, cfg, ledger, items, spawner, 1.0, checker=None)[0] == gw.EX_TEMPFAIL
    assert lines(tmp / "items.jsonl") == []


def test_a_request_naming_another_release_is_refused(env):
    cfg, ledger, items, spawner, _ = env
    ev = event(cfg, ALL(), ts(PUBLISHED) + 2401)
    req = json.loads(ev["evidence"])
    req["facts"]["tag"] = "v9.9.9"
    with pytest.raises(gw.RequestError, match="its own release"):
        gw.deliver(dict(ev, evidence=json.dumps(req)), cfg, ledger, items, spawner, 1.0, checker=FakeChecker(3))
    req["facts"]["tag"], req["facts"]["trigger"] = TAG, "timer"
    with pytest.raises(gw.RequestError, match="trigger"):
        gw.deliver(dict(ev, evidence=json.dumps(req)), cfg, ledger, items, spawner, 1.0, checker=FakeChecker(3))


def test_kind_disabled_at_the_receiver_is_refused(tmp_path):
    on = gr.parse_config({**doc(), "ledger": str(tmp_path / "w.db")})
    ev = event(on, ALL(), ts(PUBLISHED) + 2401)
    off = gr.parse_config({**doc(enabled=False), "ledger": str(tmp_path / "w.db")})
    ledger = gw.Ledger(off.ledger)
    try:
        with pytest.raises(gw.RequestError, match="disabled"):
            gw.deliver(ev, off, ledger, None, None, 1.0, checker=FakeChecker(3))
    finally:
        ledger.close()


# ---------------------------------------------------------------- the checker port's exact calls

def _recorder(path: Path, log: Path, stdout: str, code: int) -> Path:
    path.write_text(f"#!{sys.executable}\nimport json, sys\n"
                    f"open({str(log)!r}, 'a').write(json.dumps(sys.argv[1:]) + '\\n')\n"
                    f"sys.stdout.write({stdout!r})\nsys.exit({code})\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return path


@pytest.mark.parametrize("trigger", ["check-run", "backstop"])
def test_asset_checker_reads_the_release_once_and_posts_one_judgment(tmp_path, trigger):
    log = tmp_path / "calls.jsonl"
    gh = _recorder(tmp_path / "gh", log, json.dumps({"tag_name": TAG}), 0)
    chk = _recorder(tmp_path / "chk", log, "{}", 3)
    port = gw.AssetChecker({"checker_argv": [str(chk)], "release_argv": [str(gh), "api"]}, "hammond")
    cfg = gr.parse_config(doc())
    ev = event(cfg, ALL() if trigger == "check-run" else [ep("release.published")],
               ts(PUBLISHED) + 2401 if trigger == "check-run" else BACKSTOP + 1)
    req = gw.parse_request(ev, cfg)
    assert port.check(req, 1234.5)[0] == 3
    read, judged = lines(log)
    assert read == ["api", f"repos/{REPO}/releases/tags/{TAG}"]
    assert judged[:2] == ["--repo", REPO] and judged[-1] == "--post"
    assert judged[judged.index("--actor") + 1] == "hammond"
    assert judged[judged.index("--trigger") + 1] == trigger
    if trigger == "check-run":
        assert judged[judged.index("--as-of") + 1] == "2026-10-20T22:21:34Z" and "--now" not in judged
    else:
        assert judged[judged.index("--now") + 1] == "1234.5" and "--as-of" not in judged
    assert not os.path.exists(judged[judged.index("--release-json") + 1])  # temp file removed


def test_a_failed_release_read_never_runs_the_checker(tmp_path):
    log = tmp_path / "calls.jsonl"
    gh = _recorder(tmp_path / "gh", log, "", 1)
    chk = _recorder(tmp_path / "chk", log, "{}", 3)
    port = gw.AssetChecker({"checker_argv": [str(chk)], "release_argv": [str(gh), "api"]}, "hammond")
    cfg = gr.parse_config(doc())
    req = gw.parse_request(event(cfg, ALL(), ts(PUBLISHED) + 2401), cfg)
    with pytest.raises(gw.Refused):
        port.check(req, 1.0)
    assert len(lines(log)) == 1
