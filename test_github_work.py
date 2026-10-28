"""The work-request receiver: one item and one job per request, one merge lane per repo."""
from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest

import github_routes as gr
import github_work as gw
from command_sink import CommandSink
from emitter import connect, deliver_pending, observe
from test_github_routes import BASE, GREEN_AT, HEAD, PR177, REPO, emitter_, ep, second_pr, ts, verdict

HERE = Path(__file__).parent


def cfg_doc(tmp_path, **extra):
    return {"version": 1, "graph": "https://camayoc.local/operational/github", "administrator": "hammond",
            "members": ["scbrown"], "ledger": str(tmp_path / "work.db"),
            "repos": {REPO: {"merge": {"spacing_minutes": 10, "lease_minutes": 75}}},
            "work_items": {"kind": "jsonl", "path": str(tmp_path / "items.jsonl")},
            "spawner": {"kind": "stub", "path": str(tmp_path / "spawns.jsonl")}, **extra}


@pytest.fixture
def env(tmp_path):
    cfg = gr.parse_config(cfg_doc(tmp_path))
    ledger = gw.Ledger(cfg.ledger)
    items, spawner = gw.make_ports(cfg)
    yield cfg, ledger, items, spawner, tmp_path
    ledger.close()


def events_for(cfg, episodes, now, verdicts=(), authors=None):
    """READY requests exactly as chaski's emitter would deliver them."""
    adapter = gr.Adapter(cfg, gr.FixtureFacts(episodes, list(verdicts), authors))
    out = adapter.evaluate(adapter.discover(now), now)
    return {r["work_kind"]: {"event_id": r["event_id"], "item": r["item"], "evidence": r["evidence"],
                             "work_kind": r["work_kind"], "recipient": r["assignee"]}
            for r in out["records"] if r["verdict"] == gr.READY}


def lines(path):
    return [json.loads(x) for x in Path(path).read_text().splitlines() if x] if Path(path).exists() else []


def test_review_request_files_one_item_and_one_spawn_whatever_the_replays(env):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    for _ in range(3):
        code, _ = gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 3600)
        assert code == 0
    assert len(lines(tmp / "items.jsonl")) == 1
    spawns = lines(tmp / "spawns.jsonl")
    assert len(spawns) == 1
    assert spawns[0]["argv"] == ["st", "agent", "spawn", "--ephemeral", "--job", "review",
                                 "--item", "stub-" + ev["event_id"][:12], "--repo", REPO, "--pr", "177"]
    req = lines(tmp / "items.jsonl")[0]["request"]
    assert req["facts"]["head_sha"] == HEAD and req["schema"] == gr.SCHEMA


def test_crash_between_filing_and_ledger_finds_the_item_again(env):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    req = gw.parse_request(ev, cfg)
    items.create(req)  # filed, then "crashed" before the ledger saw it
    assert gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 3600)[0] == 0
    assert len(lines(tmp / "items.jsonl")) == 1


def test_tampered_request_is_refused(env):
    cfg, ledger, items, spawner, _ = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    forged = dict(ev, event_id="0" * 64)
    with pytest.raises(gw.RequestError):
        gw.deliver(forged, cfg, ledger, items, spawner, 0)
    req = json.loads(ev["evidence"])
    req["revision"] = "f" * 40
    with pytest.raises(gw.RequestError):
        gw.deliver(dict(ev, evidence=json.dumps(req)), cfg, ledger, items, spawner, 0)


def test_merge_takes_the_lane_and_a_second_merge_waits(env):
    cfg, ledger, items, spawner, tmp = env
    iri178, head178, extra = second_pr()
    verdicts = [verdict("sattler"), verdict("sattler", sha=head178, number=178, at="2026-10-27T18:55:30Z")]
    authors = {PR177: "lex", iri178: "lex"}
    now = ts("2026-10-27T18:56:00Z")
    ev177 = events_for(cfg, [ep(n) for n in BASE] + extra, now, verdicts, authors)["merge"]
    assert json.loads(ev177["evidence"])["facts"]["pr"] == 177
    assert gw.deliver(ev177, cfg, ledger, items, spawner, now)[0] == 0
    assert gw.deliver(ev177, cfg, ledger, items, spawner, now + 5)[0] == 0  # replay: no second turn
    assert [r["pr"] for r in ledger.lane_status(REPO)] == [177]

    # A merge request for 178 that slipped past the adapter's ordering is refused by the lock.
    other = ev177 | {}
    req = json.loads(other["evidence"])
    item178 = gr.item_key("merge", REPO, "pr/178", head178)
    req.update(item=item178, request_id=gr.request_id(item178), subject="pr/178", revision=head178)
    req["facts"].update(pr=178, head_sha=head178)
    other = {"event_id": gr.request_id(item178), "item": item178, "evidence": json.dumps(req)}
    code, msg = gw.deliver(other, cfg, ledger, items, spawner, now + 10)
    assert code == gw.EX_TEMPFAIL and "busy with PR #177" in msg

    # The recorded merge of 177 lands; the adapter now emits 178 with that completion attached.
    at_merge = ts("2026-10-27T18:56:18Z")
    facts = [ep(n) for n in BASE] + extra + [ep("pull_request.closed")]
    # cfg.ledger is set, so the adapter reads the receiver's lane table (read-only).
    ev178 = events_for(cfg, facts, at_merge + 601, verdicts, authors)["merge"]
    assert json.loads(ev178["evidence"])["facts"]["lane"]["completed"][0]["outcome"] == "merged"
    assert gw.deliver(ev178, cfg, ledger, items, spawner, at_merge + 601)[0] == 0
    status = {r["pr"]: r for r in ledger.lane_status(REPO)}
    assert status[177]["outcome"] == "merged" and status[177]["released_at"] == at_merge
    assert status[178]["released_at"] is None
    assert [s["argv"][5] for s in lines(tmp / "spawns.jsonl")] == ["merge", "merge"]


def test_receiver_enforces_spacing_even_if_the_request_is_early(env):
    cfg, ledger, items, spawner, _ = env
    iri178, head178, extra = second_pr()
    at_merge = ts("2026-10-27T18:56:18Z")
    v = [verdict("sattler", sha=head178, number=178, at="2026-10-27T18:56:00Z")]
    ev = events_for(cfg, extra + [ep("pull_request.closed")], at_merge + 601, v, {iri178: "lex"})["merge"]
    code, msg = gw.deliver(ev, cfg, ledger, items, spawner, at_merge + 300)  # delivered "early"
    assert code == gw.EX_TEMPFAIL and "spacing" in msg
    assert ledger.lane_status(REPO) == []


def test_refused_spawn_releases_the_lane_and_retries(env):
    cfg, ledger, items, _, tmp = env

    class Refusing:
        calls = 0

        def spawn(self, req, work_item):
            Refusing.calls += 1
            return False, "st exited 1: governor lane base is at its cap"
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})["merge"]
    code, msg = gw.deliver(ev, cfg, ledger, items, Refusing(), ts(GREEN_AT) + 600)
    assert code == gw.EX_TEMPFAIL and "cap" in msg
    assert ledger.lane_status(REPO) == []
    assert ledger.get(ev["event_id"])["state"] == "refused"
    _, spawner = gw.make_ports(cfg)
    assert gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 700)[0] == 0
    assert len(lines(tmp / "items.jsonl")) == 1  # the retry reused the filed item


def test_outside_contribution_is_routed_never_spawned(env):
    cfg, ledger, items, spawner, tmp = env
    from test_github_routes import variant
    outside = variant("pull_request.opened", "alice", subject_props={"githubAuthorLogin": "alice"})
    evs = events_for(cfg, [outside, ep("check_run.completed")], ts(GREEN_AT) + 600, [verdict("sattler")],
                     {PR177: "lex"})
    assert "merge" not in evs
    code, msg = gw.deliver(evs["outside-contribution"], cfg, ledger, items, spawner, ts(GREEN_AT) + 600)
    assert code == 0 and "routed" in msg
    assert lines(tmp / "spawns.jsonl") == [] and ledger.lane_status() == []


def test_a_forged_member_merge_for_an_outside_author_is_routed(env):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})["merge"]
    req = json.loads(ev["evidence"])
    req["facts"]["author"] = "mallory"
    code, msg = gw.deliver(dict(ev, evidence=json.dumps(req)), cfg, ledger, items, spawner, ts(GREEN_AT) + 600)
    assert code == 0 and "routed" in msg
    assert lines(tmp / "spawns.jsonl") == [] and ledger.lane_status() == []


def test_lane_release_cli_and_expiry(env, capsys):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})["merge"]
    gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 600)
    config_file = tmp / "c.yaml"
    config_file.write_text(json.dumps(cfg_doc(tmp)))
    assert gw.main(["--config", str(config_file), "lane", "release", "--request", ev["event_id"],
                    "--outcome", "blocked"]) == 0
    assert ledger.lane_status(REPO)[0]["outcome"] == "blocked"
    assert gw.main(["--config", str(config_file), "lane", "status", "--json"]) == 0
    assert json.loads(capsys.readouterr().out.split("released\n", 1)[1])[0]["outcome"] == "blocked"


def fake_sd(tmp_path):
    """An `sd` that keeps seeds in a JSON file: create --silent and list -l."""
    store = tmp_path / "seeds.json"
    script = tmp_path / "sd"
    script.write_text(f"""#!{sys.executable}
import json, sys
store = {str(store)!r}
try:
    seeds = json.load(open(store))
except OSError:
    seeds = []
a = sys.argv[1:]
if a[0] == "create":
    labels = a[a.index("-l") + 1].split(",")
    sid = "aegis-t%d" % (len(seeds) + 1)
    seeds.append({{"id": sid, "labels": labels, "title": a[a.index("--title") + 1],
                  "assignee": a[a.index("-a") + 1]}})
    json.dump(seeds, open(store, "w"))
    print(sid)
elif a[0] == "list":
    label = a[a.index("-l") + 1]
    print(json.dumps({{"issues": [s for s in seeds if label in s["labels"]]}}))
""")
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    return script, store


def test_seeds_items_find_before_create(tmp_path):
    script, store = fake_sd(tmp_path)
    doc = cfg_doc(tmp_path, work_items={"kind": "seeds", "argv_prefix": [str(script)], "labels": ["chaski-github"]})
    cfg = gr.parse_config(doc)
    ledger = gw.Ledger(cfg.ledger)
    items, spawner = gw.make_ports(cfg)
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    req = gw.parse_request(ev, cfg)
    assert items.find(req) is None
    wid = items.create(req)
    assert items.find(req) == wid
    ledger.close()
    ledger = gw.Ledger(tmp_path / "fresh.db")  # a lost ledger still cannot duplicate the item
    assert gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 3600)[0] == 0
    seeds = json.loads(store.read_text())
    assert len(seeds) == 1 and seeds[0]["assignee"] == "hammond"
    assert "job:review" in seeds[0]["labels"] and gw.dedupe_label(req["request_id"]) in seeds[0]["labels"]
    ledger.close()


def test_end_to_end_emitter_command_sink_receiver(tmp_path):
    """chaski's own emitter + command sink -> the receiver CLI, replayed."""
    config_file = tmp_path / "routing.yaml"
    config_file.write_text(json.dumps(cfg_doc(tmp_path)))
    cfg = gr.load_config(config_file)
    conn = connect(tmp_path / "emitter.db")
    e = emitter_()
    adapter = gr.Adapter(cfg, gr.FixtureFacts([ep(n) for n in BASE], [verdict("sattler")], {PR177: "lex"}))
    now = ts(GREEN_AT) + 600
    out = adapter.evaluate(adapter.discover(now), now)
    observe(conn, e, out["records"], now, scope=set(out["scope"]))
    env_path = os.pathsep.join([str(HERE), os.environ.get("PYTHONPATH", "")])
    sink = CommandSink(["env", f"PYTHONPATH={env_path}", sys.executable, str(HERE / "github_work.py"),
                        "--config", str(config_file), "deliver"])
    assert deliver_pending(conn, sink, now, e.label) == (1, 0)
    # A lost checkpoint re-sends the same event: the receiver deduplicates.
    conn.execute("UPDATE outbox SET delivered_at=NULL")
    assert deliver_pending(conn, sink, now + 1, e.label) == (1, 0)
    assert len(lines(tmp_path / "spawns.jsonl")) == 1
    assert len(lines(tmp_path / "items.jsonl")) == 1
    assert lines(tmp_path / "spawns.jsonl")[0]["argv"][5] == "merge"


def test_merge_handoff_pins_the_exact_head(env):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 600, [verdict("sattler")], {PR177: "lex"})["merge"]
    assert gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 600)[0] == 0
    argv = lines(tmp / "spawns.jsonl")[0]["argv"]
    assert argv[argv.index("--head") + 1] == HEAD and argv[argv.index("--job") + 1] == "merge"
    review = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    gw.deliver(review, cfg, ledger, items, spawner, ts(GREEN_AT) + 3600)
    assert "--head" not in lines(tmp / "spawns.jsonl")[1]["argv"]


def test_a_live_claim_from_another_delivery_defers_and_a_stale_one_lapses(env):
    cfg, ledger, items, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]
    other = gw.Ledger(cfg.ledger)  # a second process on the same ledger
    now = ts(GREEN_AT) + 3600
    other.claim(gw.parse_request(ev, cfg), now)
    code, msg = gw.deliver(ev, cfg, ledger, items, spawner, now + 1)
    assert code == gw.EX_TEMPFAIL and "another process" in msg
    assert lines(tmp / "items.jsonl") == []
    assert gw.deliver(ev, cfg, ledger, items, spawner, now + gw.CLAIM_TTL_S + 1)[0] == 0
    assert len(lines(tmp / "items.jsonl")) == 1
    other.close()


def test_concurrent_deliveries_of_one_request_file_one_item(env):
    import threading
    import time as _time
    cfg, _, _, spawner, tmp = env
    ev = events_for(cfg, [ep(n) for n in BASE], ts(GREEN_AT) + 3600)["review"]

    class SlowItems(gw.JsonlItems):
        def create(self, req):
            _time.sleep(0.3)  # widen the find-then-create window
            return super().create(req)
    items = SlowItems({"path": str(tmp / "items.jsonl")}, "hammond")
    codes = []

    def run():
        ledger = gw.Ledger(cfg.ledger)
        try:
            codes.append(gw.deliver(ev, cfg, ledger, items, spawner, ts(GREEN_AT) + 3600)[0])
        finally:
            ledger.close()
    threads = [threading.Thread(target=run) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(lines(tmp / "items.jsonl")) == 1 and len(lines(tmp / "spawns.jsonl")) == 1
    assert sorted(codes)[0] == 0 and set(codes) <= {0, gw.EX_TEMPFAIL}
