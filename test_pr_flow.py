"""PR-flow deadlines share the existing rules and queue named crew work."""
import copy
import json
from types import SimpleNamespace

import pytest

import github_routes as gr
import github_work as gw
from test_github_routes import BASE, GREEN_AT, HEAD, PR177, REPO, ep, ts, variant, verdict
from test_github_work import cfg_doc, events_for, lines

SHA = 'f' * 64


def cfg(tmp_path, crew=False):
    doc = cfg_doc(tmp_path, crew_reviews={'enabled': crew, 'sdk_sha256': SHA, 'reviewers': ['reviewer']})
    doc['repos'][REPO].update(review_after_minutes=120,
                             stale_prs={'enabled': True, 'approved_after_minutes': 120})
    return gr.parse_config(doc)


def snapshot(episodes, words=()):
    return gr.FixtureFacts(episodes, list(words), {PR177: 'author'}).snapshot(REPO, 0)


def test_green_review_and_approved_deadlines_are_two_hours(tmp_path):
    config = cfg(tmp_path)
    episodes = [ep(n) for n in BASE]
    snap = snapshot(episodes)
    start = ts(GREEN_AT)
    decisions = gr.evaluate_repo(config, config.repos[REPO], snap, [], start + 7199)
    review = decisions[gr.item_key('review', REPO, 'pr/177', HEAD)]
    assert review.verdict == gr.WAIT and review.next_check == start + 7200
    snap = snapshot(episodes, [verdict('reviewer')])
    approved = ts('2026-10-27T18:55:00Z')
    item = gr.item_key('merge-stalled', REPO, 'pr/177', HEAD)
    before = gr.evaluate_repo(config, config.repos[REPO], snap, [], approved + 7199)[item]
    assert before.verdict == gr.WAIT and before.next_check == approved + 7200
    after = gr.evaluate_repo(config, config.repos[REPO], snap, [], approved + 7200)[item]
    assert after.verdict == gr.READY and after.next_check is None
    summary = gr.flow_summary(config, {REPO: snap}, approved + 7200)
    measured = summary['records'][0]
    assert measured['first_review_at'] == gr.iso(approved)
    assert measured['observed_head_to_first_review_seconds'] >= 0
    assert gr.record_for(config, after)['event_id'] == gr.record_for(config, after)['event_id']


def test_closed_or_changed_head_retires_approved_deadline(tmp_path):
    config = cfg(tmp_path)
    episodes = [ep(n) for n in BASE]
    old_item = gr.item_key('merge-stalled', REPO, 'pr/177', HEAD)
    for extra in [ep('pull_request.closed'), variant('pull_request.opened', 'new-head',
            props={'commitSha': 'a' * 40, 'occurredAt': '2026-10-27T19:00:00Z'})]:
        facts = gr.FixtureFacts(episodes + [extra], [verdict('reviewer')], {PR177: 'author'})
        adapter = gr.Adapter(config, facts)
        assert adapter.evaluate([old_item], ts('2026-10-27T22:00:00Z'))['records'] == []


def test_conflict_routes_only_to_declared_author_and_suppresses_merge(tmp_path):
    config = cfg(tmp_path)
    conflict = variant('pull_request.opened', 'conflict', props={
        'githubMergeable': False, 'githubMergeableState': 'dirty',
        'githubBaseSha': 'b' * 40, 'occurredAt': '2026-10-27T19:00:00Z'})
    snap = snapshot([ep(n) for n in BASE] + [conflict], [verdict('reviewer')])
    decisions = gr.evaluate_repo(config, config.repos[REPO], snap, [], ts('2026-10-27T19:01:00Z'))
    assert {d.kind for d in decisions.values()} == {'conflict'}
    record = gr.record_for(config, next(iter(decisions.values())))
    event = {**record, 'evidence': record['evidence']}
    req = gw.parse_request(event, config)
    assert req['route'] == 'administrator' and req['assignee'] == 'author'
    forged = json.loads(record['evidence'])
    forged['assignee'] = 'someone-else'
    with pytest.raises(gw.RequestError):
        gw.parse_request({**event, 'evidence': json.dumps(forged)}, config)


def test_summary_is_qualified_and_missing_pr_coverage_is_unknown(tmp_path):
    config = cfg(tmp_path)
    snap = snapshot([ep(n) for n in BASE])
    view = gr.flow_summary(config, {REPO: snap}, ts(GREEN_AT) + 7200)
    assert view['counts'] == {'needs-review': 1} and view['oldest_age_seconds'] == 7200
    assert view['records'][0]['who_must_act'] == 'non-author-reviewer'
    assert view['metrics']['main_only_repushes'] is None
    assert view['metrics']['time_to_first_review_seconds'] is None
    empty = gr.flow_summary(config, {REPO: gr.Snapshot([])}, ts(GREEN_AT))
    assert empty['state'] == 'UNKNOWN' and empty['counts'] is None


def profile(now):
    def agent(name, **kw):
        return dict(name=name, state='up', work='idle', harness='codex', available=True,
                    assignment_checked=True, **kw)
    return dict(version=1, scope='review-routing', complete=True, routing_held=False,
                delivery_sdk_sha256=SHA, observed_at=gr.iso(now), quipu_reads=6, quipu_408s=0,
                agents=[agent('author'), agent('reviewer')])


def test_crew_review_queues_once_without_ephemeral_spawn(tmp_path, monkeypatch):
    config = cfg(tmp_path, crew=True)
    now = ts(GREEN_AT) + 7200
    event = events_for(config, [ep(n) for n in BASE], now, authors={PR177: 'author'})['review']
    calls = []
    def run(argv, **kw):
        calls.append(argv)
        assert argv[1:] == ['crew', '--review-routing', '--local', '--json']
        return SimpleNamespace(returncode=0, stdout=json.dumps(profile(now)))
    monkeypatch.setattr(gw, '_run', run)
    ledger = gw.Ledger(config.ledger)
    try:
        items, spawner = gw.make_ports(config)
        for _ in range(3):
            assert gw.deliver(event, config, ledger, items, spawner, now)[0] == 0
        records = lines(tmp_path / 'items.jsonl')
        assert len(records) == 1 and records[0]['request']['assignee'] == 'reviewer'
        assert ledger.recipient(event['event_id']) == 'reviewer'
        assert len(calls) == 1 and lines(tmp_path / 'spawns.jsonl') == []
    finally:
        ledger.close()


@pytest.mark.parametrize('change', [
    'stale', 'future', 'sdk', 'timeout', 'incomplete', 'busy', 'unchecked', 'duplicate'])
def test_unknown_profile_never_assigns_a_reviewer(tmp_path, change):
    now = ts(GREEN_AT) + 7200
    observation = copy.deepcopy(profile(now))
    if change == 'stale':
        observation['observed_at'] = gr.iso(now - 91)
    elif change == 'future':
        observation['observed_at'] = gr.iso(now + 1)
    elif change == 'sdk':
        observation['delivery_sdk_sha256'] = 'a' * 64
    elif change == 'timeout':
        observation['quipu_408s'] = 1
    elif change == 'incomplete':
        observation['complete'] = False
    elif change == 'busy':
        observation['agents'][1]['work'] = 'working'
    elif change == 'unchecked':
        observation['agents'][1]['assignment_checked'] = False
    elif change == 'duplicate':
        observation['agents'].append(observation['agents'][1])
    with pytest.raises(gw.Refused):
        gw.review_recipient(observation, 'author', SHA, now, 'a' * 64, ['reviewer'])


def test_projection_replaces_atomically_and_refuses_symlinks(tmp_path):
    target = tmp_path / 'export' / 'pr-flow.json'
    gr.write_brief(target, {'state': 'UNKNOWN'})
    gr.write_brief(target, {'state': 'observed'})
    assert json.loads(target.read_text()) == {'state': 'observed'}
    assert target.stat().st_mode & 0o777 == 0o600
    assert list(target.parent.glob('.pr-flow-*')) == []
    link = tmp_path / 'link'
    link.symlink_to(target)
    with pytest.raises(ValueError):
        gr.write_brief(link, {'state': 'UNKNOWN'})
    assert json.loads(target.read_text()) == {'state': 'observed'}


def test_named_crew_handoff_has_no_ephemeral_reporting_instruction(tmp_path):
    config = cfg(tmp_path, crew=True)
    event = events_for(config, [ep(n) for n in BASE], ts(GREEN_AT) + 7200,
                       authors={PR177: 'author'})['review']
    req = gw.parse_request(event, config)
    assert 'normal crew dispatch' in gw.item_body(req)
    assert 'st agent spawn' not in gw.item_body(req)
    bad = copy.deepcopy(req)
    bad['facts']['head_sha'] = 'a' * 40
    with pytest.raises(gw.RequestError):
        gw.parse_request({**event, 'evidence': json.dumps(bad)}, config)
