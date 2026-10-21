from copy import deepcopy
from dataclasses import asdict, replace
from datetime import datetime

import pytest

from scheduled_projection import ScheduledProjection
from scheduled_receiver import ExecutionScope, run_id
from test_scheduled_projection import catalogue as fixed_catalogue
from weekly_projection import WeeklyScheduledProjection


def ts(text):
    return int(datetime.fromisoformat(text).timestamp())


@pytest.fixture
def weekly():
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host',
                           'a'*64, 'verifier', 'urn:example:workflow')
    return WeeklyScheduledProjection(scope, 'urn:example:catalogue', 'urn:example:manifest',
            'operator', ts('2026-10-26T13:17:00+00:00'), 0, '09:17',
            'America/New_York', ts('2026-10-20T00:00:00+00:00'))


def catalogue(p):
    data = fixed_catalogue(ScheduledProjection(p.scope, p.graph, p.target, p.owner,
                                               p.anchor_s, 604800, p.timezone, p.effective_at))
    data['jobs'][0].update(interval_s=None, weekday=p.weekday, local_time=p.local_time)
    return data


def test_actual_weekly_projection_deadlines_and_stable_identity(weekly):
    p = weekly
    first, following = p.anchor_s, ts('2026-11-02T14:17:00+00:00')
    before = p.evaluate(catalogue(p), first - 1)
    assert before['records'][0]['verdict'] == 'BLOCKED'
    assert before['next_checks'] == {p.scope.job: first}
    for now in (first, first + 1, ts('2026-11-01T06:30:00+00:00')):
        out = p.evaluate(catalogue(p), now)
        assert out['records'][0]['event_id'] == run_id(p.scope, first)
        assert out['next_checks'] == {p.scope.job: following}
    assert p.evaluate(catalogue(p), following)['records'][0]['event_id'] == run_id(p.scope, following)


def test_disabled_stays_queryable_and_not_due(weekly):
    data = catalogue(weekly)
    data['jobs'][0]['enabled'] = False
    out = weekly.evaluate(data, weekly.anchor_s)
    assert out['records'] == [dict(item=weekly.scope.job, owner='operator', verdict='BLOCKED')]
    assert out['next_checks'] == {weekly.scope.job: None}


@pytest.mark.parametrize('field,value', [
    ('weekday', True), ('weekday', 1), ('local_time', '10:17'),
    ('interval_s', 604800), ('interval_s', False), ('timezone', 'UTC'),
    ('anchor_s', True), ('anchor_s', 0), ('enabled', 1), ('owner', 'foreign'),
    ('revision', 'r2'), ('host', 'urn:example:foreign'), ('target', 'urn:example:foreign'),
])
def test_pinned_declaration_mutations_refuse(weekly, field, value):
    data = catalogue(weekly)
    data['jobs'][0][field] = value
    with pytest.raises(ValueError):
        weekly.evaluate(data, weekly.anchor_s)


@pytest.mark.parametrize('change', [
    lambda d: d.update(graph='urn:example:foreign'), lambda d: d.update(jobs=[]),
    lambda d: d.update(jobs=d['jobs']*2),
    lambda d: d.update(firing_eligibility='ADMITTED'),
    lambda d: d['membership'].update(required=[]),
    lambda d: d['jobs'][0].pop('weekday'), lambda d: d['jobs'][0].update(extra=True),
])
def test_missing_membership_or_mixed_protocol_is_unknown(weekly, change):
    data = deepcopy(catalogue(weekly))
    change(data)
    with pytest.raises(ValueError):
        weekly.evaluate(data, weekly.anchor_s)


def test_fixed_and_weekly_contracts_cannot_substitute(weekly):
    p = weekly
    fixed = ScheduledProjection(p.scope, p.graph, p.target, p.owner,
                                p.anchor_s, 604800, p.timezone, p.effective_at)
    with pytest.raises(ValueError):
        fixed.evaluate(catalogue(p), p.anchor_s)
    with pytest.raises(ValueError):
        p.evaluate(fixed_catalogue(fixed), p.anchor_s)
    with pytest.raises(ValueError):
        replace(p, anchor_s=p.anchor_s + 60)


def test_adapter_v2_uses_actual_reader_process_and_advertises_calendar_changes(setup):
    # Reuse the process/owned-config fixture, replacing only the projection and
    # typed reader output with the explicit weekly protocol.
    _, config, _, report, counter, _, run = setup
    p = weekly.__wrapped__()
    import json
    config.update(version=2, projection=asdict(p))
    report.write_text(json.dumps(catalogue(p)))
    result = run()
    assert result.returncode == 0
    assert 'https://schema.org/byDay' in json.loads(result.stdout)['attributes']
    assert not counter.exists()
    result = run(dict(now=ts('2026-11-01T06:30:00+00:00'), items=[p.scope.job], changes=[]))
    assert result.returncode == 0
    out = json.loads(result.stdout)
    assert out['records'][0]['event_id'] == run_id(p.scope, p.anchor_s)
    assert out['next_checks'] == {p.scope.job: ts('2026-11-02T14:17:00+00:00')}
    assert counter.read_text() == '1'
    config['version'] = 1  # old mode must refuse the weekly object, not approximate
    result = run(dict(now=p.anchor_s, discover=True))
    assert result.returncode == 2 and not result.stdout and counter.read_text() == '1'


# This imported pytest fixture performs no work until requested by the test.
from test_scheduled_catalogue_adapter import setup
