from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import json
from zoneinfo import ZoneInfo

import pytest

from emitter import Emitter, connect, observe
from scheduled_projection import MAX_TIME, ScheduledProjection
from scheduled_receiver import ExecutionScope, run_id


@pytest.fixture
def projection():
    scope = ExecutionScope('urn:example:job', 'r1', 'urn:example:host', 'a'*64,
                           'verifier', 'urn:example:workflow')
    return ScheduledProjection(scope, 'urn:example:catalogue', 'urn:example:manifest',
                               'operator', 600, 600, 'UTC', 1250)


def catalogue(p):
    return {'schema_version': 1, 'source': 'declarations_only', 'graph': p.graph,
            'membership': {'status': 'required_identities_verified', 'required': [p.scope.job]},
            'executable_admission': 'UNKNOWN', 'signed_run_status': 'UNKNOWN',
            'firing_eligibility': 'UNKNOWN', 'jobs': [{
                'iri': p.scope.job, 'name': 'fixture', 'revision': p.scope.revision,
                'owner': p.owner, 'enabled': True, 'host': p.scope.host, 'target': p.target,
                'event': 'urn:example:event', 'schedule': 'urn:example:schedule',
                'interval_s': p.interval_s, 'anchor_s': p.anchor_s, 'timezone': p.timezone}]}


def test_boundaries_and_no_historical_catchup(projection):
    p = projection
    for now in (0, 600, 1249, 1250, 1799.999):
        out = p.evaluate(catalogue(p), now)
        assert out['records'][0]['verdict'] == 'BLOCKED'
        assert out['next_checks'] == {p.scope.job: 1800}
    for now, slot in ((1800, 1800), (2399.999, 1800), (2400, 2400), (600000, 600000)):
        out = p.evaluate(catalogue(p), now)
        assert len(out['records']) == 1
        assert out['records'][0]['event_id'] == run_id(p.scope, slot)
        assert out['next_checks'] == {p.scope.job: slot+600}


def test_actual_emitter_deduplicates_slot_and_advances_without_receipt(tmp_path, projection):
    p = projection
    conn = connect(tmp_path/'outbox.sqlite')
    e = Emitter('scheduled-fixture', ['unused'], 60, p.owner,
                event='scheduled', baseline_emits=True)
    try:
        for now in (1801, 1802, 2401):
            out = p.evaluate(catalogue(p), now)
            observe(conn, e, out['records'], now, scope=set(out['scope']))
        rows = conn.execute('SELECT event_id,payload,delivered_at FROM outbox ORDER BY created').fetchall()
        assert [r[0] for r in rows] == [run_id(p.scope, 1800), run_id(p.scope, 2400)]
        assert all(json.loads(r[1])['recipient'] == p.owner and r[2] is None for r in rows)
        before = conn.execute('SELECT * FROM verdicts').fetchall()
        broken = catalogue(p)
        broken['jobs'] = []
        with pytest.raises(ValueError):
            out = p.evaluate(broken, 3001)
            observe(conn, e, out['records'], 3001, scope=set(out['scope']))
        assert conn.execute('SELECT * FROM verdicts').fetchall() == before
        assert conn.execute('SELECT COUNT(*) FROM outbox').fetchone()[0] == 2
    finally:
        conn.close()


@pytest.mark.parametrize('field,value', [
    ('revision', 'other'), ('host', 'urn:example:foreign'), ('target', 'urn:example:other'),
    ('owner', 'other'), ('anchor_s', True), ('anchor_s', 0), ('interval_s', True),
    ('interval_s', 60), ('timezone', 'Europe/London'), ('enabled', 1), ('event', ''),
    ('schedule', None), ('name', ''), ('iri', 'urn:example:missing'),
])
def test_changed_declaration_refuses(projection, field, value):
    data = catalogue(projection)
    data['jobs'][0][field] = value
    with pytest.raises(ValueError):
        projection.evaluate(data, 1800)


@pytest.mark.parametrize('mutation', [
    lambda d: d.update(schema_version=True), lambda d: d.update(source='local_toml'),
    lambda d: d.update(graph='urn:example:other'), lambda d: d.update(extra=True),
    lambda d: d.update(executable_admission='ADMITTED'), lambda d: d.update(jobs=[]),
    lambda d: d.update(jobs=d['jobs']*2), lambda d: d.update(jobs=[None]),
    lambda d: d.update(membership={'status': 'UNKNOWN', 'required': []}),
    lambda d: d['membership'].update(required=[]),
    lambda d: d['membership'].update(required=['urn:example:missing']),
    lambda d: d['membership'].update(required=d['membership']['required']*2),
    lambda d: d['jobs'][0].update(extra=True),
])
def test_failed_membership_and_protocol_never_mean_empty_healthy(projection, mutation):
    data = deepcopy(catalogue(projection))
    mutation(data)
    with pytest.raises(ValueError):
        projection.evaluate(data, 1800)


def test_disabled_is_explicit_not_missing(projection):
    data = catalogue(projection)
    data['jobs'][0]['enabled'] = False
    out = projection.evaluate(data, 600000)
    assert out['records'] == [{'item': projection.scope.job, 'owner': projection.owner,
                               'verdict': 'BLOCKED'}]
    assert out['next_checks'] == {projection.scope.job: None}


def test_fixed_elapsed_intervals_across_dst_and_many_anchors(projection):
    for anchor in (0, 17, 1700000000):
        for interval in (1, 7, 600, 86400):
            p = replace(projection, anchor_s=anchor, interval_s=interval,
                        effective_at=anchor+interval+1, timezone='America/New_York')
            for k in range(2, 20):
                now = anchor+k*interval
                out = p.evaluate(catalogue(p), now)
                assert out['records'][0]['event_id'] == run_id(p.scope, now)
                assert out['next_checks'][p.scope.job] == now+interval


def test_dst_transition_preserves_elapsed_seconds_not_wall_clock(projection):
    zone = ZoneInfo('America/New_York')
    anchor = int(datetime(2023, 11, 4, 12, tzinfo=zone).timestamp())
    p = replace(projection, anchor_s=anchor, interval_s=86400,
                effective_at=anchor, timezone=zone.key)
    slot = anchor+86400
    assert datetime.fromtimestamp(slot, zone).hour == 11  # fall-back day
    out = p.evaluate(catalogue(p), slot)
    assert out['records'][0]['event_id'] == run_id(p.scope, slot)
    assert out['next_checks'][p.scope.job] == slot+86400


@pytest.mark.parametrize('now', [-1, True, float('nan'), float('inf'), MAX_TIME+1, '1800'])
def test_clock_bounds(projection, now):
    with pytest.raises(ValueError):
        projection.evaluate(catalogue(projection), now)


@pytest.mark.parametrize('field,value', [
    ('anchor_s', True), ('anchor_s', -1), ('effective_at', MAX_TIME+1),
    ('interval_s', 0), ('interval_s', True), ('interval_s', MAX_TIME+1),
    ('timezone', 'Not/AZone'), ('timezone', '/etc/passwd'), ('owner', ''), ('owner', 'x'*257),
])
def test_pinned_configuration_bounds(projection, field, value):
    with pytest.raises(ValueError):
        replace(projection, **{field: value})


def test_last_representable_slot_has_no_overflowing_deadline(projection):
    p = replace(projection, anchor_s=MAX_TIME, interval_s=600, effective_at=MAX_TIME)
    out = p.evaluate(catalogue(p), MAX_TIME)
    assert out['records'][0]['event_id'] == run_id(p.scope, MAX_TIME)
    assert out['next_checks'] == {p.scope.job: None}
