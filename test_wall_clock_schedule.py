"""UTC golden values and mutation controls for weekly civil-time deadlines."""
from dataclasses import replace
from datetime import datetime

import pytest

from wall_clock_schedule import WeeklyWallClock


def ts(text):
    return int(datetime.fromisoformat(text).timestamp())


def monday(**changes):
    values = dict(weekday=0, local_time='09:17', timezone='America/New_York',
                  effective_at=ts('2026-10-20T00:00:00+00:00'))
    return WeeklyWallClock(**(values | changes))


@pytest.mark.parametrize('now,slot,following', [
    ('2026-10-26T13:16:59+00:00', None, '2026-10-26T13:17:00+00:00'),
    ('2026-10-26T13:17:00+00:00', '2026-10-26T13:17:00+00:00', '2026-11-02T14:17:00+00:00'),
    # Both occurrences of 01:30 on fallback Sunday retain Monday's identity.
    ('2026-11-01T05:30:00+00:00', '2026-10-26T13:17:00+00:00', '2026-11-02T14:17:00+00:00'),
    ('2026-11-01T06:30:00+00:00', '2026-10-26T13:17:00+00:00', '2026-11-02T14:17:00+00:00'),
    ('2026-11-02T14:16:59+00:00', '2026-10-26T13:17:00+00:00', '2026-11-02T14:17:00+00:00'),
    ('2026-11-02T14:17:00+00:00', '2026-11-02T14:17:00+00:00', '2026-11-09T14:17:00+00:00'),
])
def test_pilot_fallback_golden(now, slot, following):
    window = monday().window(ts(now))
    assert window.scheduled_at == (None if slot is None else ts(slot))
    assert window.next_deadline == ts(following)


def test_per_run_advance_is_not_elapsed_week():
    schedule = monday()
    before = ts('2026-10-26T13:17:00+00:00')
    after = schedule.next_after(before)
    assert after == ts('2026-11-02T14:17:00+00:00')
    assert after - before == 608400  # seven days plus the fallback hour
    assert before + 604800 != after  # falsifies the fixed-interval substitution
    assert schedule.next_after(after) == ts('2026-11-09T14:17:00+00:00')


def test_spring_forward_keeps_monday_local_time():
    schedule = monday(effective_at=0)
    before = ts('2026-03-02T14:17:00+00:00')
    after = schedule.next_after(before)
    assert after == ts('2026-03-09T13:17:00+00:00')
    assert after - before == 601200


def test_activation_is_inclusive_and_never_catches_up():
    slot = ts('2026-11-02T14:17:00+00:00')
    assert monday(effective_at=slot).window(slot).scheduled_at == slot
    future = monday(effective_at=slot + 1)
    window = future.window(ts('2026-10-20T00:00:00+00:00'))
    assert window.scheduled_at is None
    assert window.next_deadline == ts('2026-11-09T14:17:00+00:00')
    late = monday().window(ts('2027-02-02T00:00:00+00:00'))
    assert late.scheduled_at == ts('2027-02-01T14:17:00+00:00')
    assert late.next_deadline == ts('2027-02-08T14:17:00+00:00')


def test_replay_and_clock_rollback_use_the_same_utc_slot():
    schedule = monday()
    now = ts('2026-11-02T14:17:00+00:00')
    assert schedule.window(now) == schedule.window(now + 100)
    earlier = schedule.window(ts('2026-11-01T06:30:00+00:00'))
    assert earlier.scheduled_at == ts('2026-10-26T13:17:00+00:00')
    # Durable receiver/outbox dedupe, not this calculator, prevents re-execution.
    assert earlier.next_deadline == now


@pytest.mark.parametrize('day,local_time', [
    ('2026-11-01T08:00:00+00:00', '01:30'),
    ('2026-03-08T08:00:00+00:00', '02:30'),
])
def test_gap_and_fold_refuse_without_fabricating_deadline(day, local_time):
    schedule = monday(weekday=6, local_time=local_time, effective_at=0)
    with pytest.raises(ValueError, match='ambiguous or nonexistent'):
        schedule.window(ts(day))


@pytest.mark.parametrize('changes', [
    {'weekday': True}, {'weekday': -1}, {'weekday': 7}, {'weekday': 1.0},
    {'local_time': '9:17'}, {'local_time': '24:00'}, {'local_time': '09:60'},
    {'local_time': '09:17:00'}, {'local_time': '09:17\n'},
    {'timezone': '../UTC'}, {'timezone': 'Missing/Zone'}, {'timezone': ''},
    {'effective_at': True}, {'effective_at': -1}, {'effective_at': 1.0},
])
def test_invalid_pin_mutations_refuse(changes):
    with pytest.raises(ValueError):
        monday(**changes)


@pytest.mark.parametrize('now', [True, -1, float('nan'), float('inf'), '0', None])
def test_unknown_clock_refuses(now):
    with pytest.raises(ValueError):
        monday().window(now)


@pytest.mark.parametrize('slot', [True, 1.0, 0, ts('2026-11-02T14:16:59+00:00')])
def test_noncanonical_or_pre_activation_slot_refuses(slot):
    with pytest.raises(ValueError):
        monday().next_after(slot)


def test_zone_and_local_time_changes_change_deadline():
    schedule = monday()
    now = ts('2026-10-25T00:00:00+00:00')
    assert replace(schedule, timezone='UTC').window(now).next_deadline == ts('2026-10-26T09:17:00+00:00')
    assert replace(schedule, local_time='10:17').window(now).next_deadline == ts('2026-10-26T14:17:00+00:00')


def test_actual_heap_and_outbox_keep_one_identity_per_calendar_slot(tmp_path):
    from deadline_heap import DeadlineHeap
    from emitter import Emitter, connect, observe
    from scheduled_receiver import ExecutionScope, run_id

    schedule = monday()
    scope = ExecutionScope('urn:example:weekly-job', 'r1', 'urn:example:host',
                           'a' * 64, 'verifier', 'urn:example:workflow')
    first = ts('2026-10-26T13:17:00+00:00')
    following = ts('2026-11-02T14:17:00+00:00')
    heap = DeadlineHeap()
    heap.set(scope.job, schedule.window(first - 1).next_deadline)
    assert list(heap.pop_due(first - 1)) == []
    assert list(heap.pop_due(first)) == [(scope.job, first)]
    heap.set(scope.job, schedule.next_after(first))
    # The old +604800 deadline must not wake an hour early after fallback.
    assert list(heap.pop_due(first + 604800)) == []
    assert heap.peek() == following
    assert list(heap.pop_due(following)) == [(scope.job, following)]
    emitter = Emitter('weekly-fixture', ['unused'], 60, 'owner',
                      event='scheduled', baseline_emits=True)
    conn = connect(tmp_path / 'outbox.sqlite')
    try:
        for now in (first, first + 1, following, following + 1):
            slot = schedule.window(now).scheduled_at
            observe(conn, emitter, [dict(item=scope.job, owner='owner',
                    verdict='UNBLOCKED', event_id=run_id(scope, slot))],
                    now, scope={scope.job})
        rows = conn.execute('SELECT event_id,delivered_at FROM outbox ORDER BY created').fetchall()
        assert rows == [(run_id(scope, first), None), (run_id(scope, following), None)]
        # No delivery receipt is invented merely by advancing the next deadline.
    finally:
        conn.close()


def test_supported_time_bounds_and_overflow_are_explicit():
    assert monday(effective_at=0).window(0).scheduled_at is None
    with pytest.raises(ValueError):
        monday().window(253402300799)
