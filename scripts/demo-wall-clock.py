"""Synthetic weekly deadline demo; no graph, service, signing or job execution."""
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import subprocess
import sys

SOURCE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SOURCE))
from wall_clock_schedule import WeeklyWallClock


def ts(value):
    return int(datetime.fromisoformat(value).timestamp())


def utc(value):
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def main():
    base = 'fc3c017bdc91b3b08792577e5caf40b76f54904a'
    before = subprocess.run(['git', 'cat-file', '-e', base + ':wall_clock_schedule.py'],
                            cwd=SOURCE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    control = subprocess.run(['git', 'cat-file', '-e', base + ':scheduled_projection.py'],
                             cwd=SOURCE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    assert before.returncode != 0 and control.returncode == 0
    print('Baseline', base, ': wall-clock module absent; existing projection present')
    print('Candidate module SHA256:', hashlib.sha256(
        (SOURCE / 'wall_clock_schedule.py').read_bytes()).hexdigest())
    schedule = WeeklyWallClock(0, '09:17', 'America/New_York',
                                ts('2026-10-20T00:00:00+00:00'))
    first = ts('2026-10-26T13:17:00+00:00')
    following = schedule.next_after(first)
    assert following == ts('2026-11-02T14:17:00+00:00')
    print('Monday 09:17 America/New_York:', utc(first), '->', utc(following))
    print('Elapsed seconds:', following - first, '(not 604800)')
    for clock in ('2026-11-01T05:30:00+00:00', '2026-11-01T06:30:00+00:00'):
        window = schedule.window(ts(clock))
        assert window.scheduled_at == first and window.next_deadline == following
        print('Fallback clock', clock, ': same slot; next deadline', utc(window.next_deadline))
    try:
        WeeklyWallClock(6, '01:30', 'America/New_York', 0).window(
            ts('2026-11-01T08:00:00+00:00'))
    except ValueError:
        print('Ambiguous Sunday 01:30: refused; caller must retain UNKNOWN')
    else:
        raise AssertionError('ambiguous time was accepted')
    print('SOURCE ONLY: no calendar profile, keys, signed run, runtime or retirement')


if __name__ == '__main__':
    main()
