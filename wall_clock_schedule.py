"""Weekly local-time arithmetic; no catalogue, admission, timers or writes.

The catalogue adapter must validate the pinned declaration before using these
deadlines. Errors are UNKNOWN, never permission to clear retained delivery state.
"""
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone as dt_timezone
import math
import re
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from scheduled_projection import MAX_TIME


@dataclass(frozen=True)
class WallClockWindow:
    scheduled_at: int | None
    next_deadline: int


@dataclass(frozen=True)
class WeeklyWallClock:
    """Monday=0 through Sunday=6, HH:MM local time, pinned IANA zone.

    Return only the latest eligible slot, never a historical catch-up list.
    effective_at is trusted activation policy, inclusive of a matching slot.
    Gaps and folds refuse rather than shifting time or selecting an occurrence.
    """
    weekday: int
    local_time: str
    timezone: str
    effective_at: int

    def __post_init__(self):
        if type(self.weekday) is not int or not 0 <= self.weekday <= 6:
            raise ValueError('weekday must be Monday=0 through Sunday=6')
        if (type(self.local_time) is not str or
                not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]', self.local_time)):
            raise ValueError('local time must be HH:MM')
        if type(self.timezone) is not str or not 0 < len(self.timezone) <= 255:
            raise ValueError('bounded IANA timezone required')
        try:
            ZoneInfo(self.timezone)
        except (ValueError, ZoneInfoNotFoundError) as exc:
            raise ValueError('valid IANA timezone required') from exc
        if type(self.effective_at) is not int or not 0 <= self.effective_at <= MAX_TIME:
            raise ValueError('bounded integer activation UTC time required')

    def _utc(self, day):
        hour, minute = map(int, self.local_time.split(':'))
        local = datetime.combine(day, time(hour, minute))
        zone = ZoneInfo(self.timezone)
        candidates = set()
        for fold in (0, 1):
            utc = local.replace(tzinfo=zone, fold=fold).astimezone(dt_timezone.utc)
            # Attaching ZoneInfo alone accepts imaginary local times. Require
            # UTC round-trip equality and deduplicate ordinary (non-fold) times.
            if utc.astimezone(zone).replace(tzinfo=None) == local:
                candidates.add(int(utc.timestamp()))
        if len(candidates) != 1:
            raise ValueError('ambiguous or nonexistent scheduled local time')
        return candidates.pop()

    def window(self, now: int | float) -> WallClockWindow:
        if (type(now) not in (int, float) or not 0 <= now <= MAX_TIME or
                not math.isfinite(now)):
            raise ValueError('bounded trusted current UTC time required')
        try:
            zone = ZoneInfo(self.timezone)
            day = datetime.fromtimestamp(now, zone).date()
            day -= timedelta(days=(day.weekday() - self.weekday) % 7)
            slot = self._utc(day)
            if slot > now:
                day -= timedelta(days=7)
                slot = self._utc(day)
            following = self._utc(day + timedelta(days=7))
            if following < self.effective_at:
                # Jump directly to activation; no scanning all intervening weeks.
                future = datetime.fromtimestamp(self.effective_at, zone).date()
                future += timedelta(days=(self.weekday - future.weekday()) % 7)
                following = self._utc(future)
                if following < self.effective_at:
                    following = self._utc(future + timedelta(days=7))
            if not 0 <= following <= MAX_TIME:
                raise ValueError('next UTC deadline is outside supported range')
        except (OverflowError, OSError) as exc:
            raise ValueError('calendar deadline is outside supported range') from exc
        return WallClockWindow(slot if slot >= self.effective_at else None, following)

    def next_after(self, scheduled_at: int) -> int:
        """Advance an exact canonical run slot using local dates, not +604800."""
        if type(scheduled_at) is not int:
            raise ValueError('exact integer UTC run slot required')
        result = self.window(scheduled_at)
        if result.scheduled_at != scheduled_at:
            raise ValueError('run slot does not match the pinned wall-clock schedule')
        return result.next_deadline
