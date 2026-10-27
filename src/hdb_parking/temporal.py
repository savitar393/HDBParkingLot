from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone

SGT = timezone(timedelta(hours=8))
_SLOT_PATTERN = re.compile(r"(?P<stamp>\d{8}T\d{6})_SGT\.json$")


def requested_slot_from_path(file_path: str) -> datetime:
    """Read the *requested* time (not the potentially early/late API time)."""
    match = _SLOT_PATTERN.search(file_path)
    if match is None:
        raise ValueError(f"Missing hourly request slot in Bronze filename: {file_path}")
    return datetime.strptime(match.group("stamp"), "%Y%m%dT%H%M%S").replace(tzinfo=SGT)


def acceptable_observation_time(actual: datetime, requested: datetime, max_minutes: float = 10) -> bool:
    """Fail closed on very late/early responses; don't fabricate an observation."""
    if actual.tzinfo is None or requested.tzinfo is None:
        raise ValueError("Both timestamps must be timezone-aware")
    return abs((actual - requested).total_seconds()) <= max_minutes * 60


def exact_hour_before(when: datetime, hours: int) -> datetime:
    """A true lag requires the exact prior hourly slot, never the prior existing row."""
    if hours < 1 or when.tzinfo is None:
        raise ValueError("hours must be positive and when timezone-aware")
    return when - timedelta(hours=hours)
