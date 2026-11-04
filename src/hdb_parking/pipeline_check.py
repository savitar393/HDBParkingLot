from __future__ import annotations

from datetime import datetime, timedelta, timezone


def assess_gold_freshness(
    latest_gold_utc: datetime | None,
    expected_slot_sgt: datetime,
    *,
    max_lag_hours: float = 1.0,
) -> dict:
    """Validate that Gold has caught up to the most recent expected SGT slot.

    One missing hourly slot is tolerated by default, for genuine source gaps.
    A timestamp ahead of the expected slot is suspicious and rejected.
    """
    if max_lag_hours < 0 or expected_slot_sgt.tzinfo is None:
        raise ValueError('max_lag_hours must be nonnegative; expected slot must be timezone-aware')
    if latest_gold_utc is None:
        return {'status': 'degraded', 'latest_gold_utc': None, 'lag_hours': None,
                'reason': 'Gold has no records'}
    if latest_gold_utc.tzinfo is not None:
        gold_utc = latest_gold_utc.astimezone(timezone.utc)
    else:
        gold_utc = latest_gold_utc.replace(tzinfo=timezone.utc)
    wanted_utc = expected_slot_sgt.astimezone(timezone.utc)
    lag = (wanted_utc - gold_utc).total_seconds() / 3600.0
    valid = -0.01 <= lag <= max_lag_hours
    return {
        'status': 'healthy' if valid else 'degraded',
        'latest_gold_utc': gold_utc.isoformat().replace('+00:00', 'Z'),
        'expected_gold_slot_utc': wanted_utc.isoformat().replace('+00:00', 'Z'),
        'lag_hours': round(lag, 4),
        'reason': None if valid else 'Gold is ahead of or too far behind the expected slot',
    }


def overall_status(*, collection_failed: int, bronze_status: str,
                   gold_status: str, export_status: str) -> str:
    return ('healthy' if collection_failed == 0 and bronze_status == 'healthy' and
            gold_status == 'healthy' and export_status == 'match' else 'degraded')
