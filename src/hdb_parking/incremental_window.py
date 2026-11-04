from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

HOUR_FILE_RE = re.compile(r"^(\d{8}T\d{6})_SGT\.json$")


def parse_slot_sgt(text: str) -> datetime:
    """Parse an exact local SGT hour; reject offsets and non-hourly values."""
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None or any((value.minute, value.second, value.microsecond)):
        raise ValueError("Use timezone-naive exact Singapore hour: YYYY-MM-DDTHH:00:00")
    return value


def slot_from_file(path: Path) -> datetime:
    match = HOUR_FILE_RE.fullmatch(path.name)
    if match is None:
        raise ValueError(f"Not an hourly Bronze file: {path}")
    result = datetime.strptime(match.group(1), "%Y%m%dT%H%M%S")
    if result.minute or result.second:
        raise ValueError(f"Not an exact-hour Bronze file: {path}")
    return result


@dataclass(frozen=True)
class RefreshPlan:
    first_slot: datetime
    last_slot: datetime
    affected_gold_start: datetime
    affected_gold_end: datetime
    source_context_start: datetime
    source_context_end: datetime


def plan_refresh(first_slot: datetime, last_slot: datetime) -> RefreshPlan:
    """A changed observation at t affects Gold t-1..t+168.

    To recompute output at t+168 we need an additional 168 hours of source
    history and one future hour for its target; bounds are intentionally broad.
    """
    if first_slot > last_slot:
        raise ValueError("Start slot must precede or equal end slot")
    for value in (first_slot, last_slot):
        if value.tzinfo is not None or any((value.minute, value.second, value.microsecond)):
            raise ValueError("Expected naive, exact SGT hour")
    output_start = first_slot - timedelta(hours=1)
    output_end = last_slot + timedelta(hours=168)
    return RefreshPlan(
        first_slot=first_slot,
        last_slot=last_slot,
        affected_gold_start=output_start,
        affected_gold_end=output_end,
        source_context_start=output_start - timedelta(hours=168),
        source_context_end=output_end + timedelta(hours=1),
    )


def select_hour_files(bronze: Path, start: datetime, last: datetime) -> list[Path]:
    if start > last:
        raise ValueError("Start after end")
    selected = []
    for path in bronze.rglob("*_SGT.json"):
        slot = slot_from_file(path)
        if start <= slot <= last:
            selected.append(path)
    # The requested window is a contiguous sequence; missing files are an error
    # rather than silently shifting lag features or producing incomplete updates.
    expected = int((last - start).total_seconds() // 3600) + 1
    if len(selected) != expected:
        present = {slot_from_file(p) for p in selected}
        absent = [start + timedelta(hours=i) for i in range(expected)
                  if start + timedelta(hours=i) not in present]
        raise FileNotFoundError(f"Expected {expected} hourly Bronze files, found {len(selected)}; "
                                f"missing SGT: {absent[:12]}")
    selected.sort(key=slot_from_file)
    return selected
