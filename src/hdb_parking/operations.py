from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

from hdb_parking.bronze import snapshot_path, write_snapshot
from hdb_parking.cli import fetch_hour_with_fallback, log_recovery
from hdb_parking.client import make_session
from hdb_parking.records import SGT, as_sgt

SLOT_RE = re.compile(r"^(\d{8}T\d{6})_SGT\.json$")


def expected_latest_slot(now_sgt: datetime, *, settle_minutes: int = 10) -> datetime:
    """At HH:10 or later collect HH:00; earlier, collect the previous hour.

    Ten minutes gives the historical API time to publish data near the hour.
    """
    if not 0 <= settle_minutes <= 59:
        raise ValueError("settle_minutes must be between 0 and 59")
    if now_sgt.tzinfo is None:
        raise ValueError("now_sgt must be timezone-aware")
    local = now_sgt.astimezone(SGT)
    slot = local.replace(minute=0, second=0, microsecond=0)
    return slot if local.minute >= settle_minutes else slot - timedelta(hours=1)


def requested_slots(now_sgt: datetime, *, lookback_hours: int = 4,
                    settle_minutes: int = 10) -> list[datetime]:
    if not 1 <= lookback_hours <= 72:
        raise ValueError("lookback_hours must be 1..72")
    latest = expected_latest_slot(now_sgt, settle_minutes=settle_minutes)
    return [latest - timedelta(hours=i) for i in reversed(range(lookback_hours))]


def slot_from_path(path: Path) -> datetime | None:
    m = SLOT_RE.fullmatch(path.name)
    if m is None:
        return None
    try:
        slot = datetime.strptime(m.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=SGT)
    except ValueError:
        return None
    if slot.minute or slot.second:
        return None
    return slot


def audit_event(root: Path, entry: dict) -> None:
    path = root / "quality/hourly_collection_events.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False, default=str, allow_nan=False) + "\n")


def collect_hours(
    root: Path, *, now_sgt: datetime, lookback_hours: int = 4,
    settle_minutes: int = 10, delay_seconds: float = 2.0, dry_run: bool = False,
    session_factory: Callable = make_session,
    fetcher: Callable = fetch_hour_with_fallback,
) -> dict:
    """Collect recent hourly slots; never overwrite or fabricate observations.

    Single-job only: do not run overlapping collector instances. Failures are
    individually audited and retried on subsequent runs while in lookback.
    """
    if delay_seconds < 0:
        raise ValueError("delay_seconds cannot be negative")
    slots = requested_slots(now_sgt, lookback_hours=lookback_hours,
                            settle_minutes=settle_minutes)
    session = None if dry_run else session_factory()
    result = {"requested": len(slots), "saved": 0, "existing": 0, "failed": 0,
              "dry_run": dry_run, "latest_expected_slot_sgt": slots[-1].isoformat()}
    for index, slot in enumerate(slots):
        path = snapshot_path(root, slot)
        if path.exists():
            # A truncated file must NOT be silently treated as a good snapshot.
            try:
                saved = json.loads(path.read_text(encoding="utf-8"))
                if not saved.get("items") or not saved["items"][0].get("timestamp"):
                    raise ValueError("missing items/timestamp")
            except (OSError, ValueError, TypeError, AttributeError) as exc:
                result["failed"] += 1
                entry = {"slot_sgt": slot.isoformat(), "status": "corrupt_existing", "error": str(exc)}
                print("ERROR " + json.dumps(entry), flush=True)
                if not dry_run:
                    audit_event(root, entry)
            else:
                result["existing"] += 1
                print(f"SKIP existing {slot.isoformat()}", flush=True)
            continue
        if dry_run:
            print(f"DRY RUN hourly slot {slot.isoformat()} -> {path}", flush=True)
            continue
        try:
            document, fallback = fetcher(session, slot)
            items = document.get("items")
            if not isinstance(items, list) or not items:
                raise ValueError("Empty or malformed items array")
            actual = as_sgt(items[0]["timestamp"])
            drift = abs((actual - slot).total_seconds()) / 60
            if drift > 10:
                raise ValueError(f"Source timestamp drift {drift:.2f} min exceeds 10 min")
            _, created = write_snapshot(root, slot, document)
            if not created:
                result["existing"] += 1
                continue
            result["saved"] += 1
            if fallback is not None:
                log_recovery(root / "quality/recovered_snapshots.csv",
                             requested=slot, retry=fallback, actual=actual)
            entry = {"slot_sgt": slot.isoformat(), "status": "saved",
                     "source_timestamp_sgt": actual.isoformat(), "drift_minutes": round(drift, 3),
                     "recovered": fallback is not None, "carparks": len(items[0].get("carpark_data", []))}
            audit_event(root, entry)
            print("SAVED " + json.dumps(entry), flush=True)
        except Exception as exc:
            # Each failed hour remains missing; the next scheduled run retries it.
            result["failed"] += 1
            entry = {"slot_sgt": slot.isoformat(), "status": "failed",
                     "error_type": type(exc).__name__, "error": str(exc)[:350]}
            audit_event(root, entry)
            print("ERROR " + json.dumps(entry), flush=True)
        if index < len(slots) - 1 and delay_seconds:
            time.sleep(delay_seconds)
    print("SUMMARY " + json.dumps(result), flush=True)
    return result


def check_bronze_health(
    root: Path, *, now_sgt: datetime, settle_minutes: int = 10,
    max_age_minutes: float = 90.0, min_24h_coverage: float = 0.8,
) -> dict:
    """Read-only health check over Bronze hourly snapshots, NOT Silver/Gold."""
    if max_age_minutes <= 0 or not (0 <= min_24h_coverage <= 1):
        raise ValueError("Invalid freshness/coverage thresholds")
    files = list((root / "bronze/carpark_availability").rglob("*.json"))
    slots = {s: p for p in files if (s := slot_from_path(p)) is not None}
    latest_expected = expected_latest_slot(now_sgt, settle_minutes=settle_minutes)
    last24 = [latest_expected - timedelta(hours=i) for i in range(24)]
    missing = sorted([slot.isoformat() for slot in last24 if slot not in slots])
    coverage = 1.0 - len(missing) / 24
    newest_slot = max(slots) if slots else None
    age = None
    source_ts = None
    warning = None
    if newest_slot is not None:
        try:
            doc = json.loads(slots[newest_slot].read_text(encoding="utf-8"))
            source_ts = as_sgt(doc["items"][0]["timestamp"])
            age = (now_sgt.astimezone(SGT) - source_ts).total_seconds() / 60
        except (OSError, KeyError, ValueError, IndexError, TypeError) as exc:
            warning = f"Latest file unreadable or malformed: {exc}"
    fresh = age is not None and 0 <= age <= max_age_minutes
    status = "healthy" if fresh and coverage >= min_24h_coverage and warning is None else "degraded"
    return {"status": status, "scope": "bronze_hourly_only",
            "checked_at_sgt": now_sgt.astimezone(SGT).isoformat(),
            "latest_expected_slot_sgt": latest_expected.isoformat(),
            "latest_saved_slot_sgt": newest_slot.isoformat() if newest_slot else None,
            "latest_source_timestamp_sgt": source_ts.isoformat() if source_ts else None,
            "source_age_minutes": round(age, 2) if age is not None else None,
            "hourly_files": len(slots), "non_hourly_files": len(files) - len(slots),
            "last_24h_coverage": round(coverage, 4), "missing_last_24h_slots": missing,
            "warning": warning}
