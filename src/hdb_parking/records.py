"""Pure-Python data-contract validation used by tests and ingestion QA."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterator

SGT = timezone(timedelta(hours=8))


def as_sgt(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (dt.replace(tzinfo=SGT) if dt.tzinfo is None else dt.astimezone(SGT))


def parse_snapshot(document: dict, *, max_staleness_minutes: float = 15.0) -> Iterator[dict]:
    """Yield clean car-lot records. Keep raw Bronze as the audit trail for excluded rows."""
    for item in document.get("items", []):
        stamp = as_sgt(item["timestamp"])
        for carpark in item.get("carpark_data", []):
            carpark_number = carpark.get("carpark_number")
            if not carpark_number:
                continue
            try:
                updated = as_sgt(carpark["update_datetime"])
            except (KeyError, TypeError, ValueError):
                continue
            staleness = (stamp - updated).total_seconds() / 60
            if not (0 <= staleness <= max_staleness_minutes):
                continue
            for lot in carpark.get("carpark_info", []):
                if lot.get("lot_type") != "C":
                    continue
                try:
                    total = int(lot["total_lots"])
                    available = int(lot["lots_available"])
                except (KeyError, ValueError, TypeError):
                    continue
                if total <= 0 or not 0 <= available <= total:
                    continue
                yield {
                    "snapshot_timestamp": stamp.isoformat(),
                    "carpark_number": carpark_number,
                    "update_datetime": updated.isoformat(),
                    "total_lots": total,
                    "lots_available": available,
                    "occupancy_rate": 1.0 - available / total,
                    "staleness_minutes": staleness,
                }
