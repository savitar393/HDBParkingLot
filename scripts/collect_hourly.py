from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

from hdb_parking.operations import collect_hours
from hdb_parking.records import as_sgt, SGT


def main() -> None:
    p = argparse.ArgumentParser(description="Collect near-hour HDB snapshots into Bronze")
    p.add_argument("--data-dir", default=os.getenv("HDB_DATA_DIR", "data"))
    p.add_argument("--lookback-hours", type=int, default=4,
                   help="Include most recent expected hour and prior 3 hours by default")
    p.add_argument("--settle-minutes", type=int, default=10,
                   help="Before HH:10 use previous hour as latest expected slot")
    p.add_argument("--delay", type=float, default=2.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--as-of-sgt", help="Override current time, for dry-run diagnosis only")
    args = p.parse_args()
    if args.as_of_sgt and not args.dry_run:
        p.error("--as-of-sgt requires --dry-run; use `hdb backfill` for historical collection")
    now = as_sgt(args.as_of_sgt) if args.as_of_sgt else datetime.now(timezone.utc).astimezone(SGT)
    summary = collect_hours(Path(args.data_dir), now_sgt=now,
                            lookback_hours=args.lookback_hours,
                            settle_minutes=args.settle_minutes,
                            delay_seconds=args.delay, dry_run=args.dry_run)
    if summary["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
