from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from hdb_parking.operations import check_bronze_health
from hdb_parking.records import as_sgt, SGT


def main() -> None:
    p = argparse.ArgumentParser(description="Check hourly Bronze freshness, not Gold readiness")
    p.add_argument("--data-dir", default=os.getenv("HDB_DATA_DIR", "data"))
    p.add_argument("--max-age-minutes", type=float, default=90)
    p.add_argument("--min-24h-coverage", type=float, default=0.8)
    p.add_argument("--as-of-sgt", help="Optional historical diagnostic time")
    p.add_argument("--no-fail", action="store_true", help="Print degraded health without exit code 1")
    args = p.parse_args()
    now = as_sgt(args.as_of_sgt) if args.as_of_sgt else datetime.now(timezone.utc).astimezone(SGT)
    result = check_bronze_health(Path(args.data_dir), now_sgt=now,
                                 max_age_minutes=args.max_age_minutes,
                                 min_24h_coverage=args.min_24h_coverage)
    print(json.dumps(result, indent=2))
    if result["status"] != "healthy" and not args.no_fail:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
