from __future__ import annotations

import argparse
import json
import time

from hdb_parking.client import AVAILABILITY_URL, make_session

DEFAULT_DATES = [
    "2026-10-01T00:00:00",
    "2026-10-01T00:05:00",
    "2026-10-01T08:00:00",
    "2026-09-17T08:00:00",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare HDB historical/current API responses")
    parser.add_argument("--date", action="append", help="SGT YYYY-MM-DDTHH:MM:SS; repeatable")
    parser.add_argument("--no-current", action="store_true")
    args = parser.parse_args()
    dates = args.date if args.date else DEFAULT_DATES
    if not args.no_current:
        dates += [None]
    session = make_session()
    for i, date in enumerate(dates):
        if i:
            time.sleep(2)
        try:
            r = session.get(
                AVAILABILITY_URL,
                params={"date_time": date} if date is not None else None,
                timeout=35,
            )
            print(f"\nRequest: {date or 'CURRENT'} | HTTP {r.status_code}", flush=True)
            r.raise_for_status()
            body = r.json()
            if not isinstance(body, dict):
                print("Unexpected body type:", type(body).__name__)
                continue
            items = body.get("items")
            print("api_info:", json.dumps(body.get("api_info"), ensure_ascii=False))
            print("items_count:", len(items) if isinstance(items, list) else "MISSING")
            if isinstance(items, list) and items:
                item = items[0]
                print("returned_timestamp:", item.get("timestamp"))
                print("carparks:", len(item.get("carpark_data", [])))
            else:
                print("body_preview:", json.dumps(body, ensure_ascii=False)[:400])
        except Exception as exc:
            print(f"{type(exc).__name__}: {exc}", flush=True)


if __name__ == "__main__":
    main()
