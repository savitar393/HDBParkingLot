from __future__ import annotations

import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta
from pathlib import Path

from hdb_parking.bronze import snapshot_path, write_snapshot
from hdb_parking.client import EmptyAvailabilityError, fetch_availability, fetch_metadata_page, make_session
from hdb_parking.records import SGT, as_sgt, parse_snapshot


def parse_local_sgt(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    if value.tzinfo is not None:
        return value.astimezone(SGT)
    return value.replace(tzinfo=SGT)


def get_data_root(path: str | None) -> Path:
    return Path(path or os.getenv("HDB_DATA_DIR", "data")).resolve()


def fetch_hour_with_fallback(session, requested: datetime, retry_minutes: int = 5) -> tuple[dict, datetime | None]:
    """Retry an empty exact-hour API response at a nearby timestamp.

    Keep the ORIGINAL requested hour as the Bronze file key. The JSON preserves
    the actual response timestamp. Reject a fallback that is too far from the
    requested time rather than silently substituting an unrelated observation.
    """
    try:
        return fetch_availability(session, requested), None
    except EmptyAvailabilityError as original:
        if retry_minutes <= 0:
            raise
        fallback_time = requested + timedelta(minutes=retry_minutes)
        try:
            doc = fetch_availability(session, fallback_time)
        except EmptyAvailabilityError as second:
            raise EmptyAvailabilityError(f"{original}; fallback {fallback_time.isoformat()} also empty: {second}") from second
        actual = as_sgt(doc["items"][0]["timestamp"])
        delta_minutes = abs((actual - requested).total_seconds()) / 60
        if delta_minutes > 10:
            raise EmptyAvailabilityError(
                f"Original hour {requested.isoformat()} was empty; fallback query at "
                f"{fallback_time.isoformat()} returned {actual.isoformat()} "
                f"({delta_minutes:.1f} minutes away; max 10)"
            ) from original
        return doc, fallback_time


def log_recovery(path: Path, *, requested: datetime, retry: datetime, actual: datetime) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as fp:
        writer = csv.writer(fp)
        if write_header:
            writer.writerow(["requested_hour_sgt", "retry_query_sgt", "returned_timestamp_sgt"])
        writer.writerow([requested.isoformat(), retry.isoformat(), actual.isoformat()])


def run_backfill(args: argparse.Namespace) -> None:
    root = get_data_root(args.data_dir)
    start = parse_local_sgt(args.start)
    session = make_session()
    saved = skipped = missing = consecutive_missing = 0
    missing_file = root / "quality" / "missing_snapshots.csv"
    for index in range(args.hours):
        when = start + timedelta(hours=index)
        path = snapshot_path(root, when)
        if path.exists():
            skipped += 1
            consecutive_missing = 0
            print(f"SKIP existing {path}", flush=True)
            continue
        if args.dry_run:
            print(f"DRY RUN GET date_time={when.strftime('%Y-%m-%dT%H:%M:%S')} -> {path}")
            continue
        try:
            doc, fallback_time = fetch_hour_with_fallback(
                session, when, retry_minutes=getattr(args, "empty_retry_minutes", 5)
            )
        except EmptyAvailabilityError as exc:
            missing += 1
            consecutive_missing += 1
            missing_file.parent.mkdir(parents=True, exist_ok=True)
            new_file = not missing_file.exists() or missing_file.stat().st_size == 0
            with missing_file.open("a", encoding="utf-8", newline="") as fp:
                writer = csv.writer(fp)
                if new_file:
                    writer.writerow(["requested_at_sgt", "problem"])
                writer.writerow([when.isoformat(), str(exc)])
            print(f"WARNING: {exc} (logged: {missing_file})", flush=True)
            if consecutive_missing >= args.max_consecutive_empty:
                raise SystemExit(
                    f"Stopped after {consecutive_missing} consecutive empty API responses. "
                    "Run `python scripts/probe_availability.py` to compare dates and current data. "
                    "No values were fabricated for missing hours."
                ) from exc
        else:
            consecutive_missing = 0
            _, created = write_snapshot(root, when, doc)
            saved += int(created)
            extra = ""
            if fallback_time is not None:
                actual = as_sgt(doc["items"][0]["timestamp"])
                log_recovery(
                    root / "quality/recovered_snapshots.csv",
                    requested=when, retry=fallback_time, actual=actual,
                )
                extra = f" | recovered via {fallback_time.isoformat()} (actual={actual.isoformat()})"
            print(f"SAVED {when.isoformat()} ({len(doc['items'][0].get('carpark_data', []))} carparks){extra}", flush=True)
        if index + 1 < args.hours:
            time.sleep(args.delay)
    print(f"Completed: saved={saved}, already_present={skipped}, empty_hours={missing}, requested={args.hours}")
    if not args.dry_run and saved == 0 and skipped == 0:
        raise SystemExit("No availability snapshots were obtained. Inspect API probe output before Silver ETL.")


def run_current(args: argparse.Namespace) -> None:
    root = get_data_root(args.data_dir)
    session = make_session()
    doc = fetch_availability(session)
    stamp = parse_local_sgt(doc["items"][0]["timestamp"])
    path, created = write_snapshot(root, stamp, doc)
    print(f"{'SAVED' if created else 'SKIPPED'} {path}")


def run_metadata(args: argparse.Namespace) -> None:
    root = get_data_root(args.data_dir)
    session = make_session()
    offset = 0
    all_records: list[dict] = []
    while True:
        result = fetch_metadata_page(session, offset=offset, limit=args.page_size)
        rows = result["records"]
        all_records.extend(rows)
        offset += len(rows)
        total = int(result.get("total", offset))
        print(f"Metadata: fetched {offset}/{total} rows")
        if not rows or offset >= total:
            break
        time.sleep(args.delay)
    if not all_records:
        raise RuntimeError("Metadata download returned no records")
    root.joinpath("bronze/hdb_carpark_information").mkdir(parents=True, exist_ok=True)
    path = root / "bronze/hdb_carpark_information/metadata.json"
    path.write_text(json.dumps(all_records, ensure_ascii=False, indent=2), encoding="utf-8")
    csv_path = root / "bronze/hdb_carpark_information/metadata.csv"
    headers = list(dict.fromkeys(k for row in all_records for k in row if k != "_id"))
    with csv_path.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_records)
    print(f"SAVED {len(all_records)} metadata rows into {csv_path}")


def run_validate(args: argparse.Namespace) -> None:
    base = get_data_root(args.data_dir) / "bronze/carpark_availability"
    paths = sorted(base.rglob("*.json"))
    if not paths:
        raise SystemExit(f"No Bronze JSON under {base}. Run `hdb backfill` first.")
    valid = 0
    examples: set[str] = set()
    for path in paths:
        doc = json.loads(path.read_text(encoding="utf-8"))
        for record in parse_snapshot(doc):
            valid += 1
            examples.add(record["carpark_number"])
    print(f"Bronze snapshots: {len(paths)}")
    print(f"Valid car-lot observations after freshness/range filters: {valid}")
    print(f"Unique carparks: {len(examples)}")
    if valid == 0:
        raise SystemExit("Validation failed: zero clean observations")


def main() -> None:
    parser = argparse.ArgumentParser(description="HDB local-first collector and quality checks")
    parser.add_argument("--data-dir", help="Override HDB_DATA_DIR/local data root")
    subs = parser.add_subparsers(dest="command", required=True)
    backfill = subs.add_parser("backfill", help="Fetch hourly historical snapshots")
    backfill.add_argument("--start", required=True, help="YYYY-MM-DDTHH:MM:SS in Singapore time")
    backfill.add_argument("--hours", type=int, default=24, choices=range(1, 2001), metavar="N")
    backfill.add_argument("--delay", type=float, default=2.0)
    backfill.add_argument("--dry-run", action="store_true")
    backfill.add_argument("--empty-retry-minutes", type=int, choices=range(0, 11), default=5,
                          metavar="0..10", help="Retry empty hourly responses this many minutes later (default 5; 0 disables)")
    backfill.add_argument("--max-consecutive-empty", type=int, default=3, help="Stop after N empty API responses in a row (default 3)")
    subs.add_parser("current", help="Fetch and store one latest snapshot")
    meta = subs.add_parser("metadata", help="Fetch paginated HDB metadata")
    meta.add_argument("--page-size", type=int, default=500)
    meta.add_argument("--delay", type=float, default=3.0)
    subs.add_parser("validate", help="Validate all local Bronze records")
    args = parser.parse_args()
    if args.command == "backfill":
        run_backfill(args)
    elif args.command == "current":
        run_current(args)
    elif args.command == "metadata":
        run_metadata(args)
    elif args.command == "validate":
        run_validate(args)


if __name__ == "__main__":
    main()
