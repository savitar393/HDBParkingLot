from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from hdb_parking.operations import check_bronze_health, collect_hours, expected_latest_slot
from hdb_parking.pipeline_check import assess_gold_freshness, overall_status
from hdb_parking.records import SGT


def latest_gold_hour(root: Path):
    import duckdb
    glob = str(root / 'gold/carpark_features_parquet/**/*.parquet')
    with duckdb.connect() as con:
        return con.execute('SELECT MAX(hour_slot_sgt) FROM read_parquet(?)', [glob]).fetchone()[0]


def run_checked(command: list[str]) -> None:
    print('RUN:', ' '.join(command), flush=True)
    subprocess.run(command, check=True)


def run(root: Path, *, lookback_hours: int, max_gold_lag_hours: float,
        dry_run: bool = False) -> dict:
    now_sgt = datetime.now(timezone.utc).astimezone(SGT)
    report: dict = {
        'mode': 'dry_run' if dry_run else 'manual_hourly_cycle',
        'checked_at_utc': datetime.now(timezone.utc).isoformat(),
        'latest_expected_slot_sgt': expected_latest_slot(now_sgt).isoformat(),
    }
    logs = root / 'quality/hourly_pipeline_runs.jsonl'
    try:
        collection = collect_hours(root, now_sgt=now_sgt, lookback_hours=lookback_hours,
                                   dry_run=dry_run)
        report['collection'] = collection
        sync_cmd = [sys.executable, 'scripts/sync_bronze_changes.py', '--data-dir', str(root),
                    '--dry-run' if dry_run else '--apply']
        if dry_run:
            run_checked(sync_cmd)
            report['status'] = 'dry_run_complete'
        else:
            # Process successfully collected files even if another hour failed to fetch.
            # The final cycle status still reports the collector failures.
            run_checked(sync_cmd)
            run_checked([sys.executable, 'scripts/audit_export_parity.py',
                         '--data-dir', str(root)])
            report['export_parity'] = 'match'
            bronze = check_bronze_health(root, now_sgt=datetime.now(timezone.utc).astimezone(SGT))
            report['bronze_health'] = bronze
            current_expected = expected_latest_slot(datetime.now(timezone.utc).astimezone(SGT))
            gold = assess_gold_freshness(latest_gold_hour(root), current_expected,
                                         max_lag_hours=max_gold_lag_hours)
            report['gold_health'] = gold
            report['status'] = overall_status(
                collection_failed=collection['failed'], bronze_status=bronze['status'],
                gold_status=gold['status'], export_status='match')
    except Exception as exc:
        report['status'] = 'failed'
        report['error_type'] = type(exc).__name__
        report['error'] = str(exc)[:500]
    if not dry_run:
        logs.parent.mkdir(parents=True, exist_ok=True)
        with logs.open('a', encoding='utf-8') as f:
            f.write(json.dumps(report, allow_nan=False) + '\n')
    print('HOURLY PIPELINE:', json.dumps(report, indent=2, allow_nan=False), flush=True)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description='One guarded HDB hourly ingestion + incremental sync cycle')
    parser.add_argument('--data-dir', default=os.getenv('HDB_DATA_DIR', 'data'))
    parser.add_argument('--lookback-hours', type=int, default=4)
    parser.add_argument('--max-gold-lag-hours', type=float, default=1.0)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if not 1 <= args.lookback_hours <= 72 or args.max_gold_lag_hours < 0:
        parser.error('lookback-hours must be 1..72 and max-gold-lag-hours >=0')
    report = run(Path(args.data_dir).resolve(), lookback_hours=args.lookback_hours,
                 max_gold_lag_hours=args.max_gold_lag_hours, dry_run=args.dry_run)
    if report['status'] not in {'healthy', 'dry_run_complete'}:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
