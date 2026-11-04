from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from hdb_parking.bronze_manifest import (
    SCHEMA_VERSION, compare_entries, file_fingerprint, group_contiguous,
    load_checkpoint, now_utc, save_checkpoint, snapshot, validate_changed_json,
)


def assert_initial_gold_current(root: Path, entries: dict[str, dict]) -> None:
    """Ensure latest observed Gold hour corresponds to latest Bronze before bootstrap.

    This is a coarse safety guard; users must also have run inspect_gold and
    confirmed successful full-refresh jobs first.
    """
    import duckdb
    gold = str(root / 'gold/carpark_features_parquet/**/*.parquet')
    try:
        with duckdb.connect() as con:
            n, latest = con.execute(
                'SELECT COUNT(*), MAX(hour_slot_sgt) FROM read_parquet(?)', [gold]
            ).fetchone()
    except Exception as exc:
        raise RuntimeError('Gold Parquet unavailable; complete Silver/Gold refresh first') from exc
    newest_sgt = max(datetime.fromisoformat(row['slot_sgt']) for row in entries.values())
    newest_utc = newest_sgt.replace(tzinfo=timezone.utc).timestamp() - 8 * 3600
    if n < 1 or latest is None or latest.replace(tzinfo=timezone.utc).timestamp() != newest_utc:
        raise RuntimeError(
            f'Latest Bronze slot is {newest_sgt} SGT but Gold latest is {latest} UTC. '
            'Sync Silver/Gold with a full refresh before bootstrapping.'
        )


def main() -> None:
    p = argparse.ArgumentParser(description='Manual Bronze change checkpoint; no scheduling')
    p.add_argument('--data-dir', default='data')
    actions = p.add_mutually_exclusive_group(required=True)
    actions.add_argument('--bootstrap-after-verified-full-refresh', action='store_true')
    actions.add_argument('--dry-run', action='store_true')
    actions.add_argument('--apply', action='store_true')
    p.add_argument('--max-batch-hours', type=int, default=24)
    args = p.parse_args()
    if not 1 <= args.max_batch_hours <= 168:
        p.error('max-batch-hours must be 1..168')
    root = Path(args.data_dir).resolve()
    bronze = root / 'bronze/carpark_availability'
    path = root / 'quality/bronze_sync_manifest.json'
    entries = snapshot(bronze)
    if args.bootstrap_after_verified_full_refresh:
        if path.exists():
            raise SystemExit(f'Checkpoint already exists at {path}; refusing to overwrite')
        assert_initial_gold_current(root, entries)
        save_checkpoint(path, {
            'schema_version': SCHEMA_VERSION,
            'bootstrapped_at_utc': now_utc(),
            'last_success_utc': now_utc(),
            'entries': entries,
        })
        print(f'BOOTSTRAPPED {len(entries)} already processed Bronze files into {path}')
        print('Important: bootstrap is an assertion of your previously verified full refresh.')
        return

    state = load_checkpoint(path)
    added, changed, deleted = compare_entries(state['entries'], entries)
    if deleted:
        raise SystemExit(
            f'ERROR: {len(deleted)} previously processed Bronze files are now missing '
            f'(first: {deleted[:3]}). Restore/reconcile them manually; no automatic deletion.'
        )
    selected = added + changed
    groups = group_contiguous(entries, selected, max_hours=args.max_batch_hours)
    print(f'BRONZE CHANGES: new={len(added)} modified={len(changed)} removed=0 batches={len(groups)}')
    for i, (start, n, keys) in enumerate(groups, 1):
        print(f'BATCH {i}: {start.isoformat()} SGT, {n} consecutive hour(s)')
    if args.dry_run:
        print('DRY RUN: no Spark jobs or checkpoint writes')
        return
    if not groups:
        print('NOOP: Bronze unchanged; no Spark job started')
        return
    validate_changed_json(bronze, selected)
    for i, (start, n, keys) in enumerate(groups, 1):
        command = [sys.executable, 'scripts/incremental_lakehouse.py',
                   '--from-sgt', start.isoformat(timespec='seconds'), '--hours', str(n)]
        print(f'APPLY BATCH {i}/{len(groups)}: {start.isoformat()} + {n}h', flush=True)
        subprocess.run(command, check=True)
        # Detect a changed file during the Spark job; don't mark its hash as processed.
        for key in keys:
            if file_fingerprint(bronze / key, bronze) != entries[key]:
                raise RuntimeError(f'Bronze changed while processing: {key}; rerun after source stabilizes')
        state['entries'].update({k: entries[k] for k in keys})
        state['last_success_utc'] = now_utc()
        save_checkpoint(path, state)
        print(f'CHECKPOINT SAVED: {len(keys)} files in batch {i}', flush=True)
    print(f'SYNC COMPLETE: {len(selected)} new/modified Bronze files processed')


if __name__ == '__main__':
    main()
