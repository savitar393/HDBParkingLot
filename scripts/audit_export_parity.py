from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def audit(root: Path) -> dict:
    silver = str(root / 'silver/carpark_availability_parquet/**/*.parquet')
    gold = str(root / 'gold/carpark_features_parquet/**/*.parquet')
    con = duckdb.connect()
    try:
        sql = '''
        WITH s AS (
          SELECT hour_slot_sgt, carpark_number, occupancy_rate
          FROM read_parquet(?)
        ), g AS (
          SELECT hour_slot_sgt, carpark_number, occupancy_rate
          FROM read_parquet(?)
        )
        SELECT
          COUNT(*) FILTER (WHERE s.carpark_number IS NULL) AS missing_in_silver,
          COUNT(*) FILTER (WHERE g.carpark_number IS NULL) AS missing_in_gold,
          COUNT(*) FILTER (WHERE s.carpark_number IS NOT NULL AND g.carpark_number IS NOT NULL
                AND (s.occupancy_rate IS DISTINCT FROM g.occupancy_rate)) AS changed_occupancy,
          COUNT(*) FILTER (WHERE s.carpark_number IS NOT NULL AND g.carpark_number IS NOT NULL) AS matched_pairs
        FROM s FULL OUTER JOIN g
          ON s.hour_slot_sgt = g.hour_slot_sgt AND s.carpark_number = g.carpark_number
        '''
        a, b, c, matched = con.execute(sql, [silver, gold]).fetchone()
        dup_query = '''SELECT COUNT(*) FROM (
          SELECT hour_slot_sgt, carpark_number FROM read_parquet(?)
          GROUP BY hour_slot_sgt, carpark_number HAVING COUNT(*) > 1
        )'''
        dup_silver = con.execute(dup_query, [silver]).fetchone()[0]
        dup_gold = con.execute(dup_query, [gold]).fetchone()[0]
        result = {
            'missing_in_silver': int(a),
            'missing_in_gold': int(b),
            'changed_occupancy': int(c),
            'duplicate_silver_keys': int(dup_silver),
            'duplicate_gold_keys': int(dup_gold),
            'matched_pairs': int(matched),
            'status': 'match' if a == b == c == dup_silver == dup_gold == 0 else 'mismatch',
        }
        return result
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description='Audit exported Silver/Gold occupancy and key parity')
    parser.add_argument('--data-dir', default='data')
    args = parser.parse_args()
    result = audit(Path(args.data_dir))
    print(json.dumps(result, indent=2))
    if result['status'] != 'match':
        raise SystemExit('Parquet exports differ; stop before model retraining')


if __name__ == '__main__':
    main()
