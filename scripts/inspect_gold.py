from pathlib import Path

import duckdb

sql = Path("sql/inspect_gold.sql").read_text(encoding="utf-8")
con = duckdb.connect()
cursor = con.execute(sql)
fields = [item[0] for item in cursor.description]
values = cursor.fetchone()
for name, value in zip(fields, values):
    print(f"{name}: {value}")

src = "data/gold/carpark_features_parquet/**/*.parquet"
issues = con.execute(
    """
    SELECT COUNT(*) FILTER (
               WHERE date_part('minute', hour_slot_sgt) != 0
                  OR date_part('second', hour_slot_sgt) != 0
           ) AS non_hourly_slots,
           COUNT(*) FILTER (WHERE target_occupancy_1h < 0 OR target_occupancy_1h > 1) AS bad_targets,
           COUNT(*) FILTER (WHERE occupancy_rate < 0 OR occupancy_rate > 1) AS bad_rates
    FROM read_parquet(?)
    """, [src]
).fetchone()
print(f"Quality problems (non_hourly, bad_target, bad_rate): {issues}")

duplicates = con.execute(
    """SELECT COUNT(*) FROM (
        SELECT carpark_number, hour_slot_sgt
        FROM read_parquet(?)
        GROUP BY carpark_number, hour_slot_sgt
        HAVING COUNT(*) > 1
    )""", [src]
).fetchone()[0]
print(f"Duplicate carpark/hour keys: {duplicates}")
if duplicates or any(issues):
    raise SystemExit("Gold checks failed; inspect the tables before training")
