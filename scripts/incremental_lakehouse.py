from __future__ import annotations

import argparse
import os
from datetime import timedelta
from pathlib import Path

from hdb_parking.incremental_window import parse_slot_sgt, plan_refresh, select_hour_files


def ts_expr(F, value):
    # Explicitly parse SGT wall-clock literals using spark.sql.session.timeZone.
    return F.to_timestamp(F.lit(value.strftime("%Y-%m-%d %H:%M:%S")), "yyyy-MM-dd HH:mm:ss")


def silver_from_raw(spark, F, Window, paths: list[Path]):
    raw = (spark.read.option("multiLine", "true").json([str(p) for p in paths])
           .withColumn("bronze_file", F.input_file_name()))
    stamp_text = F.regexp_extract("bronze_file", r"(\d{8}T\d{6})_SGT\.json$", 1)
    raw = raw.withColumn("slot_text", stamp_text)
    if raw.where(F.col("slot_text") == "").limit(1).count():
        raise ValueError("Bronze filename missing the exact-hour SGT key")
    raw = raw.withColumn("hour_slot_sgt", F.to_timestamp("slot_text", "yyyyMMdd'T'HHmmss"))
    expanded = (
        raw.select("hour_slot_sgt", "bronze_file", F.explode("items").alias("item"))
        .select("hour_slot_sgt", "bronze_file", F.to_timestamp("item.timestamp").alias("snapshot_timestamp"),
                F.explode("item.carpark_data").alias("cp"))
        .select("hour_slot_sgt", "bronze_file", "snapshot_timestamp",
                F.col("cp.carpark_number").alias("carpark_number"),
                F.to_timestamp("cp.update_datetime").alias("update_datetime"),
                F.explode("cp.carpark_info").alias("info"))
        .where(F.col("info.lot_type") == "C")
        .select("hour_slot_sgt", "bronze_file", "snapshot_timestamp", "carpark_number", "update_datetime",
                F.col("info.total_lots").cast("integer").alias("total_lots"),
                F.col("info.lots_available").cast("integer").alias("lots_available"))
    )
    silver = (
        expanded.withColumn("staleness_minutes",
            (F.unix_timestamp("snapshot_timestamp") - F.unix_timestamp("update_datetime")) / F.lit(60.0))
        .withColumn("slot_drift_minutes",
            F.abs(F.unix_timestamp("snapshot_timestamp") - F.unix_timestamp("hour_slot_sgt")) / F.lit(60.0))
        .where((F.col("total_lots") > 0) & (F.col("lots_available") >= 0) &
               (F.col("lots_available") <= F.col("total_lots")) &
               (F.col("staleness_minutes") >= 0) & (F.col("staleness_minutes") <= 15) &
               (F.col("slot_drift_minutes") <= 10) &
               F.col("carpark_number").isNotNull() & F.col("hour_slot_sgt").isNotNull())
        .withColumn("occupancy_rate", 1.0 - F.col("lots_available") / F.col("total_lots"))
    )
    w = Window.partitionBy("hour_slot_sgt", "carpark_number").orderBy(
        F.col("update_datetime").desc(), F.col("snapshot_timestamp").desc())
    return silver.withColumn("rank", F.row_number().over(w)).where(F.col("rank") == 1).drop("rank")


def gold_from_silver(spark, F, Window, silver, metadata_file):
    # This mirrors the full-refresh gold_spark.py transformation. Range-based
    # windows are identical; the caller supplies 168h of history on each side.
    hourly = (
        silver.where((F.minute("hour_slot_sgt") == 0) & (F.second("hour_slot_sgt") == 0))
        .select("hour_slot_sgt", "carpark_number", "snapshot_timestamp", "total_lots",
                "lots_available", "occupancy_rate")
        .withColumn("slot_epoch", F.col("hour_slot_sgt").cast("long"))
    )
    w = Window.partitionBy("carpark_number").orderBy("slot_epoch")
    for h in (1, 2, 3, 24, 168):
        hourly = hourly.withColumn(
            f"lag_{h}h", F.max("occupancy_rate").over(w.rangeBetween(-3600 * h, -3600 * h)))
    for h in (3, 6):
        period = w.rangeBetween(-3600 * h, -1)
        hourly = hourly.withColumn(
            f"rolling_mean_{h}h", F.when(F.count("occupancy_rate").over(period) == h,
                                          F.avg("occupancy_rate").over(period)))
    period24 = w.rangeBetween(-3600 * 24, -1)
    hourly = hourly.withColumn(
        "rolling_std_24h", F.when(F.count("occupancy_rate").over(period24) == 24,
                                  F.stddev_samp("occupancy_rate").over(period24)))
    hourly = hourly.withColumn(
        "target_occupancy_1h", F.max("occupancy_rate").over(w.rangeBetween(3600, 3600)))
    gold = (
        hourly.withColumn("hour", F.hour("hour_slot_sgt"))
        .withColumn("day_of_week", F.pmod(F.dayofweek("hour_slot_sgt") + 5, 7))
        .withColumn("is_weekend", F.col("day_of_week") >= 5)
        .withColumn("occupancy_change_1h", F.col("occupancy_rate") - F.col("lag_1h"))
        .drop("slot_epoch")
    )
    if metadata_file.exists():
        meta = spark.read.option("header", True).csv(str(metadata_file))
        if "car_park_no" in meta.columns:
            chosen = ["car_park_no", "address", "car_park_type", "type_of_parking_system",
                      "car_park_decks", "night_parking", "free_parking", "x_coord", "y_coord"]
            meta = meta.select(*(c for c in chosen if c in meta.columns)).withColumnRenamed(
                "car_park_no", "carpark_number")
            if "car_park_decks" in meta.columns:
                meta = meta.withColumn("car_park_decks", F.col("car_park_decks").cast("int"))
            for coordinate in ("x_coord", "y_coord"):
                if coordinate in meta.columns:
                    meta = meta.withColumn(coordinate, F.col(coordinate).cast("double"))
            gold = gold.join(F.broadcast(meta.dropDuplicates(["carpark_number"])),
                             on="carpark_number", how="left")
        else:
            print("WARNING: metadata.csv lacks car_park_no; keeping no metadata columns")
    else:
        print("WARNING: metadata.csv missing; Gold generated without metadata")
    return gold


def merge_window(DeltaTable, spark, F, *, path: Path, rows, start, end):
    """Idempotent MERGE, including deletes for replaced Bronze files.

    The not-matched-by-source condition is bound to explicitly requested
    hourly boundaries; it cannot delete any other Gold/Silver history.
    """
    target = DeltaTable.forPath(spark, str(path))
    cond = ("t.hour_slot_sgt >= TIMESTAMP '" + start.strftime("%Y-%m-%d %H:%M:%S") +
            "' AND t.hour_slot_sgt <= TIMESTAMP '" + end.strftime("%Y-%m-%d %H:%M:%S") + "'")
    # Source is explicitly restricted to the affected bounds too.
    source = rows.where((F.col("hour_slot_sgt") >= ts_expr(F, start)) &
                        (F.col("hour_slot_sgt") <= ts_expr(F, end)))
    (target.alias("t")
     .merge(source.alias("s"),
            "t.hour_slot_sgt = s.hour_slot_sgt AND t.carpark_number = s.carpark_number")
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .whenNotMatchedBySourceDelete(condition=cond)
     .execute())


def main():
    parser = argparse.ArgumentParser(description="Experimental incremental Delta Lake refresh")
    parser.add_argument("--data-dir", default=os.getenv("HDB_DATA_DIR", "data"))
    parser.add_argument("--from-sgt", required=True, dest="from_sgt",
                        help="First exact Singapore hour, e.g. 2026-11-02T06:00:00")
    parser.add_argument("--hours", type=int, required=True,
                        help="Contiguous number of hours to reprocess (max 168 per run)")
    parser.add_argument("--dry-run", action="store_true", help="Show paths and impact without changing Delta")
    args = parser.parse_args()
    if not 1 <= args.hours <= 168:
        parser.error("--hours must be between 1 and 168")
    start = parse_slot_sgt(args.from_sgt)
    last = start + timedelta(hours=args.hours - 1)
    root = Path(args.data_dir).resolve()
    paths = select_hour_files(root / "bronze/carpark_availability", start, last)
    plan = plan_refresh(start, last)
    print(f"BRONZE INPUT: {len(paths)} files, {start.isoformat()} to {last.isoformat()} SGT")
    print(f"GOLD REBUILD: {plan.affected_gold_start.isoformat()} to "
          f"{plan.affected_gold_end.isoformat()} SGT (clipped to available Silver rows)")
    if args.dry_run:
        print("DRY RUN: no changes made")
        return

    from delta import DeltaTable, configure_spark_with_delta_pip
    from pyspark.sql import SparkSession, Window, functions as F
    builder = (
        SparkSession.builder.appName("hdb-parking-incremental-lakehouse")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        silver_path = root / "silver/carpark_availability_delta"
        gold_path = root / "gold/carpark_features_delta"
        export = root / "gold/carpark_features_parquet"
        if not (silver_path / "_delta_log").is_dir() or not (gold_path / "_delta_log").is_dir():
            raise SystemExit("Requires existing Silver and Gold Delta tables; run full refresh first")
        incoming = silver_from_raw(spark, F, Window, paths)
        n = incoming.count()
        print(f"SILVER CANDIDATES: {n} valid car-lot rows")
        # Zero is legitimate if an entire source hour fails freshness/drift;
        # handle that via delete-by-source. For mixed batches Spark infers schema.
        merge_window(DeltaTable, spark, F, path=silver_path, rows=incoming,
                     start=start, end=last)
        print("SILVER MERGE: succeeded")
        silver_context = (
            spark.read.format("delta").load(str(silver_path))
            .where((F.col("hour_slot_sgt") >= ts_expr(F, plan.source_context_start)) &
                   (F.col("hour_slot_sgt") <= ts_expr(F, plan.source_context_end)))
        )
        recomputed = gold_from_silver(
            spark, F, Window, silver_context,
            root / "bronze/hdb_carpark_information/metadata.csv")
        affected = recomputed.where(
            (F.col("hour_slot_sgt") >= ts_expr(F, plan.affected_gold_start)) &
            (F.col("hour_slot_sgt") <= ts_expr(F, plan.affected_gold_end)))
        print(f"GOLD RECOMPUTED: {affected.count()} rows in affected interval")
        merge_window(DeltaTable, spark, F, path=gold_path, rows=affected,
                     start=plan.affected_gold_start, end=plan.affected_gold_end)
        print("GOLD MERGE: succeeded")
        # Compatibility exports: both Parquet datasets must reflect the current
        # Delta commits. Neither is incremental yet; do not schedule unattended.
        silver_export = root / "silver/carpark_availability_parquet"
        spark.read.format("delta").load(str(silver_path)).write.mode("overwrite").parquet(str(silver_export))
        print("SILVER PARQUET: full compatibility export succeeded (NOT incremental)")
        spark.read.format("delta").load(str(gold_path)).write.mode("overwrite").parquet(str(export))
        print("GOLD PARQUET: full compatibility export succeeded (NOT incremental)")
        print("CHECKPOINT: run inspect_gold.py, then compare with full-refresh results")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
