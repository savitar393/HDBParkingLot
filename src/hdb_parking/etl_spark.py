"""Phase-1 local Spark: raw Bronze JSON -> validated Silver Delta and Parquet export.

Important: this is an initial FULL-REFRESH ETL, not an incremental MERGE yet.
"""
from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession, Window, functions as F

    root = Path(os.environ.get("HDB_DATA_DIR", "data")).resolve()
    bronze = root / "bronze/carpark_availability"
    if not any(bronze.rglob("*.json")):
        raise SystemExit("No Bronze snapshots found; run `hdb backfill` first")
    builder = (
        SparkSession.builder.appName("hdb-parking-silver-local")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        raw = spark.read.option("multiLine", "true").option("recursiveFileLookup", "true").json(str(bronze))
        expanded = (
            raw.select(F.explode("items").alias("item"))
            .select(F.to_timestamp("item.timestamp").alias("snapshot_timestamp"), F.explode("item.carpark_data").alias("cp"))
            .select("snapshot_timestamp", F.col("cp.carpark_number").alias("carpark_number"),
                    F.to_timestamp("cp.update_datetime").alias("update_datetime"),
                    F.explode("cp.carpark_info").alias("info"))
            .where(F.col("info.lot_type") == "C")
            .select("snapshot_timestamp", "carpark_number", "update_datetime",
                    F.col("info.total_lots").cast("integer").alias("total_lots"),
                    F.col("info.lots_available").cast("integer").alias("lots_available"))
        )
        silver = (
            expanded.withColumn("staleness_minutes",
                                (F.unix_timestamp("snapshot_timestamp") - F.unix_timestamp("update_datetime")) / F.lit(60.0))
            .where((F.col("total_lots") > 0) & (F.col("lots_available") >= 0) &
                   (F.col("lots_available") <= F.col("total_lots")) &
                   (F.col("staleness_minutes") >= 0) & (F.col("staleness_minutes") <= 15))
            .withColumn("occupancy_rate", 1.0 - F.col("lots_available") / F.col("total_lots"))
        )
        win = Window.partitionBy("snapshot_timestamp", "carpark_number").orderBy(F.col("update_datetime").desc())
        silver = silver.withColumn("rank", F.row_number().over(win)).where(F.col("rank") == 1).drop("rank")
        out_delta = root / "silver/carpark_availability_delta"
        out_parquet = root / "silver/carpark_availability_parquet"
        silver.write.format("delta").mode("overwrite").save(str(out_delta))
        silver.write.mode("overwrite").parquet(str(out_parquet))
        print(f"Silver rows: {silver.count()} | Delta: {out_delta} | Parquet: {out_parquet}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
