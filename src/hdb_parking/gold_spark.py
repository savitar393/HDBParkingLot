from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession, Window, functions as F

    root = Path(os.environ.get("HDB_DATA_DIR", "data")).resolve()
    silver_path = root / "silver/carpark_availability_delta"
    if not (silver_path / "_delta_log").is_dir():
        raise SystemExit("Silver Delta not found. Run `python -m hdb_parking.etl_spark` first.")

    builder = (
        SparkSession.builder.appName("hdb-parking-gold-local")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "Asia/Singapore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    try:
        silver = spark.read.format("delta").load(str(silver_path))
        if "hour_slot_sgt" not in silver.columns:
            raise SystemExit("Silver schema lacks hour_slot_sgt; run the UPDATED Silver ETL first.")
        # Only complete on-the-hour slots belong to the hourly forecasting grid.
        hourly = (
            silver.where((F.minute("hour_slot_sgt") == 0) & (F.second("hour_slot_sgt") == 0))
            .select("hour_slot_sgt", "carpark_number", "snapshot_timestamp", "total_lots",
                    "lots_available", "occupancy_rate")
            .withColumn("slot_epoch", F.col("hour_slot_sgt").cast("long"))
        )
        # Time ranges, rather than row offsets, ensure gaps never become fake lags.
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

        metadata_file = root / "bronze/hdb_carpark_information/metadata.csv"
        if metadata_file.exists():
            meta = spark.read.option("header", True).csv(str(metadata_file))
            if "car_park_no" in meta.columns:
                chosen = ["car_park_no", "address", "car_park_type", "type_of_parking_system",
                          "car_park_decks", "night_parking", "free_parking", "x_coord", "y_coord"]
                cols = [c for c in chosen if c in meta.columns]
                meta = meta.select(*cols).withColumnRenamed("car_park_no", "carpark_number")
                if "car_park_decks" in meta.columns:
                    meta = meta.withColumn("car_park_decks", F.col("car_park_decks").cast("int"))
                for coordinate in ("x_coord", "y_coord"):
                    if coordinate in meta.columns:
                        meta = meta.withColumn(coordinate, F.col(coordinate).cast("double"))
                gold = gold.join(F.broadcast(meta.dropDuplicates(["carpark_number"])),
                                 on="carpark_number", how="left")
            else:
                print("WARNING: Metadata missing car_park_no; Gold generated without metadata join.")
        else:
            print("WARNING: metadata.csv missing; Gold generated without metadata join.")

        delta_path = root / "gold/carpark_features_delta"
        parquet_path = root / "gold/carpark_features_parquet"
        gold.write.format("delta").mode("overwrite").save(str(delta_path))
        gold.write.mode("overwrite").parquet(str(parquet_path))
        print(f"Gold rows: {gold.count()}")
        print(f"Gold Delta: {delta_path} | Parquet: {parquet_path}")
        print("NOTE: target_occupancy_1h is null for rows without a real next-hour observation.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
