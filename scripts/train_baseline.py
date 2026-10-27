from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import joblib
import pandas as pd

from hdb_parking.ml import (
    FEATURES, REQUIRED_HISTORY, improvement_pct, make_model, metrics,
    prepare_features, split_chronologically,
)


def quote_column(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_eligible_slots(con: duckdb.DuckDBPyConnection, parquet_glob: str) -> list[pd.Timestamp]:
    condition = " AND ".join(f"{quote_column(c)} IS NOT NULL" for c in REQUIRED_HISTORY)
    rows = con.execute(
        f"SELECT DISTINCT hour_slot_sgt FROM read_parquet(?) WHERE {condition} "
        "ORDER BY hour_slot_sgt", [parquet_glob]
    ).fetchall()
    return [pd.Timestamp(row[0]) for row in rows]


def load_selected(
    con: duckdb.DuckDBPyConnection, parquet_glob: str, val_start: pd.Timestamp,
    sample_carparks: int,
) -> tuple[pd.DataFrame, list[str]]:
    # The only future-dependent selection criterion would be selecting on test
    # coverage. Instead, select exclusively from pre-validation training slots.
    condition = " AND ".join(f"g.{quote_column(c)} IS NOT NULL" for c in REQUIRED_HISTORY)
    eligible = f"SELECT * FROM read_parquet(?) AS g WHERE {condition}"
    ranking = con.execute(
        f"SELECT carpark_number, COUNT(*) AS n FROM ({eligible}) "
        "WHERE hour_slot_sgt < ? GROUP BY carpark_number "
        "ORDER BY n DESC, carpark_number ASC LIMIT ?",
        [parquet_glob, val_start.to_pydatetime(), sample_carparks],
    ).fetchall()
    carparks = [c for c, _ in ranking]
    if len(carparks) < sample_carparks:
        raise ValueError(f"Only {len(carparks)} eligible training carparks; requested {sample_carparks}")
    # Read Gold rows for selected entities, preserving exact timestamps.
    condition = " AND ".join(f"{quote_column(c)} IS NOT NULL" for c in REQUIRED_HISTORY)
    selected = con.execute(
        f"SELECT * FROM read_parquet(?) WHERE {condition} "
        "AND carpark_number IN (SELECT UNNEST(?::VARCHAR[]))",
        [parquet_glob, carparks],
    ).df()
    return selected, carparks


def evaluate_segment(model, data: pd.DataFrame) -> dict:
    y = data["target_occupancy_1h"].to_numpy(dtype=float)
    ml_pred = model.predict(prepare_features(data))
    persistence = data["occupancy_rate"].to_numpy(dtype=float)
    result = {
        "gradient_boosting": metrics(y, ml_pred),
        "persistence": metrics(y, persistence),
    }
    result["mae_improvement_over_persistence_pct"] = improvement_pct(
        result["gradient_boosting"]["mae"], result["persistence"]["mae"]
    )
    return result


def run(data_dir: Path, *, carparks: int = 200, val_hours: int = 24, test_hours: int = 24) -> dict:
    parquet = str(data_dir / "gold/carpark_features_parquet/**/*.parquet")
    con = duckdb.connect()
    slots = get_eligible_slots(con, parquet)
    if len(slots) < 96 or len(slots) < val_hours + test_hours + 48:
        raise RuntimeError(
            f"Only {len(slots)} feature-complete hourly slots; need at least "
            f"{max(96, val_hours + test_hours + 48)}. Backfill more historical data."
        )
    # Split boundaries are based on sorted eligible slots; the model only uses
    # observed labels, and never imputes missing hours.
    val_start, test_start = slots[-(val_hours + test_hours)], slots[-test_hours]
    df, selected = load_selected(con, parquet, val_start, carparks)
    segments = split_chronologically(df, val_start=val_start, test_start=test_start)
    if segments.train["hour_slot_sgt"].nunique() < 48:
        raise RuntimeError("Fewer than 48 distinct train hours; add more historical data")
    model = make_model()
    model.fit(prepare_features(segments.train), segments.train["target_occupancy_1h"].astype(float))
    val = evaluate_segment(model, segments.validation)
    # The test is evaluated ONCE with the fixed feature set and hyperparameters.
    test = evaluate_segment(model, segments.test)
    stamped = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = data_dir.parent / "models" / f"pilot-{stamped}"
    out.mkdir(parents=True, exist_ok=False)
    joblib.dump({"estimator": model, "features": FEATURES, "carparks": selected}, out / "model.joblib")
    summary = {
        "run_type": "development_pilot_not_final_evaluation",
        "timestamp": stamped,
        "selected_carparks": len(selected),
        "selection": "top 200 training-period completeness (NOT geographically representative)",
        "features": FEATURES,
        "split": {
            "train_rows": int(len(segments.train)),
            "validation_rows": int(len(segments.validation)),
            "test_rows": int(len(segments.test)),
            "training_end_utc": str(segments.train["hour_slot_sgt"].max()),
            "validation_start_utc": str(val_start),
            "test_start_utc": str(test_start),
            "test_end_utc": str(segments.test["hour_slot_sgt"].max()),
            "target_horizon_hours": 1,
            "purged_boundary_hours": 1,
        },
        "validation": val,
        "test": test,
        "artifact_path": str(out),
        "warning": "Test metrics are descriptive for this first locked pilot, not a tuned final result.",
    }
    (out / "metrics.json").write_text(json.dumps(summary, indent=2, allow_nan=False), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="First HDB local ML model versus persistence")
    parser.add_argument("--data-dir", default=os.getenv("HDB_DATA_DIR", "data"))
    parser.add_argument("--carparks", type=int, default=200)
    parser.add_argument("--validation-hours", type=int, default=24)
    parser.add_argument("--test-hours", type=int, default=24)
    args = parser.parse_args()
    if args.carparks < 1 or args.validation_hours < 1 or args.test_hours < 1:
        parser.error("carparks, validation-hours and test-hours must be positive")
    run(Path(args.data_dir), carparks=args.carparks,
        val_hours=args.validation_hours, test_hours=args.test_hours)
