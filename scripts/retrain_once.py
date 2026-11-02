"""On-prem MLOps checkpoint: one safe retraining and conditional promotion.

Historical replay is intentional: never conflate a replayed event with a real
scheduled run or a currently fresh source. Later a scheduler will call this.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import joblib
import pandas as pd

from hdb_parking.ml import FEATURES, REQUIRED_HISTORY, make_model
from hdb_parking.retraining import (
    assert_model_compatible, compare_candidate, plan_windows,
    score_model, score_persistence, split_for_retraining, validate_hourly_coverage,
)


def parse_asof(text: str) -> datetime:
    """Explicit UTC timestamp only; avoids silently interpreting SGT as UTC."""
    d = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if d.tzinfo is None:
        raise ValueError("--as-of-utc requires timezone, e.g. 2026-10-20T00:00:00Z")
    return d.astimezone(timezone.utc).replace(tzinfo=None)


def save_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, allow_nan=False), encoding="utf-8")
    os.replace(tmp, path)


def run(data_dir: Path, model_dir: Path, *, as_of_utc: datetime | None,
        carparks: int, validation_hours: int, training_days: int,
        min_coverage: float, minimum_new_hours: int) -> dict:
    parquet_glob = str(data_dir / "gold/carpark_features_parquet/**/*.parquet")
    con = duckdb.connect()
    try:
        columns = " AND ".join(f'"{name}" IS NOT NULL' for name in REQUIRED_HISTORY)
        if as_of_utc is None:
            # Gold's final hour has no mature target. This is a *historical*
            # as-of, NOT current wall clock time.
            last_labeled_slot = con.execute(
                f"SELECT MAX(hour_slot_sgt) FROM read_parquet(?) WHERE {columns}",
                [parquet_glob],
            ).fetchone()[0]
            if last_labeled_slot is None:
                raise RuntimeError("No mature labeled Gold rows")
            as_of_utc = pd.Timestamp(last_labeled_slot).to_pydatetime() + timedelta(hours=1)
        plan = plan_windows(as_of_utc, training_days=training_days,
                            validation_hours=validation_hours)
        registry = model_dir / "retraining"
        latest = registry / "latest.json"
        prior = json.loads(latest.read_text(encoding="utf-8")) if latest.exists() else None
        if prior is not None:
            old_asof = datetime.fromisoformat(prior["as_of_utc"].replace("Z", "+00:00")).replace(tzinfo=None)
            elapsed = (as_of_utc - old_asof).total_seconds() / 3600
            if elapsed < minimum_new_hours:
                raise RuntimeError(
                    f"Only {elapsed:.1f} hours since last promoted model as-of; "
                    f"need >= {minimum_new_hours} hours. No new version created."
                )
            incumbent = joblib.load(registry / prior["model_path"])
            selected = list(incumbent["carparks"])
            if len(selected) != carparks:
                raise ValueError("Requested cohort size must match incumbent cohort")
        else:
            incumbent = None
            # Rank carparks exclusively on eligible historical TRAINING rows.
            selected = [r[0] for r in con.execute(
                "SELECT carpark_number, COUNT(*) AS n FROM read_parquet(?) "
                f"WHERE {columns} AND hour_slot_sgt >= ? AND hour_slot_sgt < ? "
                "GROUP BY carpark_number ORDER BY n DESC, carpark_number ASC LIMIT ?",
                [parquet_glob, plan.train_from_utc,
                 plan.train_until_exclusive_utc, carparks],
            ).fetchall()]
            if len(selected) != carparks:
                raise RuntimeError(f"Only {len(selected)} eligible training carparks, requested {carparks}")

        df = con.execute(
            "SELECT * FROM read_parquet(?) "
            f"WHERE {columns} AND hour_slot_sgt >= ? AND hour_slot_sgt < ? "
            "AND carpark_number IN (SELECT UNNEST(?::VARCHAR[]))",
            [parquet_glob, plan.train_from_utc, plan.as_of_utc, selected],
        ).df()
    finally:
        con.close()
    train, val = split_for_retraining(df, plan)
    coverage = validate_hourly_coverage(train, val, carparks, validation_hours, min_coverage)
    if train["target_occupancy_1h"].isna().any() or val["target_occupancy_1h"].isna().any():
        raise RuntimeError("A target is missing; aborting")
    from hdb_parking.ml import prepare_features
    candidate = make_model()
    candidate.fit(prepare_features(train), train["target_occupancy_1h"].astype(float))
    candidate_metrics = score_model(candidate, val)
    persistence_metrics = score_persistence(val)
    if incumbent is not None:
        assert_model_compatible(incumbent, selected)
        incumbent_metrics = score_model(incumbent["estimator"], val)
    else:
        incumbent_metrics = None
    decision = compare_candidate(candidate_metrics, persistence=persistence_metrics,
                                 incumbent=incumbent_metrics)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    run_path = registry / "runs" / run_id
    run_path.mkdir(parents=True, exist_ok=False)
    joblib.dump({"estimator": candidate, "features": FEATURES, "carparks": selected},
                run_path / "candidate.joblib")
    version = (int(prior["version_number"]) + 1) if prior else 1
    report = {
        "run_id": run_id, "mode": "historical_replay_manual_not_scheduled",
        "as_of_utc": as_of_utc.isoformat() + "Z", "training_days": training_days,
        "validation_hours": validation_hours, "train_rows": len(train),
        "validation_rows": len(val), "selected_carparks": len(selected),
        "window": {
            "train_from_utc": plan.train_from_utc.isoformat() + "Z",
            "validation_from_utc": plan.validation_from_utc.isoformat() + "Z",
        },
        "data_quality": coverage, "candidate": candidate_metrics,
        "persistence": persistence_metrics, "incumbent": incumbent_metrics,
        "decision": decision, "next_version_if_promoted": f"v{version:04d}",
    }
    # Preserve the audit log before attempting model publication.
    save_json_atomic(run_path / "summary.json", report)
    if decision["promote"]:
        new_rel = f"versions/v{version:04d}.joblib"
        new_path = registry / new_rel
        new_path.parent.mkdir(parents=True, exist_ok=True)
        if new_path.exists():
            raise RuntimeError(f"Version already exists: {new_path}; refusing overwrite")
        shutil.copy2(run_path / "candidate.joblib", new_path)
        save_json_atomic(latest, {
            "version": f"v{version:04d}", "version_number": version,
            "model_path": new_rel, "as_of_utc": as_of_utc.isoformat() + "Z",
            "validation_mae": candidate_metrics["mae"], "run_id": run_id,
        })
    print(json.dumps(report, indent=2, allow_nan=False))
    print(f"MODEL_REGISTRY: {registry}")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual, historically replayable retraining checkpoint")
    parser.add_argument("--data-dir", default=os.getenv("HDB_DATA_DIR", "data"))
    parser.add_argument("--model-dir", default="models")
    parser.add_argument("--as-of-utc", help="Explicit replay boundary with Z (UTC). Default: last mature Gold label.")
    parser.add_argument("--carparks", type=int, default=200)
    parser.add_argument("--validation-hours", type=int, default=72)
    parser.add_argument("--training-days", type=int, default=28)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--minimum-new-hours", type=int, default=72)
    args = parser.parse_args()
    if args.carparks < 1 or args.minimum_new_hours < 1:
        parser.error("--carparks and --minimum-new-hours must be positive")
    run(Path(args.data_dir), Path(args.model_dir),
        as_of_utc=parse_asof(args.as_of_utc) if args.as_of_utc else None,
        carparks=args.carparks, validation_hours=args.validation_hours,
        training_days=args.training_days, min_coverage=args.min_coverage,
        minimum_new_hours=args.minimum_new_hours)


if __name__ == "__main__":
    main()
