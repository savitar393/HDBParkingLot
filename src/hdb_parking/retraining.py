from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from hdb_parking.ml import FEATURES, metrics, prepare_features


@dataclass(frozen=True)
class WindowPlan:
    as_of_utc: datetime
    train_from_utc: datetime
    train_until_exclusive_utc: datetime
    validation_from_utc: datetime
    validation_until_exclusive_utc: datetime


def plan_windows(as_of_utc: datetime, *, training_days: int = 28,
                 validation_hours: int = 72, horizon_hours: int = 1) -> WindowPlan:
    if as_of_utc.tzinfo is not None or any((training_days < 14, validation_hours < 24, horizon_hours < 1)):
        raise ValueError("Use UTC-naive as-of timestamp, >=14 training days, >=24 validation hours, positive horizon")
    if as_of_utc.minute or as_of_utc.second or as_of_utc.microsecond:
        raise ValueError("As-of must be at an exact UTC hour")
    val_start = as_of_utc - timedelta(hours=validation_hours)
    # The prediction made at t is labeled with the actual observation at t+h.
    # No training label may be observed in the validation interval.
    train_end_exclusive = val_start - timedelta(hours=horizon_hours)
    return WindowPlan(
        as_of_utc=as_of_utc,
        train_from_utc=val_start - timedelta(days=training_days),
        train_until_exclusive_utc=train_end_exclusive,
        validation_from_utc=val_start,
        validation_until_exclusive_utc=as_of_utc,
    )


def split_for_retraining(df: pd.DataFrame, plan: WindowPlan,
                         horizon_hours: int = 1) -> tuple[pd.DataFrame, pd.DataFrame]:
    stamp = pd.to_datetime(df["hour_slot_sgt"])
    if (stamp.isna().any() or getattr(stamp.dt, "tz", None) is not None):
        raise ValueError("Expected valid UTC-naive timestamps from Gold")
    label_time = stamp + pd.Timedelta(hours=horizon_hours)
    train_mask = ((stamp >= plan.train_from_utc) & (label_time < plan.validation_from_utc))
    val_mask = ((stamp >= plan.validation_from_utc) & (label_time <= plan.as_of_utc))
    train = df.loc[train_mask].copy()
    val = df.loc[val_mask].copy()
    if train.empty or val.empty:
        raise ValueError("Empty train/validation split. Need more hourly data.")
    return train, val


def compare_candidate(candidate: dict, *, persistence: dict, incumbent: dict | None,
                      max_rmse_regression: float = 0.05,
                      min_relative_mae_gain: float = 0.0) -> dict:
    """Decide promotion from paired validation metrics (NEVER previous-run MAE).

    With no incumbent, only promote a candidate better than persistence.
    Otherwise require MAE improvement over the incumbent measured on the
    SAME validation rows and protect against a large RMSE regression.
    """
    if not (0 <= max_rmse_regression < 1 and min_relative_mae_gain >= 0):
        raise ValueError("Invalid promotion guardrails")
    reference = incumbent if incumbent is not None else persistence
    reference_name = "incumbent" if incumbent is not None else "persistence"
    required = ("mae", "rmse", "rows")
    if any(k not in d for d in (reference, candidate) for k in required):
        raise ValueError("Incomplete metrics")
    if candidate["rows"] != reference["rows"]:
        raise ValueError("Comparison must use the same validation rows")
    if candidate["rows"] <= 0:
        raise ValueError("Empty validation comparison")
    if reference["mae"] <= 0:
        passed_mae = candidate["mae"] < reference["mae"]
    else:
        passed_mae = candidate["mae"] < reference["mae"] * (1.0 - min_relative_mae_gain)
    passed_rmse = candidate["rmse"] <= reference["rmse"] * (1.0 + max_rmse_regression)
    passed_persistence = candidate["mae"] < persistence["mae"]
    passed = bool(passed_mae and passed_rmse and passed_persistence)
    reason = ("promoted: improved paired validation MAE, beat persistence, RMSE guard passed" if passed
              else "rejected: candidate failed MAE, persistence, or RMSE promotion guard")
    return {
        "promote": passed, "reference": reference_name,
        "reference_mae": reference["mae"], "candidate_mae": candidate["mae"],
        "reference_rmse": reference["rmse"], "candidate_rmse": candidate["rmse"],
        "mae_gain_percent": (100 * (reference["mae"] - candidate["mae"]) / reference["mae"]
                             if reference["mae"] > 0 else None),
        "beats_persistence": bool(passed_persistence), "reason": reason,
    }


def score_model(estimator, df: pd.DataFrame) -> dict:
    y = df["target_occupancy_1h"].to_numpy(dtype=float)
    return metrics(y, estimator.predict(prepare_features(df)))


def score_persistence(df: pd.DataFrame) -> dict:
    return metrics(df["target_occupancy_1h"].to_numpy(dtype=float),
                   df["occupancy_rate"].to_numpy(dtype=float))


def assert_model_compatible(artifact: dict, selected_carparks: list[str]) -> None:
    if artifact.get("features") != FEATURES:
        raise ValueError("Incumbent feature schema differs from current pipeline")
    if list(artifact.get("carparks", [])) != list(selected_carparks):
        raise ValueError("Incumbent carpark cohort differs from candidate cohort")
    if "estimator" not in artifact:
        raise ValueError("Incumbent artifact has no estimator")


def validate_hourly_coverage(train: pd.DataFrame, val: pd.DataFrame,
                             cohort_size: int, validation_hours: int,
                             min_fraction: float = 0.8) -> dict:
    if not (0 < min_fraction <= 1):
        raise ValueError("min_fraction must be between 0 and 1")
    expected = cohort_size * validation_hours
    fraction = len(val) / expected
    train_hours = train["hour_slot_sgt"].nunique()
    if train_hours < 14 * 24:
        raise RuntimeError(f"Only {train_hours} distinct training hours; need at least 336")
    if fraction < min_fraction:
        raise RuntimeError(f"Validation coverage {fraction:.1%} is below {min_fraction:.0%}")
    if val.duplicated(["carpark_number", "hour_slot_sgt"]).any():
        raise RuntimeError("Duplicate carpark-hour keys in validation data")
    return {"train_hours": int(train_hours), "validation_rows": int(len(val)),
            "expected_validation_rows": int(expected), "validation_coverage": float(fraction)}
