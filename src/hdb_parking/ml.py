from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Iterable

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

FEATURES = [
    "occupancy_rate", "total_lots", "lag_1h", "lag_2h", "lag_3h",
    "lag_24h", "lag_168h", "rolling_mean_3h", "rolling_mean_6h",
    "rolling_std_24h", "occupancy_change_1h", "hour", "day_of_week",
    "is_weekend", "car_park_decks", "x_coord", "y_coord",
]
REQUIRED_HISTORY = [
    "occupancy_rate", "target_occupancy_1h", "lag_1h", "lag_2h", "lag_3h",
    "lag_24h", "lag_168h", "rolling_mean_3h", "rolling_mean_6h",
    "rolling_std_24h", "occupancy_change_1h",
]


@dataclass(frozen=True)
class Splits:
    train: pd.DataFrame
    validation: pd.DataFrame
    test: pd.DataFrame
    val_start: pd.Timestamp
    test_start: pd.Timestamp


def split_chronologically(
    df: pd.DataFrame, *, val_start: pd.Timestamp, test_start: pd.Timestamp,
    label_horizon_hours: int = 1,
) -> Splits:
    """Purge rows whose future label crosses a split boundary.

    A row at t has a label observed at t+1h. If the first validation slot
    is T, then the training row at T-1h must not be used as its label is
    observed at T. Same rule applies to validation/test boundary.
    """
    if label_horizon_hours <= 0 or val_start >= test_start:
        raise ValueError("Invalid horizon or split boundaries")
    stamp = pd.to_datetime(df["hour_slot_sgt"])
    train_mask = stamp + timedelta(hours=label_horizon_hours) < val_start
    val_mask = (stamp >= val_start) & (
        stamp + timedelta(hours=label_horizon_hours) < test_start
    )
    test_mask = stamp >= test_start
    segments = [df.loc[x].copy() for x in (train_mask, val_mask, test_mask)]
    if any(s.empty for s in segments):
        raise ValueError("Empty train/validation/test segment; backfill more data")
    return Splits(*segments, val_start=val_start, test_start=test_start)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in FEATURES if c not in df.columns]
    if missing:
        raise ValueError(f"Gold features missing columns: {missing}")
    x = df[FEATURES].copy()
    for column in FEATURES:
        # Booleans and missing numeric metadata are handled by HGB natively.
        x[column] = pd.to_numeric(x[column], errors="coerce")
    return x.astype("float32")


def make_model() -> HistGradientBoostingRegressor:
    return HistGradientBoostingRegressor(
        max_iter=120, learning_rate=0.07, max_leaf_nodes=31,
        l2_regularization=1.0, random_state=42, early_stopping=False,
    )


def _safe_r2(y: np.ndarray, pred: np.ndarray) -> float | None:
    if len(y) < 2:
        return None
    value = float(r2_score(y, pred))
    return value if np.isfinite(value) else None


def metrics(y: Iterable[float], prediction: Iterable[float]) -> dict[str, float | int | None]:
    y = np.asarray(y, dtype=float)
    pred = np.clip(np.asarray(prediction, dtype=float), 0.0, 1.0)
    if len(y) == 0 or len(y) != len(pred) or not np.isfinite(y).all() or not np.isfinite(pred).all():
        raise ValueError("Empty, mismatched, or non-finite evaluation arrays")
    return {
        "rows": int(len(y)),
        "mae": float(mean_absolute_error(y, pred)),
        "rmse": float(np.sqrt(mean_squared_error(y, pred))),
        "r2": _safe_r2(y, pred),
    }


def improvement_pct(candidate_mae: float, baseline_mae: float) -> float | None:
    """Positive means candidate beats persistence; negative means worse."""
    if baseline_mae <= 0:
        return None
    return float(100 * (baseline_mae - candidate_mae) / baseline_mae)
