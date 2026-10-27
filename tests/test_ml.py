from datetime import timedelta

import numpy as np
import pandas as pd
import pytest

from hdb_parking.ml import (
    FEATURES, improvement_pct, make_model, metrics, prepare_features, split_chronologically,
)


def test_purged_split_boundaries():
    start = pd.Timestamp("2026-10-20T00:00:00")
    times = pd.date_range(start, periods=100, freq="h")
    df = pd.DataFrame({"hour_slot_sgt": times, "target_occupancy_1h": np.arange(100) / 100})
    splits = split_chronologically(df, val_start=times[60], test_start=times[80])
    assert splits.train["hour_slot_sgt"].max() == times[58]
    assert splits.validation["hour_slot_sgt"].min() == times[60]
    assert splits.validation["hour_slot_sgt"].max() == times[78]
    assert splits.test["hour_slot_sgt"].min() == times[80]
    assert splits.train["hour_slot_sgt"].max() + timedelta(hours=1) < splits.val_start
    assert splits.validation["hour_slot_sgt"].max() + timedelta(hours=1) < splits.test_start


def test_no_random_split_or_future_exposure():
    df = pd.DataFrame({"hour_slot_sgt": pd.date_range("2026-10-20", periods=36, freq="h")})
    splits = split_chronologically(df, val_start=pd.Timestamp("2026-10-20 20:00:00"), test_start=pd.Timestamp("2026-10-21 04:00:00"))
    assert set(splits.train.index).isdisjoint(set(splits.validation.index))
    assert set(splits.validation.index).isdisjoint(set(splits.test.index))


def test_numeric_features_and_optional_metadata():
    df = pd.DataFrame({c: [1.0, 0.1] for c in FEATURES})
    df["x_coord"] = [None, "12.5"]
    df["is_weekend"] = [True, False]
    x = prepare_features(df)
    assert list(x.columns) == FEATURES
    assert np.isnan(x["x_coord"].iloc[0])
    assert x["x_coord"].iloc[1] == 12.5
    assert x["is_weekend"].tolist() == [1, 0]


def test_metrics_and_improvement_sign():
    good = metrics([0, 0.8], [0.1, 0.9])
    assert good["rows"] == 2
    assert good["mae"] == pytest.approx(0.1)
    assert improvement_pct(0.06, 0.10) == pytest.approx(40.0)
    assert improvement_pct(0.12, 0.10) == pytest.approx(-20.0)
    assert improvement_pct(0.1, 0) is None


def test_predictor_fits_small_synthetic_data():
    rng = np.random.default_rng(42)
    train = pd.DataFrame(rng.random((200, len(FEATURES))), columns=FEATURES)
    train.loc[0, "x_coord"] = np.nan
    model = make_model()
    model.fit(prepare_features(train), train["occupancy_rate"])
    pred = model.predict(prepare_features(train.iloc[:4]))
    assert len(pred) == 4
    assert np.isfinite(pred).all()
