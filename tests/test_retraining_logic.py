"""Offline test coverage for retraining guards, without live API/Spark."""
from datetime import datetime, timedelta

import pandas as pd
import pytest

from hdb_parking.retraining import (
    assert_model_compatible, compare_candidate, plan_windows,
    split_for_retraining, validate_hourly_coverage,
)
from hdb_parking.ml import FEATURES


def sample_metrics(mae, rmse, rows=50):
    return {"mae": mae, "rmse": rmse, "rows": rows}


def test_windows_correct_purge():
    asof = datetime(2026, 10, 23)
    p = plan_windows(asof, training_days=28, validation_hours=72)
    assert p.validation_from_utc == datetime(2026, 10, 20)
    slots = pd.date_range(start="2026-10-19 21:00:00", periods=7, freq="h")
    data = pd.DataFrame({"hour_slot_sgt": slots, "carpark_number": ["CP1"] * 7})
    a, b = split_for_retraining(data, p)
    assert a["hour_slot_sgt"].max() == pd.Timestamp("2026-10-19 22:00:00")
    assert b["hour_slot_sgt"].min() == pd.Timestamp("2026-10-20 00:00:00")
    assert pd.Timestamp("2026-10-19 23:00:00") not in set(a.hour_slot_sgt)


def test_missing_hours_not_treated_as_adjacent():
    asof = datetime(2026, 10, 23)
    p = plan_windows(asof)
    slots = [datetime(2026, 9, 25), datetime(2026, 10, 19, 22),
             datetime(2026, 10, 20), datetime(2026, 10, 20, 3)]
    data = pd.DataFrame({"hour_slot_sgt": slots, "carpark_number": ["CP1"] * 4})
    train, val = split_for_retraining(data, p)
    assert len(train) == 2 and len(val) == 2


def test_reject_worse_candidate():
    p = compare_candidate(sample_metrics(.06, .08), persistence=sample_metrics(.07, .09),
                          incumbent=sample_metrics(.05, .075))
    assert not p["promote"] and p["reference"] == "incumbent"


def test_promote_better_candidate_same_rows():
    p = compare_candidate(sample_metrics(.03, .06), persistence=sample_metrics(.06, .10),
                          incumbent=sample_metrics(.04, .07))
    assert p["promote"] and p["mae_gain_percent"] == pytest.approx(25)


def test_bootstrap_baseline_better():
    p = compare_candidate(sample_metrics(.03, .06), persistence=sample_metrics(.05, .07), incumbent=None)
    assert p["promote"] and p["reference"] == "persistence"


def test_reject_rmse_regression():
    p = compare_candidate(sample_metrics(.03, .10), persistence=sample_metrics(.05, .07),
                          incumbent=sample_metrics(.04, .06))
    assert not p["promote"]


def test_rows_must_match():
    with pytest.raises(ValueError, match="same validation rows"):
        compare_candidate(sample_metrics(.03, .06, 15), persistence=sample_metrics(.05, .07, 20), incumbent=None)


def test_model_cohort_guard():
    artifact = {"features": FEATURES, "carparks": ["A", "B"], "estimator": object()}
    assert_model_compatible(artifact, ["A", "B"])
    with pytest.raises(ValueError, match="cohort"):
        assert_model_compatible(artifact, ["A", "C"])
    with pytest.raises(ValueError, match="schema"):
        assert_model_compatible({**artifact, "features": ["x"]}, ["A", "B"])


def test_validation_coverage():
    slots = pd.date_range("2026-09-01", periods=340, freq="h")
    train = pd.DataFrame({"hour_slot_sgt": slots})
    val = pd.DataFrame({"hour_slot_sgt": [slots[0]] * 135,
                        "carpark_number": ["A", "B"] * 67 + ["C"]})
    with pytest.raises(RuntimeError, match="Duplicate"):
        validate_hourly_coverage(train, val, 2, 72, .80)
    val = pd.DataFrame({"hour_slot_sgt": pd.date_range("2026-10-01", periods=72, freq="h").tolist() * 2,
                        "carpark_number": ["A"] * 72 + ["B"] * 72})
    assert validate_hourly_coverage(train, val, 2, 72)["validation_coverage"] == 1


def test_aware_asof_rejected():
    with pytest.raises(ValueError, match="UTC-naive"):
        plan_windows(pd.Timestamp("2026-10-23T00:00:00Z").to_pydatetime())


def test_candidate_must_beat_persistence_even_when_incumbent_worse():
    p = compare_candidate(sample_metrics(.045, .06), persistence=sample_metrics(.04, .07),
                          incumbent=sample_metrics(.06, .08))
    assert not p["promote"] and not p["beats_persistence"]
