from datetime import datetime, timedelta, timezone

import pytest

from hdb_parking.temporal import (
    acceptable_observation_time,
    exact_hour_before,
    requested_slot_from_path,
)

SGT = timezone(timedelta(hours=8))


def test_api_timestamp_before_requested_hour_uses_filename_slot():
    key = "file:///workspace/data/bronze/carpark_availability/year=2026/month=10/day=01/20261001T080000_SGT.json"
    expected = datetime(2026, 10, 1, 8, 0, tzinfo=SGT)
    assert requested_slot_from_path(key) == expected
    assert acceptable_observation_time(datetime(2026, 10, 1, 7, 59, 36, tzinfo=SGT), expected)


def test_recovered_midnight_timestamp_within_10_min():
    slot = requested_slot_from_path("20261001T000000_SGT.json")
    assert acceptable_observation_time(datetime(2026, 10, 1, 0, 4, 36, tzinfo=SGT), slot)


def test_bad_source_drift_not_accepted():
    slot = requested_slot_from_path("20261001T000000_SGT.json")
    assert not acceptable_observation_time(datetime(2026, 10, 1, 0, 40, tzinfo=SGT), slot)


def test_unparsable_slot_rejected():
    with pytest.raises(ValueError):
        requested_slot_from_path("metadata.json")


def test_gap_does_not_count_as_previous_hour():
    observed = {datetime(2026, 10, 1, 23, tzinfo=SGT), datetime(2026, 10, 13, 0, tzinfo=SGT)}
    slot = datetime(2026, 10, 13, 0, tzinfo=SGT)
    assert exact_hour_before(slot, 1) not in observed
    assert exact_hour_before(slot, 24) not in observed


def test_next_hour_exact_check():
    slot = datetime(2026, 10, 25, 15, tzinfo=SGT)
    assert slot + timedelta(hours=1) == datetime(2026, 10, 25, 16, tzinfo=SGT)


def test_naive_datetime_fails_closed():
    with pytest.raises(ValueError):
        acceptable_observation_time(datetime(2026, 10, 1), datetime(2026, 10, 1, tzinfo=SGT))
