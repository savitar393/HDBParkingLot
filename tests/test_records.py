import json
from pathlib import Path

from hdb_parking.records import as_sgt, parse_snapshot


def fixture():
    return json.loads((Path(__file__).parent / "fixtures/sample_availability.json").read_text())


def test_only_car_lots_and_fresh_valid_records():
    rows = list(parse_snapshot(fixture()))
    assert len(rows) == 1
    assert rows[0]["carpark_number"] == "HLM"
    assert rows[0]["staleness_minutes"] == 1.0
    assert rows[0]["occupancy_rate"] == 0.6


def test_naive_updates_are_singapore_time():
    assert as_sgt("2026-10-01T07:59:00").utcoffset().total_seconds() == 28800


def test_bad_lots_excluded():
    sample = fixture()
    sample["items"][0]["carpark_data"][0]["carpark_info"][0]["lots_available"] = "101"
    assert list(parse_snapshot(sample)) == []
