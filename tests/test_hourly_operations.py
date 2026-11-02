from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hdb_parking.bronze import snapshot_path, write_snapshot
from hdb_parking.operations import (
    check_bronze_health, collect_hours, expected_latest_slot, requested_slots, slot_from_path,
)
from hdb_parking.records import SGT


def sgt(h: int, minute: int = 0, day: int = 2) -> datetime:
    return datetime(2026, 11, day, h, minute, tzinfo=SGT)


def doc(stamp: datetime) -> dict:
    return {"items": [{"timestamp": stamp.isoformat(), "carpark_data": [{"carpark_number": "HLM"}]}]}


def test_before_settle_minute_collect_previous_hour():
    assert expected_latest_slot(sgt(8, 9)) == sgt(7)
    assert expected_latest_slot(sgt(8, 10)) == sgt(8)
    assert expected_latest_slot(sgt(0, 1)) == sgt(23, day=1)


def test_lookback_is_chronological_and_hour_aligned():
    assert requested_slots(sgt(8, 12), lookback_hours=3) == [sgt(6), sgt(7), sgt(8)]
    with pytest.raises(ValueError):
        requested_slots(sgt(8), lookback_hours=0)
    with pytest.raises(ValueError):
        expected_latest_slot(datetime(2026, 11, 2, 8))


def test_collect_stores_exact_hour_key_and_preserves_true_timestamp(tmp_path: Path):
    seen = []

    def fetcher(session, when):
        seen.append(when)
        return doc(when - timedelta(seconds=24)), None

    out = collect_hours(tmp_path, now_sgt=sgt(8, 12), lookback_hours=1,
                        delay_seconds=0, session_factory=lambda: object(), fetcher=fetcher)
    assert (out["saved"], out["failed"]) == (1, 0)
    stored = snapshot_path(tmp_path, sgt(8))
    assert stored.exists()
    assert json.loads(stored.read_text())["items"][0]["timestamp"].endswith("07:59:36+08:00")
    assert seen == [sgt(8)]
    again = collect_hours(tmp_path, now_sgt=sgt(8, 15), lookback_hours=1,
                          delay_seconds=0, session_factory=lambda: object(), fetcher=fetcher)
    assert (again["saved"], again["existing"], len(seen)) == (0, 1, 1)


def test_recovered_snapshot_also_uses_requested_hour_key(tmp_path: Path):
    def recovered(session, when):
        return doc(when + timedelta(minutes=4, seconds=36)), when + timedelta(minutes=5)

    out = collect_hours(tmp_path, now_sgt=sgt(0, 12), lookback_hours=1,
                        delay_seconds=0, session_factory=lambda: object(), fetcher=recovered)
    assert out["saved"] == 1
    assert snapshot_path(tmp_path, sgt(0)).exists()
    assert (tmp_path / "quality/recovered_snapshots.csv").exists()


def test_reject_distant_snapshot_without_writing_bronze(tmp_path: Path):
    def bad(session, when):
        return doc(when - timedelta(minutes=20)), None

    out = collect_hours(tmp_path, now_sgt=sgt(8, 12), lookback_hours=1,
                        delay_seconds=0, session_factory=lambda: object(), fetcher=bad)
    assert (out["failed"], out["saved"]) == (1, 0)
    assert not snapshot_path(tmp_path, sgt(8)).exists()
    event = json.loads((tmp_path / "quality/hourly_collection_events.jsonl").read_text())
    assert event["status"] == "failed"


def test_corrupt_existing_file_fails_loudly(tmp_path: Path):
    p = snapshot_path(tmp_path, sgt(8))
    p.parent.mkdir(parents=True)
    p.write_text('{"items": [')
    out = collect_hours(tmp_path, now_sgt=sgt(8, 15), lookback_hours=1,
                        delay_seconds=0, session_factory=lambda: object())
    assert out["failed"] == 1
    assert out["existing"] == 0
    assert p.read_text() == '{"items": ['


def test_health_reports_bronze_freshness_coverage_and_non_hour_files(tmp_path: Path):
    for h in (5, 6, 7, 8):
        write_snapshot(tmp_path, sgt(h), doc(sgt(h) - timedelta(seconds=24)))
    write_snapshot(tmp_path, sgt(8, 7), doc(sgt(8, 7)))
    result = check_bronze_health(tmp_path, now_sgt=sgt(8, 15),
                                  min_24h_coverage=0.1)
    assert result["status"] == "healthy"
    assert result["hourly_files"] == 4
    assert result["non_hourly_files"] == 1
    assert result["latest_saved_slot_sgt"] == sgt(8).isoformat()
    assert result["source_age_minutes"] == 15.4
    assert result["last_24h_coverage"] == round(4 / 24, 4)
    assert check_bronze_health(tmp_path, now_sgt=sgt(8, 15))["status"] == "degraded"


def test_health_stale_even_when_last_day_files_exist(tmp_path: Path):
    for h in range(24):
        day = 1 if h < 20 else 2
        hour = (h + 4) % 24
        when = sgt(hour, day=day)
        write_snapshot(tmp_path, when, doc(when))
    result = check_bronze_health(tmp_path, now_sgt=sgt(18, 25), min_24h_coverage=0)
    assert result["status"] == "degraded"
    assert result["source_age_minutes"] > 90


def test_dry_run_does_not_contact_network_or_make_files(tmp_path: Path):
    def fail_factory():
        raise AssertionError("session should not be created")

    out = collect_hours(tmp_path, now_sgt=sgt(8, 12), lookback_hours=2,
                        dry_run=True, session_factory=fail_factory)
    assert out["dry_run"] and out["requested"] == 2
    assert not (tmp_path / "bronze").exists()


def test_path_validation_ignores_arbitrary_minute_snapshot():
    assert slot_from_path(Path("20261102T080000_SGT.json")) == sgt(8)
    assert slot_from_path(Path("20261102T080700_SGT.json")) is None
    assert slot_from_path(Path("bad.json")) is None
