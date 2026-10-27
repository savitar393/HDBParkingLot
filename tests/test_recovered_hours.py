from argparse import Namespace
from datetime import datetime
import json

import pytest

from hdb_parking.client import EmptyAvailabilityError
from hdb_parking.records import SGT
import hdb_parking.cli as cli


def _args(tmp_path):
    return Namespace(data_dir=str(tmp_path), start="2026-10-01T00:00:00", hours=1,
                     delay=0, dry_run=False, max_consecutive_empty=3, empty_retry_minutes=5)


def test_recover_missing_midnight_using_five_minutes(tmp_path, monkeypatch):
    queries = []

    def fake_fetch(_session, when):
        queries.append(when)
        if when.minute == 0:
            raise EmptyAvailabilityError("empty at midnight")
        return {"items": [{"timestamp": "2026-10-01T00:04:36+08:00", "carpark_data": []}]}

    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", fake_fetch)
    cli.run_backfill(_args(tmp_path))
    assert queries == [datetime(2026, 10, 1, 0, 0, tzinfo=SGT), datetime(2026, 10, 1, 0, 5, tzinfo=SGT)]
    path = tmp_path / "bronze/carpark_availability/year=2026/month=10/day=01/20261001T000000_SGT.json"
    assert path.exists()
    assert json.loads(path.read_text())["items"][0]["timestamp"] == "2026-10-01T00:04:36+08:00"
    recovered = (tmp_path / "quality/recovered_snapshots.csv").read_text()
    assert "00:04:36" in recovered
    assert not (tmp_path / "quality/missing_snapshots.csv").exists()


def test_reject_stale_fallback_and_log_missing(tmp_path, monkeypatch):
    def fake_fetch(_session, when):
        if when.minute == 0:
            raise EmptyAvailabilityError("empty midnight")
        return {"items": [{"timestamp": "2026-10-01T02:04:36+08:00", "carpark_data": []}]}

    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", fake_fetch)
    with pytest.raises(SystemExit, match="No availability snapshots"):
        cli.run_backfill(_args(tmp_path))
    assert not list(tmp_path.rglob("*_SGT.json"))
    assert "minutes away" in (tmp_path / "quality/missing_snapshots.csv").read_text()


def test_existing_slot_is_idempotent_after_recovery(tmp_path, monkeypatch):
    calls=[]
    def fake_fetch(_session, when):
        calls.append(when)
        if when.minute == 0:
            raise EmptyAvailabilityError("missing")
        return {"items": [{"timestamp": "2026-10-01T00:04:36+08:00", "carpark_data": []}]}

    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", fake_fetch)
    cli.run_backfill(_args(tmp_path))
    cli.run_backfill(_args(tmp_path))
    assert len(calls) == 2
    assert len(list(tmp_path.rglob("*_SGT.json"))) == 1
    assert len((tmp_path / "quality/recovered_snapshots.csv").read_text().splitlines()) == 2


def test_disabled_fallback_preserves_old_empty_handling(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", lambda *args: (_ for _ in ()).throw(EmptyAvailabilityError("empty")))
    args=_args(tmp_path)
    args.empty_retry_minutes=0
    with pytest.raises(SystemExit, match="No availability snapshots"):
        cli.run_backfill(args)
    assert (tmp_path / "quality/missing_snapshots.csv").exists()
