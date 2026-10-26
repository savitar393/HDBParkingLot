from argparse import Namespace
from datetime import datetime

import pytest

from hdb_parking.client import EmptyAvailabilityError, fetch_availability
from hdb_parking.records import SGT
import hdb_parking.cli as cli


class FakeResponse:
    def __init__(self, items, status="healthy"):
        self.items = items
        self.status = status

    def raise_for_status(self):
        return None

    def json(self):
        return {"items": self.items, "api_info": {"status": self.status}}


class FakeSession:
    def get(self, *args, **kwargs):
        return FakeResponse([])


def test_empty_has_context():
    with pytest.raises(EmptyAvailabilityError, match="2026-10-01T00:00:00") as exc:
        fetch_availability(FakeSession(), datetime(2026, 10, 1, tzinfo=SGT))
    assert "healthy" in str(exc.value)


def _args(tmp_path, hours=3, max_consecutive_empty=3):
    return Namespace(data_dir=str(tmp_path), start="2026-10-01T00:00:00", hours=hours,
                     delay=0, dry_run=False, max_consecutive_empty=max_consecutive_empty)


def test_one_missing_hour_does_not_erase_valid_hours(tmp_path, monkeypatch):
    count = 0

    def fake_fetch(_session, when):
        nonlocal count
        count += 1
        if count == 1:
            raise EmptyAvailabilityError("no items")
        return {"items": [{"timestamp": when.isoformat(), "carpark_data": []}]}

    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", fake_fetch)
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    cli.run_backfill(_args(tmp_path))
    assert len(list(tmp_path.rglob("*_SGT.json"))) == 2
    assert len((tmp_path / "quality" / "missing_snapshots.csv").read_text().splitlines()) == 2


def test_three_consecutive_empty_hours_stop(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", lambda *_: (_ for _ in ()).throw(EmptyAvailabilityError("no items")))
    monkeypatch.setattr(cli.time, "sleep", lambda _: None)
    with pytest.raises(SystemExit, match="3 consecutive"):
        cli.run_backfill(_args(tmp_path, hours=10))
    assert len((tmp_path / "quality" / "missing_snapshots.csv").read_text().splitlines()) == 4
    assert not list(tmp_path.rglob("*_SGT.json"))
