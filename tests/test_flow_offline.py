"""Validate the Bronze ingestion/QA path without any live network requests."""
import argparse
import json
from pathlib import Path

from hdb_parking import cli


def test_mocked_ingestion_then_quality_check(tmp_path, monkeypatch, capsys):
    fixture = json.loads((Path(__file__).parent / "fixtures/sample_availability.json").read_text())
    monkeypatch.setattr(cli, "make_session", lambda: object())
    monkeypatch.setattr(cli, "fetch_availability", lambda session, when: fixture)
    cli.run_backfill(argparse.Namespace(data_dir=str(tmp_path), start="2026-10-01T08:00:00", hours=1, delay=0.0, dry_run=False))
    cli.run_validate(argparse.Namespace(data_dir=str(tmp_path)))
    printed = capsys.readouterr().out
    assert "saved=1" in printed
    assert "Valid car-lot observations after freshness/range filters: 1" in printed
    assert len(list((tmp_path / "bronze").rglob("*.json"))) == 1
