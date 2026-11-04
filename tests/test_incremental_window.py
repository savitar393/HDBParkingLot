from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hdb_parking.incremental_window import (
    parse_slot_sgt, plan_refresh, select_hour_files, slot_from_file,
)


def test_slot_parsing_and_validation():
    assert parse_slot_sgt('2026-11-02T09:00:00') == datetime(2026, 11, 2, 9)
    with pytest.raises(ValueError):
        parse_slot_sgt('2026-11-02T09:05:00')
    with pytest.raises(ValueError):
        parse_slot_sgt('2026-11-02T09:00:00+08:00')


def test_slot_file_parser():
    assert slot_from_file(Path('20261102T090000_SGT.json')) == datetime(2026, 11, 2, 9)
    with pytest.raises(ValueError):
        slot_from_file(Path('20261102T091000_SGT.json'))


def test_refresh_window_for_one_new_hour():
    t = datetime(2026, 11, 2, 9)
    p = plan_refresh(t, t)
    assert p.affected_gold_start == t - timedelta(hours=1)  # 1-hour target
    assert p.affected_gold_end == t + timedelta(hours=168)  # 168-hour lag
    assert p.source_context_start == t - timedelta(hours=169)
    assert p.source_context_end == t + timedelta(hours=169)


def test_reject_bad_window():
    with pytest.raises(ValueError):
        plan_refresh(datetime(2026, 11, 2, 10), datetime(2026, 11, 2, 9))


def test_select_contiguous_hours(tmp_path):
    bronze = tmp_path / 'bronze'
    bronze.mkdir()
    for stamp in ('20261102T060000_SGT.json', '20261102T070000_SGT.json'):
        (bronze / stamp).write_text('{}', encoding='utf-8')
    start = datetime(2026, 11, 2, 6)
    assert len(select_hour_files(bronze, start, start + timedelta(hours=1))) == 2
    with pytest.raises(FileNotFoundError):
        select_hour_files(bronze, start, start + timedelta(hours=2))
