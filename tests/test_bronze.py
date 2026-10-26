import json
from datetime import datetime

from hdb_parking.bronze import snapshot_path, write_snapshot
from hdb_parking.records import SGT


def test_partition_path_and_idempotency(tmp_path):
    date = datetime(2026, 10, 1, 8, tzinfo=SGT)
    p = snapshot_path(tmp_path, date)
    assert "year=2026/month=10/day=01" in p.as_posix()
    saved, created = write_snapshot(tmp_path, date, {"items": [1]})
    assert saved == p and created
    _, created_again = write_snapshot(tmp_path, date, {"items": [999]})
    assert not created_again
    assert json.loads(saved.read_text()) == {"items": [1]}
