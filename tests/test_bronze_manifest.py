import json
from datetime import datetime
from pathlib import Path

import pytest

from hdb_parking.bronze_manifest import (
    compare_entries, file_fingerprint, group_contiguous, load_checkpoint,
    save_checkpoint, snapshot, validate_changed_json,
)


def sample(root, slot, *, content=None):
    path = root / f'{slot}_SGT.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content if content is not None else json.dumps({
        'items': [{'timestamp': '2026-11-02T09:00:00+08:00', 'carpark_data': []}]
    }), encoding='utf-8')
    return path


def test_hash_changes_when_same_size_content_changes(tmp_path):
    p = sample(tmp_path, '20261102T090000')
    a = file_fingerprint(p, tmp_path)
    p.write_text(p.read_text().replace('carpark_data', 'carpark_fata'), encoding='utf-8')
    b = file_fingerprint(p, tmp_path)
    assert a['bytes'] == b['bytes'] and a['sha256'] != b['sha256']


def test_detect_additions_modifications_and_deletions():
    old = {'a': {'sha256': 'a'}, 'b': {'sha256': 'b'}, 'd': {'sha256': 'd'}}
    new = {'a': {'sha256': 'z'}, 'b': {'sha256': 'b'}, 'c': {'sha256': 'c'}}
    assert compare_entries(old, new) == (['c'], ['a'], ['d'])


def test_checkpoint_roundtrip_and_invalid(tmp_path):
    p = tmp_path / 'quality/m.json'
    x = {'schema_version': 1, 'entries': {'a': {'sha256': 'hi'}}}
    save_checkpoint(p, x)
    assert load_checkpoint(p) == x
    p.write_text('{"schema_version": 99, "entries": {}}')
    with pytest.raises(ValueError):
        load_checkpoint(p)


def test_snapshot_duplicate_hour_rejected(tmp_path):
    sample(tmp_path / 'a', '20261102T090000')
    sample(tmp_path / 'b', '20261102T090000')
    with pytest.raises(ValueError, match='Duplicate'):
        snapshot(tmp_path)


def test_group_contiguous_and_gaps():
    entries = {f'k{i}': {'slot_sgt': f'2026-11-02T{h:02}:00:00'}
               for i, h in enumerate([5, 6, 7, 9, 10])}
    groups = group_contiguous(entries, list(entries), max_hours=2)
    assert [(x[0].hour, x[1]) for x in groups] == [(5, 2), (7, 1), (9, 2)]


def test_group_requires_valid_limit():
    with pytest.raises(ValueError):
        group_contiguous({}, [], 0)


def test_validate_changed_json(tmp_path):
    p = sample(tmp_path, '20261102T090000')
    validate_changed_json(tmp_path, [p.name])
    p.write_text('{', encoding='utf-8')
    with pytest.raises(json.JSONDecodeError):
        validate_changed_json(tmp_path, [p.name])


def test_deletion_detection_empty_manifest(tmp_path):
    sample(tmp_path, '20261102T090000')
    assert len(snapshot(tmp_path)) == 1
