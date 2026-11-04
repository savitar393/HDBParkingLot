from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from hdb_parking.incremental_window import slot_from_file

SCHEMA_VERSION = 1


def file_fingerprint(path: Path, bronze_root: Path) -> dict:
    """Hash the whole file, not mtime; replacements with the same length count."""
    h = hashlib.sha256()
    with path.open('rb') as fp:
        for chunk in iter(lambda: fp.read(1024 * 1024), b''):
            h.update(chunk)
    return {
        'slot_sgt': slot_from_file(path).isoformat(timespec='seconds'),
        'sha256': h.hexdigest(),
        'bytes': path.stat().st_size,
    }


def snapshot(bronze_root: Path) -> dict[str, dict]:
    if not bronze_root.is_dir():
        raise FileNotFoundError(f'Bronze root does not exist: {bronze_root}')
    result = {}
    slots = set()
    for path in sorted(bronze_root.rglob('*_SGT.json')):
        slot = slot_from_file(path)
        if slot in slots:
            raise ValueError(f'Duplicate Bronze hourly slot: {slot}')
        slots.add(slot)
        result[path.relative_to(bronze_root).as_posix()] = file_fingerprint(path, bronze_root)
    if not result:
        raise ValueError(f'No Bronze hourly snapshots: {bronze_root}')
    return result


def load_checkpoint(path: Path) -> dict:
    if not path.is_file():
        raise FileNotFoundError(f'No checkpoint at {path}. Bootstrap only after a verified full refresh.')
    data = json.loads(path.read_text(encoding='utf-8'))
    if data.get('schema_version') != SCHEMA_VERSION or not isinstance(data.get('entries'), dict):
        raise ValueError('Unsupported or malformed Bronze manifest; refusing to run')
    return data


def save_checkpoint(path: Path, checkpoint: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staged = path.with_suffix(path.suffix + '.tmp')
    staged.write_text(json.dumps(checkpoint, indent=2, sort_keys=True) + '\n', encoding='utf-8')
    os.replace(staged, path)


def compare_entries(old: dict[str, dict], new: dict[str, dict]) -> tuple[list[str], list[str], list[str]]:
    deleted = sorted(set(old) - set(new))
    changed = sorted(k for k in old.keys() & new.keys() if old[k] != new[k])
    added = sorted(set(new) - set(old))
    return added, changed, deleted


def group_contiguous(entries: dict[str, dict], selected: list[str], max_hours: int = 168) -> list[tuple[datetime, int, list[str]]]:
    if not (1 <= max_hours <= 168):
        raise ValueError('max_hours must be between 1 and 168')
    ordered = sorted(((datetime.fromisoformat(entries[k]['slot_sgt']), k) for k in selected))
    result: list[tuple[datetime, int, list[str]]] = []
    for slot, k in ordered:
        if result and slot == result[-1][0] + timedelta(hours=result[-1][1]) and result[-1][1] < max_hours:
            start, n, keys = result[-1]
            result[-1] = (start, n + 1, keys + [k])
        else:
            if result and slot <= result[-1][0] + timedelta(hours=result[-1][1] - 1):
                raise ValueError(f'Duplicate or nonmonotonic slot: {slot}')
            result.append((slot, 1, [k]))
    return result


def validate_changed_json(bronze_root: Path, keys: list[str]) -> None:
    """Reject malformed/partially written files before changing Delta."""
    for key in keys:
        path = bronze_root / key
        obj = json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(obj, dict) or not isinstance(obj.get('items'), list) or not obj['items']:
            raise ValueError(f'Malformed or empty availability items in {path}')
        for item in obj['items']:
            if not isinstance(item, dict) or 'timestamp' not in item or not isinstance(item.get('carpark_data'), list):
                raise ValueError(f'Unexpected item structure in {path}')


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()
