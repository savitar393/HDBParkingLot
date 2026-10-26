"""Portable local Bronze storage, using the same partition contract as ADLS."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path


def snapshot_path(root: Path, requested_at_sgt: datetime) -> Path:
    stamp = requested_at_sgt.strftime("%Y%m%dT%H%M%S")
    return (
        root
        / "bronze/carpark_availability"
        / f"year={requested_at_sgt:%Y}"
        / f"month={requested_at_sgt:%m}"
        / f"day={requested_at_sgt:%d}"
        / f"{stamp}_SGT.json"
    )


def write_snapshot(root: Path, requested_at_sgt: datetime, document: dict) -> tuple[Path, bool]:
    """Idempotent snapshot writes; never silently overwrite an existing raw response."""
    path = snapshot_path(root, requested_at_sgt)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as fp:
            json.dump(document, fp, ensure_ascii=False, separators=(",", ":"))
            fp.write("\n")
    except FileExistsError:
        return path, False
    return path, True
