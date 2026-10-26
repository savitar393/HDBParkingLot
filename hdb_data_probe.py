import json
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
METADATA_URL = "https://data.gov.sg/api/action/datastore_search"
METADATA_ID = "d_23f946fa557947f93a8043bbef41dd09"

# 12 small representative probes: weekday/weekend, 2025/2026, morning/midday/evening.
PROBE_TIMESTAMPS = [
    "2025-01-15T08:00:00",
    "2025-01-15T13:00:00",
    "2025-01-15T19:00:00",
    "2025-07-19T08:00:00",
    "2025-07-19T13:00:00",
    "2025-07-19T19:00:00",
    "2026-03-16T08:00:00",
    "2026-03-16T13:00:00",
    "2026-03-16T19:00:00",
    "2026-09-13T08:00:00",
    "2026-09-13T13:00:00",
    "2026-09-13T19:00:00",
]

OUT_DIR = Path("hdb_probe_output")


def headers():
    h = {"User-Agent": "hdb-data-feasibility-probe/1.0"}
    api_key = os.getenv("DATA_GOV_SG_API_KEY")
    if api_key:
        h["x-api-key"] = api_key
    return h


def fetch_metadata(session):
    r = session.get(
        METADATA_URL,
        params={"resource_id": METADATA_ID, "limit": 5000},
        headers=headers(),
        timeout=30,
    )
    r.raise_for_status()
    payload = r.json()
    if not payload.get("success"):
        raise RuntimeError(f"Metadata API returned success=false: {payload}")
    records = payload["result"]["records"]
    return pd.DataFrame(records)


def fetch_snapshot(session, requested_ts):
    r = session.get(
        AVAILABILITY_URL,
        params={"date_time": requested_ts},
        headers=headers(),
        timeout=30,
    )
    r.raise_for_status()
    payload = r.json()

    items = payload.get("items", [])
    if not items:
        return [], None

    item = items[0]
    response_ts = item.get("timestamp")
    rows = []

    for cp in item.get("carpark_data", []):
        cp_no = cp.get("carpark_number")
        update_dt = cp.get("update_datetime")

        for info in cp.get("carpark_info", []):
            rows.append(
                {
                    "requested_timestamp": requested_ts,
                    "response_timestamp": response_ts,
                    "carpark_number": cp_no,
                    "update_datetime": update_dt,
                    "lot_type": info.get("lot_type"),
                    "total_lots": info.get("total_lots"),
                    "lots_available": info.get("lots_available"),
                }
            )
    return rows, response_ts


def pct(n, d):
    return round(100.0 * n / d, 2) if d else None


def main():
    OUT_DIR.mkdir(exist_ok=True)
    session = requests.Session()

    print("Downloading current HDB Carpark Information metadata...")
    metadata = fetch_metadata(session)

    if "car_park_no" not in metadata.columns:
        raise RuntimeError(
            f"Unexpected metadata schema. Columns: {list(metadata.columns)}"
        )

    metadata["car_park_no"] = metadata["car_park_no"].astype(str).str.strip()

    print(f"Metadata rows: {len(metadata):,}")
    print(f"Unique metadata carparks: {metadata['car_park_no'].nunique():,}")

    all_rows = []
    request_status = []

    print("\nRequesting 12 historical snapshots...")
    for i, ts in enumerate(PROBE_TIMESTAMPS, start=1):
        try:
            rows, response_ts = fetch_snapshot(session, ts)
            all_rows.extend(rows)
            request_status.append(
                {
                    "requested_timestamp": ts,
                    "ok": True,
                    "response_timestamp": response_ts,
                    "rows": len(rows),
                    "error": None,
                }
            )
            print(f"[{i:02d}/12] {ts} -> {len(rows):,} flattened rows")
        except Exception as e:
            request_status.append(
                {
                    "requested_timestamp": ts,
                    "ok": False,
                    "response_timestamp": None,
                    "rows": 0,
                    "error": str(e),
                }
            )
            print(f"[{i:02d}/12] {ts} -> FAILED: {e}")

        # Public no-key rate limit is conservative; keep this probe gentle.
        if i < len(PROBE_TIMESTAMPS):
            time.sleep(2)

    status_df = pd.DataFrame(request_status)
    status_df.to_csv(OUT_DIR / "request_status.csv", index=False)

    if not all_rows:
        print("\nNo availability rows were returned.")
        print("See hdb_probe_output/request_status.csv for errors.")
        return

    df = pd.DataFrame(all_rows)

    for col in ["total_lots", "lots_available"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Keep timestamps in Singapore local time. update_datetime from this API is local.
    df["requested_dt"] = pd.to_datetime(
        df["requested_timestamp"], errors="coerce"
    )
    df["update_dt"] = pd.to_datetime(df["update_datetime"], errors="coerce")
    df["staleness_minutes"] = (
        df["requested_dt"] - df["update_dt"]
    ).dt.total_seconds() / 60.0

    df.to_csv(OUT_DIR / "probe_availability_all_lot_types.csv", index=False)

    cars = df[df["lot_type"] == "C"].copy()

    cars["occupancy_rate"] = pd.NA
    valid_capacity = cars["total_lots"] > 0
    cars.loc[valid_capacity, "occupancy_rate"] = (
        1 - cars.loc[valid_capacity, "lots_available"]
        / cars.loc[valid_capacity, "total_lots"]
    )

    metadata_cols = [
        c
        for c in [
            "car_park_no",
            "address",
            "x_coord",
            "y_coord",
            "car_park_type",
            "type_of_parking_system",
            "short_term_parking",
            "free_parking",
            "night_parking",
            "car_park_decks",
            "gantry_height",
            "car_park_basement",
        ]
        if c in metadata.columns
    ]

    joined = cars.merge(
        metadata[metadata_cols],
        how="left",
        left_on="carpark_number",
        right_on="car_park_no",
        indicator=True,
    )
    joined.to_csv(OUT_DIR / "probe_joined_car_lots.csv", index=False)

    successful_requests = int(status_df["ok"].sum())
    car_rows = len(cars)
    matched = int((joined["_merge"] == "both").sum())

    duplicate_key_rows = int(
        cars.duplicated(
            subset=["requested_timestamp", "carpark_number"],
            keep=False,
        ).sum()
    )

    invalid_zero_capacity = int((cars["total_lots"] <= 0).sum())
    invalid_negative_available = int((cars["lots_available"] < 0).sum())
    invalid_over_capacity = int(
        (
            (cars["total_lots"] >= 0)
            & (cars["lots_available"] > cars["total_lots"])
        ).sum()
    )

    stale_valid = cars["staleness_minutes"].dropna()

    summary = {
        "probe_timestamp_count": len(PROBE_TIMESTAMPS),
        "successful_requests": successful_requests,
        "failed_requests": len(PROBE_TIMESTAMPS) - successful_requests,
        "metadata_rows": int(len(metadata)),
        "metadata_unique_carparks": int(metadata["car_park_no"].nunique()),
        "metadata_duplicate_carpark_ids": int(
            metadata["car_park_no"].duplicated(keep=False).sum()
        ),
        "flattened_all_lot_type_rows": int(len(df)),
        "lot_type_counts": {
            str(k): int(v) for k, v in df["lot_type"].value_counts(dropna=False).items()
        },
        "car_lot_rows": int(car_rows),
        "unique_carparks_seen": int(cars["carpark_number"].nunique()),
        "join_matched_rows": matched,
        "join_coverage_percent": pct(matched, car_rows),
        "duplicate_car_key_rows": duplicate_key_rows,
        "total_lots_le_zero_rows": invalid_zero_capacity,
        "lots_available_negative_rows": invalid_negative_available,
        "lots_available_over_total_rows": invalid_over_capacity,
        "staleness_gt_5m_rows": int((stale_valid > 5).sum()),
        "staleness_gt_15m_rows": int((stale_valid > 15).sum()),
        "staleness_gt_60m_rows": int((stale_valid > 60).sum()),
        "median_staleness_minutes": (
            round(float(stale_valid.median()), 2) if len(stale_valid) else None
        ),
        "p95_staleness_minutes": (
            round(float(stale_valid.quantile(0.95)), 2) if len(stale_valid) else None
        ),
    }

    with open(OUT_DIR / "probe_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\n=== FEASIBILITY SUMMARY ===")
    for key, value in summary.items():
        print(f"{key}: {value}")

    print("\nSuggested interpretation:")
    if successful_requests < len(PROBE_TIMESTAMPS):
        print("- CAUTION: Some historical requests failed; inspect request_status.csv.")
    else:
        print("- Historical timestamp access: PASS")

    join_cov = summary["join_coverage_percent"]
    if join_cov is not None and join_cov >= 95:
        print(f"- Metadata join coverage: PASS ({join_cov}%)")
    elif join_cov is not None and join_cov >= 90:
        print(f"- Metadata join coverage: CAUTION ({join_cov}%)")
    else:
        print(f"- Metadata join coverage: INVESTIGATE ({join_cov}%)")

    if invalid_over_capacity == 0 and invalid_negative_available == 0:
        print("- Basic availability consistency: PASS")
    else:
        print("- Basic availability consistency: CAUTION")

    if duplicate_key_rows == 0:
        print("- One car record per carpark/timestamp: PASS")
    else:
        print(
            "- Duplicate carpark/timestamp rows detected; "
            "we need an aggregation rule before training."
        )

    print(f"\nOutputs written to: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()
