"""Source API access; no Azure or local filesystem concerns."""
from __future__ import annotations

import os
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

AVAILABILITY_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"
METADATA_URL = "https://data.gov.sg/api/action/datastore_search"
METADATA_DATASET_ID = "d_23f946fa557947f93a8043bbef41dd09"


def make_session() -> requests.Session:
    session = requests.Session()
    key = os.getenv("DATA_GOV_SG_API_KEY", "").strip()
    if key:
        session.headers["x-api-key"] = key
    session.headers["Accept"] = "application/json"
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_availability(session: requests.Session, when_sgt: datetime | None = None) -> dict:
    """For historical queries, send local Singapore time, not UTC or a timezone suffix."""
    params = {"date_time": when_sgt.strftime("%Y-%m-%dT%H:%M:%S")} if when_sgt else None
    response = session.get(AVAILABILITY_URL, params=params, timeout=35)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, dict) or not isinstance(body.get("items"), list):
        raise ValueError("Unexpected availability response: missing items[]")
    if not body["items"]:
        raise ValueError("Availability response items[] is empty")
    return body


def fetch_metadata_page(session: requests.Session, *, offset: int, limit: int = 500) -> dict:
    response = session.get(
        METADATA_URL,
        params={"resource_id": METADATA_DATASET_ID, "offset": offset, "limit": limit},
        timeout=35,
    )
    response.raise_for_status()
    body = response.json()
    result = body.get("result") if isinstance(body, dict) else None
    if body.get("success") is False or not isinstance(result, dict):
        raise ValueError("Unexpected metadata response")
    if not isinstance(result.get("records"), list):
        raise ValueError("Metadata response missing result.records[]")
    return result
