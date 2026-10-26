# HDB Parking Availability Prediction

Aplikasi dashboard analitik yang memprediksi ketersediaan parkir di HDB Singapore

## Host requirements
- Windows 11 with WSL2 + Docker Desktop **or** an Ubuntu machine with Docker Engine and the Compose plugin.
- Recommended: 8 GB RAM minimum, 12–16 GB preferable; ~5 GB of free disk for the development image/dependencies. Docker is used for reproducibility, **not** Kubernetes.
- Internet access from the container to `api.data.gov.sg`, `data.gov.sg`, PyPI, and Maven repositories.

## PowerShell setup
```powershell
cd D:\HDBParkingLot
# Extract this ZIP here as a directory named hdb-parking-onprem-starter, then:
cd .\hdb-parking-onprem-starter
Copy-Item .env.example .env
docker compose build
```

## First commands
```powershell
# All tests work WITHOUT contacting the external API
# and do not require a Spark startup.
docker compose run --rm dev pytest -q

# Confirm exact historical API requests and output paths without downloading anything.
docker compose run --rm dev hdb backfill --start 2026-10-01T00:00:00 --hours 3 --dry-run

# Fetch metadata and a small historical sample (each HTTP request is rate-limited/retried).
docker compose run --rm dev hdb metadata
docker compose run --rm dev hdb backfill --start 2026-10-01T00:00:00 --hours 24

docker compose run --rm dev hdb validate

# Bronze -> Silver (Delta + Parquet) using local PySpark.
# The first Spark invocation may download compatible Delta JARs from Maven.
docker compose run --rm dev python -m hdb_parking.etl_spark
```

## Other operations
```powershell
# One latest real-time snapshot:
docker compose run --rm dev hdb current

# FOUR weeks hourly: 28 * 24 = 672 snapshots.
# This will send up to 672 historical requests; use the 24-hour sample first.
docker compose run --rm dev hdb backfill --start 2026-09-01T00:00:00 --hours 672

# Explore the exported Silver Parquet in DuckDB (local):
docker compose run --rm dev python -c "import duckdb; print(duckdb.sql(\"SELECT COUNT(*) FROM read_parquet('data/silver/carpark_availability_parquet/**/*.parquet')\").fetchall())"
```

## Layout
```text
data/
  bronze/
    carpark_availability/year=2026/month=10/day=01/*.json
    hdb_carpark_information/metadata.json + metadata.csv
  silver/
    carpark_availability_delta/_delta_log/...
    carpark_availability_parquet/part-*.parquet
  gold/                   # future feature engineering & prediction tables
models/                   # future versioned model artifacts
```

## Notes
- Source API query uses **SGT (UTC+08:00)**. A no-offset `update_datetime` value is interpreted as SGT, not UTC.
- Bronze keeps all carparks and all lot types from the source. Silver filters to cars (`C`), removes invalid capacities, excludes staleness above 15 minutes, and deduplicates by snapshot timestamp/carpark.
- Every existing snapshot filename is skipped, so backfill is safe to resume. Deleted/truncated files need manual investigation.
- Rate controls default to two seconds between availability calls; API retries back off on 429/5xx. Avoid parallel bulk retrieval.
- Metadata CSV is reference data; it is not yet joined into the initial Silver table.
- The first Silver job uses a full refresh rather than incremental Delta MERGE. MERGE, Gold features, model training, retraining scheduler, inference API, structured-streaming demo, Power BI, and Azure deployment are explicitly next phases.
- **Passing pytest does not prove live API connectivity, Docker build success, or Spark startup.** Validate those on the target machine.

## Official source docs
- https://data.gov.sg/datasets/d_ca933a644e55d34fe21f28b8052fac63/view
- https://data.gov.sg/datasets/d_23f946fa557947f93a8043bbef41dd09/view
- https://guide.data.gov.sg/developer-guide/dataset-apis/search-and-filter-within-dataset
- https://docs.delta.io/releases/
