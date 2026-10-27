-- Run against the full-refresh Gold Parquet export.
-- Prediction target requires a real observation exactly one hour later.
SELECT
    COUNT(*) AS rows,
    COUNT(DISTINCT carpark_number) AS carparks,
    COUNT(DISTINCT hour_slot_sgt) AS hourly_slots,
    COUNT(target_occupancy_1h) AS labeled_rows,
    COUNT(lag_1h) AS rows_with_1h_lag,
    COUNT(lag_24h) AS rows_with_24h_lag,
    COUNT(lag_168h) AS rows_with_168h_lag,
    COUNT(*) FILTER (
        WHERE target_occupancy_1h IS NOT NULL
        AND lag_1h IS NOT NULL AND lag_24h IS NOT NULL
        AND lag_168h IS NOT NULL
        AND rolling_mean_6h IS NOT NULL
    ) AS rows_with_full_model_history,
    MIN(hour_slot_sgt) AS earliest_slot,
    MAX(hour_slot_sgt) AS latest_slot
FROM read_parquet('data/gold/carpark_features_parquet/**/*.parquet');
