-- Run with DuckDB after the PySpark Silver ETL completes.
SELECT carpark_number, COUNT(*) AS observations,
       ROUND(AVG(occupancy_rate), 3) AS mean_occupancy
FROM read_parquet('data/silver/carpark_availability_parquet/**/*.parquet')
GROUP BY carpark_number
ORDER BY observations DESC
LIMIT 20;
