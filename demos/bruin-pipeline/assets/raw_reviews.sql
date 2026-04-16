/* @bruin

name: raw_reviews
type: duckdb.sql

materialization:
    type: table

columns:
  - name: rating
    type: integer
    checks:
      - name: not_null
      - name: accepted_values
        value: [1, 2, 3, 4, 5]
  - name: review_date
    type: date
    checks:
      - name: not_null

@bruin */

-- Full refresh: reads ALL parquet files in data/.
-- When new files appear, this asset picks them up on next run.
--
-- To read from S3 instead of local files, replace the path:
--   read_parquet('s3://dsci525-data-2026/bruin-demo/*.parquet')
-- DuckDB picks up instance-profile credentials automatically.
SELECT *
FROM read_parquet('data/*.parquet')
