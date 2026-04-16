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

-- Full refresh: always reads ALL parquet files in data/.
-- When new files appear in data/, this asset picks them up on next run.
SELECT *
FROM read_parquet('data/*.parquet')

-- To read directly from S3 instead of local files:
--   SELECT *
--   FROM read_parquet('s3://dsci525-data-2026/bruin-demo/*.parquet')
-- DuckDB picks up credentials from the instance profile automatically.
-- We use local files here so we can simulate "new data arrives"
-- by adding a parquet file between pipeline runs.
