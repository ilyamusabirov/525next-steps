/* @bruin

name: monthly_summary
type: duckdb.sql

materialization:
    type: table
    strategy: delete+insert
    incremental_key: review_month

depends:
  - raw_reviews

columns:
  - name: review_month
    type: date
    checks:
      - name: not_null
      - name: unique
  - name: n_reviews
    type: integer
    checks:
      - name: positive

@bruin */

-- Incremental: only processes the date range specified by --start-date / --end-date.
-- Bruin's delete+insert strategy:
--   1. Runs this query, collects the result
--   2. Gets DISTINCT review_month values from the result
--   3. DELETEs those months from the existing monthly_summary table
--   4. INSERTs the new rows
-- Months outside the date range are untouched.
SELECT
    DATE_TRUNC('month', review_date)::DATE AS review_month,
    COUNT(*) AS n_reviews,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(DISTINCT user_id) AS unique_users,
    COUNT(DISTINCT product_id) AS unique_products
FROM raw_reviews
WHERE review_date >= '{{ start_date }}'::DATE
  AND review_date <  '{{ end_date }}'::DATE
GROUP BY review_month
ORDER BY review_month
