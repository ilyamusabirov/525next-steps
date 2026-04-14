/* @bruin

name: food_products
type: duckdb.sql

materialization:
    type: table

columns:
  - name: code
    type: varchar
    checks:
      - name: not_null
  - name: nutriscore_grade
    type: varchar
    checks:
      - name: not_null

@bruin */

-- Read from DigitalOcean Spaces over HTTPS (S3-compatible, no credentials)
SELECT *
FROM read_parquet('https://mds26.tor1.digitaloceanspaces.com/data/525next-steps/food_sample_50k.parquet')
WHERE nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
