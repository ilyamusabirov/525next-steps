/* @bruin

name: raw_products
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
      - name: accepted_values
        value:
          - a
          - b
          - c
          - d
          - e

@bruin */

SELECT *
FROM read_parquet('https://mds26.tor1.digitaloceanspaces.com/data/525next-steps/food_batch_1.parquet')
WHERE nutriscore_grade IN ('a', 'b', 'c', 'd', 'e')
  AND product_name_en IS NOT NULL
  AND LENGTH(product_name_en) > 2
