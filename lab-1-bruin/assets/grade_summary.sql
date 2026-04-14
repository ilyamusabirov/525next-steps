/* @bruin

name: grade_summary
type: duckdb.sql

materialization:
    type: table

depends:
  - raw_products

columns:
  - name: nutriscore_grade
    type: varchar
    checks:
      - name: not_null
      - name: unique
  - name: n_products
    type: integer
    checks:
      - name: positive

@bruin */

SELECT
    nutriscore_grade,
    COUNT(*) AS n_products,
    ROUND(AVG(energy_kcal_100g), 1) AS avg_kcal,
    ROUND(AVG(sugars_100g), 2) AS avg_sugar,
    ROUND(AVG(proteins_100g), 2) AS avg_protein
FROM raw_products
GROUP BY nutriscore_grade
ORDER BY nutriscore_grade
