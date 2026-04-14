/* @bruin

name: suspicious_products
type: duckdb.sql

materialization:
    type: table

depends:
  - clean_products

@bruin */

-- TODO: Find products where the nutriscore grade doesn't match the numbers.
-- Examples of suspicious patterns:
--   Grade 'a' (healthiest) but sugars_100g > 20
--   Grade 'e' (least healthy) but energy_kcal_100g < 100
--
-- Hint: use CASE WHEN to label the type of mismatch

SELECT
    code,
    product_name_en,
    nutriscore_grade,
    energy_kcal_100g,
    sugars_100g,
    proteins_100g,
    CASE
        WHEN nutriscore_grade = 'a' AND sugars_100g > 20
            THEN 'Grade a but high sugar'
        WHEN nutriscore_grade = 'e' AND energy_kcal_100g < 100
            THEN 'Grade e but low energy'
        -- TODO: add more conditions
    END AS flag_reason
FROM clean_products
WHERE (nutriscore_grade = 'a' AND sugars_100g > 20)
   OR (nutriscore_grade = 'e' AND energy_kcal_100g < 100)
