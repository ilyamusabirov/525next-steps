/* @bruin

name: export_summary
type: duckdb.sql

depends:
  - grade_summary

@bruin */

COPY grade_summary TO 'grade_summary.parquet' (FORMAT PARQUET)
