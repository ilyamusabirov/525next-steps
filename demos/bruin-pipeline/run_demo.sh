#!/usr/bin/env bash
# ============================================================
# Bruin Pipeline Demo: Incremental Updates
#
# Simulates a real scenario: review data arrives in batches.
# The pipeline reprocesses only the new time window.
#
#   Run 1: initial load (H1 2022, Jan-Jun)
#   Run 2: new data arrives (H2 2022), only Jul-Dec reprocessed
#   Run 3: break a quality check, show failure blocks downstream
#
# Run this INTERACTIVELY: copy-paste sections one at a time.
#
# Prerequisites:
#   curl -LsSf https://getbruin.com/install/cli | sh
#   uv (for generating test data)
#
# Textbook: Data Pipelines with Bruin
# https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4_pipelines.html
# ============================================================

cd "$(dirname "$0")"

# ============================================================
# SETUP: generate source data (H1 only)
# ============================================================
# Creates data/reviews_2022_h1.parquet (~1080 reviews, Jan-Jun 2022).
# H2 data exists but is NOT in the data/ directory yet.
#
# EXPECT: one parquet file in data/

rm -f food.db data/*.parquet
uv run python generate_data.py --h1

echo ""
echo "=== Source data ==="
ls -lh data/

# ============================================================
# SHOW THE PIPELINE STRUCTURE
# ============================================================
# Open these files in VS Code and walk through:
#
#   assets/raw_reviews.sql      -- reads ALL parquet in data/
#     materialization: table (full refresh every run)
#     quality checks: rating in [1-5], review_date not null
#
#   assets/monthly_summary.sql  -- GROUP BY month, INCREMENTAL
#     materialization: table, strategy: delete+insert
#     incremental_key: review_month
#     uses {{ start_date }} and {{ end_date }} to filter
#
# TALK THROUGH:
#   "raw_reviews always re-reads all source files (full refresh).
#    monthly_summary only reprocesses the date range you specify.
#    Bruin's delete+insert: get the months in the result, DELETE
#    those months from the target, INSERT the new rows.
#    Months outside the range are untouched."

# ============================================================
# RUN 1: Initial load (H1: Jan-Jun 2022)
# ============================================================
# --full-refresh creates the tables for the first time.
# --start-date / --end-date control which months monthly_summary processes.
# The {{ start_date }} and {{ end_date }} template variables in the SQL
# get populated from these flags.
#
# EXPECT:
#   - raw_reviews: loads 1080 reviews from H1 parquet
#   - monthly_summary: computes 6 rows (Jan-Jun)
#   - All quality checks pass

echo ""
echo "=== RUN 1: Initial load (Jan-Jun 2022) ==="
bruin run --full-refresh --start-date 2022-01-01 --end-date 2022-07-01 pipeline.yml

# ============================================================
# QUERY: 6 months of summaries
# ============================================================
# EXPECT: 6 rows (Jan-Jun), each with n_reviews, avg_rating, etc.
# Note the exact values -- we'll verify they don't change after Run 2.

echo ""
echo "=== Monthly summary after Run 1 ==="
bruin query --c duckdb-default --q "SELECT * FROM monthly_summary ORDER BY review_month"

# ============================================================
# NEW DATA ARRIVES: add H2 parquet
# ============================================================
# Simulate: the data team delivers the second half of 2022.
# A new parquet file appears in data/.
# In production this would be a new S3 upload or database dump.
#
# EXPECT: two parquet files in data/

echo ""
echo "=== New data arrives: adding H2 (Jul-Dec 2022) ==="
uv run python generate_data.py
echo ""
ls -lh data/

# ============================================================
# RUN 2: Incremental update (H2: Jul-Dec 2022)
# ============================================================
# No --full-refresh: delete+insert only touches months in the result.
# raw_reviews re-reads ALL files (now both H1 + H2 = 2000 reviews).
# monthly_summary only processes Jul-Dec (the --start-date/--end-date range).
# Jan-Jun rows are UNTOUCHED.
#
# EXPECT:
#   - raw_reviews: loads 2000 reviews (both files)
#   - monthly_summary: computes 6 NEW rows (Jul-Dec)
#   - Jan-Jun rows unchanged from Run 1

echo ""
echo "=== RUN 2: Incremental update (Jul-Dec 2022) ==="
bruin run --start-date 2022-07-01 --end-date 2023-01-01 pipeline.yml

# ============================================================
# VERIFY: 12 months, Jan-Jun untouched
# ============================================================
# EXPECT: 12 rows. Jan-Jun values IDENTICAL to Run 1.
# Jul-Dec are new.
#
# TALK THROUGH: "raw_reviews saw both files and loaded all 2000
# reviews. But monthly_summary only processed Jul-Dec because
# of the date range. Delete+insert removed nothing for those
# months (they didn't exist), then inserted the 6 new rows.
# January through June: untouched."

echo ""
echo "=== Monthly summary after Run 2 (all 12 months) ==="
bruin query --c duckdb-default --q "SELECT * FROM monthly_summary ORDER BY review_month"

# ============================================================
# PAUSE: why this matters
# ============================================================
# TALK THROUGH:
#   "Imagine this is 500M reviews, not 2000. Full refresh of the
#    summary would reprocess all 500M rows every day.
#    With delete+insert, yesterday's run processes only yesterday's
#    data: the same result, fraction of the compute.
#    That's why pipelines have incremental strategies."

# ============================================================
# RUN 3 (optional): break a quality check
# ============================================================
# EDIT in VS Code: open assets/raw_reviews.sql
# In accepted_values for rating, remove "5":
#
#   BEFORE: [1, 2, 3, 4, 5]
#   AFTER:  [1, 2, 3, 4]
#
# EXPECT:
#   - raw_reviews SQL succeeds (table created)
#   - accepted_values check FAILS: reviews with rating=5 are invalid
#   - monthly_summary: UPSTREAM FAILED (never runs)
#
# "The quality gate caught a bad assumption before it reached
#  the summary. In a folder-of-scripts world, the summary would
#  silently compute on bad data."

# bruin run --start-date 2022-07-01 --end-date 2023-01-01 pipeline.yml

# ============================================================
# CLEANUP (after demo)
# ============================================================
# git checkout demos/bruin-pipeline/assets/
# rm -f demos/bruin-pipeline/food.db demos/bruin-pipeline/data/*.parquet
