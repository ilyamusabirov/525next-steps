"""Generate synthetic review parquet files for the Bruin pipeline demo.

Creates two partitions in data/:
  reviews_2022_h1.parquet  (Jan-Jun 2022, ~1000 reviews)
  reviews_2022_h2.parquet  (Jul-Dec 2022, ~1000 reviews)

Run this once before the demo. The demo starts with only H1,
then "new data arrives" by copying H2 into place.

Usage:
  uv run python generate_data.py          # generates both files
  uv run python generate_data.py --h1     # generates only H1
"""
import duckdb
import sys
from pathlib import Path

# Always write into data/ next to this script, regardless of cwd.
here = Path(__file__).parent
data_dir = here / "data"
data_dir.mkdir(exist_ok=True)

h1_path = data_dir / "reviews_2022_h1.parquet"
h2_path = data_dir / "reviews_2022_h2.parquet"

conn = duckdb.connect()

# Deterministic reviews: 2000 total, dates spread across 2022.
# No random() so results are identical every time.
conn.execute("""
    CREATE TABLE all_reviews AS
    SELECT
        i AS review_id,
        'user_' || (1 + (i * 7 + 13) % 500) AS user_id,
        'product_' || (1 + (i * 3 + 7) % 100) AS product_id,
        1 + (i % 5) AS rating,
        DATE '2022-01-01' + INTERVAL ((i - 1) % 365) DAY AS review_date
    FROM generate_series(1, 2000) AS t(i)
""")

# H1: Jan-Jun 2022
conn.execute(f"""
    COPY (SELECT * FROM all_reviews WHERE review_date < '2022-07-01')
    TO '{h1_path}' (FORMAT PARQUET)
""")
h1_count = conn.execute(
    "SELECT COUNT(*) FROM all_reviews WHERE review_date < '2022-07-01'"
).fetchone()[0]
print(f"{h1_path}: {h1_count} reviews (Jan-Jun 2022)")

if "--h1" not in sys.argv:
    # H2: Jul-Dec 2022
    conn.execute(f"""
        COPY (SELECT * FROM all_reviews WHERE review_date >= '2022-07-01')
        TO '{h2_path}' (FORMAT PARQUET)
    """)
    h2_count = conn.execute(
        "SELECT COUNT(*) FROM all_reviews WHERE review_date >= '2022-07-01'"
    ).fetchone()[0]
    print(f"{h2_path}: {h2_count} reviews (Jul-Dec 2022)")
