# %% [markdown]
# # DuckDB on EC2: querying S3 without a cluster
#
# Three settings + one credential line = full SQL on 22.5 GB of S3 parquet.
# No Spark, no YARN, no HDFS.
#
# **Textbook**: [SQL on the Cluster > DuckDB in the cloud](https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#duckdb-in-cloud)
#
# **Prerequisites**:
# - EC2 instance (t3a.xlarge or larger, 16+ GB RAM)
# - IAM instance profile with S3 read access (see textbook: Instance Profiles)
# - Run `bash setup.sh` first to install DuckDB and register the Jupyter kernel
#
# **To adapt for your own account**: change the S3 path below to your
# own bucket, and make sure your instance profile grants read access to it.

# %% Setup
import duckdb
import time

conn = duckdb.connect()
conn.execute("SET memory_limit = '4GB';")
conn.execute("SET threads = 4;")
conn.execute("SET temp_directory = '/tmp/duckdb';")

# credential_chain picks up the IAM instance profile automatically.
# It checks (in order): env vars, ~/.aws/credentials, instance metadata.
# On EC2 with an instance profile, it uses the role's temporary credentials.
# No access keys in code, no tokens to refresh.
conn.execute("""
    CREATE SECRET (
        TYPE s3, PROVIDER credential_chain, REGION 'ca-central-1'
    )
""")

S3 = "s3://dsci525-data-2026/amazon_reviews/**/*.parquet"
print("Connected. DuckDB version:", duckdb.__version__)

# %% Quick look: what's in this dataset?
t0 = time.time()
conn.execute(f"""
    SELECT
        COUNT(*)                    AS total_reviews,
        COUNT(DISTINCT category)    AS n_categories,
        MIN(year) || '-' || MAX(year) AS year_range
    FROM read_parquet('{S3}', hive_partitioning=true)
""").df()
# ~207M reviews, 4 categories, 1996-2023

# %% Query 1: Category summary (low-cardinality GROUP BY = streaming)
t0 = time.time()
result = conn.execute(f"""
    SELECT category,
           COUNT(*)                   AS n_reviews,
           ROUND(AVG(rating), 2)      AS avg_rating,
           ROUND(AVG(helpful_vote), 2) AS avg_helpful
    FROM read_parquet('{S3}', hive_partitioning=true)
    GROUP BY category
    ORDER BY n_reviews DESC
""").df()
print(f"Category summary: {time.time()-t0:.1f}s")
result

# %% [markdown]
# Only 4 groups in the hash table: trivial for DuckDB.
# The time is almost entirely S3 I/O (streaming 22.5 GB).

# %% Query 2: Filter + sort with LIMIT (top-k optimization)
t0 = time.time()
result = conn.execute(f"""
    SELECT asin, title, helpful_vote, rating
    FROM read_parquet('{S3}', hive_partitioning=true)
    WHERE helpful_vote > 0
    ORDER BY helpful_vote DESC
    LIMIT 20
""").df()
print(f"Filter + sort: {time.time()-t0:.1f}s")
result

# %% [markdown]
# DuckDB never sorts all 49M qualifying rows.
# `ORDER BY ... LIMIT 20` uses a top-k heap: tracks the top 20 as it
# streams, discards everything else. Memory usage is constant.

# %% Query 3: Cross-category join (two DISTINCT sets + hash join)
t0 = time.time()
result = conn.execute(f"""
    SELECT COUNT(*) AS shared_users FROM (
        SELECT DISTINCT user_id
        FROM read_parquet('{S3}', hive_partitioning=true)
        WHERE category = 'Electronics' AND rating >= 4
    ) e JOIN (
        SELECT DISTINCT user_id
        FROM read_parquet('{S3}', hive_partitioning=true)
        WHERE category = 'Books' AND rating >= 4
    ) b USING (user_id)
""").df()
print(f"Cross-category join: {time.time()-t0:.1f}s")
result

# %% [markdown]
# Hive partitioning helps: DuckDB only reads Electronics/ and Books/
# partitions, skipping Home_and_Kitchen/ and Clothing/ entirely.
# The DISTINCT sets are large (~30M + ~20M user IDs) but fit in 4 GB.
