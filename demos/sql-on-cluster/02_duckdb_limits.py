# %% [markdown]
# # DuckDB: where single-machine SQL hits the wall
#
# Same 22.5 GB dataset. Harder queries. Watch the hash table
# grow until it exceeds what one machine can handle.
#
# **Textbook**: [SQL on the Cluster > How query complexity affects performance](https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#duckdb-complex-queries)
#
# **Key concept**: DuckDB streams data column-by-column and never loads
# the full dataset into RAM. The memory limit is not about data size,
# it is about intermediate structures (hash tables for GROUP BY, sort
# buffers, DISTINCT sets). High-cardinality GROUP BY is where it breaks.

# %% Setup
import duckdb
import time

conn = duckdb.connect()
conn.execute("SET memory_limit = '4GB';")
conn.execute("SET threads = 4;")
conn.execute("SET temp_directory = '/tmp/duckdb';")
conn.execute("""
    CREATE SECRET (
        TYPE s3, PROVIDER credential_chain, REGION 'ca-central-1'
    )
""")

S3 = "s3://dsci525-data-2026/amazon_reviews/**/*.parquet"

# %% Query 4: GROUP BY parent_asin (~10M unique products)
# For each of ~10 million products, count reviews and average rating.
# DuckDB must build a hash table with one entry per product.
# 10M entries x ~100 bytes each = ~1 GB. Fits in our 4 GB budget.
t0 = time.time()
result = conn.execute(f"""
    SELECT parent_asin,
           COUNT(*) AS n_reviews,
           ROUND(AVG(rating), 2) AS avg_rating
    FROM read_parquet('{S3}', hive_partitioning=true)
    GROUP BY parent_asin
    ORDER BY n_reviews DESC
    LIMIT 20
""").df()
print(f"GROUP BY 10M products: {time.time()-t0:.1f}s")
result

# %% [markdown]
# 52 seconds. The hash table holds ~10M entries:
#
# | Component | Bytes per entry |
# |-----------|----------------|
# | Key (parent_asin string) | ~26 |
# | Hash value | 8 |
# | COUNT + SUM + COUNT for AVG | 24 |
# | Pointer / metadata | 16 |
# | **Total (with alignment)** | **~100** |
#
# 10M entries x 100 bytes = **~1 GB**. With 4 thread-local partitions,
# each holds ~250 MB. Total ~1-1.5 GB. Fits in the 4 GB budget.

# %% Query 5: GROUP BY + WINDOW (heaviest query)
# Find the top 10 most-reviewed products in each category.
# This needs two expensive operators back to back:
# (1) a hash table for GROUP BY, then (2) a window buffer for ROW_NUMBER.
# Both compete for the same 4 GB memory budget.
t0 = time.time()
result = conn.execute(f"""
    SELECT * FROM (
        SELECT category, parent_asin,
               COUNT(*) AS n,
               ROW_NUMBER() OVER (
                   PARTITION BY category ORDER BY COUNT(*) DESC
               ) AS rank
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY parent_asin, category
    ) WHERE rank <= 10
""").df()
print(f"GROUP BY + WINDOW: {time.time()-t0:.1f}s")
result

# %% [markdown]
# 74 seconds. Two memory-intensive operators back to back:
# hash table for GROUP BY + window buffer for ROW_NUMBER().
# Still fits, but we're pushing the limit.
#
# ## The boundary question
#
# What if we GROUP BY user_id instead of parent_asin?
#
# - ~10M unique products x 100 bytes = ~1 GB hash table -> OK at 4 GB
# - ~50M unique users x 100 bytes = ~5 GB hash table -> ??? at 4 GB
#
# With 4 threads, DuckDB partitions the hash table. Each thread
# builds its own slice: 50M / 4 = 12.5M entries per thread.
# 4 slices x 12.5M x 100 bytes = ~5 GB just for the hash table.
# On top of that, DuckDB needs memory for S3 read buffers,
# parquet decompression, and the ORDER BY sort buffer.
# Total: well over 4 GB.
#
# **What happens?** It depends on whether DuckDB can spill to disk.

# %% DEMO: what happens if we disable spill-to-disk?
# If we turn off the opportunity to store overflow on disk,
# DuckDB must fit the entire hash table in RAM. At 4 GB it cannot.
conn.execute("SET temp_directory = '';")    # disable spill-to-disk
print("Attempting GROUP BY on ~50M unique user IDs...")
print("memory_limit = 4GB, threads = 4, spill-to-disk = OFF")
print()
t0 = time.time()
try:
    conn.execute(f"""
        SELECT user_id,
               COUNT(*) AS n_reviews,
               ROUND(AVG(rating), 2) AS avg_rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY user_id
        ORDER BY n_reviews DESC
        LIMIT 20
    """).df()
except Exception as e:
    elapsed = time.time() - t0
    print(f"FAILED after {elapsed:.0f}s")
    print(f"Error: {e}")
    print()
    print("Why:")
    print("  ~50M unique users x ~100 bytes/entry = ~5 GB hash table")
    print("  memory_limit = 4 GB, spill disabled -> does not fit")

# %% [markdown]
# ## Scaling up: more memory + spill-to-disk
#
# DuckDB **can** handle this on one machine. Re-enable spill-to-disk
# and give it more memory so the hash table fits mostly in RAM
# (overflow pages go to disk via spill).
#
# Our t3a.xlarge has 16 GB total. Setting memory_limit to 12 GB
# leaves ~4 GB for the OS, Python, and S3 read buffers.

# %% Rescue: 12 GB, 4 threads, spill-to-disk ON
conn.execute("SET temp_directory = '/tmp/duckdb';")   # re-enable spill
conn.execute("SET memory_limit = '12GB';")
# Keep threads = 4 (the honest config for this machine).
print("Retrying: threads=4, memory_limit=12GB, spill-to-disk = ON")
t0 = time.time()
try:
    result = conn.execute(f"""
        SELECT user_id,
               COUNT(*) AS n_reviews,
               ROUND(AVG(rating), 2) AS avg_rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY user_id
        ORDER BY n_reviews DESC
        LIMIT 20
    """).df()
    print(f"Succeeded in {time.time()-t0:.0f}s (vs ~52s for 10M groups)")
    print("It works, but uses 75% of this machine's RAM for one query.")
    result
except Exception as e:
    print(f"Still OOM: {e}")

# %% [markdown]
# ## Takeaway
#
# DuckDB handles 22.5 GB of data in 4 GB of RAM because it streams.
# The limit is not data size but **intermediate structure size**:
#
# - 4 categories (GROUP BY category): hash table ~400 bytes -> trivial
# - 10M products (GROUP BY parent_asin): hash table ~1 GB -> fits in 4 GB
# - 50M users (GROUP BY user_id): hash table ~5 GB -> needs 12 GB + spill
#
# Scaling up (more RAM, spill-to-disk) works, but has limits:
#
# - **Memory ceiling**: we're using 75% of this machine for one query.
#   A bigger hash table (100M+ keys, or wider aggregates) may not fit
#   even with all 16 GB.
# - **CPU bound**: only the CPUs on this one machine. 4 threads on a
#   t3a.xlarge is all we get. A second machine's CPUs cannot help.
# - **Network throughput**: one machine's NIC reads all 22.5 GB from S3.
#
# | Approach | 50M-user GROUP BY | Tradeoff |
# |----------|-------------------|----------|
# | DuckDB 4 GB, no spill | OOM | - |
# | DuckDB 12 GB, 4 threads, spill | ~61s | Uses 75% of a 16 GB machine |
# | SparkSQL 3-node cluster | ~32s | Costs $0.71/hr vs $0.15/hr |
#
# SparkSQL distributes the hash table across 3 nodes: more total
# memory, more CPUs reading from S3 in parallel, and no single
# machine is pegged at its limits. Same SQL, different engine.
