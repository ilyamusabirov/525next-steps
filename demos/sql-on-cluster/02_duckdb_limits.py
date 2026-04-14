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
# Total: well over 4 GB. This will not fit.
#
# **This will OOM.** Let's try it.

# %% INTENTIONAL FAILURE: GROUP BY user_id (~50M unique users)
# Disable spill-to-disk so the hash table must fit entirely in RAM.
# Without this, DuckDB would silently spill the overflow to /tmp/duckdb
# and succeed (slowly). We want to isolate the memory limit.
conn.execute("SET temp_directory = '';")
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
    print("  With 4 threads: 4 partitions x 12.5M entries x 100 B = ~5 GB")
    print("  Plus S3 buffers, decompression, sort buffer -> well over 4 GB")
    print("  memory_limit = 4 GB, spill disabled -> does not fit")

# %% [markdown]
# ## What are our options?
#
# | Option | Tradeoff |
# |--------|----------|
# | Enable spill-to-disk | Hash table overflows to EBS; slower but works if disk is big enough |
# | Reduce threads to 1 | Fewer hash table partitions, but much slower |
# | Increase memory_limit to 12 GB | Uses most of our 16 GB machine |
# | Use SparkSQL on EMR | Distributes hash table across nodes |
#
# Let's try option 1 first: rescue with fewer threads + spill-to-disk.

# %% Rescue attempt: 1 thread, 12 GB
# Re-enable spill-to-disk for a fair retry.
# Fewer threads = fewer hash table partitions = less memory duplication.
# But single-threaded is much slower than 4 threads.
conn.execute("SET temp_directory = '/tmp/duckdb';")
conn.execute("SET threads = 1;")
conn.execute("SET memory_limit = '12GB';")
print("Retrying: threads=1, memory_limit=12GB, spill-to-disk = ON")
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
    print(f"Succeeded in {time.time()-t0:.0f}s (vs ~52s for 10M groups at 4 threads)")
    result
except Exception as e:
    print(f"Still OOM: {e}")
    print("Even 12 GB with 1 thread is not enough for 50M groups.")
    print("-> This is where SparkSQL earns its keep.")

# %% [markdown]
# ## Takeaway
#
# DuckDB handles 22.5 GB of data in 4 GB of RAM because it streams.
# The limit is not data size but **intermediate structure size**:
#
# - 4 categories (GROUP BY category): hash table ~400 bytes -> trivial
# - 10M products (GROUP BY parent_asin): hash table ~1 GB -> fits in RAM
# - 50M users (GROUP BY user_id): hash table ~5 GB -> exceeds 4 GB RAM
#
# DuckDB **can** finish the 50M-user query on one machine: enable
# spill-to-disk, give it more memory, reduce threads. It works,
# but you're consuming most of the machine and running much slower.
# The tradeoff is cost and speed, not capability.
#
# | Approach | 50M-user GROUP BY | Tradeoff |
# |----------|-------------------|----------|
# | DuckDB 4 GB, no spill | OOM | - |
# | DuckDB 12 GB, 1 thread, spill | Succeeds (slow) | Uses most of 16 GB machine, single-threaded |
# | SparkSQL 3-node cluster | Succeeds (fast) | Costs $0.71/hr vs $0.15/hr |
#
# SparkSQL distributes the hash table across nodes: more total memory,
# more parallelism, and the machine is not pegged at its limits.
# Same SQL, different engine.
