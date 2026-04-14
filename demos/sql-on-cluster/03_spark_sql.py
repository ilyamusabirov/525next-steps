# %% [markdown]
# # SparkSQL on EMR: same SQL, distributed
#
# Run this on the **EMR primary node** (via VS Code Remote SSH or pyspark shell).
# The exact same queries that DuckDB ran on one machine,
# now distributed across the cluster.
#
# **Textbook**: [SQL on the Cluster > SparkSQL](https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#sparksql)
#
# **How to run**:
#   - Interactive (VS Code): open this file on EMR primary node, run cells
#   - Interactive (terminal): `pyspark` shell, paste cells one at a time
#   - Batch: `spark-submit 03_spark_sql.py`
#
# **To adapt for your own account**: change the S3 path below.
# PySpark is pre-installed on EMR; no additional setup needed.
#
# **While this runs**: open the Spark UI at http://localhost:4040
# (VS Code Remote SSH auto-forwards the port) to watch stages and tasks.

# %% Setup
from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("SQL-Demo") \
    .getOrCreate()

S3 = "s3://dsci525-data-2026/amazon_reviews/"

# Read all partitions at once
df = spark.read.parquet(S3)
df.createOrReplaceTempView("reviews")
print(f"Partitions: {df.rdd.getNumPartitions()}")

# %% Row count
# Quick sanity check: count all rows to verify Spark can read the S3 data.
# This also forces Spark to discover all parquet files (lazy until first action).
t0 = time.time()
spark.sql("SELECT COUNT(*) AS total_reviews FROM reviews").show()
print(f"{time.time()-t0:.1f}s")

# %% Query 1: Category summary
# Same query as DuckDB Query 1: count reviews per category.
# Only 4 groups, so the distributed shuffle is overkill here.
# Spark is slower (~7s vs DuckDB ~4s) because of YARN scheduling overhead.
t0 = time.time()
spark.sql("""
    SELECT category,
           COUNT(*)              AS n_reviews,
           ROUND(AVG(rating), 2) AS avg_rating
    FROM reviews
    GROUP BY category
    ORDER BY n_reviews DESC
""").show()
print(f"Category summary: {time.time()-t0:.1f}s")

# %% Query 2: Filter + sort
# Find the 20 most-helpful reviews across 207M rows.
# Spark distributes the top-k selection across executors,
# then merges the per-executor top-20 lists on the driver.
t0 = time.time()
spark.sql("""
    SELECT asin, title, helpful_vote, rating
    FROM reviews
    WHERE helpful_vote > 0
    ORDER BY helpful_vote DESC
    LIMIT 20
""").show(truncate=40)
print(f"Filter + sort: {time.time()-t0:.1f}s")

# %% Query 3: GROUP BY parent_asin (~10M products)
# For each of ~10M products, count reviews and average rating.
# The hash table is distributed across executors (each holds a slice).
# 8 executor cores read S3 in parallel vs DuckDB's 4 threads on one machine.
t0 = time.time()
spark.sql("""
    SELECT parent_asin,
           COUNT(*) AS n_reviews,
           ROUND(AVG(rating), 2) AS avg_rating
    FROM reviews
    GROUP BY parent_asin
    ORDER BY n_reviews DESC
    LIMIT 20
""").show()
print(f"GROUP BY 10M products: {time.time()-t0:.1f}s")
# DuckDB: 52s. SparkSQL: ~32s on 3-node cluster.

# %% Query 4: GROUP BY + WINDOW
# Top 10 most-reviewed products per category.
# Two expensive operators: GROUP BY builds a distributed hash table,
# then ROW_NUMBER() runs a window sort on each category partition.
t0 = time.time()
spark.sql("""
    SELECT * FROM (
        SELECT category, parent_asin,
               COUNT(*) AS n,
               ROW_NUMBER() OVER (
                   PARTITION BY category ORDER BY COUNT(*) DESC
               ) AS rank
        FROM reviews
        GROUP BY parent_asin, category
    ) WHERE rank <= 10
""").show()
print(f"GROUP BY + WINDOW: {time.time()-t0:.1f}s")
# DuckDB: 74s. SparkSQL: ~27s.

# %% Query 5: the one DuckDB couldn't do -- GROUP BY user_id
# ~50M unique users. DuckDB OOM'd because the hash table (~5 GB)
# exceeded its 4 GB memory limit. Spark distributes the hash table
# across executors, each with its own JVM heap. Combined: ~20 GB.
t0 = time.time()
spark.sql("""
    SELECT user_id,
           COUNT(*) AS n_reviews,
           ROUND(AVG(rating), 2) AS avg_rating
    FROM reviews
    GROUP BY user_id
    ORDER BY n_reviews DESC
    LIMIT 20
""").show()
print(f"GROUP BY ~50M users: {time.time()-t0:.1f}s")
# DuckDB: OOM. SparkSQL: distributes the hash table across executors.

# %% [markdown]
# ## Why SparkSQL handles the 50M-user GROUP BY
#
# SparkSQL distributes the hash table across executors.
# Each executor holds a partition of the hash table in its own JVM heap.
# With 8 executor cores across 2 core nodes (each 16 GB), the combined
# memory budget is ~20 GB, vs DuckDB's 4-12 GB on one machine.
#
# The tradeoff: SparkSQL adds shuffle overhead (serialization + network)
# for every GROUP BY. On small queries, this overhead makes it slower
# than DuckDB. On large queries, the distributed memory wins.

# %% Cross-category join
# How many users gave 4+ star reviews in BOTH Electronics and Books?
# Spark builds two distributed DISTINCT sets, then hash-joins them.
# Same logic as DuckDB Query 3, but the join runs across the cluster.
t0 = time.time()
spark.sql("""
    SELECT COUNT(*) AS shared_users FROM (
        SELECT DISTINCT user_id FROM reviews
        WHERE category = 'Electronics' AND rating >= 4
    ) e JOIN (
        SELECT DISTINCT user_id FROM reviews
        WHERE category = 'Books' AND rating >= 4
    ) b ON e.user_id = b.user_id
""").show()
print(f"Cross-category join: {time.time()-t0:.1f}s")

# %% Comparison table
print("""
| Query                   | DuckDB (4GB/4t) | SparkSQL (3-node) |
|-------------------------|-----------------|-------------------|
| Category summary        |             ~4s |               ~7s |
| Filter + sort (LIMIT)   |            ~14s |               ~5s |
| GROUP BY 10M products   |            ~52s |              ~32s |
| GROUP BY + WINDOW       |            ~74s |              ~27s |
| Cross-category join     |            ~19s |              ~13s |
| GROUP BY 50M users      |         OOM     |              runs |

Cost: DuckDB on t3a.xlarge = $0.15/hr
      SparkSQL on 3-node EMR = $0.71/hr
""")
