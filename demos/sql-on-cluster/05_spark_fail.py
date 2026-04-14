# %% [markdown]
# # Intentionally failing Spark job
#
# Purpose: show how to read Spark logs when something goes wrong.
# This script creates a job that fails, then shows where to find the error.
#
# **Textbook**: [Observing Your Spark Cluster](https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4a_spark_debugging.html)
#
# **Run on**: EMR primary node (same as 03_spark_sql.py).
#
# **Key debugging tools** (all accessible from VS Code Remote SSH via port forwarding):
# - Spark UI (port 4040): live view while the application is running
# - History Server (port 18080): replay completed applications
# - YARN ResourceManager (port 8088): cluster-level resource allocation

# %% Setup
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Intentional-Failure") \
    .getOrCreate()

# %% [markdown]
# ## Failure 1: Reference a non-existent column
#
# A common mistake: column name typo or schema mismatch.

# %%
df = spark.read.parquet("s3://dsci525-data-2026/amazon_reviews/")
df.createOrReplaceTempView("reviews")

# This will fail: 'review_score' does not exist (it's called 'rating')
try:
    spark.sql("""
        SELECT category, AVG(review_score) AS avg_score
        FROM reviews
        GROUP BY category
    """).show()
except Exception as e:
    print("FAILED (as expected)")
    print(f"Error type: {type(e).__name__}")
    print(f"Message: {e}")
    print()
    print("Where to look:")
    print("  - Spark UI (port 4040) -> SQL tab -> failed query")
    print("  - Driver logs: stderr shows the AnalysisException")

# %% [markdown]
# ## Failure 2: Executor OOM on a skewed aggregation
#
# Force all data to one partition, then try to collect it.
# This simulates a data skew problem.

# %%
try:
    # Repartition to 1 partition forces all data through one executor
    spark.sql("""
        SELECT /*+ REPARTITION(1) */
            user_id, COLLECT_LIST(title) AS all_titles
        FROM reviews
        WHERE category = 'Electronics'
        GROUP BY user_id
    """).show()
except Exception as e:
    print("FAILED (as expected)")
    print(f"Error: {e}")
    print()
    print("Where to look:")
    print("  - Spark UI -> Stages -> failed stage -> task details")
    print("  - Executor logs: stderr shows java.lang.OutOfMemoryError")
    print("  - YARN ResourceManager (port 8088) -> application -> logs")

# %% [markdown]
# ## Reading Spark logs: a checklist
#
# | What happened | Where to look |
# |---------------|--------------|
# | Query syntax/schema error | Driver stderr, Spark UI SQL tab |
# | Task OOM | Executor stderr, Spark UI Stages tab |
# | Shuffle failure | Spark UI Stages tab, executor logs |
# | Job stuck (no progress) | Spark UI Stages tab (pending tasks), YARN queue |
# | Cluster terminated | EMR Console step logs, S3 log bucket |
#
# While a job is running: **Spark UI at port 4040**
# After a job finishes: **History Server at port 18080**
# Cluster-level issues: **YARN ResourceManager at port 8088**
