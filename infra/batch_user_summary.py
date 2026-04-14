"""
Batch Spark job for transient cluster step submission.

This is the query that OOM'd DuckDB: GROUP BY user_id (~50M unique users).
On SparkSQL with 3 nodes, it completes in minutes.

This script is submitted as an EMR step via infra/transient_step.sh.
The cluster starts, runs this script, writes results to S3, and
auto-terminates. No SSH, no interactive session needed.

Textbook: SQL on the Cluster > SparkSQL
https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#sparksql

Upload to S3 before submitting:
  aws s3 cp infra/batch_user_summary.py s3://dsci525-data-2026/scripts/ \
    --profile ilya-ubc-aws-student --region ca-central-1

To adapt for your own account:
  - Change the S3 paths below to your own bucket
  - Upload this file to your S3 bucket
  - Update SCRIPT_S3 in infra/transient_step.sh
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Batch-User-Summary") \
    .getOrCreate()

df = spark.read.parquet("s3://dsci525-data-2026/amazon_reviews/")
df.createOrReplaceTempView("reviews")

# The query DuckDB couldn't handle: 50M unique users
result = spark.sql("""
    SELECT user_id,
           COUNT(*) AS n_reviews,
           ROUND(AVG(rating), 2) AS avg_rating,
           MIN(year) AS first_year,
           MAX(year) AS last_year,
           COUNT(DISTINCT category) AS n_categories
    FROM reviews
    GROUP BY user_id
    HAVING COUNT(*) >= 5
    ORDER BY n_reviews DESC
""")

# Write results to S3
output_path = "s3://dsci525-data-2026/results/user_summary/"
result.write.mode("overwrite").parquet(output_path)

n_users = result.count()
print(f"Wrote {n_users} user summaries to {output_path}")

spark.stop()
