# %% [markdown]
# # SparkSQL + MLlib: feature engineering to model training
#
# The killer feature of Spark: SQL for feature generation, then MLlib
# for training, all in one session, no intermediate files.
#
# **Textbook**: [SQL on the Cluster > Feature generation with SparkSQL + ML](https://pages.github.ubc.ca/mds-2025-26/DSCI_525_web-cloud-comp_book/lectures/w4c_sql_on_cluster.html#sparksql-ml)
#
# **Run on**: EMR primary node (same as 03_spark_sql.py).

# %% Setup
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import time

spark = SparkSession.builder \
    .appName("ReviewFeatures-ML") \
    .getOrCreate()

df = spark.read.parquet("s3://dsci525-data-2026/amazon_reviews/")
df.createOrReplaceTempView("reviews")

# %% Step 1: Feature engineering with SparkSQL
t0 = time.time()
features = spark.sql("""
    SELECT
        parent_asin,
        COUNT(*)                                AS review_count,
        AVG(rating)                             AS avg_rating,
        STDDEV(rating)                          AS rating_spread,
        AVG(helpful_vote)                       AS avg_helpful,
        SUM(CASE WHEN verified_purchase THEN 1 ELSE 0 END)
            / COUNT(*)                          AS verified_ratio,
        AVG(LENGTH(text))                       AS avg_text_length
    FROM reviews
    WHERE year >= 2020
    GROUP BY parent_asin
    HAVING COUNT(*) >= 10
""")
n_products = features.count()
print(f"Feature engineering: {time.time()-t0:.1f}s, {n_products} products")
features.show(5)

# %% Step 2: Assemble feature vector
feature_cols = [
    "review_count", "rating_spread",
    "avg_helpful", "verified_ratio", "avg_text_length"
]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
ml_data = assembler.transform(features).select("features", "avg_rating")

train, test = ml_data.randomSplit([0.8, 0.2], seed=42)
print(f"Train: {train.count()}, Test: {test.count()}")

# %% Step 3: Train a Random Forest
t0 = time.time()
rf = RandomForestRegressor(
    featuresCol="features",
    labelCol="avg_rating",
    numTrees=50,
    seed=42,
)
model = rf.fit(train)
print(f"Training: {time.time()-t0:.1f}s")

# %% Step 4: Evaluate
predictions = model.transform(test)
evaluator = RegressionEvaluator(
    labelCol="avg_rating", predictionCol="prediction", metricName="rmse"
)
rmse = evaluator.evaluate(predictions)
print(f"RMSE: {rmse:.4f}")

predictions.select("avg_rating", "prediction").show(10)

# %% Feature importance
import pandas as pd
importances = model.featureImportances.toArray()
pd.DataFrame({
    "feature": feature_cols,
    "importance": importances
}).sort_values("importance", ascending=False)

# %% [markdown]
# ## What just happened
#
# 1. SparkSQL aggregated 207M reviews into ~N product-level features
# 2. MLlib trained a Random Forest on those features
# 3. Both ran on the same cluster, reading from S3
# 4. No intermediate files, no data movement between systems
#
# This is the SparkSQL + MLlib pattern: SQL for feature engineering,
# ML for training, all distributed.
