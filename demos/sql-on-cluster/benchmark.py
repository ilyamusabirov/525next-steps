"""Benchmark DuckDB queries at two memory settings on the same machine."""
import duckdb
import time

S3 = "s3://dsci525-data-2026/amazon_reviews/**/*.parquet"

QUERIES = [
    ("Category summary",
     """SELECT category, COUNT(*) AS n_reviews, ROUND(AVG(rating), 2) AS avg_rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY category ORDER BY n_reviews DESC"""),
    ("Filter + sort (LIMIT 20)",
     """SELECT asin, title, helpful_vote, rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        WHERE helpful_vote > 0 ORDER BY helpful_vote DESC LIMIT 20"""),
    ("Cross-category join",
     """SELECT COUNT(*) AS shared_users FROM (
            SELECT DISTINCT user_id FROM read_parquet('{S3}', hive_partitioning=true)
            WHERE category = 'Electronics' AND rating >= 4
        ) e JOIN (
            SELECT DISTINCT user_id FROM read_parquet('{S3}', hive_partitioning=true)
            WHERE category = 'Books' AND rating >= 4
        ) b ON e.user_id = b.user_id"""),
    ("GROUP BY 10M products",
     """SELECT parent_asin, COUNT(*) AS n_reviews, ROUND(AVG(rating), 2) AS avg_rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY parent_asin ORDER BY n_reviews DESC LIMIT 20"""),
    ("GROUP BY + WINDOW",
     """SELECT * FROM (
            SELECT category, parent_asin, COUNT(*) AS n,
                   ROW_NUMBER() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) AS rank
            FROM read_parquet('{S3}', hive_partitioning=true)
            GROUP BY parent_asin, category
        ) WHERE rank <= 10"""),
    ("GROUP BY 50M users",
     """SELECT user_id, COUNT(*) AS n_reviews, ROUND(AVG(rating), 2) AS avg_rating
        FROM read_parquet('{S3}', hive_partitioning=true)
        GROUP BY user_id ORDER BY n_reviews DESC LIMIT 20"""),
]


def run_config(label, memory_limit, threads):
    print(f"\n{'='*60}")
    print(f"Config: {label}")
    print(f"  memory_limit = {memory_limit}, threads = {threads}")
    print(f"{'='*60}")

    conn = duckdb.connect()
    conn.execute(f"SET memory_limit = '{memory_limit}';")
    conn.execute(f"SET threads = {threads};")
    conn.execute("SET temp_directory = '/tmp/duckdb';")
    conn.execute("""
        CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'ca-central-1')
    """)

    results = {}
    for name, sql in QUERIES:
        sql_final = sql.replace("{S3}", S3)
        t0 = time.time()
        try:
            conn.execute(sql_final).fetchall()
            elapsed = time.time() - t0
            results[name] = f"{elapsed:.1f}s"
            print(f"  {name}: {elapsed:.1f}s")
        except Exception as e:
            elapsed = time.time() - t0
            results[name] = f"FAILED ({elapsed:.0f}s)"
            print(f"  {name}: FAILED after {elapsed:.0f}s - {e}")

    conn.close()
    return results


# Run both configs
r1 = run_config("Constrained (demo settings)", "4GB", 4)
r2 = run_config("Default (75% of 16 GB)", "12GB", 4)

# Print comparison
print(f"\n{'='*60}")
print("COMPARISON")
print(f"{'='*60}")
print(f"{'Query':<30} {'4GB/4t':<15} {'12GB/4t':<15}")
print("-" * 60)
for name, _ in QUERIES:
    print(f"{name:<30} {r1[name]:<15} {r2[name]:<15}")
