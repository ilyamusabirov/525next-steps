"""Embed product names and store vectors in DuckDB.

Usage:
    export GITHUB_TOKEN="ghp_your_token_here"
    uv run python embed_to_duckdb.py
"""

import os
import time

import duckdb
from openai import OpenAI

DATA_URL = "https://mds26.tor1.digitaloceanspaces.com/data/525next-steps/food_sample_50k.parquet"
DB_PATH = "products.db"
N_PRODUCTS = 1000
BATCH_SIZE = 50  # GitHub Models handles 50 well within rate limits
MODEL = "text-embedding-3-small"

client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=os.environ["GITHUB_TOKEN"],
)

# --- Load product names from parquet ---
con = duckdb.connect(DB_PATH)

con.execute(f"""
    CREATE OR REPLACE TABLE products AS
    SELECT code, product_name_en, nutriscore_grade, energy_kcal_100g, sugars_100g
    FROM read_parquet('{DATA_URL}')
    WHERE product_name_en IS NOT NULL AND LENGTH(product_name_en) > 3
    LIMIT {N_PRODUCTS}
""")

products = con.execute(
    "SELECT code, product_name_en FROM products ORDER BY code"
).fetchall()

print(f"Loaded {len(products)} products from parquet")

# --- Create embeddings table ---
con.execute("""
    CREATE OR REPLACE TABLE product_embeddings (
        code VARCHAR,
        product_name VARCHAR,
        embedding FLOAT[1536]
    )
""")

# --- Batch embed and insert ---
start = time.time()
total_embedded = 0

for i in range(0, len(products), BATCH_SIZE):
    batch = products[i : i + BATCH_SIZE]
    names = [name for _, name in batch]
    codes = [code for code, _ in batch]

    response = client.embeddings.create(model=MODEL, input=names)

    for code, name, item in zip(codes, names, response.data):
        con.execute(
            "INSERT INTO product_embeddings VALUES (?, ?, ?)",
            [code, name, item.embedding],
        )

    total_embedded += len(batch)
    elapsed = time.time() - start
    print(f"  Embedded {total_embedded}/{len(products)} ({elapsed:.1f}s)")

    if i + BATCH_SIZE < len(products):
        time.sleep(0.5)

elapsed = time.time() - start
print(f"\nDone: {total_embedded} products in {elapsed:.1f}s")
print(f"Database: {DB_PATH}")
print(f"Rate: {total_embedded / elapsed:.1f} products/s")

con.close()
