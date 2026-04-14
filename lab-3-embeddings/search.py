"""Semantic search over food products using embeddings in DuckDB.

Usage:
    export GITHUB_TOKEN="ghp_your_token_here"
    uv run python search.py "healthy breakfast cereal"
"""

import os
import sys

import duckdb
from openai import OpenAI

DB_PATH = "products.db"

if len(sys.argv) < 2:
    print("Usage: uv run python search.py <query>")
    print('Example: uv run python search.py "sugar free snack"')
    sys.exit(1)

query = " ".join(sys.argv[1:])

client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=os.environ["GITHUB_TOKEN"],
)

# Embed the search query
response = client.embeddings.create(
    model="text-embedding-3-small",
    input=[query],
)
query_vec = response.data[0].embedding

# Search in DuckDB
con = duckdb.connect(DB_PATH, read_only=True)

results = con.execute("""
    SELECT
        pe.product_name,
        p.nutriscore_grade,
        p.energy_kcal_100g,
        p.sugars_100g,
        ROUND(array_cosine_similarity(pe.embedding, ?::FLOAT[1536]), 4) AS similarity
    FROM product_embeddings pe
    JOIN products p ON pe.code = p.code
    ORDER BY similarity DESC
    LIMIT 10
""", [query_vec]).fetchall()

print(f'\nSearch: "{query}"\n')
print(f"{'Product':<50s} {'Grade':>5s} {'kcal':>6s} {'Sugar':>6s} {'Sim':>6s}")
print("-" * 80)
for name, grade, kcal, sugar, sim in results:
    grade_str = grade if grade else "?"
    kcal_str = f"{kcal:.0f}" if kcal else "?"
    sugar_str = f"{sugar:.1f}" if sugar else "?"
    print(f"{name[:50]:<50s} {grade_str:>5s} {kcal_str:>6s} {sugar_str:>6s} {sim:>6.4f}")

con.close()
