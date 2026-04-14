"""@bruin
name: product_embeddings
type: python

depends:
  - clean_products
@bruin"""

import os
import time
from pathlib import Path

import duckdb
from openai import OpenAI

DB_PATH = str(Path(__file__).resolve().parent.parent / "food.db")
N_PRODUCTS = 2000
BATCH_SIZE = 50
MODEL = "text-embedding-3-small"

def main():
    client = OpenAI(
        base_url="https://models.inference.ai.azure.com",
        api_key=os.environ["GITHUB_TOKEN"],
    )

    con = duckdb.connect(DB_PATH)

    # TODO: Load product names from clean_products
    # Hint:
    # products = con.execute("""
    #     SELECT code, product_name_en FROM clean_products
    #     WHERE product_name_en IS NOT NULL
    #     LIMIT ?
    # """, [N_PRODUCTS]).fetchall()

    # TODO: Create embeddings table
    # con.execute("""
    #     CREATE OR REPLACE TABLE product_embeddings (
    #         code VARCHAR,
    #         product_name VARCHAR,
    #         embedding FLOAT[1536]
    #     )
    # """)

    # TODO: Batch embed and insert (use the pattern from Lab 3)
    # for i in range(0, len(products), BATCH_SIZE):
    #     ...

    print("TODO: implement embedding pipeline")
    print("See lab-3-embeddings/embed_to_duckdb.py for the pattern")

    con.close()

if __name__ == "__main__":
    main()
