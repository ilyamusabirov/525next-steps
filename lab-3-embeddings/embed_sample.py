"""Embed a handful of product names using GitHub Models.

Usage:
    export GITHUB_TOKEN="ghp_your_token_here"
    uv run python embed_sample.py
"""

from openai import OpenAI
import os

client = OpenAI(
    base_url="https://models.inference.ai.azure.com",
    api_key=os.environ["GITHUB_TOKEN"],
)

products = [
    "Organic dark chocolate 85% cocoa",
    "Whole milk 3.25% fat",
    "Crunchy peanut butter",
    "Sparkling mineral water",
    "Frozen cheese pizza",
    "Gluten-free oat cereal",
    "Extra virgin olive oil",
    "Strawberry yogurt 0% fat",
    "Instant ramen noodles",
    "Raw almond butter unsalted",
]

response = client.embeddings.create(
    model="text-embedding-3-small",
    input=products,
)

print(f"Model: text-embedding-3-small")
print(f"Dimensions: {len(response.data[0].embedding)}")
print(f"Products embedded: {len(response.data)}")
print()

for item, product in zip(response.data, products):
    vec = item.embedding
    print(f"  {product[:45]:45s}  [{vec[0]:+.4f}, {vec[1]:+.4f}, {vec[2]:+.4f}, ...]")
