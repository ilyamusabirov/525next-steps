"""@bruin
name: clean_products
type: python

depends:
  - raw_products
@bruin"""

from pathlib import Path

import duckdb
from pydantic import BaseModel, Field, field_validator
from typing import Literal

DB_PATH = str(Path(__file__).resolve().parent.parent / "food.db")

# ---------- Pydantic schema ----------
# TODO: Add fields for fat_100g, saturated_fat_100g,
#       carbohydrates_100g, salt_100g, fiber_100g.
#       Use Field(ge=0) to reject negative values.

class Product(BaseModel):
    code: str
    product_name_en: str
    nutriscore_grade: Literal["a", "b", "c", "d", "e"]
    nutriscore_score: int = Field(ge=-15, le=40)
    energy_kcal_100g: float = Field(ge=0, le=5000)
    sugars_100g: float = Field(ge=0)
    proteins_100g: float = Field(ge=0)

    @field_validator("product_name_en")
    @classmethod
    def name_not_empty(cls, v: str) -> str:
        if len(v.strip()) < 2:
            raise ValueError("Product name too short")
        return v.strip()


def main():
    con = duckdb.connect(DB_PATH)

    # Read from the upstream Bruin asset (raw_products table)
    rows = con.execute("SELECT * FROM raw_products").fetchall()
    col_names = [desc[0] for desc in con.description]

    clean_rows = []
    errors = 0

    for row in rows:
        row_dict = dict(zip(col_names, row))
        try:
            product = Product(**row_dict)
            clean_rows.append(product.model_dump())
        except Exception:
            errors += 1

    print(f"Valid: {len(clean_rows)}, Rejected: {errors}")

    # Write clean data back to DuckDB
    if clean_rows:
        import pandas as pd
        df = pd.DataFrame(clean_rows)
        con.execute("CREATE OR REPLACE TABLE clean_products AS SELECT * FROM df")

    con.close()


if __name__ == "__main__":
    main()
