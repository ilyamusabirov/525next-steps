"""Train HistGradientBoosting to predict nutriscore_grade.

Usage:
    uv run python train_model.py
"""

from pathlib import Path

import duckdb
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix

DB_PATH = str(Path(__file__).resolve().parent / "food.db")

FEATURES = [
    "energy_kcal_100g",
    "sugars_100g",
    "proteins_100g",
    # TODO: add more features from the clean_products table
    # "fat_100g", "saturated_fat_100g", "carbohydrates_100g",
    # "salt_100g", "fiber_100g",
]
TARGET = "nutriscore_grade"

def main():
    con = duckdb.connect(DB_PATH, read_only=True)

    df = con.execute(f"""
        SELECT {', '.join(FEATURES)}, {TARGET}
        FROM clean_products
        WHERE {TARGET} IS NOT NULL
    """).fetchdf()

    con.close()

    print(f"Dataset: {len(df)} rows, {len(FEATURES)} features")
    print(f"Grade distribution:\n{df[TARGET].value_counts().sort_index()}\n")

    X = df[FEATURES]
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=525, stratify=y
    )

    model = HistGradientBoostingClassifier(
        max_iter=200,
        max_depth=6,
        random_state=525,
    )

    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    print("Classification report:")
    print(classification_report(y_test, y_pred))

    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_pred))

    # TODO: save confusion matrix data for the Quarto report
    # TODO: save model with joblib for potential API deployment

if __name__ == "__main__":
    main()
