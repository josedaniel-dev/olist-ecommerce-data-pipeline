# cleaner.py
"""
Data cleaning script for Olist dataset.

Purpose:
- Prepare raw CSVs to match test expectations for ETL pipeline.
- Keep all keys (no dropping orphans), leave filtering to SQL later.

Cleaning Gates:
1. Monetary columns as floats, NaNs → 0.0, rounded to 2 decimals.
   No unintended negatives unless inherent to source data.
2. Merge product categories to English using translation table.
   Missing translations → 'unknown'.
3. Parse order/review date columns to datetime64[ns] (NaT allowed).
4. Save cleaned CSVs to CLEAN_DATA_ROOT_PATH with original filenames.
"""

import pandas as pd
from pathlib import Path
from src.config import DATASET_ROOT_PATH, CLEAN_DATA_ROOT_PATH, get_csv_to_table_mapping

# Monetary columns by table
MONETARY_COLS = {
    "olist_order_items": ["price", "freight_value"],
    "olist_order_payments": ["payment_value"],
}

# Orders date columns
ORDER_DATE_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

# Reviews date columns
REVIEW_DATE_COLS = [
    "review_creation_date",
    "review_answer_timestamp",
]


def clean_monetary(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Ensure monetary columns are float, NaNs → 0.0, rounded to 2 decimals."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).round(2)
            # Ensure no unintended negatives
            if (df[col] < 0).any():
                print(f"Warning: Negative values found in {col}, keeping as per source.")
    return df


def parse_dates(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Convert specified columns to datetime64[ns] (NaT allowed)."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def merge_category_translation(products_df: pd.DataFrame, translation_df: pd.DataFrame) -> pd.DataFrame:
    """Merge Portuguese product categories into English; nulls → 'unknown'."""
    merged = products_df.merge(
        translation_df,
        how="left",
        left_on="product_category_name",
        right_on="product_category_name"
    )
    merged["product_category_name"] = merged["product_category_name_english"].fillna("unknown")
    merged = merged.drop(columns=["product_category_name_english"])
    return merged


def main():
    CLEAN_DATA_ROOT_PATH.mkdir(parents=True, exist_ok=True)
    mapping = get_csv_to_table_mapping()

    # Load translation table
    translation_df = pd.read_csv(DATASET_ROOT_PATH / mapping["product_category_name_translation"])

    for table, filename in mapping.items():
        file_path = DATASET_ROOT_PATH / filename
        df = pd.read_csv(file_path)

        # Apply monetary cleaning if needed
        if table in MONETARY_COLS:
            df = clean_monetary(df, MONETARY_COLS[table])

        # Parse dates where relevant
        if table == "olist_orders":
            df = parse_dates(df, ORDER_DATE_COLS)
        elif table == "olist_order_reviews":
            df = parse_dates(df, REVIEW_DATE_COLS)

        # Merge product categories translation
        if table == "olist_products":
            df = merge_category_translation(df, translation_df)

        # Save cleaned CSV
        df.to_csv(CLEAN_DATA_ROOT_PATH / filename, index=False)

    print(f"Cleaning complete. Files saved to {CLEAN_DATA_ROOT_PATH}")


if __name__ == "__main__":
    main()
