"""
Cleaning routines P1 (stability / hygiene).

- Idempotent (no side-effects), always work on copies.
- Safe parsing of known date columns.
- Normalization of strings using only strip() (no lower/upper changes).
- Does not recalculate business metrics or states.
- Optional: saves cleaned data into `data_cleaned/`.

Usage example (programmatic):
    from cleaner import clean, save_cleaned
    cleaned = clean(raw_dataframes)  # dict[str, pd.DataFrame]
    save_cleaned(cleaned)            # writes to data_cleaned/

You can also run this file directly to clean CSVs from ./dataset and save to ./data_cleaned
"""

from __future__ import annotations
from typing import Dict, Iterable
import os
import sys
import pandas as pd
import json

def _infer_date_columns(df: pd.DataFrame) -> list[str]:
    """Return the list of known date columns present in df."""
    return sorted(list(set(df.columns) & _DATE_COLUMNS_KNOWN))

# ----------------------------
# Configuration (constants)
# ----------------------------

# Known date columns in the Olist dataset
_DATE_COLUMNS_KNOWN: frozenset[str] = frozenset(
    {
        # orders
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        # order_items
        "shipping_limit_date",
        # reviews
        "review_creation_date",
        "review_creation_timestamp",
        "review_answer_timestamp",
        "review_answer_date",
        # generic (holidays, auxiliary tables)
        "date",
    }
)

# Table name hints for public holidays (to normalize 'date' to midnight)
_HOLIDAYS_TABLE_HINTS: frozenset[str] = frozenset(
    {"public_holidays", "holidays", "brazil_public_holidays", "feriados"}
)

# Default I/O directories for standalone execution
DEFAULT_DATASET_DIR = "dataset"
DEFAULT_OUT_DIR = "data_cleaned"


# ----------------------------
# Internal helpers
# ----------------------------

def _strip_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    print("[Cleaner] Stripping whitespace from object columns...")
    if df.empty:
        print("[Cleaner] DataFrame is empty, skipping strip.")
        return df.copy()
    out = df.copy()
    obj_cols = out.select_dtypes(include=["object"]).columns
    for col in obj_cols:
        print(f"[Cleaner]   - Stripping column: {col}")
        out[col] = out[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return out


def _parse_dates_safe(df: pd.DataFrame, only: Iterable[str] | None = None) -> pd.DataFrame:
    print("[Cleaner] Parsing date columns safely...")
    if df.empty:
        print("[Cleaner] DataFrame is empty, skipping date parsing.")
        return df.copy()
    out = df.copy()
    candidates = set(only) if only is not None else _DATE_COLUMNS_KNOWN
    for col in (set(out.columns) & candidates):
        print(f"[Cleaner]   - Parsing column as datetime: {col}")
        out[col] = pd.to_datetime(out[col], errors="coerce", utc=False)
    return out


def _normalize_holidays_date(df: pd.DataFrame) -> pd.DataFrame:
    print("[Cleaner] Checking for holidays normalization...")
    if df.empty or "date" not in df.columns:
        print("[Cleaner] No 'date' column found or DataFrame empty.")
        return df.copy()
    out = df.copy()
    print("[Cleaner] Normalizing 'date' column to midnight.")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    return out


# ----------------------------
# Public API
# ----------------------------

def clean(data_frames: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Return a new dictionary with safely cleaned DataFrames.

    Per-table operations:
      1) strip() on text columns
      2) safe date parsing
      3) if table looks like holidays, normalize 'date' column
    """
    print("[Cleaner] Starting cleaning process for all tables...")
    cleaned: Dict[str, pd.DataFrame] = {}

    for table_name, df in (data_frames or {}).items():
        print(f"[Cleaner] Processing table: {table_name}")
        step = _strip_object_columns(df)
        step = _parse_dates_safe(step)

        if table_name.lower() in _HOLIDAYS_TABLE_HINTS:
            print(f"[Cleaner] {table_name} identified as holidays table. Normalizing 'date'.")
            step = _normalize_holidays_date(step)

        cleaned[table_name] = step
        print(f"[Cleaner] Finished table: {table_name} (rows={len(step)}, cols={len(step.columns)})")

    print("[Cleaner] Cleaning process completed.")
    return cleaned


def save_cleaned(cleaned: Dict[str, pd.DataFrame], out_dir: str = DEFAULT_OUT_DIR) -> None:
    """
    Save cleaned DataFrames as CSV and write a _schema.json containing date columns per table.
    """
    print(f"[Cleaner] Saving cleaned data to directory: {out_dir}")
    os.makedirs(out_dir, exist_ok=True)
    schema: dict[str, list[str]] = {}
    for name, df in cleaned.items():
        out_path = os.path.join(out_dir, f"{name}.csv")
        # Collect which date columns this table has (to restore later)
        schema[name] = _infer_date_columns(df)
        print(f"[Cleaner]   - Saving table '{name}' to {out_path} (rows={len(df)}, cols={len(df.columns)})")
        df.to_csv(out_path, index=False)
    schema_path = os.path.join(out_dir, "_schema.json")
    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)
    print(f"[Cleaner] Wrote schema with date columns to {schema_path}")

def load_cleaned(out_dir: str = DEFAULT_OUT_DIR) -> Dict[str, pd.DataFrame]:
    """
    Load all CSVs from out_dir and parse date columns based on _schema.json.
    """
    print(f"[Cleaner] Loading cleaned data from: {out_dir}")
    schema_path = os.path.join(out_dir, "_schema.json")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema: dict[str, list[str]] = json.load(f)

    loaded: Dict[str, pd.DataFrame] = {}
    for name, date_cols in schema.items():
        csv_path = os.path.join(out_dir, f"{name}.csv")
        if not os.path.exists(csv_path):
            print(f"[Cleaner] WARNING: Missing CSV for '{name}' at {csv_path}, skipping.")
            continue
        print(f"[Cleaner]   - Reading {csv_path} with parse_dates={date_cols}")
        loaded[name] = pd.read_csv(csv_path, parse_dates=date_cols)
    print("[Cleaner] Finished loading cleaned data.")
    return loaded


def clean_and_save_from_folder(dataset_dir: str = DEFAULT_DATASET_DIR,
                               out_dir: str = DEFAULT_OUT_DIR) -> None:
    """
    Convenience runner:
      - Loads expected CSVs from `dataset_dir`
      - Cleans them with `clean`
      - Saves output to `out_dir`
    """
    print(f"[Cleaner] Standalone run: loading from '{dataset_dir}', writing to '{out_dir}'")

    expected_files = {
        "orders": "olist_orders_dataset.csv",
        "order_items": "olist_order_items_dataset.csv",
        "reviews": "olist_order_reviews_dataset.csv",
        "sellers": "olist_sellers_dataset.csv",
        "products": "olist_products_dataset.csv",
        "payments": "olist_order_payments_dataset.csv",
        "geolocation": "olist_geolocation_dataset.csv",
        # Add more here if you want to clean them too (e.g., customers, translation)
        "customers": "olist_customers_dataset.csv",
        "translation": "product_category_name_translation.csv",
    }

    raw_data: Dict[str, pd.DataFrame] = {}
    missing: list[str] = []

    for name, fname in expected_files.items():
        path = os.path.join(dataset_dir, fname)
        if not os.path.exists(path):
            print(f"[Cleaner] WARNING: {path} not found, skipping '{name}'.")
            missing.append(path)
            continue
        print(f"[Cleaner] Loading {path} ...")
        try:
            raw_data[name] = pd.read_csv(path)
            print(f"[Cleaner]   - Loaded '{name}' (rows={len(raw_data[name])}, cols={len(raw_data[name].columns)})")
        except Exception as e:
            print(f"[Cleaner] ERROR: failed to load {path}: {e}")

    if not raw_data:
        print("[Cleaner] No datasets loaded. Nothing to clean. Exiting.")
        return

    cleaned = clean(raw_data)
    save_cleaned(cleaned, out_dir=out_dir)

    if missing:
        print("[Cleaner] NOTE: Some expected files were missing:")
        for m in missing:
            print(f"[Cleaner]   - {m}")

    print("[Cleaner] Standalone execution complete.")


__all__ = ["clean", "save_cleaned", "clean_and_save_from_folder"]


# ----------------------------
# CLI entrypoint
# ----------------------------
if __name__ == "__main__":
    # Allow optional CLI args:
    #   python cleaner.py               -> uses defaults: dataset/ -> data_cleaned/
    #   python cleaner.py data inputs   -> data_cleaned/
    #   python cleaner.py data out out_dir
    args = sys.argv[1:]
    dataset_dir = args[0] if len(args) >= 1 else DEFAULT_DATASET_DIR
    out_dir = args[1] if len(args) >= 2 else DEFAULT_OUT_DIR
    clean_and_save_from_folder(dataset_dir=dataset_dir, out_dir=out_dir)
