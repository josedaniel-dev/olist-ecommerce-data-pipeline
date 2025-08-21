# src/run_pipeline.py
from __future__ import annotations
import argparse
from typing import Dict
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.extract import extract
from src.load import load


def _default_table_to_csv(csv_folder: str) -> Dict[str, str]:
    """
    Map physical table names -> CSV filenames (relative to csv_folder).
    Matches the dataset provided in the repo.
    """
    return {
        "olist_customers": "olist_customers_dataset.csv",
        "olist_geolocation": "olist_geolocation_dataset.csv",
        "olist_order_items": "olist_order_items_dataset.csv",
        "olist_order_payments": "olist_order_payments_dataset.csv",
        "olist_order_reviews": "olist_order_reviews_dataset.csv",
        "olist_orders": "olist_orders_dataset.csv",
        "olist_products": "olist_products_dataset.csv",
        "olist_sellers": "olist_sellers_dataset.csv",
        "product_category_name_translation": "product_category_name_translation.csv",
        # public_holidays is handled by extract() if implemented; no CSV here.
    }


def run_extract_load(csv_folder: str, engine: Engine) -> Dict[str, pd.DataFrame]:
    print(f"[Pipeline] Extracting from: {csv_folder}")
    table_to_csv = _default_table_to_csv(csv_folder)
    tables = extract(csv_folder=csv_folder, table_to_csv=table_to_csv)
    print("[Pipeline] Loading tables into database…")
    load(data_frames=tables, database=engine)
    print("[Pipeline] DB ready.")
    return tables


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the ETL (extract + load) for Olist data.")
    parser.add_argument(
        "--csv-folder",
        required=True,
        help="Folder containing the CSV files (e.g., dataset).",
    )
    parser.add_argument(
        "--db",
        required=True,
        help="SQLAlchemy DB URL, e.g. sqlite:////tmp/olist.db",
    )
    parser.add_argument(
        "--echo-sql",
        action="store_true",
        help="Echo SQL emitted by SQLAlchemy (debug).",
    )
    args = parser.parse_args()

    engine = create_engine(args.db, future=True, echo=args.echo_sql)
    run_extract_load(args.csv_folder, engine)
    print("[Pipeline] Done.")


if __name__ == "__main__":
    main()
