"""
Configuration settings for Sprint Project 01.

Defines project paths, constants, and helper functions to retrieve
dataset filename mappings.

Usage:
    from src.config import CLEAN_DATA_ROOT_PATH, get_csv_to_table_mapping
"""

from pathlib import Path

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATASET_ROOT_PATH = Path("datasets")
CLEAN_DATA_ROOT_PATH = Path("cleaned_data")    # where cleaner.py outputs cleaned CSVs
QUERY_RESULTS_ROOT_PATH = PROJECT_ROOT / "tests" / "query_results"  # expected test query outputs

# --- Constants ---
PUBLIC_HOLIDAYS_URL = "https://date.nager.at/api/v3/PublicHolidays"


def get_csv_to_table_mapping() -> dict:
    """
    Returns mapping of SQLite table names to their source CSV filenames.

    :return: dict where key=table_name, value=CSV filename
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
    }
