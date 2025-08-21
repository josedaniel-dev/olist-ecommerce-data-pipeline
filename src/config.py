# src/config.py
from __future__ import annotations
from pathlib import Path
from typing import Dict


# Raíz del repo
ROOT_DIR = Path(__file__).resolve().parents[1]

# Carpetas base (usadas por cleaner, extract y tests)
DATASET_ROOT_PATH = ROOT_DIR / "dataset"          # datos crudos (los tests leen de aquí)
CLEAN_DATA_ROOT_PATH = ROOT_DIR / "data_cleaned"  # salida del cleaner.py
ARTIFACTS_DIR = ROOT_DIR / "artifacts"
DB_PATH = ARTIFACTS_DIR / "olist.sqlite"

# Carpeta única para SQLs y resultados esperados de tests
QUERIES_DIR = ROOT_DIR / "queries"                                   # donde viven los .sql
QUERY_RESULTS_ROOT_PATH = ROOT_DIR / "tests" / "query_results"       # JSONs de verificación

# API de feriados públicos (usado por tests)
PUBLIC_HOLIDAYS_URL = "https://date.nager.at/api/v3/PublicHolidays"

# ---------------------------------------------------------------------
# Mapa de nombres físicos (SQLite) → canónicos usados por las queries
# ---------------------------------------------------------------------
# Estas claves "canónicas" son las vistas que creará load.create_compat_views()
# para eliminar drift (orders vs olist_orders, etc.).
TABLES: Dict[str, str] = {
    "orders": "olist_orders",
    "order_items": "olist_order_items",
    "products": "olist_products",
    "customers": "olist_customers",
    "payments": "olist_order_payments",
    "sellers": "olist_sellers",
    "reviews": "olist_order_reviews",
    "geolocation": "olist_geolocation",
    # Auxiliares que no se exponen como vista 1:1
    "category_tr": "product_category_name_translation",
    "public_holidays": "public_holidays",
}

# ---------------------------------------------------------------------
# Definiciones de negocio compartidas (una sola fuente de verdad)
# ---------------------------------------------------------------------
# Se usan para construir vistas de negocio (order_item_revenue, delivered_orders,
# product_category_en) y evitar definiciones divergentes en cada SQL.

REVENUE_EXPR = "price + freight_value"  # antes: "price"

DELIVERED_WHERE = (
    "order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL"
)

def get_csv_to_table_mapping() -> Dict[str, str]:
    """
    Mapea nombre de tabla -> nombre de archivo CSV en DATASET_ROOT_PATH.
    (Coincide con lo que esperan los tests.)
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
