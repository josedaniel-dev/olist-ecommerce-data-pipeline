# src/load.py

from __future__ import annotations
from typing import Dict
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

# Intentamos tomar los valores desde config; si no existen todavía,
# definimos respaldos seguros para que los tests corran igual.
try:
    from src.config import TABLES, REVENUE_EXPR, DELIVERED_WHERE  # type: ignore
except Exception:  # fallback si aún no actualizaste config.py
    TABLES = {
        "orders": "olist_orders",
        "order_items": "olist_order_items",
        "products": "olist_products",
        "customers": "olist_customers",
        "payments": "olist_order_payments",
        "sellers": "olist_sellers",
        "reviews": "olist_order_reviews",
        "geolocation": "olist_geolocation",
        "category_tr": "product_category_name_translation",
        "public_holidays": "public_holidays",
    }
    REVENUE_EXPR = "price + freight_value"
    DELIVERED_WHERE = (
        "order_status = 'delivered' AND order_delivered_customer_date IS NOT NULL"
    )


def _sanitize_for_sqlite(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte list/dict a None en columnas object (evita errores al insertar)."""
    if df.empty:
        return df
    safe = df.copy()
    for col in safe.select_dtypes(include=["object"]).columns:
        safe[col] = safe[col].map(lambda x: None if isinstance(x, (list, dict)) else x)
    return safe


def create_compat_views(engine: Engine) -> None:
    """
    Crea:
      - Vistas canónicas (orders, order_items, ...): SELECT * FROM olist_*
      - product_category_en: COALESCE(traducción en inglés, nombre original)
      - order_item_revenue: revenue = price + freight_value
      - delivered_orders: pedidos entregados según regla única
      - per_order_revenue / per_order_with_delivery / delivered_monthly_revenue: vistas auxiliares
        para estabilizar queries por mes/año y comparaciones de ingresos.
    """
    stmts: list[str] = []

    # 1) Views canónicas para evitar "schema drift"
    #    (omitimos tablas auxiliares que no necesitan alias canónico)
    for canon, physical in TABLES.items():
        if canon in ("category_tr", "public_holidays"):
            continue
        stmts.append(f"CREATE VIEW IF NOT EXISTS {canon} AS SELECT * FROM {physical};")

    # 2) Traducción de categorías (inglés si existe; si no, original)
    stmts.append(
        f"""
        CREATE VIEW IF NOT EXISTS product_category_en AS
        SELECT
          p.product_id,
          COALESCE(t.product_category_name_english, p.product_category_name) AS category,
          p.product_category_name
        FROM {TABLES['products']} p
        LEFT JOIN {TABLES['category_tr']} t
          ON t.product_category_name = p.product_category_name;
        """
    )

    # 3) Revenue unificado por item
    stmts.append(
        f"""
        CREATE VIEW IF NOT EXISTS order_item_revenue AS
        SELECT
          order_id,
          order_item_id,
          {REVENUE_EXPR} AS revenue,
          product_id,
          seller_id
        FROM {TABLES['order_items']};
        """
    )

    # 4) Pedidos entregados (regla única)
    stmts.append(
        f"""
        CREATE VIEW IF NOT EXISTS delivered_orders AS
        SELECT *
        FROM {TABLES['orders']}
        WHERE {DELIVERED_WHERE};
        """
    )

    # 5) Revenue por pedido (agregado una vez, reutilizable)
    stmts.append(
        """
        CREATE VIEW IF NOT EXISTS per_order_revenue AS
        SELECT
          order_id,
          SUM(revenue) AS order_revenue
        FROM order_item_revenue
        GROUP BY order_id;
        """
    )

    # 6) Pedido entregado con fecha/mes/año (mes cero-padded)
    stmts.append(
        """
        CREATE VIEW IF NOT EXISTS per_order_with_delivery AS
        SELECT
          d.order_id,
          por.order_revenue,
          date(d.order_delivered_customer_date) AS delivered_date,
          strftime('%Y', d.order_delivered_customer_date) AS yr,
          printf('%02d', CAST(strftime('%m', d.order_delivered_customer_date) AS INTEGER)) AS month_no
        FROM delivered_orders d
        JOIN per_order_revenue por USING(order_id);
        """
    )

    # 7) Revenue mensual (útil para múltiples queries)
    stmts.append(
        """
        CREATE VIEW IF NOT EXISTS delivered_monthly_revenue AS
        SELECT
          yr,
          month_no,
          SUM(order_revenue) AS revenue
        FROM per_order_with_delivery
        GROUP BY yr, month_no;
        """
    )

    # Ejecutar todas las statements en una transacción
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))



def load(data_frames: Dict[str, pd.DataFrame] | Engine,
         database: Engine | Dict[str, pd.DataFrame]) -> None:
    """
    Carga los DataFrames en tablas SQLite y luego crea las vistas de compatibilidad.

    Soporta ambos órdenes de argumentos:
      - load(data_frames=..., database=engine)   (estilo original)
      - load(engine, tables_dict)                (estilo run_pipeline)

    Detecta y corrige si los parámetros vienen invertidos.
    """
    # Auto-detección de orden
    engine: Engine
    tables: Dict[str, pd.DataFrame]

    if isinstance(data_frames, Engine) and isinstance(database, dict):
        # Llamada tipo: load(engine, tables)
        engine = data_frames
        tables = database
    elif isinstance(database, Engine) and isinstance(data_frames, dict):
        # Llamada tipo: load(tables, engine) o con keywords originales
        engine = database
        tables = data_frames
    else:
        raise TypeError(
            "load() espera (Engine, Dict[str, DataFrame]) o (Dict[str, DataFrame], Engine). "
            f"Recibido: data_frames={type(data_frames)}, database={type(database)}"
        )

    # Insertar tablas usando una conexión SQLAlchemy (no raw DB-API)
    for name, df in tables.items():
        safe_df = _sanitize_for_sqlite(df)
        with engine.begin() as conn:
            safe_df.to_sql(name, con=conn, if_exists="replace", index=False)

    # Importante: crear las views después de cargar las tablas
    create_compat_views(engine)

