# src/transform.py
"""
Query runners and programmatic transforms for the assignment.

Key behaviors:
- Load SQL text from queries/<name>.sql
- Support multi-statement files (DROP/CREATE VIEW; SELECT ...) and return the
  rows from the final SELECT. If there is no final SELECT, we try to infer the
  created view name and do a `SELECT * FROM <view>`.
- Never return None to the tests; always a DataFrame (possibly empty).
- Programmatic transforms reproduce the JSON fixtures.
"""
from __future__ import annotations
import os
import hashlib
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
import re
import pandas as pd
from sqlalchemy.engine import Engine


# ---------- Small result wrapper ----------

@dataclass
class QueryResult:
    """Simple named result wrapper used by tests."""
    name: str
    result: pd.DataFrame


# ---------- IO helpers ----------

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_QUERIES_DIR_CWD = Path("queries")
_QUERIES_DIR_FILE = _PROJECT_ROOT / "queries"
_QUERIES_DIR = _QUERIES_DIR_CWD if _QUERIES_DIR_CWD.exists() else _QUERIES_DIR_FILE

def _shield_debug_sql(query_name: str, sql_text: str, path_hint: str = ""):
    """Imprime huella md5 y preview del SQL realmente ejecutado (gobernado por SHIELD_DEBUG=1)."""
    if os.getenv("SHIELD_DEBUG") != "1":
        return
    h = hashlib.md5(sql_text.encode("utf-8")).hexdigest()
    head = textwrap.shorten(" ".join(sql_text.split()), width=180, placeholder="…")
    print(f"[Transform][DEBUG] query={query_name} md5={h} bytes={len(sql_text)} path={path_hint}")
    print(f"[Transform][DEBUG] head={head}")


def _read_sql(name: str) -> str:
    """Read queries/<name>.sql and return raw SQL string."""
    path = _QUERIES_DIR / f"{name}.sql"
    print(f"[Transform] Loading SQL: {path}")
    txt = path.read_text(encoding="utf-8", errors="strict")
    # Normalize line-endings and strip potential BOM
    return txt.replace("\r\n", "\n").replace("\r", "\n").lstrip("\ufeff")


# ---------- Multi-statement handling ----------

_STMT_SPLIT = re.compile(r";\s*(?:--.*)?\n", re.IGNORECASE)

def _split_sql_statements(sql: str) -> list[str]:
    """
    Split on semicolons that delimit statements; ignore empties and comments.
    Assumes our queries don't embed semicolons in string literals.
    """
    
    parts = [s for s in re.split(r";\s*(?:--[^\n]*\r?\n|\r?\n|$)", sql) if s.strip()]
    return [p for p in (_normalize_stmt(x) for x in parts) if p]

def _strip_sql_comments(sql: str) -> str:
    """Remove SQL line comments (--) and block comments (/* ... */)."""
    no_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)         # block comments
    no_line = re.sub(r"(?m)--.*?$", "", no_block)                # -- comments
    return no_line

def _normalize_stmt(stmt: str) -> str:
    """Trim whitespace and comments; return '' if nothing SQL-like remains."""
    if not stmt:
        return ""
    cleaned = _strip_sql_comments(stmt).strip()
    if not _looks_like_sql(cleaned):
        return ""
    return cleaned

def _looks_like_sql(stmt: str) -> bool:
    """Heuristic: does this chunk look like an executable SQL statement?"""
    if not stmt:
        return False
    s = stmt.lstrip().lower()
    starters = ("select", "with", "create", "drop", "insert",
                "update", "delete", "alter", "pragma", "vacuum")
    return s.startswith(starters)

def _is_select_like(stmt: str) -> bool:
    s = stmt.lstrip().lower()
    return s.startswith("select") or s.startswith("with")

_VIEW_RE = re.compile(
    r"create\s+(?:temp|temporary\s+)?view\s+(?:if\s+not\s+exists\s+)?([A-Za-z_][\w]*)\s+as\s+",
    re.IGNORECASE,
)

def _extract_created_view_name(stmt: str) -> Optional[str]:
    """Return view name if the statement is a CREATE VIEW ... AS ..."""
    m = _VIEW_RE.search(stmt)
    return m.group(1) if m else None


def _run_df(query_name: str, database: Engine, params: Optional[Dict] = None) -> pd.DataFrame:
    """
    Execute queries/<query_name>.sql and return a DataFrame.
    - If there are multiple statements, execute all setup statements (DDL/DML)
      and fetch rows from the **last SELECT-like** statement.
    - If there is no SELECT-like statement but a CREATE VIEW is present, run
      `SELECT * FROM <that_view>` as a sensible default.
    """
    sql = _read_sql(query_name)
    _shield_debug_sql(query_name, sql, str(_QUERIES_DIR / f"{query_name}.sql"))
    if not sql.strip():
        print(f"[Transform] No SQL found for {query_name}, returning empty DataFrame")
        return pd.DataFrame()
    stmts = _split_sql_statements(sql)
    print(f"[Transform] Executing: {query_name} ({len(stmts)} stmt{'s' if len(stmts)!=1 else ''})")

    with database.connect() as conn:
        if not stmts:
            # Nothing to run, return an empty frame
            return pd.DataFrame()

        # Find final SELECT/CTE
        last_select_idx = None
        for i in range(len(stmts) - 1, -1, -1):
            if _is_select_like(stmts[i]):
                last_select_idx = i
                break
                

        # If there is no final SELECT, try to infer a view and select from it
        fallback_view: Optional[str] = None
        if last_select_idx is None:
            # Check from last to first to capture the most recent CREATE VIEW
            for j in range(len(stmts) - 1, -1, -1):
                name = _extract_created_view_name(stmts[j])
                if name:
                    fallback_view = name
                    break
        # DEBUG: muestra el SELECT final que sí se ejecutará
        if os.getenv("SHIELD_DEBUG") == "1" and last_select_idx is not None:
            final_stmt = stmts[last_select_idx]
            h2 = hashlib.md5(final_stmt.encode("utf-8")).hexdigest()
            head2 = textwrap.shorten(" ".join(final_stmt.split()), width=180, placeholder="…")
            print(f"[Transform][DEBUG] final_select_md5={h2}")
            print(f"[Transform][DEBUG] final_select_head={head2}")

        # Execute all non-SELECT statements first
        for idx, stmt in enumerate(stmts):
            if idx == last_select_idx:
                continue
            if not stmt:
                continue
            if _is_select_like(stmt):
                # A non-final SELECT: execute and discard rows
                conn.exec_driver_sql(stmt)
            else:
                conn.exec_driver_sql(stmt)

        # Decide what to read into Pandas
        if last_select_idx is not None:
            df = pd.read_sql_query(stmts[last_select_idx], conn, params=params or {})
            print(f"[Transform]   -> rows={len(df)}, cols={len(df.columns)}")
            return df

        if fallback_view:
            df = pd.read_sql_query(f"SELECT * FROM {fallback_view}", conn, params=params or {})
            print(f"[Transform]   -> rows={len(df)}, cols={len(df.columns)} (fallback from view {fallback_view})")
            return df

        # Nothing fetchable
        return pd.DataFrame()


# ---------- Thin wrappers for each SQL file ----------

def query_shield_full_diag(database: Engine) -> QueryResult:
    name = "shield_full_diag"
    df = _run_df(name, database)
    return QueryResult(name=name, result=df)

def query_revenue_by_month_year(database: Engine) -> QueryResult:
    name = "revenue_by_month_year"
    df = _run_df(name, database)
    _run_shield_diag(database)
    return QueryResult(name=name, result=df)

def query_revenue_per_state(database: Engine) -> QueryResult:
    name = "revenue_per_state"
    df = _run_df(name, database)
    return QueryResult(name=name, result=df)

def query_top_10_revenue_categories(database: Engine) -> QueryResult:
    name = "top_10_revenue_categories"
    df = _run_df(name, database)
    _run_shield_diag(database)
    return QueryResult(name=name, result=df)

def query_top_10_least_revenue_categories(database: Engine) -> QueryResult:
    name = "top_10_least_revenue_categories"
    df = _run_df(name, database)
    _run_shield_diag(database)
    return QueryResult(name=name, result=df)

def query_delivery_date_difference(database: Engine) -> QueryResult:
    name = "delivery_date_difference"
    df = _run_df(name, database)
    return QueryResult(name=name, result=df)

def query_global_ammount_order_status(database: Engine) -> QueryResult:
    name = "global_ammount_order_status"
    df = _run_df(name, database)
    return QueryResult(name=name, result=df)

def query_real_vs_estimated_delivered_time(database: Engine) -> QueryResult:
    name = "real_vs_estimated_delivered_time"
    df = _run_df(name, database)
    return QueryResult(name=name, result=df)

def _run_shield_diag(database: Engine):
    """Pequeño set de diagnósticos; solo imprime si SHIELD_DEBUG=1."""
    if os.getenv("SHIELD_DEBUG") != "1":
        return

    print("[Transform][DEBUG] Running SHIELD diagnostics")
    with database.connect() as conn:
        # (D1) Breakdown Ene-2018 por tipo de pago (detecta 'voucher' vs no-voucher y variantes)
        q1 = """
        SELECT LOWER(TRIM(COALESCE(payment_type,''))) AS pt,
               COUNT(*) AS n_rows,
               ROUND(SUM(payment_value),2) AS total_value
        FROM olist_orders o
        JOIN olist_order_payments p USING(order_id)
        WHERE o.order_status='delivered'
          AND o.order_delivered_customer_date IS NOT NULL
          AND STRFTIME('%Y-%m', o.order_delivered_customer_date)='2018-01'
        GROUP BY pt ORDER BY total_value DESC
        """
        print("[Transform][DEBUG] payments_breakdown_2018_01 (top 5):",
              conn.exec_driver_sql(q1).fetchmany(5))

        # (D2) Duplicados en traducciones de categorías (posible fanout)
        q2 = """
        SELECT product_category_name,
               COUNT(*) AS n_rows,
               COUNT(DISTINCT product_category_name_english) AS n_english
        FROM product_category_name_translation
        GROUP BY product_category_name
        HAVING COUNT(*)>1 OR COUNT(DISTINCT product_category_name_english)>1
        ORDER BY n_rows DESC
        """
        print("[Transform][DEBUG] cat_translation_fanout (top 5):",
              conn.exec_driver_sql(q2).fetchmany(5))

        # (D3) Comparativa sin flete para dos categorías clave (ordena como fixture)
        q3 = """
        WITH cat AS (
          SELECT DISTINCT product_category_name, product_category_name_english
          FROM product_category_name_translation
          WHERE product_category_name_english IS NOT NULL
        )
        SELECT cat.product_category_name_english AS Category,
               COUNT(DISTINCT oi.order_id) AS Num_order,
               ROUND(SUM(oi.price),2) AS Revenue
        FROM olist_order_items oi
        JOIN olist_products p USING(product_id)
        JOIN cat ON cat.product_category_name = p.product_category_name
        WHERE Category IN ('bed_bath_table','health_beauty')
        GROUP BY Category ORDER BY Revenue DESC
        """
        print("[Transform][DEBUG] two_cat_without_freight:",
              conn.exec_driver_sql(q3).fetchall())


# ---------- Programmatic (not driven by .sql) ----------

def query_orders_per_day_and_holidays_2017(database: Engine) -> QueryResult:
    """
    Output columns: ['date', 'order_count', 'holiday']
    - Count orders per day for 2017.
    - Left-join with 'public_holidays' produced by extract().
    - 'holiday' is boolean.
    - 'date' is epoch milliseconds (UTC midnight).
    """
    name = "orders_per_day_and_holidays_2017"
    with database.connect() as conn:
        orders = pd.read_sql_query(
            """
            SELECT
              date(order_purchase_timestamp) AS date,
              COUNT(*) AS order_count
            FROM olist_orders
            WHERE strftime('%Y', order_purchase_timestamp) = '2017'
            GROUP BY 1
            ORDER BY 1
            """,
            conn,
        )
        holidays = pd.read_sql_query(
            """
            SELECT date(date) AS date, 1 AS is_holiday
            FROM public_holidays
            """,
            conn,
        )

    out = orders.merge(holidays, on="date", how="left")
    out["holiday"] = out["is_holiday"].fillna(False).astype(bool)
    out = out.drop(columns=["is_holiday"])

    # Convert 'date' (YYYY-MM-DD) to epoch milliseconds at UTC midnight
    ts = pd.to_datetime(out["date"], format="%Y-%m-%d", utc=True)
    out["date"] = (ts.view("int64") // 10**6).astype("int64")

    return QueryResult(name=name, result=out)



def query_freight_value_weight_relationship(database: Engine) -> QueryResult:
    """
    Output: ['order_id', 'freight_value', 'product_weight_g']
    - Sum freight and product weight per order for orders with status='delivered'.
    - LEFT JOIN products so orders missing a product record are kept with weight 0.
    - Deterministic ordering by order_id to match the JSON fixture.
    """
    name = "get_freight_value_weight_relationship"
    print(f"[Transform] Building: {name}")
    with database.connect() as conn:
        df = pd.read_sql_query(
            """
            SELECT
              i.order_id,
              SUM(COALESCE(i.freight_value, 0))                  AS freight_value,
              SUM(COALESCE(p.product_weight_g, 0))               AS product_weight_g
            FROM olist_orders o
            JOIN olist_order_items i ON i.order_id = o.order_id
            LEFT JOIN olist_products p ON p.product_id = i.product_id
            WHERE o.order_status = 'delivered'
            GROUP BY i.order_id
            ORDER BY i.order_id ASC
            """,
            conn,
        )
    print(f"[Transform]   -> rows={len(df)}, cols={len(df.columns)}")
    return QueryResult(name=name, result=df)
def query_orders_per_month(database: Engine) -> QueryResult:
    """
    Output: ['month', 'orders']
    - Count orders per month for 2017.
    - Output is ordered by month ascending.
    """
    name = "orders_per_month"
    with database.connect() as conn:
        df = pd.read_sql_query(
            """
            SELECT
              strftime('%Y-%m', order_purchase_timestamp) AS month,
              COUNT(*) AS orders
            FROM olist_orders
            WHERE strftime('%Y', order_purchase_timestamp) = '2017'
            GROUP BY month
            ORDER BY month ASC
            """,
            conn,
        )
    return QueryResult(name=name, result=df)    