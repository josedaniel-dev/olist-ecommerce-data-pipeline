# src/transform.py
import pandas as pd
from typing import NamedTuple
from sqlalchemy.engine import Engine
from utils.sql import read_sql  # reads queries/<name>.sql and returns the SQL text

class QueryResult(NamedTuple):
    name: str
    result: pd.DataFrame

def _run_query(engine: Engine, basename: str) -> QueryResult:
    """
    Read SQL from queries/<basename>.sql and execute it against the DB.

    Use a raw DB-API connection so pandas works cleanly with SQLAlchemy 2.x.
    """
    sql = read_sql(basename)
    raw = engine.raw_connection()           # sqlite3.Connection (has .cursor)
    try:
        df = pd.read_sql(sql, raw)          # OK with DB-API connection
    finally:
        raw.close()
    return QueryResult(name=basename, result=df)

def query_revenue_by_month_year(engine: Engine) -> QueryResult:
    return _run_query(engine, "revenue_by_month_year")

def query_delivery_date_difference(engine: Engine) -> QueryResult:
    return _run_query(engine, "delivery_date_difference")

def query_global_ammount_order_status(engine: Engine) -> QueryResult:
    return _run_query(engine, "global_ammount_order_status")

def query_revenue_per_state(engine: Engine) -> QueryResult:
    return _run_query(engine, "revenue_per_state")

def query_top_10_least_revenue_categories(engine: Engine) -> QueryResult:
    return _run_query(engine, "top_10_least_revenue_categories")

def query_top_10_revenue_categories(engine: Engine) -> QueryResult:
    return _run_query(engine, "top_10_revenue_categories")

def query_real_vs_estimated_delivered_time(engine: Engine) -> QueryResult:
    return _run_query(engine, "real_vs_estimated_delivered_time")

def query_orders_per_day_and_holidays_2017(engine: Engine) -> QueryResult:
    return _run_query(engine, "orders_per_day_and_holidays_2017")

def query_freight_value_weight_relationship(engine: Engine) -> QueryResult:
    return _run_query(engine, "get_freight_value_weight_relationship")
