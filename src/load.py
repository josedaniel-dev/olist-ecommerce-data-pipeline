# src/load.py
from typing import Dict
import json
import pandas as pd
from sqlalchemy.engine import Engine


def _coerce_sql_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make a copy of df where any non-scalar values (lists/dicts/sets/tuples) in
    object columns are converted to JSON strings so DB-API bindings succeed.
    """
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == "object":
            out[col] = out[col].apply(
                lambda v: json.dumps(v) if isinstance(v, (list, dict, set, tuple)) else v
            )
    return out


def load(data_frames: Dict[str, pd.DataFrame], database: Engine) -> None:
    """
    Load DataFrames into the DB using a raw DB-API connection so Pandas works
    cleanly with SQLAlchemy 2.x.
    """
    raw = database.raw_connection()  # sqlite3.Connection (has .cursor)
    try:
        for name, df in data_frames.items():
            safe_df = _coerce_sql_friendly(df)
            safe_df.to_sql(name, con=raw, if_exists="replace", index=False)
        raw.commit()
    finally:
        raw.close()
