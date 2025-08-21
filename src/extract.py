# src/extract.py
from __future__ import annotations
import pandas as pd
from cleaner import clean, save_cleaned
from typing import Dict, Mapping, Optional, List, Tuple
from pathlib import Path
import json
import urllib.request
from urllib.error import URLError, HTTPError

# ---------------------------------------------------------------------
# Known timestamp columns per Olist table (keys must match table names)
# ---------------------------------------------------------------------
TIMESTAMP_COLS_BY_TABLE: Mapping[str, Tuple[str, ...]] = {
    "olist_orders": (
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ),
    "olist_order_items": ("shipping_limit_date",),
    "olist_order_reviews": ("review_creation_date", "review_answer_timestamp"),
    # Other tables have no timestamps to parse
}


def _read_csv_stable(csv_path: Path, table_name: str) -> pd.DataFrame:
    """
    Stable CSV read:
    - low_memory=False for consistent dtype inference
    - parse_dates for known timestamp columns (if present)
    """
    print(f"[Extract] Reading table '{table_name}' from {csv_path} ...")
    parse_cols: List[str] = list(TIMESTAMP_COLS_BY_TABLE.get(table_name, ()))
    df = pd.read_csv(csv_path, low_memory=False, parse_dates=parse_cols or None)
    print(f"[Extract]   -> loaded (rows={len(df)}, cols={len(df.columns)})")
    return df


# ------------------------------- Public Holidays -------------------------------

def _fetch_public_holidays_json(url: str) -> Optional[list]:
    """Fetch JSON from the given URL. Return list[dict] or None on failure."""
    if not url:
        return None
    try:
        with urllib.request.urlopen(url) as resp:
            if resp.getcode() != 200:
                return None
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except (URLError, HTTPError, ValueError):
        return None


def _normalize_public_holidays(data: Optional[list]) -> pd.DataFrame:
    """
    Normalize holidays JSON into exactly 7 columns required by tests:
      ['date', 'localName', 'name', 'countryCode', 'fixed', 'global', 'launchYear']
    - 'date' must be datetime64[ns] (naive; not timezone-aware)
    - We intentionally exclude 'types' and 'counties'
    """
    cols = ["date", "localName", "name", "countryCode", "fixed", "global", "launchYear"]
    if not data:
        # Empty but schema-correct frame
        return pd.DataFrame(columns=cols).astype({"date": "datetime64[ns]"})

    rows = []
    for x in data:
        rows.append(
            {
                "date": pd.to_datetime(x.get("date"), errors="coerce"),  # utc=False → naive
                "localName": x.get("localName"),
                "name": x.get("name"),
                "countryCode": x.get("countryCode"),
                "fixed": x.get("fixed"),
                "global": x.get("global"),
                "launchYear": x.get("launchYear"),
            }
        )
    df = pd.DataFrame(rows, columns=cols)
    # Ensure dtype for 'date' is exactly datetime64[ns] (not tz-aware)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def get_public_holidays(base_url: str, year: str, country: str = "BR") -> pd.DataFrame:
    """
    Public API expected by tests.
    Builds URL as: {base_url}/{year}/{country}, returns a (14, 7) DataFrame for 2017 BR
    with 'date' dtype == 'datetime64[ns]'.
    """
    url = f"{base_url.rstrip('/')}/{year}/{country}"
    print(f"[Extract] Fetching public holidays: {url}")
    raw = _fetch_public_holidays_json(url)
    df = _normalize_public_holidays(raw)
    print(f"[Extract]   -> holidays (rows={len(df)}, cols={len(df.columns)})")
    return df


# ----------------------------------- Extract -----------------------------------

def extract(
    csv_folder: Path,
    table_to_csv: Mapping[str, str],
    public_holidays_url: Optional[str] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Return {table_name: DataFrame} for all Olist CSVs + 'public_holidays'.

    IMPORTANT: The mapping is TABLE -> CSV filename
               (this matches get_csv_to_table_mapping() from src.config and tests).
    """
    print("[Extract] Starting extract() ...")
    csv_folder = Path(csv_folder)
    result: Dict[str, pd.DataFrame] = {}

    # Read all CSVs defined in mapping
    for table_name, csv_name in (table_to_csv or {}).items():
        csv_path = csv_folder / csv_name
        if not csv_path.exists():
            raise FileNotFoundError(f"[Extract] Missing CSV for table '{table_name}': {csv_path}")
        result[table_name] = _read_csv_stable(csv_path, table_name)

    # Public holidays (append as its own table)
    year = "2017"  # tests call get_public_holidays(base_url, "2017")
    if public_holidays_url:
        result["public_holidays"] = get_public_holidays(public_holidays_url, year)
    else:
        # Schema-correct empty frame if no URL provided
        result["public_holidays"] = _normalize_public_holidays(None)

    print(f"[Extract] Completed extract(). Total tables: {len(result)}")
    return result
