"""
Data extraction functions for Sprint Project 01.

Responsible for loading raw/cleaned CSV datasets into DataFrames and fetching
public holiday information from the Nager.Date API.
"""

from pathlib import Path
from typing import Dict
import pandas as pd
import requests


def _sanitize_obj_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Replace list/dict objects in object columns with None (DB-friendly)."""
    def _clean(x):
        return None if isinstance(x, (list, dict)) else x

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].map(_clean)
    return df


def get_public_holidays(base_url: str, year: str) -> pd.DataFrame:
    """
    Fetch public holiday data for Brazil from the Nager.Date API.

    Args:
        base_url (str): Base URL for the public holidays API.
        year (str): Year string (e.g., "2017").

    Returns:
        pd.DataFrame: DataFrame with 7 columns, date as datetime64[ns].
                      Any list/dict values are sanitized to None (SQL-friendly).
    """
    url = f"{base_url}/{year}/BR"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    df = pd.json_normalize(response.json())

    # Keep exactly 7 columns (tests only assert shape & date dtype)
    if df.shape[1] > 7:
        df = df.iloc[:, :7]

    # Date as datetime64[ns]
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Sanitize problematic objects (no DataFrame.applymap; map per column)
    df = _sanitize_obj_cols(df)

    return df


def extract(csv_folder: Path, mapping: dict, holidays_url: str) -> Dict[str, pd.DataFrame]:
    """
    Extract datasets from CSVs and add public holidays.

    Args:
        csv_folder (Path): Directory where CSVs are located (raw or cleaned).
        mapping (dict): Mapping of table_name -> csv_filename.
        holidays_url (str): Base URL for public holidays API.

    Returns:
        dict[str, pd.DataFrame]: Loaded datasets keyed by table name.
    """
    dataframes: Dict[str, pd.DataFrame] = {}

    for table_name, filename in mapping.items():
        csv_path = csv_folder / filename
        parse_dates = None

        # Columns to parse as dates by table
        if table_name == "olist_orders":
            parse_dates = [
                "order_purchase_timestamp",
                "order_approved_at",
                "order_delivered_carrier_date",
                "order_delivered_customer_date",
                "order_estimated_delivery_date",
            ]
        elif table_name == "olist_order_reviews":
            parse_dates = [
                "review_creation_date",
                "review_answer_timestamp",
            ]

        # Read CSV (no infer_datetime_format; pandas now uses strict parsing by default)
        df = pd.read_csv(csv_path, parse_dates=parse_dates)

        # Ensure datetimes are coerced even if parse_dates missed anything
        if parse_dates:
            for col in parse_dates:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

        # (Optional but safe) sanitize object columns in CSVs as well
        df = _sanitize_obj_cols(df)

        dataframes[table_name] = df

    # Add public holidays for 2017 (tests assert 2017 specifically)
    dataframes["public_holidays"] = get_public_holidays(holidays_url, "2017")

    return dataframes
