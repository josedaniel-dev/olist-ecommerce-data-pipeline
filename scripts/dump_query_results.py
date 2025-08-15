#!/usr/bin/env python3
"""
Generate the JSON snapshots that tests/test_transform.py expects in tests/query_results/.
- If --db-path is provided, queries run against that SQLite file.
- If omitted, we build an in-memory DB from the cleaned CSVs via extract→load.
"""

from pathlib import Path
import argparse
import json
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.config import (
    CLEAN_DATA_ROOT_PATH,
    PUBLIC_HOLIDAYS_URL,
    get_csv_to_table_mapping,
    # If your config already exposes QUERY_RESULTS_ROOT_PATH pointing to tests/query_results,
    # we’ll use it; otherwise we will fall back to repo_root/tests/query_results.
    QUERY_RESULTS_ROOT_PATH as CONFIG_RESULTS_DIR  # if not present in your config, remove this import and the try/except below
)
from src.extract import extract
from src.load import load
from src import transform


def pandas_to_json_object(df: pd.DataFrame) -> list:
    # Match the structure tests expect (records, ISO dates)
    return json.loads(df.to_json(orient="records", date_format="iso"))


def main():
    parser = argparse.ArgumentParser(description="Generate expected query results JSONs.")
    parser.add_argument(
        "--db-path",
        type=str,
        default=None,
        help="Path to an existing SQLite database. If omitted, build an in-memory DB from cleaned CSVs."
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Output directory. Defaults to tests/query_results."
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]

    # Decide output dir (tests read from tests/query_results)
    if args.out:
        out_dir = Path(args.out)
    else:
        # Prefer config value if it points to tests/query_results, else fall back to repo/tests/query_results
        try:
            out_dir = Path(CONFIG_RESULTS_DIR)
        except NameError:
            out_dir = repo_root / "tests" / "query_results"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Connect/build the DB
    if args.db_path:
        db_path = Path(args.db_path)
        if not db_path.exists():
            raise FileNotFoundError(f"Database file not found: {db_path}")
        print(f"[INFO] Using existing SQLite DB: {db_path}")
        engine = create_engine(f"sqlite:///{db_path}")
    else:
        print("[INFO] Building in-memory DB from cleaned CSVs …")
        engine = create_engine("sqlite://")
        frames = extract(
            csv_folder=CLEAN_DATA_ROOT_PATH,
            mapping=get_csv_to_table_mapping(),
            holidays_url=PUBLIC_HOLIDAYS_URL,
        )
        load(frames, engine)

    # Map query names -> functions (so we can name files correctly)
    queries = {
        "revenue_by_month_year": transform.query_revenue_by_month_year,
        "delivery_date_difference": transform.query_delivery_date_difference,
        "global_ammount_order_status": transform.query_global_ammount_order_status,
        "revenue_per_state": transform.query_revenue_per_state,
        "top_10_least_revenue_categories": transform.query_top_10_least_revenue_categories,
        "top_10_revenue_categories": transform.query_top_10_revenue_categories,
        "real_vs_estimated_delivered_time": transform.query_real_vs_estimated_delivered_time,
        "orders_per_day_and_holidays_2017": transform.query_orders_per_day_and_holidays_2017,
        "get_freight_value_weight_relationship": transform.query_freight_value_weight_relationship,
    }

    # Run and write
    for name, fn in queries.items():
        df = fn(engine).result
        payload = pandas_to_json_object(df)
        dest = out_dir / f"{name}.json"
        with dest.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"[OK] {dest}")

    print("[DONE] All expected results written.")


if __name__ == "__main__":
    main()
