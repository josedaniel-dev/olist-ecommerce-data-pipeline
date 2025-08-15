#!/usr/bin/env python3
"""
Run the full Olist ETL + analytics pipeline:
1) Clean raw CSVs
2) Extract cleaned CSVs + public holidays into DataFrames
3) Load into SQLite (in-memory by default)
4) Run all SQL queries from src/transform
5) Save query results to config.QUERY_RESULTS_ROOT_PATH
6) (Optional) Visualization via --visualize
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Tuple

# --- Make repository root importable (so `from src ...` works when run from anywhere)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from sqlalchemy import create_engine  # noqa: E402

from src import config, extract, load, transform  # noqa: E402

# Cleaner can be at /cleaner.py or /scripts/cleaner.py
try:
    from scripts.cleaner import main as clean_data  # noqa: E402
except Exception:  # pragma: no cover
    try:
        from cleaner import main as clean_data  # noqa: E402
    except Exception:
        def clean_data():
            print("[WARN] cleaner.py not found; skipping cleaning step.")


# Visualization is optional
try:
    from src.visualization import plots  # noqa: E402
    HAS_VIZ = True
except Exception:
    HAS_VIZ = False


def _engine_from_db_path(db_path: str):
    """Create a SQLAlchemy engine for SQLite.
    ':memory:' -> in-memory. Otherwise create file-backed DB."""
    if db_path == ":memory:":
        return create_engine("sqlite://")
    path = Path(db_path).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return create_engine(f"sqlite:///{path}")


def _query_plan() -> Iterable[Tuple[str, callable]]:
    """Name → function mapping for all analytics queries."""
    return [
        ("global_ammount_order_status", transform.query_global_ammount_order_status),
        ("delivery_date_difference", transform.query_delivery_date_difference),
        ("revenue_per_state", transform.query_revenue_per_state),
        ("top_10_revenue_categories", transform.query_top_10_revenue_categories),
        ("top_10_least_revenue_categories", transform.query_top_10_least_revenue_categories),
        ("revenue_by_month_year", transform.query_revenue_by_month_year),
        ("real_vs_estimated_delivered_time", transform.query_real_vs_estimated_delivered_time),
        ("orders_per_day_and_holidays_2017", transform.query_orders_per_day_and_holidays_2017),
        ("get_freight_value_weight_relationship", transform.query_freight_value_weight_relationship),
    ]


def _save_query_results(results: Iterable[Tuple[str, "pd.DataFrame"]], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, df in results:
        file_path = output_dir / f"{name}.json"
        file_path.write_text(df.to_json(orient="records", date_format="iso"), encoding="utf-8")
        print(f"[INFO] Saved: {file_path}")


def main():
    parser = argparse.ArgumentParser(description="Run full Olist ETL + analytics pipeline.")
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate charts for selected queries (requires src/visualization/plots.py)",
    )
    parser.add_argument(
        "--db-path",
        type=str,
        default=":memory:",
        help="SQLite DB path (default: in-memory). Example: --db-path artifacts/olist.sqlite",
    )
    args = parser.parse_args()

    print("[STEP 1] Cleaning raw CSVs...")
    clean_data()

    print("[STEP 2] Extracting cleaned data + public holidays...")
    dfs = extract.extract(
        config.CLEAN_DATA_ROOT_PATH,
        config.get_csv_to_table_mapping(),
        config.PUBLIC_HOLIDAYS_URL,
    )

    print("[STEP 3] Loading into SQLite database...")
    engine = _engine_from_db_path(args.db_path)
    load.load(dfs, engine)

    print("[STEP 4] Running queries...")
    results: list[Tuple[str, "pd.DataFrame"]] = []
    for name, func in _query_plan():
        qr = func(engine)
        df = getattr(qr, "result", qr)  # support either QueryResult(result=...) or raw DataFrame
        results.append((name, df))
        print(f"[QUERY] {name} → {len(df)} rows")

    print("[STEP 5] Saving results to tests/query_results/ (via config.QUERY_RESULTS_ROOT_PATH)")
    _save_query_results(results, config.QUERY_RESULTS_ROOT_PATH)

    if args.visualize:
        if not HAS_VIZ:
            print("[WARN] Visualization module not available; skipping.")
        else:
            print("[STEP 6] Generating visualizations...")
            for name, df in results:
                if name == "revenue_by_month_year":
                    plots.plot_revenue_by_month_year(df)
                elif name == "real_vs_estimated_delivered_time":
                    plots.plot_real_vs_estimated(df)
                elif name == "orders_per_day_and_holidays_2017":
                    plots.plot_orders_per_day_and_holidays_2017(df)
                elif name == "get_freight_value_weight_relationship":
                    plots.plot_freight_vs_weight(df)

    print("[DONE] Pipeline execution complete.")


if __name__ == "__main__":
    main()
