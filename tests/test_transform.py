import json
import math
from typing import List, Dict

import pandas as pd
from pytest import fixture
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from src.config import (
    QUERY_RESULTS_ROOT_PATH,
    DATASET_ROOT_PATH,
    PUBLIC_HOLIDAYS_URL,
    get_csv_to_table_mapping,
    CLEAN_DATA_ROOT_PATH, # Added this import
)
from src.extract import extract
from src.load import load
from src.transform import (
    QueryResult,
    query_delivery_date_difference,
    query_global_ammount_order_status,
    query_revenue_by_month_year,
    query_revenue_per_state,
    query_top_10_least_revenue_categories,
    query_top_10_revenue_categories,
    query_real_vs_estimated_delivered_time,
    query_orders_per_day_and_holidays_2017,
    query_freight_value_weight_relationship,
)

TOLERANCE = 0.1


def to_float(objs, year_col):
    return list(map(lambda obj: float(obj[year_col]) if obj[year_col] else 0.0, objs))


def float_vectors_are_close(a: List[float], b: List[float], tolerance: float = TOLERANCE) -> bool:
    """Check if two vectors of floats are close."""
    return len(a) == len(b) and all(math.isclose(a[i], b[i], abs_tol=tolerance) for i in range(len(a)))


def read_query_result(query_name: str) -> dict:
    with open(f"{QUERY_RESULTS_ROOT_PATH}/{query_name}.json", "r") as f:
        return json.load(f)


def pandas_to_json_object(df: pd.DataFrame) -> list:
    return json.loads(df.to_json(orient="records"))


def sort_records(records: List[dict], by: List[str], ascending: List[bool]) -> List[dict]:
    """Return a new list sorted deterministically by columns."""
    if not records:
        return records
    df = pd.DataFrame(records)
    df = df.sort_values(by=by, ascending=ascending, kind="mergesort").reset_index(drop=True)
    return json.loads(df.to_json(orient="records"))


def key_index(records: List[dict], key: str) -> Dict[str, dict]:
    """Index a list of records by a unique key column."""
    return {row[key]: row for row in records}


@fixture(scope="session", autouse=True)
def database() -> Engine:
    """Initialize the database for testing."""
    engine = create_engine("sqlite://")
    # This now correctly points to your cleaned data
    csv_folder = CLEAN_DATA_ROOT_PATH 
    public_holidays_url = PUBLIC_HOLIDAYS_URL
    csv_table_mapping = get_csv_to_table_mapping()
    csv_dataframes = extract(csv_folder, csv_table_mapping, public_holidays_url)
    load(data_frames=csv_dataframes, database=engine)
    return engine


def test_query_revenue_by_month_year(database: Engine):
    query_name = "revenue_by_month_year"
    actual = pandas_to_json_object(query_revenue_by_month_year(database).result)
    expected = read_query_result(query_name)

    # Order by month_no (already zero-padded '01'..'12').
    actual = sort_records(actual, ["month_no"], [True])
    expected = sort_records(expected, ["month_no"], [True])

    assert len(actual) == len(expected)
    assert [obj["month_no"] for obj in actual] == [obj["month_no"] for obj in expected]
    assert float_vectors_are_close(to_float(actual, "Year2016"), to_float(expected, "Year2016"))
    assert float_vectors_are_close(to_float(actual, "Year2017"), to_float(expected, "Year2017"))
    assert float_vectors_are_close(to_float(actual, "Year2018"), to_float(expected, "Year2018"))
    assert list(actual[0].keys()) == list(expected[0].keys())


def test_query_delivery_date_difference(database: Engine):
    query_name = "delivery_date_difference"
    actual = pandas_to_json_object(query_delivery_date_difference(database).result)
    expected = read_query_result(query_name)

    # Compare by State, order-insensitive.
    actual = sort_records(actual, ["State"], [True])
    expected = sort_records(expected, ["State"], [True])
    assert actual == expected


def test_query_global_ammount_order_status(database: Engine):
    query_name = "global_ammount_order_status"
    actual = pandas_to_json_object(query_global_ammount_order_status(database).result)
    expected = read_query_result(query_name)

    # Order-insensitive comparison by order_status, then Ammount for determinism in display.
    actual = sort_records(actual, ["order_status", "Ammount"], [True, True])
    expected = sort_records(expected, ["order_status", "Ammount"], [True, True])
    assert actual == expected


def test_query_revenue_per_state(database: Engine):
    query_name = "revenue_per_state"
    actual = pandas_to_json_object(query_revenue_per_state(database).result)
    expected = read_query_result(query_name)

    # Order-insensitive; compare by state key and then revenue with tolerance.
    # First, ensure same states returned and same schema.
    assert list(actual[0].keys()) == list(expected[0].keys())

    actual_idx = key_index(actual, "customer_state")
    expected_idx = key_index(expected, "customer_state")
    assert set(actual_idx.keys()) == set(expected_idx.keys())

    # Compare revenues with tolerance after aligning by state.
    a_revenue = [actual_idx[s]["Revenue"] for s in sorted(actual_idx.keys())]
    e_revenue = [expected_idx[s]["Revenue"] for s in sorted(expected_idx.keys())]
    assert float_vectors_are_close(a_revenue, e_revenue)


def _assert_category_block(actual: List[dict], expected: List[dict], order_desc: bool):
    """Shared comparator for top/bottom-10 category queries.
    Compares by Category (set), Num_order equality, and Revenue close, order-insensitive."""
    # Schema
    assert list(actual[0].keys()) == list(expected[0].keys())

    # Index by Category
    a_idx = key_index(actual, "Category")
    e_idx = key_index(expected, "Category")

    # Same set of categories (not order)
    assert set(a_idx.keys()) == set(e_idx.keys())

    # Compare Num_order equality and Revenue close per category
    cats_sorted = sorted(a_idx.keys())
    a_num = [a_idx[c]["Num_order"] for c in cats_sorted]
    e_num = [e_idx[c]["Num_order"] for c in cats_sorted]
    assert a_num == e_num

    a_rev = [a_idx[c]["Revenue"] for c in cats_sorted]
    e_rev = [e_idx[c]["Revenue"] for c in cats_sorted]
    assert float_vectors_are_close(a_rev, e_rev)

    # Also assert internal ordering property on 'actual' output (determinism property)
    # Revenue DESC for top-10, ASC for least-10; Category ASC as tie-breaker.
    actual_sorted = sort_records(
        actual,
        ["Revenue", "Category"],
        [not order_desc, True]  # if order_desc=True -> sort by Revenue DESC => ascending=False
    )
    assert actual == actual_sorted  # ensure query returns deterministically sorted output


def test_query_top_10_least_revenue_categories(database: Engine):
    query_name = "top_10_least_revenue_categories"
    actual = pandas_to_json_object(query_top_10_least_revenue_categories(database).result)
    expected = read_query_result(query_name)

    assert len(actual) == len(expected) == 10
    _assert_category_block(actual, expected, order_desc=False)


def test_query_top_10_revenue_categories(database: Engine):
    query_name = "top_10_revenue_categories"
    actual = pandas_to_json_object(query_top_10_revenue_categories(database).result)
    expected = read_query_result(query_name)

    assert len(actual) == len(expected) == 10
    _assert_category_block(actual, expected, order_desc=True)


def test_real_vs_estimated_delivered_time(database: Engine):
    query_name = "real_vs_estimated_delivered_time"
    actual = pandas_to_json_object(query_real_vs_estimated_delivered_time(database).result)
    expected = read_query_result(query_name)

    # Order by month_no (strings '01'..'12')
    actual = sort_records(actual, ["month_no"], [True])
    expected = sort_records(expected, ["month_no"], [True])

    assert len(actual) == len(expected)
    assert list(actual[0].keys()) == list(expected[0].keys())
    assert [obj["month_no"] for obj in actual] == [obj["month_no"] for obj in expected]

    def _tf(objs, col):
        return [float(o[col]) if o[col] else 0.0 for o in objs]

    for col in [
        "Year2016_real_time",
        "Year2017_real_time",
        "Year2018_real_time",
        "Year2016_estimated_time",
        "Year2017_estimated_time",
        "Year2018_estimated_time",
    ]:
        assert float_vectors_are_close(_tf(actual, col), _tf(expected, col))


def test_query_orders_per_day_and_holidays_2017(database: Engine):
    query_name = "orders_per_day_and_holidays_2017"
    actual = pandas_to_json_object(query_orders_per_day_and_holidays_2017(database).result)
    expected = read_query_result(query_name)

    # Order-insensitive; sort by date for deterministic comparison
    actual = sort_records(actual, ["date"], [True])
    expected = sort_records(expected, ["date"], [True])
    assert actual == expected


def test_query_get_freight_value_weight_relationship(database: Engine):
    query_name = "get_freight_value_weight_relationship"
    actual = pandas_to_json_object(query_freight_value_weight_relationship(database).result)
    expected = read_query_result(query_name)

    # Order-insensitive; sort by order_id for deterministic comparison
    actual = sort_records(actual, ["order_id"], [True])
    expected = sort_records(expected, ["order_id"], [True])
    assert actual == expected