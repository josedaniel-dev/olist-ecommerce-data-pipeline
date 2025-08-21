# tests/conftest.py
from __future__ import annotations

import sys
from pathlib import Path

# Asegura que el paquete "src" sea importable al ejecutar los tests
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from src.config import DATASET_ROOT_PATH, PUBLIC_HOLIDAYS_URL, get_csv_to_table_mapping
from src.extract import extract
from src.load import load


@pytest.fixture(scope="session")
def database() -> Engine:
    """
    Devuelve un Engine de SQLite en memoria pre-cargado con los CSVs.
    El Engine se cierra al finalizar la sesión de tests.
    """
    # SQLAlchemy 2.x + sqlite en memoria (driver pysqlite). "future=True" reduce warnings.
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)

    mapping = get_csv_to_table_mapping()
    dataframes = extract(DATASET_ROOT_PATH, mapping, PUBLIC_HOLIDAYS_URL)

    # Carga efectiva a SQLite (tablas disponibles para las queries)
    load(dataframes, engine)

    try:
        yield engine
    finally:
        engine.dispose()
