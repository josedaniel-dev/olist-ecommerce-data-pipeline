# src/utils.py
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

import pandas as pd


def pandas_to_json_object(
    df: pd.DataFrame,
    datetime_cols: Optional[Sequence[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Convierte un DataFrame en una lista de dicts *estable* para asserts.
    - Cols datetime -> epoch-ms (UTC) para evitar problemas de tz/formatos.
    - Mantiene el orden de columnas del DataFrame.
    """
    if df is None or df.empty:
        return []

    out = df.copy()

    # Detecta autom ticamente columnas datetime si no se pasan
    if datetime_cols is None:
        datetime_cols = [
            c for c in out.columns
            if pd.api.types.is_datetime64_any_dtype(out[c])
        ]

    for c in datetime_cols:
        # Convertir a epoch-ms (sin tz) de forma segura
        out[c] = (
            pd.to_datetime(out[c], utc=True, errors="coerce")
              .view("int64") // 10**6
        )

    # Asegurar tipos b sicos JSON-serializables
    for c in out.columns:
        if pd.api.types.is_float_dtype(out[c]) or pd.api.types.is_integer_dtype(out[c]):
            continue
        if pd.api.types.is_bool_dtype(out[c]):
            continue
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            continue
        # Convertir objetos a str para evitar listas/dicts en asserts
        if pd.api.types.is_object_dtype(out[c]):
            out[c] = out[c].apply(lambda x: x if isinstance(x, (str, int, float, bool)) or pd.isna(x) else str(x))

    # to_dict con records preserva el orden de columnas
    return out.to_dict(orient="records")

