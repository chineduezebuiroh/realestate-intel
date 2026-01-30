from __future__ import annotations
# forecast/features/fact_loader.py

import os
from datetime import date
from typing import Dict, Optional

import duckdb
import pandas as pd

from forecast.core.asof import normalize_month_end


def get_connection() -> duckdb.DuckDBPyConnection:
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


def load_series_from_fact_with_source(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str],
    source_id: Optional[str],
    data_asof: Optional[date] = None,
    asof_by_source: Optional[Dict[str, date]] = None,
) -> pd.Series:
    effective_asof = data_asof
    if asof_by_source and source_id:
        effective_asof = asof_by_source.get(source_id, effective_asof)

    effective_asof = normalize_month_end(effective_asof)

    con = get_connection()
    pt_id = property_type_id if property_type_id is not None else "all"

    try:
        if source_id:
            sql = """
                SELECT date, value
                FROM fact_timeseries
                WHERE metric_id = ?
                  AND geo_id = ?
                  AND property_type_id = ?
                  AND source_id = ?
                  AND (? IS NULL OR date <= ?)
                ORDER BY date
            """
            df = con.execute(
                sql,
                [metric_id, geo_id, pt_id, source_id, effective_asof, effective_asof],
            ).fetchdf()
        else:
            sql = """
                SELECT date, value
                FROM fact_timeseries
                WHERE metric_id = ?
                  AND geo_id = ?
                  AND property_type_id = ?
                  AND (? IS NULL OR date <= ?)
                ORDER BY date
            """
            df = con.execute(
                sql,
                [metric_id, geo_id, pt_id, effective_asof, effective_asof],
            ).fetchdf()
    finally:
        con.close()

    if df.empty:
        raise ValueError(f"No data for metric={metric_id}, geo={geo_id}, pt={pt_id}, source={source_id}")

    s = df.set_index("date")["value"].astype(float)
    return s


def load_series_from_fact(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str],
    data_asof: Optional[date] = None,
    asof_by_source: Optional[Dict[str, date]] = None,
    source_id: Optional[str] = None,
) -> pd.Series:
    """
    Backward-compatible loader. If you pass source_id, it can use asof_by_source[source_id].
    Query itself is NOT pinned to source_id unless you explicitly call *_with_source.
    """
    effective_asof = data_asof
    if asof_by_source and source_id:
        effective_asof = asof_by_source.get(source_id, effective_asof)

    effective_asof = normalize_month_end(effective_asof)

    con = get_connection()
    pt_id = property_type_id if property_type_id is not None else "all"

    sql = """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
          AND (? IS NULL OR date <= ?)
        ORDER BY date
    """

    try:
        df = con.execute(sql, [metric_id, geo_id, pt_id, effective_asof, effective_asof]).fetchdf()
    finally:
        con.close()

    if df.empty:
        raise ValueError(f"No data for metric={metric_id}, geo={geo_id}, pt={pt_id}")

    s = df.set_index("date")["value"].astype(float)
    return s
