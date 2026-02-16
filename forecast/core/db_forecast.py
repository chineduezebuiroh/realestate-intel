from __future__ import annotations
# forecast/core/db_forecast.py

import os, json
import duckdb

import datetime as dt
import numpy as np

from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ====================================
# Main Functions
# ====================================
def get_connection():
    return duckdb.connect(os.getenv("DUCKDB_PATH", "./data/market.duckdb"))


def new_batch_id() -> str:
    # stable, sortable, human-readable
    return dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def _jsonify(d: Optional[Dict[str, Any]]) -> Optional[str]:
    if d is None:
        return None
    return json.dumps(d, default=str)


def _coerce_date(x):
    if x is None:
        return None
    # already a date (but not datetime)
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    # datetime -> date
    if isinstance(x, dt.datetime):
        return x.date()
    # string -> date
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return None
        return dt.date.fromisoformat(s)  # expects YYYY-MM-DD
    raise TypeError(f"data_asof must be date|datetime|YYYY-MM-DD str|None, got {type(x)}")


def deactivate_live_runs(con, target_metric_id: str, target_geo_id: str, target_property_type_id: Optional[str]):
    # Deactivate only *live* runs (not backtests)
    con.execute(
        """
        UPDATE forecast_runs
        SET is_active = FALSE
        WHERE target_metric_id = ?
          AND target_geo_id = ?
          AND (
                (target_property_type_id IS NULL AND ? IS NULL)
             OR (target_property_type_id = ?)
          )
          AND is_active = TRUE
          AND run_kind = 'live'
        """,
        [target_metric_id, target_geo_id, target_property_type_id, target_property_type_id],
    )


def insert_run(
    *,
    con,
    model_name: str,
    model_version: str,
    target_metric_id: str,
    target_geo_id: str,
    target_property_type_id: Optional[str],
    freq: str,
    train_start,
    train_end,
    horizon_max_months: int,
    algo_params: Optional[Dict[str, Any]],
    notes: Optional[str],
    is_active: bool,
    run_kind: str,               # 'backtest' | 'live' | 'adhoc'
    batch_id: Optional[str],     # required for backtest batches; ok None for adhoc/live if you want
    data_asof,                   # date|datetime|YYYY-MM-DD str (or None)
) -> int:
    # We let DuckDB generate run_id if you have identity; otherwise use MAX+1
    # Your schema currently has run_id BIGINT PRI with no identity, so we do MAX+1.
    data_asof = _coerce_date(data_asof)
    
    row = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM forecast_runs").fetchone()
    run_id = int(row[0])

    con.execute(
        """
        INSERT INTO forecast_runs (
            run_id, created_at,
            model_name, model_version,
            target_metric_id, target_geo_id, target_property_type_id,
            freq, train_start, train_end, horizon_max_months,
            algo_params_json, notes, is_active,
            batch_id, run_kind, data_asof
        )
        VALUES (
            ?, now(),
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?
        )
        """,
        [
            run_id,
            model_name, model_version,
            target_metric_id, target_geo_id, target_property_type_id,
            freq, train_start, train_end, horizon_max_months,
            _jsonify(algo_params), notes, is_active,
            batch_id, run_kind, data_asof,
        ],
    )
    return run_id


def insert_predictions(
    *,
    con,
    run_id: int,
    target_dates: Sequence,          # iterable of python date objects
    y_hat: Sequence[float],
    y_hat_lo: Optional[Sequence[Optional[float]]] = None,
    y_hat_hi: Optional[Sequence[Optional[float]]] = None,
):
    rows = []
    for i, dt_ in enumerate(target_dates, start=1):
        lo = None if y_hat_lo is None else y_hat_lo[i - 1]
        hi = None if y_hat_hi is None else y_hat_hi[i - 1]
        rows.append((run_id, dt_, i, i, float(y_hat[i - 1]), None if lo is None else float(lo), None if hi is None else float(hi)))

    con.executemany(
        """
        INSERT INTO forecast_predictions (
            run_id, target_date, horizon_steps, horizon_months, y_hat, y_hat_lo, y_hat_hi
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def store_selected_features_in_params(
    algo_params: Dict[str, Any],
    selected_features: List[str],
    selector_meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    algo_params = dict(algo_params or {})
    algo_params["feature_selection"] = {
        "selected_features": list(selected_features),
        "selector_meta": selector_meta or {},
    }
    return algo_params


def record_xgb_selector_run(
    con: duckdb.DuckDBPyConnection,
    *,
    batch_id: str,
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[int],
    freq: str,
    anchor_date: date,              # train_end
    train_start: date,
    horizon_months: int,            # selector horizon (3)
    data_asof: Optional[date],
    seed: int,
    stage1_mode: str,               # e.g. "cheap_lift" or "default"
    algo_params: Dict[str, Any],    # extra selector params (K, min_non_redfin, etc.)
    notes: Optional[str] = None,
    model_version: str = "v01",
    run_kind: str = "selector_backtest",   # NEW
) -> int:
    """
    Writes one row into forecast_runs for a single selector anchor.
    Returns run_id.
    """
    run_id = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM forecast_runs").fetchone()[0]

    payload = {
        "seed": int(seed),
        "stage1_mode": str(stage1_mode),
        **(algo_params or {}),
    }

    con.execute(
        """
        INSERT INTO forecast_runs (
            run_id,
            created_at,
            model_name,
            model_version,
            target_metric_id,
            target_geo_id,
            target_property_type_id,
            freq,
            train_start,
            train_end,
            horizon_max_months,
            algo_params_json,
            notes,
            is_active,
            batch_id,
            run_kind,
            data_asof
        ) VALUES (
            ?,
            now(),
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            CAST('t' AS BOOLEAN),
            ?,
            ?,
            ?
        )
        """,
        [
            int(run_id),
            "xgb_selector",
            str(model_version),
            str(metric_id),
            str(geo_id),
            None if property_type_id is None else str(property_type_id),
            str(freq),
            train_start,
            anchor_date,
            int(horizon_months),
            json.dumps(payload),
            notes,
            str(batch_id),
            run_kind,
            data_asof,
        ],
    )
    return int(run_id)
