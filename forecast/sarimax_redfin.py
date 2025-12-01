# forecast/sarimax_redfin.py

import os
import json
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX


# -----------------------------------------
# DB helpers
# -----------------------------------------

def get_connection():
    """
    Simple connection helper that respects DUCKDB_PATH.
    Avoids touching utils.db so we don't risk rebuilds.
    """
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


# -----------------------------------------
# Target specification
# -----------------------------------------

@dataclass
class TargetSpec:
    metric_id: str
    geo_id: str
    # For Redfin, this is '-1', '6', '13', etc. For non-Redfin, use None -> 'all'.
    property_type_id: Optional[str] = None
    freq: str = "M"  # conceptual frequency; we won't force it on the index


# -----------------------------------------
# Data loading
# -----------------------------------------

def load_series(
    target: TargetSpec,
    min_obs: int = 36,
) -> pd.Series:
    """
    Load a univariate series from fact_timeseries for a given target.

    Uses:
      - metric_id
      - geo_id
      - property_type_id (or 'all' if None)

    Returns a pandas Series indexed by date.
    """
    con = get_connection()

    # Map None -> 'all' to match your fact_timeseries schema
    pt_id = target.property_type_id if target.property_type_id is not None else "all"

    sql = """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
        ORDER BY date
    """
    df = con.execute(sql, [target.metric_id, target.geo_id, pt_id]).fetchdf()

    if df.empty:
        raise ValueError(
            f"No data found for metric={target.metric_id}, geo={target.geo_id}, pt={pt_id}"
        )

    s = df.set_index("date")["value"].astype(float)

    if len(s) < min_obs:
        raise ValueError(
            f"Not enough observations for {target.metric_id}/{target.geo_id}/{pt_id}: "
            f"{len(s)} < {min_obs}"
        )

    # We could enforce monthly frequency here, but to avoid surprises,
    # just return the series as-is for now.
    return s


# -----------------------------------------
# Model fitting
# -----------------------------------------

def fit_sarimax(
    y: pd.Series,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
) -> SARIMAX:
    """
    Fit a univariate SARIMAX model.
    """
    model = SARIMAX(
        endog=y,
        order=order,
        seasonal_order=seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    results = model.fit(disp=False)
    return results


# -----------------------------------------
# Forecast_runs / forecast_predictions writes
# -----------------------------------------

def _next_run_id(con) -> int:
    """
    Manually allocate a new run_id, since we're not using IDENTITY.
    """
    row = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM forecast_runs").fetchone()
    return int(row[0])


def insert_forecast_run(
    target: TargetSpec,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    horizon_max_months: int,
    algo_params: Dict,
    model_name: str = "sarimax",
    model_version: str = "v1",
    notes: Optional[str] = None,
) -> int:
    """
    Insert a row into forecast_runs and return run_id.
    """
    con = get_connection()
    run_id = _next_run_id(con)

    sql = """
        INSERT INTO forecast_runs (
            run_id,
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
            is_active
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
    """

    params = [
        run_id,
        model_name,
        model_version,
        target.metric_id,
        target.geo_id,
        # Store Redfin pt IDs as strings ('-1', '6', etc.). None => NULL.
        target.property_type_id,
        target.freq,
        train_start.date(),
        train_end.date(),
        horizon_max_months,
        json.dumps(algo_params),
        notes,
    ]

    con.execute(sql, params)
    return run_id


def insert_predictions(
    run_id: int,
    forecast_values: np.ndarray,
    conf_int: np.ndarray,
    last_date: pd.Timestamp,
    horizon_max_months: int,
):
    """
    Insert horizon_max_months rows into forecast_predictions.

    We compute target_date as the month-end of each future month after last_date.
    """
    con = get_connection()

    # Build monthly target dates from the last observed month
    last_period = last_date.to_period("M")
    future_periods = [last_period + i for i in range(1, horizon_max_months + 1)]
    target_dates = [p.to_timestamp(how="end") for p in future_periods]

    records = []
    for i, (dt, y_hat, ci_row) in enumerate(zip(target_dates, forecast_values, conf_int), start=1):
        horizon_steps = i
        horizon_months = i  # you can deviate later if you want non-monthly steps
        y_hat = float(y_hat)
        y_hat_lo = float(ci_row[0]) if ci_row is not None else None
        y_hat_hi = float(ci_row[1]) if ci_row is not None else None

        records.append(
            (
                run_id,
                dt.date(),
                horizon_steps,
                horizon_months,
                y_hat,
                y_hat_lo,
                y_hat_hi,
            )
        )

    sql = """
        INSERT INTO forecast_predictions (
            run_id,
            target_date,
            horizon_steps,
            horizon_months,
            y_hat,
            y_hat_lo,
            y_hat_hi
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """

    con.executemany(sql, records)


# -----------------------------------------
# End-to-end runner
# -----------------------------------------

def run_sarimax_forecast(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str] = None,
    horizon_max_months: int = 12,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
    notes: Optional[str] = None,
) -> int:
    """
    End-to-end SARIMAX forecasting run for a single target series.

    - Loads the time series from fact_timeseries
    - Fits SARIMAX
    - Generates forecast + 95% CI
    - Writes into forecast_runs and forecast_predictions
    - Returns run_id
    """
    # Normalize property_type_id to string (Redfin IDs) or None
    pt_id_str: Optional[str]
    if property_type_id is None:
        pt_id_str = None
    else:
        pt_id_str = str(property_type_id)

    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=pt_id_str,
        freq="M",
    )

    y = load_series(target)
    train_start = y.index[0]
    train_end = y.index[-1]

    results = fit_sarimax(y, order=order, seasonal_order=seasonal_order)

    fc = results.get_forecast(steps=horizon_max_months)
    mean_forecast = fc.predicted_mean.values
    ci = fc.conf_int().values  # shape: (horizon, 2)

    algo_params = {
        "order": order,
        "seasonal_order": seasonal_order,
        "n_obs": len(y),
    }

    run_id = insert_forecast_run(
        target=target,
        train_start=train_start,
        train_end=train_end,
        horizon_max_months=horizon_max_months,
        algo_params=algo_params,
        model_name="sarimax",
        model_version="v1",
        notes=notes,
    )

    insert_predictions(
        run_id=run_id,
        forecast_values=mean_forecast,
        conf_int=ci,
        last_date=train_end,
        horizon_max_months=horizon_max_months,
    )

    return run_id


# -----------------------------------------
# CLI entry point
# -----------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SARIMAX forecast for a single target series.")
    parser.add_argument("--metric_id", required=True)
    parser.add_argument("--geo_id", required=True)
    parser.add_argument(
        "--property_type_id",
        help="Redfin property type id as string (e.g. -1, 6, 13). Omit for non-Redfin/all.",
    )
    parser.add_argument("--horizon", type=int, default=12)

    args = parser.parse_args()

    run_id = run_sarimax_forecast(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon_max_months=args.horizon,
        notes="CLI SARIMAX demo run",
    )

    print(f"Created forecast run_id={run_id}")
