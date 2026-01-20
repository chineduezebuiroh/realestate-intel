# forecast/sarimax_univariate.py

import os
import json
from dataclasses import dataclass
from typing import Optional, Tuple, Dict
from datetime import date

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
    *,
    data_asof: Optional[pd.Timestamp] = None,  # month-end cutoff (optional)
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
    """
    params = [target.metric_id, target.geo_id, pt_id]
    
    if data_asof is not None:
        # Ensure we compare apples-to-apples with your monthly indexing
        # (use date-only; DuckDB DATE compares cleanly)
        sql += " AND date <= ?"
        params.append(pd.Timestamp(data_asof).date())
    
    sql += " ORDER BY date"
    
    df = con.execute(sql, params).fetchdf()

    con.close()

    if df.empty:
        raise ValueError(
            f"No data found for metric={target.metric_id}, geo={target.geo_id}, pt={pt_id}"
        )

    # Build series
    s = df.set_index("date")["value"].astype(float)

    # Normalize index: month-end timestamps (matches the rest of your system)
    s.index = pd.to_datetime(s.index)
    s.index = s.index.to_period("M").to_timestamp(how="end")

    # Deduplicate + sort (keep last)
    s = s[~s.index.duplicated(keep="last")].sort_index()

    # Enforce monthly grid so statsmodels gets a supported index
    full_idx = pd.date_range(s.index.min(), s.index.max(), freq="ME")  # month-end
    s = s.reindex(full_idx)

    # Hard fail on missing values (otherwise your model “works” but is garbage)
    if s.isna().any():
        miss = s.index[s.isna()]
        # Allow small gaps by dropping them, but be strict:
        # - only allow up to 2 missing months
        # - and only if they are within the last 12 months (publication gaps / revisions)
        if len(miss) <= 2 and miss.min() >= (s.index.max() - pd.offsets.MonthEnd(12)):
            print(
                f"[sarimax_univariate] WARNING: dropping missing months after reindex: "
                f"n_missing={len(miss)} first={miss[0].date()} last={miss[-1].date()}"
            )
            s = s.dropna()
        else:
            raise ValueError(
                f"Missing target values after monthly reindex: n_missing={len(miss)} "
                f"first_missing={miss[0].date()} last_missing={miss[-1].date()}"
            )


    if len(s) < min_obs:
        raise ValueError(
            f"Not enough observations for {target.metric_id}/{target.geo_id}/{pt_id}: "
            f"{len(s)} < {min_obs}"
        )

    return s

# -----------------------------------------
# Model fitting
# -----------------------------------------

def fit_sarimax(
    y: pd.Series,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
):
    """
    Fit a univariate SARIMAX model.
    """
    endog = pd.Series(y.values)  # RangeIndex for statsmodels stability
    model = SARIMAX(
        endog=endog,
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
    *,
    run_kind: str = "live",
    data_asof: Optional[date] = None,
    batch_id: Optional[str] = None,
    model_name: str = "sarimax_univariate",
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
            is_active,
            run_kind,
            batch_id,
            data_asof
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?)
    """
    params = [
        run_id,
        model_name,
        model_version,
        target.metric_id,
        target.geo_id,
        target.property_type_id,
        target.freq,
        train_start.date(),
        train_end.date(),
        horizon_max_months,
        json.dumps(algo_params),
        notes,
        run_kind,
        batch_id,
        data_asof,
    ]


    con.execute(sql, params)
    con.close()
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
    *,
    data_asof: Optional[str] = None,
    run_kind: str = "live",
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

    data_asof_dt = pd.to_datetime(data_asof).date() if data_asof else None
    y = load_series(target, data_asof=data_asof_dt)
    train_start = y.index[0]
    train_end = y.index[-1]

    results = fit_sarimax(y, order=order, seasonal_order=seasonal_order)

    fc = results.get_forecast(steps=horizon_max_months)
    mean_forecast = fc.predicted_mean.values
    ci = fc.conf_int().values  # shape: (horizon, 2)

    algo_params = {
        "order": order,
        "seasonal_order": seasonal_order,
        "n_obs": int(len(y)),
        "data_asof": str(data_asof_dt) if data_asof_dt else None,
        "run_kind": str(run_kind) if run_kind else None,
    }
    algo_params["dropped_missing_months"] = True  # set to False if none were dropped


    run_id = insert_forecast_run(
        target=target,
        train_start=train_start,
        train_end=train_end,
        horizon_max_months=horizon_max_months,
        algo_params=algo_params,
        run_kind=run_kind,
        data_asof=str(data_asof_dt) if data_asof_dt else None,
        batch_id=None,
        model_name="sarimax_univariate",
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
    parser.add_argument(
        "--data_asof",
        type=str,
        default=None,
        help="YYYY-MM-DD cutoff for training data (live runs)"
    )
    parser.add_argument(
        "--run_kind",
        type=str,
        default="live",
        help="Run classification (e.g. live_near, live_outlook)"
    )


    args = parser.parse_args()

    run_id = run_sarimax_forecast(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon_max_months=args.horizon,
        data_asof=args.data_asof,
        run_kind=args.run_kind,
        notes="AUTO: promoted winner from model_select_single - SARIMAX",
    )

    print(f"Created forecast run_id={run_id}")
