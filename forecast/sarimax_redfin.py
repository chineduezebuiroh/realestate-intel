# forecast/sarimax_redfin.py

import json
from dataclasses import dataclass
from typing import Optional, Tuple, Dict

import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from utils.db import get_connection  # assumes you already have this


@dataclass
class TargetSpec:
    metric_id: str
    geo_id: str
    property_type_id: Optional[int] = None
    freq: str = "M"


def load_series(
    target: TargetSpec,
    min_obs: int = 36,
) -> pd.Series:
    """
    Load a univariate monthly series from fact_timeseries.

    Returns a pandas Series indexed by date.
    """
    con = get_connection()
    params = [target.metric_id, target.geo_id]
    where_pt = "AND property_type_id IS NULL"
    if target.property_type_id is not None:
        where_pt = "AND property_type_id = ?"
        params.append(target.property_type_id)

    sql = f"""
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          {where_pt}
        ORDER BY date
    """
    df = con.execute(sql, params).fetchdf()

    if df.empty:
        raise ValueError("No data found for target series")

    s = df.set_index("date")["value"].astype(float)

    if len(s) < min_obs:
        raise ValueError(f"Not enough observations: {len(s)} < {min_obs}")

    # Ensure monthly frequency, forward fill any occasional gaps
    s = s.asfreq(target.freq)
    s = s.ffill()

    return s


def fit_sarimax(
    y: pd.Series,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
) -> SARIMAX:
    """
    Fit SARIMAX model (univariate).
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
    sql = """
        INSERT INTO forecast_runs (
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
        RETURNING run_id
    """

    params = [
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
    ]

    run_id = con.execute(sql, params).fetchone()[0]
    return int(run_id)


def insert_predictions(
    run_id: int,
    forecast: pd.Series,
    conf_int: pd.DataFrame,
):
    """
    Insert forecast_predictions rows for a given run_id.
    """
    con = get_connection()

    records = []
    for i, (dt, y_hat) in enumerate(forecast.items(), start=1):
        ci_row = conf_int.loc[dt]
        y_hat_lo = float(ci_row.iloc[0])
        y_hat_hi = float(ci_row.iloc[1])
        horizon_months = i  # assuming monthly

        records.append(
            (
                run_id,
                dt.date(),
                i,
                horizon_months,
                float(y_hat),
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


def run_sarimax_forecast(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[int] = None,
    horizon_max_months: int = 12,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
    notes: Optional[str] = None,
) -> int:
    """
    End-to-end SARIMAX run:
    - load series
    - fit model
    - generate forecast
    - store run + predictions

    Returns run_id.
    """
    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=property_type_id,
        freq="M",
    )

    y = load_series(target)
    train_start = y.index[0]
    train_end = y.index[-1]

    results = fit_sarimax(y, order=order, seasonal_order=seasonal_order)

    # Forecast horizon_max_months steps ahead
    forecast = results.get_forecast(steps=horizon_max_months)
    mean_forecast = forecast.predicted_mean
    conf_int = forecast.conf_int()

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

    insert_predictions(run_id, mean_forecast, conf_int)

    return run_id


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SARIMAX forecast for a single target series.")
    parser.add_argument("--metric_id", required=True)
    parser.add_argument("--geo_id", required=True)
    parser.add_argument("--property_type_id", type=int)
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
