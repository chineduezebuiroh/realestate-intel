# forecast/sarimax_exog.py

import os
import json
from typing import Optional, List, Dict, Tuple

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from forecast.feature_loader import TargetSpec, FeatureSpec, build_design_matrix
from forecast.sarimax_redfin import run_sarimax_forecast as run_sarimax_univariate


# -----------------------------------------
# DB helpers
# -----------------------------------------

def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


def _next_run_id(con) -> int:
    row = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM forecast_runs").fetchone()
    return int(row[0])


def insert_forecast_run(
    target: TargetSpec,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    horizon_max_months: int,
    algo_params: Dict,
    model_name: str = "sarimax_exog",
    model_version: str = "v1",
    notes: Optional[str] = None,
) -> int:
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
        target.property_type_id,
        "M",  # monthly
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
    Insert forecast rows into forecast_predictions.

    We build future dates as month-end stamps after last_date.
    """
    con = get_connection()

    last_period = last_date.to_period("M")
    future_periods = [last_period + i for i in range(1, horizon_max_months + 1)]
    target_dates = [p.to_timestamp(how="end") for p in future_periods]

    records = []
    for i, (dt, y_hat, ci_row) in enumerate(zip(target_dates, forecast_values, conf_int), start=1):
        horizon_steps = i
        horizon_months = i
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
# Core: SARIMAX with exogenous regressors
# -----------------------------------------

def fit_sarimax_exog(
    y: pd.Series,
    X: pd.DataFrame,
    order: Tuple[int, int, int],
    seasonal_order: Tuple[int, int, int, int],
) -> SARIMAX:
    """
    Fit SARIMAX with exogenous regressors.
    """
    model = SARIMAX(
        endog=y,
        exog=X,
        order=order,
        seasonal_order=seasonal_order,
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    results = model.fit(disp=False)
    return results


def run_sarimax_exog(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str] = None,
    feature_specs: Optional[List[FeatureSpec]] = None,
    horizon_max_months: int = 12,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
    notes: Optional[str] = None,
) -> int:
    """
    End-to-end SARIMAX run with exogenous regressors.

    Cases:
      - If feature_specs is None or empty -> delegate to univariate SARIMAX.
      - Else:
          * build_design_matrix() to get y, X, base_series
          * fit SARIMAX(y, exog=X)
          * carry-forward last exog row for future horizon
          * write to forecast_runs + forecast_predictions
    """
    # Normalize pt_id to string or None
    pt_id_str = str(property_type_id) if property_type_id is not None else None

    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=pt_id_str,
    )

    # If no exogenous feature specs provided, just reuse the univariate pipeline
    if not feature_specs:
        return run_sarimax_univariate(
            metric_id=metric_id,
            geo_id=geo_id,
            property_type_id=pt_id_str,
            horizon_max_months=horizon_max_months,
            order=order,
            seasonal_order=seasonal_order,
            notes=notes or "SARIMAX (no exog, delegated from sarimax_exog)",
        )

    # Build design matrix using the shared feature loader
    y, X, base_series = build_design_matrix(
        target=target,
        feature_specs=feature_specs,
        min_obs=60,
    )

    train_start = y.index[0]
    train_end = y.index[-1]

    # Fit SARIMAX with exogenous regressors
    results = fit_sarimax_exog(
        y=y,
        X=X,
        order=order,
        seasonal_order=seasonal_order,
    )

    # Build future exog:
    # simplest version: carry-forward the last observed row of X for all future steps
    last_exog_row = X.iloc[[-1]]  # shape (1, k)
    exog_future = np.repeat(last_exog_row.values, horizon_max_months, axis=0)

    fc = results.get_forecast(steps=horizon_max_months, exog=exog_future)
    mean_forecast = fc.predicted_mean.values
    ci = fc.conf_int().values  # shape: (horizon, 2)

    algo_params = {
        "order": order,
        "seasonal_order": seasonal_order,
        "n_obs": int(y.shape[0]),
        "exog_features": [f"{f.name} lags={f.lags}" for f in feature_specs],
    }

    run_id = insert_forecast_run(
        target=target,
        train_start=train_start,
        train_end=train_end,
        horizon_max_months=horizon_max_months,
        algo_params=algo_params,
        model_name="sarimax_exog",
        model_version="v1",
        notes=notes or "SARIMAX with exogenous regressors",
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
# CLI entry (univariate or default feature config only)
# -----------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run SARIMAX (optionally with exogenous regressors).")
    parser.add_argument("--metric_id", required=True)
    parser.add_argument("--geo_id", required=True)
    parser.add_argument(
        "--property_type_id",
        help="Redfin property type id as string (e.g. -1, 6, 13). Omit for non-Redfin/all.",
    )
    parser.add_argument("--horizon", type=int, default=12)
    parser.add_argument(
        "--with_default_exog",
        action="store_true",
        help="If set, use a simple default exog config for this metric/geo (if defined in code).",
    )

    args = parser.parse_args()
    pt_id = args.property_type_id

    feature_specs_cli: Optional[List[FeatureSpec]] = None

    # Example: very basic default exog for median_sale_price in dc_city using median_dom lags.
    # You can extend this section or ignore it and call run_sarimax_exog() from Python instead.
    if args.with_default_exog and args.metric_id == "median_sale_price" and args.geo_id == "dc_city":
        feature_specs_cli = [
            FeatureSpec(
                name="median_dom",
                metric_id="median_dom",
                geo_id="dc_city",
                property_type_id=pt_id,
                lags=[1, 2, 3],
            )
        ]

    run_id = run_sarimax_exog(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=pt_id,
        feature_specs=feature_specs_cli,
        horizon_max_months=args.horizon,
        notes="CLI SARIMAX exog demo run",
    )

    print(f"Created SARIMAX(exog) run_id={run_id}")
