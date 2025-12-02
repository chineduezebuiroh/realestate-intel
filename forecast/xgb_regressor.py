# forecast/xgb_regressor.py

import os
import json
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import duckdb
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from forecast.feature_loader import TargetSpec, FeatureSpec, build_design_matrix


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
    model_name: str = "xgboost",
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
    last_date: pd.Timestamp,
    horizon_max_months: int,
):
    """
    Insert XGBoost predictions into forecast_predictions.
    We don't compute intervals here, so y_hat_lo/hi are NULL.
    """
    con = get_connection()

    last_period = last_date.to_period("M")
    future_periods = [last_period + i for i in range(1, horizon_max_months + 1)]
    target_dates = [p.to_timestamp(how="end") for p in future_periods]

    records = []
    for i, (dt, y_hat) in enumerate(zip(target_dates, forecast_values), start=1):
        records.append(
            (
                run_id,
                dt.date(),
                i,        # horizon_steps
                i,        # horizon_months
                float(y_hat),
                None,     # y_hat_lo
                None,     # y_hat_hi
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
# XGBoost forecasting logic
# -----------------------------------------

def _make_base_series_for_forecast(
    base_series: Dict[str, pd.Series],
    feature_specs: List[FeatureSpec],
    target: TargetSpec,
) -> Dict[str, pd.Series]:
    """
    Prepare base series dict for iterative forecasting.

    For now:
      - We treat feature series as known only up to last observed date,
        and assume they remain constant at their last value into the future.
      - We will extend the target 'y' series with predicted values as we go.
    """
    # Shallow copy is fine
    series = {k: v.copy() for k, v in base_series.items()}
    return series


def _build_single_row_design(
    series: Dict[str, pd.Series],
    feature_specs: List[FeatureSpec],
) -> Tuple[pd.Timestamp, pd.DataFrame]:
    """
    Given base series extended up to some future date, rebuild the design matrix
    and return the last row as a single-sample DataFrame (for prediction).
    """
    # Align base series on common index
    df_base = pd.concat(series.values(), axis=1, join="inner")
    df_base.columns = list(series.keys())

    # Build lagged features
    feature_cols = {}
    for spec in feature_specs:
        col_name = spec.name
        for lag in spec.lags:
            lag_col = f"{col_name}_lag{lag}"
            feature_cols[lag_col] = df_base[col_name].shift(lag)

    df_features = pd.DataFrame(feature_cols, index=df_base.index)
    df_all = pd.concat([df_base["y"], df_features], axis=1).dropna()

    # We only need the last row for prediction
    last_idx = df_all.index[-1]
    X_future = df_all.drop(columns=["y"]).loc[[last_idx]]  # shape (1, n_features)
    return last_idx, X_future


def run_xgb_forecast(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str] = None,
    horizon_max_months: int = 12,
    feature_specs: Optional[List[FeatureSpec]] = None,
    notes: Optional[str] = None,
) -> int:
    """
    End-to-end XGBoost forecast:

      - Build design matrix (y, X) using feature_loader.
      - Fit XGBRegressor on full history.
      - Iteratively forecast horizon_max_months steps ahead.
      - Write into forecast_runs + forecast_predictions.

    By default, uses self-lags of the target as features.
    """
    # Normalize pt_id
    pt_id_str = str(property_type_id) if property_type_id is not None else None

    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=pt_id_str,
    )

    # Default: self-lags of the target only (univariate, but easily extended)
    if feature_specs is None:
        feature_specs = [
            FeatureSpec(
                name=metric_id,
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id_str,
                lags=[1, 2, 3, 6, 12],
            )
        ]

    # Build training design matrix
    y, X, base_series = build_design_matrix(
        target=target,
        feature_specs=feature_specs,
        min_obs=60,
    )

    train_start = y.index[0]
    train_end = y.index[-1]

    # Fit XGBRegressor (minimal hyperparams for now)
    model = XGBRegressor(
        n_estimators=400,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="reg:squarederror",
        random_state=42,
    )
    model.fit(X, y)

    # Prepare for iterative forecasting
    series = _make_base_series_for_forecast(base_series, feature_specs, target)
    # We'll treat series["y"] as the target series we extend with predictions
    # and other feature series as exogenous, extended via carry-forward.

    preds = []

    for step in range(1, horizon_max_months + 1):
        # Extend feature series dates by 1 month, carrying last value forward
        last_observed_date = series["y"].index[-1]
        last_period = last_observed_date.to_period("M")
        next_period = last_period + 1
        next_date = next_period.to_timestamp(how="end")

        for name, s in series.items():
            last_val = s.iloc[-1]
            series[name] = s.reindex(s.index.union([next_date])).sort_index()
            series[name].loc[next_date] = last_val  # carry-forward

        # Build design row for this new date
        _, X_future = _build_single_row_design(series, feature_specs)

        # Predict
        y_hat = model.predict(X_future)[0]
        preds.append(y_hat)

        # Update target series with the new prediction
        s_y = series["y"]
        series["y"].loc[next_date] = y_hat

    preds_array = np.array(preds, dtype=float)

    algo_params = {
        "model": "XGBRegressor",
        "n_estimators": 400,
        "max_depth": 4,
        "learning_rate": 0.05,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "n_obs": int(y.shape[0]),
        "features": [f"{f.name} lags={f.lags}" for f in feature_specs],
    }

    run_id = insert_forecast_run(
        target=target,
        train_start=train_start,
        train_end=train_end,
        horizon_max_months=horizon_max_months,
        algo_params=algo_params,
        model_name="xgboost",
        model_version="v1",
        notes=notes or "XGBoost forecast",
    )

    insert_predictions(
        run_id=run_id,
        forecast_values=preds_array,
        last_date=train_end,
        horizon_max_months=horizon_max_months,
    )

    return run_id


# -----------------------------------------
# CLI entry
# -----------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run XGBoost forecast for a single target series.")
    parser.add_argument("--metric_id", required=True)
    parser.add_argument("--geo_id", required=True)
    parser.add_argument(
        "--property_type_id",
        help="Redfin property type id as string (e.g. -1, 6, 13). Omit for non-Redfin/all.",
    )
    parser.add_argument("--horizon", type=int, default=12)

    args = parser.parse_args()

    run_id = run_xgb_forecast(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon_max_months=args.horizon,
        notes="CLI XGBoost demo run",
    )

    print(f"Created XGBoost forecast run_id={run_id}")
