# forecast/backtest_xgb_single.py

import os
from typing import List, Dict, Optional

import duckdb
import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from .feature_loader import (
    TargetSpec,
    build_universal_feature_specs,
    build_design_matrix,
    build_design_matrix_incremental,
)



# -----------------------------
# DB helpers
# -----------------------------

def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


def _next_run_id(con) -> int:
    row = con.execute("SELECT COALESCE(MAX(run_id), 0) + 1 FROM forecast_runs").fetchone()
    return int(row[0])


def insert_forecast_run_backtest(
    target: TargetSpec,
    train_start: pd.Timestamp,
    train_end: pd.Timestamp,
    horizon_max_months: int,
    algo_params: Dict,
    anchor_date: pd.Timestamp,
) -> int:
    """
    Insert an XGBoost backtest run into forecast_runs.
    Mark is_active = FALSE so these are never used as live forecasts.
    """
    con = get_connection()
    run_id = _next_run_id(con)

    import json
    notes = f"XGB backtest anchor={anchor_date.date()}"

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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
    """

    params = [
        run_id,
        "xgb_backtest",
        "v1",
        target.metric_id,
        target.geo_id,
        target.property_type_id,
        "M",
        train_start.date(),
        train_end.date(),
        horizon_max_months,
        json.dumps(algo_params),
        notes,
    ]

    con.execute(sql, params)
    return run_id


def insert_predictions_backtest(
    run_id: int,
    forecast_values: np.ndarray,
    last_date: pd.Timestamp,
    horizon_max_months: int,
):
    """
    Insert XGBoost backtest predictions into forecast_predictions.
    No intervals here (y_hat_lo/hi = NULL).
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
                i,       # horizon_steps
                i,       # horizon_months
                float(y_hat),
                None,    # y_hat_lo
                None,    # y_hat_hi
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


# -----------------------------
# Anchor selection (reuse SARIMAX logic)
# -----------------------------

def choose_anchor_indices(
    y: pd.Series,
    horizon: int,
    min_train_len: int = 60,
    max_anchors: int = 3,
) -> List[int]:
    """
    Choose a few anchor indices for backtesting.
    """
    n = len(y)
    if n < (min_train_len + horizon):
        return []

    last_idx = n - 1
    max_anchor_idx = last_idx - horizon
    min_anchor_idx = min_train_len - 1

    anchors = []
    idx = max_anchor_idx
    while idx >= min_anchor_idx and len(anchors) < max_anchors:
        anchors.append(idx)
        idx -= 12

    anchors.sort()
    return anchors


# -----------------------------
# Helpers for iterative forecasting
# -----------------------------

def _truncate_base_series_to_anchor(
    base_series: Dict[str, pd.Series],
    anchor_date: pd.Timestamp,
) -> Dict[str, pd.Series]:
    """
    Given base_series={name: full_series}, return a copy truncated to <= anchor_date.
    """
    out = {}
    for k, s in base_series.items():
        out[k] = s.loc[:anchor_date].copy()
    return out


def _build_single_row_design(
    series: Dict[str, pd.Series],
    feature_specs,
) -> pd.DataFrame:
    """
    Given truncated base series (up to some date), rebuild the lagged design matrix
    and return the last row (features at the most recent date).
    """
    # base df with all raw series
    df_base = pd.concat(series.values(), axis=1, join="inner")
    df_base.columns = list(series.keys())

    # build lagged features consistent with build_design_matrix
    feature_cols = {}
    for spec in feature_specs:
        col_name = spec.name
        for lag in spec.lags:
            lag_col = f"{col_name}_lag{lag}"
            feature_cols[lag_col] = df_base[col_name].shift(lag)

    df_features = pd.DataFrame(feature_cols, index=df_base.index)
    df_all = df_features.dropna()

    # single row: last index
    last_idx = df_all.index[-1]
    return df_all.loc[[last_idx]]  # shape (1, n_features)


# -----------------------------
# Main backtest entry
# -----------------------------

def run_backtest_xgb_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
):
    """
    Backtest XGBoost for a single target series using a universal feature set.

    For each anchor date:
      - build design matrix up to full history
      - restrict to rows <= anchor_date for training
      - iteratively forecast up to horizon months ahead using carry-forward exogs
      - store as backtest runs (is_active=FALSE)
    """

    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)

    candidate_specs = build_universal_feature_specs(target)
    if not candidate_specs:
        print("[xgb_backtest] No candidate features; skipping XGB backtest.")
        return

    try:
        y_full, X_full, base_series_full, selected_specs = build_design_matrix_incremental(
            target=target,
            candidate_specs=candidate_specs,
            min_obs=60,
            max_features=None,
        )
    except ValueError as e:
        print(f"[xgb_backtest] Incremental design matrix build failed: {e}")
        print("[xgb_backtest] Skipping XGB backtest for this target.")
        return

    print(
        f"[xgb_backtest] Final design matrix: "
        f"n_obs={len(y_full)}, n_features={X_full.shape[1]}, "
        f"selected_series={len(selected_specs)}"
    )

    
    anchors = choose_anchor_indices(y_full, horizon=horizon, min_train_len=60, max_anchors=3)
    if not anchors:
        print("[xgb_backtest] Not enough history to run backtests.")
        return

    print(f"[xgb_backtest] Found {len(anchors)} anchors.")
    last_date = y_full.index[-1]
    feature_names = list(X_full.columns)
    results_summary = []

    for idx in anchors:
        anchor_date = y_full.index[idx]
        print(f"\n[xgb_backtest] Anchor at index={idx}, date={anchor_date.date()}")

        y_train = y_full.loc[:anchor_date]
        X_train = X_full.loc[:anchor_date]

        anchor_period = anchor_date.to_period("M")
        last_period = last_date.to_period("M")
        months_available = (last_period.year - anchor_period.year) * 12 + (last_period.month - anchor_period.month)
        horizon_bt = min(horizon, months_available)
        if horizon_bt <= 0:
            print("[xgb_backtest] No future months available for this anchor; skipping.")
            continue

        print(
            f"[xgb_backtest] Training length={len(y_train)}, "
            f"backtest horizon={horizon_bt} months, "
            f"n_features={X_train.shape[1]}"
        )

        # Fit XGBoost
        model = XGBRegressor(
            n_estimators=400,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Prepare truncated base_series up to anchor
        series = _truncate_base_series_to_anchor(base_series_full, anchor_date)

        preds = []

        for step in range(1, horizon_bt + 1):
            # compute next date
            last_obs_date = list(series.values())[0].index[-1]
            last_period_step = last_obs_date.to_period("M")
            next_period = last_period_step + 1
            next_date = next_period.to_timestamp(how="end")

            # carry-forward exogs + y
            for name, s in series.items():
                last_val = s.iloc[-1]
                series[name] = s.reindex(s.index.union([next_date])).sort_index()
                series[name].loc[next_date] = last_val

            # build design row for this new date
            X_future = _build_single_row_design(series, feature_specs)
            # ensure same column order as training
            X_future = X_future.reindex(columns=feature_names)

            # predict
            y_hat = model.predict(X_future)[0]
            preds.append(y_hat)

            # update y series with the prediction
            series["y"].loc[next_date] = y_hat

        preds_array = np.array(preds, dtype=float)

        algo_params = {
            "model": "XGBRegressor",
            "n_estimators": 400,
            "max_depth": 4,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "n_obs": int(y_train.shape[0]),
            "n_features": int(X_train.shape[1]),
        }

        run_id = insert_forecast_run_backtest(
            target=target,
            train_start=y_train.index[0],
            train_end=anchor_date,
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            anchor_date=anchor_date,
        )

        insert_predictions_backtest(
            run_id=run_id,
            forecast_values=preds_array,
            last_date=anchor_date,
            horizon_max_months=horizon_bt,
        )

        print(f"[xgb_backtest] Created XGB backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})

    print("\n[xgb_backtest] Summary:")
    for r in results_summary:
        print(f"  anchor={r['anchor_date'].date()} -> run_id={r['run_id']}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backtest XGBoost for a single target series.")
    parser.add_argument("--metric_id", default="median_sale_price")
    parser.add_argument("--geo_id", default="dc_city")
    parser.add_argument("--property_type_id", default="-1")
    parser.add_argument("--horizon", type=int, default=12)

    args = parser.parse_args()

    run_backtest_xgb_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
    )
