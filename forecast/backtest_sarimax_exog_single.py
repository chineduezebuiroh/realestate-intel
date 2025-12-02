# forecast/backtest_sarimax_exog_single.py

import os
from typing import List, Dict, Optional, Tuple

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from xgboost import XGBRegressor

from .feature_loader import TargetSpec, FeatureSpec, build_design_matrix, build_universal_feature_specs


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
    Insert a SARIMAX-exog backtest run into forecast_runs.
    Mark is_active = FALSE so these are never used as live forecasts.
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
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE)
    """

    import json
    notes = f"SARIMAX-exog backtest anchor={anchor_date.date()}"

    params = [
        run_id,
        "sarimax_exog_backtest",
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
    conf_int: np.ndarray,
    last_date: pd.Timestamp,
    horizon_max_months: int,
):
    """
    Insert forecast rows into forecast_predictions for a backtest run.
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


# -----------------------------
# Anchor selection
# -----------------------------

def choose_anchor_indices(
    y: pd.Series,
    horizon: int,
    min_train_len: int = 60,
    max_anchors: int = 3,
) -> List[int]:
    """
    Same logic as univariate backtest:
    - Work backwards from the latest possible anchor (last_idx - horizon)
    - Ensure at least min_train_len observations before anchor
    - Step back 12 months per anchor
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
# Default "kitchen sink" spec for this target
# -----------------------------
"""
from .feature_loader import (
    TargetSpec,
    FeatureSpec,
    build_design_matrix,
    build_auto_feature_specs_for_target,
)
"""

def build_universal_feature_specs(
    target: TargetSpec,
    lag_scheme=[1, 2, 3, 6, 12],
) -> List[FeatureSpec]:

    exclude = {(target.metric_id, target.geo_id, target.property_type_id)}
    all_series = discover_all_series(
        exclude_metrics=[target.metric_id],
        exclude_targets=exclude,
    )

    specs = []
    for (metric_id, geo_id, pt_id) in all_series:
        specs.append(
            FeatureSpec(
                name=f"{metric_id}__{geo_id}__{pt_id}",
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id,
                lags=list(lag_scheme),
            )
        )
    return specs


def get_default_feature_specs_for_target(
    metric_id: str,
    geo_id: str,
    property_type_id: str,
) -> List[FeatureSpec]:
    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)
    return build_universal_feature_specs(target)

# -----------------------------
# XGBoost-based feature selection
# -----------------------------

def select_features_with_xgb(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    max_features: int = 8,
) -> List[str]:
    """
    Run XGBRegressor on (X_train, y_train), rank features by importance,
    and return the top 'max_features' column names.
    """
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

    importances = model.feature_importances_
    cols = np.array(X_train.columns)

    order = np.argsort(importances)[::-1]  # descending
    top = cols[order][:max_features]
    top = [c for c in top if importances[cols == c][0] > 0]  # drop zero-importance

    return top


# -----------------------------
# Main backtest entry
# -----------------------------

def run_backtest_sarimax_exog_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    use_xgb_feature_selection: bool = True,
    max_features_from_xgb: int = 8,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
):
    """
    Backtest SARIMAX with exogenous regressors for a single target series.

    - Builds a "kitchen sink" design matrix using FeatureSpec.
    - Optionally uses XGBoost to pick top features for SARIMAX exog.
    - Writes each anchor's forecasts as a backtest run (never is_active).
    """
    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=property_type_id,
    )

    feature_specs = get_default_feature_specs_for_target(metric_id, geo_id, property_type_id)
    if not feature_specs:
        print("[backtest_exog] No default feature specs defined for this target; nothing to do.")
        return

    # Build full design matrix once
    y_full, X_full, _ = build_design_matrix(
        target=target,
        feature_specs=feature_specs,
        min_obs=60,
    )

    anchors = choose_anchor_indices(y_full, horizon=horizon, min_train_len=60, max_anchors=3)
    if not anchors:
        print("[backtest_exog] Not enough history to run backtests.")
        return

    print(f"[backtest_exog] Found {len(anchors)} anchors.")
    last_date = y_full.index[-1]
    results_summary = []

    for idx in anchors:
        anchor_date = y_full.index[idx]
        print(f"\n[backtest_exog] Anchor at index={idx}, date={anchor_date.date()}")

        # Training data up to anchor_date
        y_train = y_full.loc[:anchor_date]
        X_train = X_full.loc[:anchor_date]

        # How many months of actuals after anchor?
        anchor_period = anchor_date.to_period("M")
        last_period = last_date.to_period("M")
        months_available = (last_period.year - anchor_period.year) * 12 + (last_period.month - anchor_period.month)
        horizon_bt = min(horizon, months_available)
        if horizon_bt <= 0:
            print("[backtest_exog] No future months available for this anchor; skipping.")
            continue

        print(
            f"[backtest_exog] Training length={len(y_train)}, "
            f"backtest horizon={horizon_bt} months, "
            f"n_features={X_train.shape[1]}"
        )

        # Optional hybrid step: XGB feature selection
        selected_feature_names = list(X_train.columns)
        if use_xgb_feature_selection:
            selected_feature_names = select_features_with_xgb(
                X_train=X_train,
                y_train=y_train,
                max_features=max_features_from_xgb,
            )
            if not selected_feature_names:
                print("[backtest_exog] XGB selected no informative features; using all features instead.")
                selected_feature_names = list(X_train.columns)

            print(f"[backtest_exog] Selected features: {selected_feature_names}")

        X_train_sel = X_train[selected_feature_names]

        # Fit SARIMAX with exog
        model = SARIMAX(
            endog=y_train,
            exog=X_train_sel,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False)

        # Future exog: carry-forward last row for horizon_bt steps
        last_exog_row = X_train_sel.iloc[[-1]].values  # shape (1,k)
        exog_future = np.repeat(last_exog_row, horizon_bt, axis=0)

        fc = res.get_forecast(steps=horizon_bt, exog=exog_future)
        mean_fc = fc.predicted_mean.values
        ci = fc.conf_int().values  # (horizon_bt, 2)

        algo_params = {
            "order": order,
            "seasonal_order": seasonal_order,
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),
            "use_xgb_feature_selection": use_xgb_feature_selection,
            "selected_features": selected_feature_names,
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
            forecast_values=mean_fc,
            conf_int=ci,
            last_date=anchor_date,
            horizon_max_months=horizon_bt,
        )

        print(f"[backtest_exog] Created SARIMAX-exog backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})

    print("\n[backtest_exog] Summary:")
    for r in results_summary:
        print(f"  anchor={r['anchor_date'].date()} -> run_id={r['run_id']}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backtest SARIMAX-exog for a single target series.")
    parser.add_argument("--metric_id", default="median_sale_price")
    parser.add_argument("--geo_id", default="dc_city")
    parser.add_argument("--property_type_id", default="-1")
    parser.add_argument("--horizon", type=int, default=12)
    parser.add_argument(
        "--no_xgb_selection",
        action="store_true",
        help="Disable XGB-based feature selection and use all exog features.",
    )

    args = parser.parse_args()

    run_backtest_sarimax_exog_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        use_xgb_feature_selection=not args.no_xgb_selection,
    )
