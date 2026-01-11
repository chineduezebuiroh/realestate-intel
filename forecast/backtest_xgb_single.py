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

from .db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
)

from .backtest_utils import (
    choose_anchor_dates, 
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)

TEMP_DEBUG_LIMIT = 300  # set to 'None' when finished debugging


# ==========================================================
# Helpers
# ==========================================================
def _parse_data_asof(s: str | None):
    if not s:
        return None
    return pd.to_datetime(s).date()

# ==========================================================
# Helpers for iterative forecasting
# ==========================================================
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
    *,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
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

    if TEMP_DEBUG_LIMIT is not None:
        candidate_specs = candidate_specs[:TEMP_DEBUG_LIMIT]
        print(f"[xgb_backtest] TEMP: truncating to {len(candidate_specs)} candidates for debugging.")

    min_train_len = args.min_train_len
    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    try:
        y_full, X_full, base_series_full, selected_specs = build_design_matrix_incremental(
            target=target,
            candidate_specs=candidate_specs,
            min_obs=required_obs,
            max_features=None,
        )
    except ValueError as e:
        print(f"[xgb_backtest] Incremental design matrix build failed: {e}")
        print("[xgb_backtest] Skipping XGB backtest for this target.")
        return

    y_full = y_full.copy()
    X_full = X_full.copy()
    y_full.index = pd.PeriodIndex(y_full.index, freq="M").to_timestamp(how="end")
    X_full.index = y_full.index

    """
    batch_id = new_batch_id()
    data_asof = y_full.index.max().date()
    """
    
    batch_id = batch_id or new_batch_id()
    
    # data_asof: if not passed, compute from series after month-end normalization
    if data_asof is None:
        data_asof = y_full.index.max().date()  # or y.index.max().date() depending on script
    else:
        data_asof = _parse_data_asof(data_asof)
    print(f"[xgb_backtest] batch_id={batch_id} data_asof={data_asof}")

    print(
        f"[xgb_backtest] Final design matrix: "
        f"n_obs={len(y_full)}, n_features={X_full.shape[1]}, "
        f"selected_series={len(selected_specs)}"
    )

    anchors = choose_anchor_dates(
        y_full,
        horizon=horizon,
        min_train_len=min_train_len,
        step_months=args.anchor_step_months,
        max_anchors=args.max_anchors,
        latest_anchor_offset_months=args.latest_anchor_offset_months,
    )
    
    if not anchors:
        print("[xgb_backtest] Not enough history to run backtests.")
        return
    
    print(f"[xgb_backtest] Found {len(anchors)} anchors.")
    last_date = y_full.index[-1]
    feature_names = list(X_full.columns)
    results_summary = []
    
    for anchor_date in anchors:
        print(f"\n[xgb_backtest] Anchor at date={anchor_date.date()}")

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

        # ---- Phase A placeholder future features ----
        # Carry-forward the last observed feature row for all future steps.
        last_row = X_train.iloc[[-1]]  # (1, n_features)
        
        # Build (horizon_bt, n_features) by repeating last_row
        X_future = pd.concat([last_row] * horizon_bt, ignore_index=True)
        
        # Predict all steps in one shot
        preds_array = model.predict(X_future).astype(float)


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

        con = get_connection()

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
        
        run_id = insert_run(
            con=con,
            model_name="xgb_backtest",
            model_version="v1",
            target_metric_id=target.metric_id,
            target_geo_id=target.geo_id,
            target_property_type_id=target.property_type_id,
            freq="M",
            train_start=y_train.index[0].date(),
            train_end=anchor_date.date(),
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            notes=f"XGB backtest anchor={anchor_date.date()}",
            is_active=False,
            run_kind="backtest",
            batch_id=batch_id,
            data_asof=data_asof,
        )
        
        last_period = anchor_date.to_period("M")
        future_periods = [last_period + i for i in range(1, horizon_bt + 1)]
        target_dates = [p.to_timestamp(how="end").date() for p in future_periods]
        
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=preds_array,
            y_hat_lo=None,
            y_hat_hi=None,
        )
        
        con.close()

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

    parser.add_argument("--min_train_len", type=int, default=DEFAULT_MIN_TRAIN_LEN)
    parser.add_argument("--anchor_step_months", type=int, default=DEFAULT_ANCHOR_STEP_MONTHS)
    parser.add_argument("--max_anchors", type=int, default=DEFAULT_MAX_ANCHORS)
    parser.add_argument("--latest_anchor_offset_months", type=int, default=None)

    parser.add_argument("--batch_id", type=str, default=None)
    parser.add_argument("--data_asof", type=str, default=None)

    args = parser.parse_args()

    run_backtest_xgb_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
    )
