# forecast/backtest_sarimax_exog_single.py
import os
from typing import List, Dict, Optional, Tuple

#import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX
from xgboost import XGBRegressor

from .feature_loader import (
    TargetSpec,
    FeatureSpec,
    #build_design_matrix,
    build_universal_feature_specs,
    build_design_matrix_incremental,
)

from .db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
    store_selected_features_in_params,
)

from .backtest_utils import (
    choose_anchor_dates, 
    month_end_index,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)


TEMP_DEBUG_LIMIT = 300 #set to 'None' when finished debugging


# ==========================================================
# Helpers
# ==========================================================
def _parse_data_asof(s: str | None):
    if not s:
        return None
    return pd.to_datetime(s).date()

# ==========================================================
# Default "kitchen sink" spec for this target
# ==========================================================
    
def get_default_feature_specs_for_target(
    metric_id: str,
    geo_id: str,
    property_type_id: str,
) -> List[FeatureSpec]:
    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)
    specs = build_universal_feature_specs(target)
    if specs:
        print(f"[backtest_exog] Universal candidate set has {len(specs)} series.")
    else:
        print("[backtest_exog] No candidate exogenous series found for this target.")
    return specs

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
    
    #top = [c for c in top if importances[cols == c][0] > 0]  # drop zero-importance
    imp = dict(zip(X_train.columns, importances))
    top = [c for c in top if imp.get(c, 0) > 0]

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
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
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

    candidate_specs = get_default_feature_specs_for_target(metric_id, geo_id, property_type_id)
    if not candidate_specs:
        print("[backtest_exog] No feature specs available; skipping SARIMAX-exog backtest.")
        return

    # TEMP DEBUG: limit candidates to speed up iteration
    if TEMP_DEBUG_LIMIT is not None:
        candidate_specs = candidate_specs[:TEMP_DEBUG_LIMIT]
        print(f"[backtest_exog] TEMP: truncating to {len(candidate_specs)} candidates for debugging.")

    min_train_len = args.min_train_len
    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    try:
        y_full, X_full, base_series_full, selected_specs = build_design_matrix_incremental(
            target=target,
            candidate_specs=candidate_specs,
            min_obs=required_obs,
            max_features=None,  # or cap at, say, 20 if you want
        )
    except ValueError as e:
        print(f"[backtest_exog] Incremental design matrix build failed: {e}")
        print("[backtest_exog] Skipping SARIMAX-exog backtest for this target.")
        return

    print(
        f"[backtest_exog] Final design matrix: "
        f"n_obs={len(y_full)}, n_features={X_full.shape[1]}, "
        f"selected_series={len(selected_specs)}"
    )

    # Normalize index to month-end timestamps with an implicit monthly frequency
    y_full = y_full.copy()
    y_full.index = month_end_index(y_full.index)
    y_full = y_full[~y_full.index.duplicated(keep="last")].sort_index()
    
    if len(X_full) != len(y_full):
        raise ValueError(f"X_full and y_full length mismatch: {len(X_full)} vs {len(y_full)}")
    X_full = X_full.copy()
    X_full.index = y_full.index
    
    batch_id = batch_id or new_batch_id()
    if data_asof is None:
        data_asof = y_full.index.max().date()
    else:
        data_asof = _parse_data_asof(data_asof)
    print(f"[backtest_exog] batch_id={batch_id} data_asof={data_asof}")

    anchors = choose_anchor_dates(
        y_full,
        horizon=horizon,
        min_train_len=min_train_len,   # use your variable, not a magic 60
        step_months=args.anchor_step_months,
        max_anchors=args.max_anchors,
        latest_anchor_offset_months=args.latest_anchor_offset_months,
    )
    
    if not anchors:
        print("[backtest_exog] Not enough history to run backtests.")
        return
    
    print(f"[backtest_exog] Found {len(anchors)} anchors.")
    last_date = y_full.index[-1]
    results_summary = []
    
    for anchor_date in anchors:
        print(f"\n[backtest_exog] Anchor at date={anchor_date.date()}")
       
        # Training data up to anchor_date
        y_train = y_full.loc[:anchor_date]
        X_train = X_full.loc[:anchor_date]

        """
        # Ensure a supported monthly index for statsmodels
        y_train = y_train.copy()
        X_train = X_train.copy()
        
        y_train.index = pd.PeriodIndex(y_train.index, freq="M").to_timestamp(how="end")
        X_train.index = y_train.index
        """

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

        converged = bool(getattr(res, "mle_retvals", {}).get("converged", True))
        aic = float(getattr(res, "aic", np.nan))
        bic = float(getattr(res, "bic", np.nan))

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
            "converged": converged,
            "aic": aic,
            "bic": bic,
        }
        
        algo_params = store_selected_features_in_params(
            algo_params,
            selected_features=selected_feature_names,
            selector_meta={
                "method": "xgb_importance",
                "max_features": max_features_from_xgb,
                "n_features_before": int(X_train.shape[1]),
            },
        )

        con = get_connection()

        run_id = insert_run(
            con=con,
            model_name="sarimax_exog_backtest",
            model_version="v1",
            target_metric_id=target.metric_id,
            target_geo_id=target.geo_id,
            target_property_type_id=target.property_type_id,
            freq="M",
            train_start=y_train.index[0].date(),
            train_end=anchor_date.date(),
            horizon_max_months=horizon_bt,
            algo_params=algo_params,
            notes=f"SARIMAX-exog backtest anchor={anchor_date.date()}",
            is_active=False,
            run_kind="backtest",
            batch_id=batch_id,
            data_asof=data_asof,
        )
        
        # build target_dates the same way you already do in insert_predictions_backtest
        last_period = anchor_date.to_period("M")
        future_periods = [last_period + i for i in range(1, horizon_bt + 1)]
        target_dates = [p.to_timestamp(how="end").date() for p in future_periods]
        
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=mean_fc,
            y_hat_lo=ci[:, 0] if ci is not None else None,
            y_hat_hi=ci[:, 1] if ci is not None else None,
        )
        
        con.close()


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

    parser.add_argument("--min_train_len", type=int, default=DEFAULT_MIN_TRAIN_LEN)
    parser.add_argument("--anchor_step_months", type=int, default=DEFAULT_ANCHOR_STEP_MONTHS)
    parser.add_argument("--max_anchors", type=int, default=DEFAULT_MAX_ANCHORS)
    parser.add_argument("--latest_anchor_offset_months", type=int, default=None)

    parser.add_argument("--batch_id", type=str, default=None)
    parser.add_argument("--data_asof", type=str, default=None)

    args = parser.parse_args()

    run_backtest_sarimax_exog_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        use_xgb_feature_selection=not args.no_xgb_selection,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
    )
