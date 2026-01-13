# forecast/backtest_sarimax_exog_single.py
import os
from typing import List, Dict, Optional, Tuple

#import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from .feature_loader import (
    TargetSpec,
    FeatureSpec,
    #build_design_matrix,
    build_universal_feature_specs,
    build_design_matrix_incremental,
    load_target_series_for_spec,
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
    month_ends_after,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)

from .backtest_sarimax_single import load_target_series  # TEMP import for debugging only
from .design_matrix import build_train_and_future_exog_forecasted

TEMP_DEBUG_LIMIT = 300 #set to 'None' when finished debugging


# ==========================================================
# Helpers
# ==========================================================
def _parse_data_asof(s: str | None):
    if not s:
        return None
    return pd.to_datetime(s).date()


def _load_selected_features_strict(
    artifact_root: str,
    xgb_batch_id: str,
    anchor_date: pd.Timestamp,
) -> List[str]:
    from pathlib import Path

    anchor_key = anchor_date.date().isoformat()
    p = Path(artifact_root).parent / xgb_batch_id / "xgb" / f"selected_features__anchor={anchor_key}.parquet"
    if not p.exists():
        raise SystemExit(f"[backtest_exog] FAIL: missing XGB selected features artifact: {p}")

    df = pd.read_parquet(p)
    if df.empty or "feature_id" not in df.columns:
        raise SystemExit(f"[backtest_exog] FAIL: invalid selected features artifact (empty or missing feature_id): {p}")

    feats = df["feature_id"].astype(str).tolist()
    return feats

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
# Main backtest entry
# -----------------------------

def run_backtest_sarimax_exog_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    order: Tuple[int, int, int] = (1, 1, 1),
    seasonal_order: Tuple[int, int, int, int] = (1, 1, 1, 12),
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
    min_train_len: int = DEFAULT_MIN_TRAIN_LEN,
    anchor_step_months: int = DEFAULT_ANCHOR_STEP_MONTHS,
    max_anchors: int = DEFAULT_MAX_ANCHORS,
    latest_anchor_offset_months: Optional[int] = None,
    xgb_batch_id: Optional[str] = None,
    artifact_root: Optional[str] = None,
    sarimax_max_exog: int = 30,
    seed: int = 1337,
    anchors_csv: Optional[str] = None,
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

    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    try:
        y_full, X_full, base_series_full, selected_specs = build_design_matrix_incremental(
            target=target,
            candidate_specs=candidate_specs,
            min_obs=required_obs,
            max_features=None,
            load_target_fn=load_target_series_for_spec,
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
    

    # Final hard check
    if len(X_full) != len(y_full):
        raise ValueError(f"X_full and y_full length mismatch after alignment: {len(X_full)} vs {len(y_full)}")


    
    batch_id = batch_id or new_batch_id()
    if data_asof is None:
        data_asof = y_full.index.max().date()
    else:
        data_asof = _parse_data_asof(data_asof)
    print(f"[backtest_exog] batch_id={batch_id} data_asof={data_asof}")

    # AFTER inc-build returns selected_specs, immediately overwrite y_full used for anchors
    y_full_for_anchors = load_target_series(metric_id, geo_id, property_type_id).copy()
    y_full_for_anchors.index = month_end_index(y_full_for_anchors.index)
    y_full_for_anchors = y_full_for_anchors[~y_full_for_anchors.index.duplicated(keep="last")].sort_index()
    
    if anchors_csv:
        anchors = [pd.Timestamp(s.strip()) for s in anchors_csv.split(",") if s.strip()]
    else:
        anchors = choose_anchor_dates(
            y_full_for_anchors,
            horizon=horizon,
            min_train_len=min_train_len,
            step_months=anchor_step_months,
            max_anchors=max_anchors,
            latest_anchor_offset_months=latest_anchor_offset_months,
        )
    
    if not anchors:
        print("[backtest_exog] Not enough history to run backtests.")
        return
    
    print(f"[backtest_exog] Found {len(anchors)} anchors.")
    #last_date = y_full.index[-1]
    results_summary = []
    
    for anchor_date in anchors:
        print(f"\n[backtest_exog] Anchor at date={anchor_date.date()}")
    
        # ------------------------------------------------------------------
        # 1) Build TRAIN + FUTURE exog using forecasted-exog (Type 2 backtest)
        # ------------------------------------------------------------------
        # NOTE: This should return:
        # - y_full_raw: raw target series on its native month-end timeline (no shrinking)
        # - X_train_raw: exog matrix aligned to y_full_raw up to anchor (may contain NaNs)
        # - X_future_fc: exog matrix for horizon months built by forecasting exogs (NO NaNs ideally)
        # - test_idx_full: month-end DatetimeIndex length=horizon
        #
        # If your helper returns slightly different names, adapt them here once.
        y_full_raw, X_train_raw, X_future_fc, test_idx_full = build_train_and_future_exog_forecasted(
            target=target,
            feature_specs=selected_specs,   # IMPORTANT: use the specs you ended up selecting
            anchor_date=anchor_date,
            horizon=horizon,
            # seasonal-naive (t-12) else last value is your rule:
            method="seasonal_naive_else_last", # once Option 2 is active
        )
    
        # y_train_raw is the target up to anchor (keep as Series)
        y_train_raw = y_full_raw.loc[:anchor_date].copy()

        # Build X_train on the target timeline up to anchor (NaNs allowed)
        X_train = X_train_raw.loc[:anchor_date].copy()
        X_train = X_train.reindex(y_train_raw.index)  # align timelines

    
        # --- enforce minimum target length (no exog involved) ---
        y_train_raw = y_train_raw.dropna()
        if len(y_train_raw) < min_train_len:
            print("[backtest_exog] Train shorter than min_train_len; skipping anchor.")
            continue
    
        # ------------------------------------------------------------------
        # 2) Build evaluation window (truncate horizon if y missing; you want this)
        # ------------------------------------------------------------------
        y_test_full = y_full_raw.reindex(test_idx_full)
        missing_y_mask = y_test_full.isna()
    
        print(
            f"[backtest_exog] horizon check: anchor={anchor_date.date()} "
            f"need[{test_idx_full[0].date()}..{test_idx_full[-1].date()}] "
            f"y_max={y_full_raw.index.max().date()} "
            f"missing_y={int(missing_y_mask.sum())}"
        )
    
        if missing_y_mask.any():
            first_missing_pos = int(np.argmax(missing_y_mask.to_numpy()))
            horizon_bt = first_missing_pos
            if horizon_bt <= 0:
                print("[backtest_exog] Missing y immediately after anchor; skipping anchor.")
                continue
            print(f"[backtest_exog] Truncating horizon to {horizon_bt} due to missing y.")
        else:
            horizon_bt = horizon
    
        test_idx = test_idx_full[:horizon_bt]
        y_test = y_full_raw.reindex(test_idx)

        # ------------------------------------------------------------------
        # 3) Load XGB-selected features (upstream selector of record) + cap for SARIMAX
        # ------------------------------------------------------------------
        if not artifact_root:
            raise SystemExit("[backtest_exog] FAIL: --artifact_root is required to load XGB selections.")
        if not xgb_batch_id:
            raise SystemExit("[backtest_exog] FAIL: --xgb_batch_id is required to load XGB selections.")

        selected_feature_names = _load_selected_features_strict(
            artifact_root=artifact_root,
            xgb_batch_id=xgb_batch_id,
            anchor_date=anchor_date,
        )

        if len(selected_feature_names) == 0:
            raise SystemExit("[backtest_exog] FAIL: XGB selection returned 0 features.")

        # Temporary safety cap for SARIMAX stability
        selected_feature_names = selected_feature_names[:int(sarimax_max_exog)]

        # Ensure selected features actually exist in X_train
        missing = [c for c in selected_feature_names if c not in X_train.columns]
        if missing:
            raise SystemExit(f"[backtest_exog] FAIL: selected features not present in design matrix. missing={missing[:10]} count={len(missing)}")

        print(f"[backtest_exog] Using {len(selected_feature_names)} exog features (capped at {sarimax_max_exog}).")

        # ------------------------------------------------------------------
        # 4) Prepare SARIMAX train matrices (complete-case on SELECTED features only)
        # ------------------------------------------------------------------
        X_train_sel = X_train[selected_feature_names]
        train_mask = y_train_raw.notna() & X_train_sel.notna().all(axis=1)
        y_train = y_train_raw.loc[train_mask]
        X_train_sel = X_train_sel.loc[train_mask]
    
        if len(y_train) < min_train_len:
            print("[backtest_exog] Train shorter than min_train_len after selection; skipping anchor.")
            continue
    
        # ------------------------------------------------------------------
        # 5) Prepare FUTURE exog for the horizon using forecasted-exog output
        # ------------------------------------------------------------------
        X_future_sel = X_future_fc.reindex(test_idx)[selected_feature_names]
        if X_future_sel.isna().any().any(): #<-- this might be redundant with what's immediately below
            bad = X_future_sel.columns[X_future_sel.isna().any(axis=0)].tolist()
            print("[backtest_exog] BUG: forecasted exog still has NaNs. bad cols:", bad[:10], "count=", len(bad))

    
        # With forecasted-exog, you should NOT see NaNs here. If you do, that's a bug in the forecaster.
        if X_future_sel.isna().any().any():
            missing_cols = X_future_sel.columns[X_future_sel.isna().any(axis=0)].tolist()
            first_bad_date = X_future_sel.index[X_future_sel.isna().any(axis=1)][0]
            print(
                "[backtest_exog] Missing FUTURE exog after forecasting; skipping anchor.",
                "missing_cols_count=", len(missing_cols),
                "first_bad_date=", first_bad_date.date(),
            )
            continue
    
        # ------------------------------------------------------------------
        # 6) Fit SARIMAX on integer index; forecast with exog_future
        # ------------------------------------------------------------------
        endog = pd.Series(y_train.values)  # RangeIndex
        exog_train = X_train_sel.to_numpy(dtype=float)

        assert not X_future_sel.isna().any().any(), "future exog contains NaNs"
        assert X_train_sel.index.is_monotonic_increasing
        assert not X_train_sel.index.duplicated().any()
        assert len(X_train_sel) == len(y_train)
    
        model = SARIMAX(
            endog=endog,
            exog=exog_train,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        res = model.fit(disp=False)
        converged = bool(getattr(res, "mle_retvals", {}).get("converged", True))
        aic = float(getattr(res, "aic", np.nan))
        bic = float(getattr(res, "bic", np.nan))
    
        exog_future = X_future_sel.to_numpy(dtype=float)
        fc = res.get_forecast(steps=horizon_bt, exog=exog_future)
    
        mean_fc = pd.Series(np.asarray(fc.predicted_mean), index=test_idx, name="y_hat")
        ci = pd.DataFrame(np.asarray(fc.conf_int()), index=test_idx, columns=["y_hat_lo", "y_hat_hi"])
    
        # ------------------------------------------------------------------
        # 7) Persist run + predictions
        # ------------------------------------------------------------------
        algo_params = {
            "order": order,
            "seasonal_order": seasonal_order,
            "n_obs": int(len(y_train)),
            "anchor_date": str(anchor_date.date()),
            "converged": converged,
            "aic": aic,
            "bic": bic,
            "exog_backtest_type": "forecasted_exog_seasonal_naive_else_last",
            "xgb_batch_id": xgb_batch_id,
            "sarimax_max_exog": int(sarimax_max_exog),
        }

        algo_params = store_selected_features_in_params(
            algo_params,
            selected_features=selected_feature_names,
            selector_meta={
                "method": "xgb_selected_features_artifact",
                "xgb_batch_id": xgb_batch_id,
                "sarimax_max_exog": int(sarimax_max_exog),
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
            notes=f"SARIMAX-exog (forecasted-exog) backtest anchor={anchor_date.date()}",
            is_active=False,
            run_kind="backtest",
            batch_id=batch_id,
            data_asof=data_asof,
        )
    
        target_dates = [d.date() for d in test_idx]
    
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=mean_fc.values,
            y_hat_lo=ci["y_hat_lo"].values,
            y_hat_hi=ci["y_hat_hi"].values,
        )
    
        con.close()
    
        print(f"[backtest_exog] Created SARIMAX-exog backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Backtest SARIMAX-exog for a single target series.")
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
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--artifact_root", type=str, required=True)
    parser.add_argument("--xgb_batch_id", type=str, required=True)
    parser.add_argument("--sarimax_max_exog", type=int, default=30)
    parser.add_argument(
        "--anchors",
        type=str,
        default=None,
        help="Comma-separated anchor dates YYYY-MM-DD. If provided, overrides internal anchor selection.",
    )

    args = parser.parse_args()

    run_backtest_sarimax_exog_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        min_train_len=args.min_train_len,
        anchor_step_months=args.anchor_step_months,
        max_anchors=args.max_anchors,
        latest_anchor_offset_months=args.latest_anchor_offset_months,
        seed=args.seed,
        artifact_root=args.artifact_root,
        xgb_batch_id=args.xgb_batch_id,
        sarimax_max_exog=args.sarimax_max_exog,
        anchors_csv=args.anchors,
    )
