# forecast/backtest_sarimax_exog_single.py
import os
from typing import List, Dict, Optional, Tuple

from pathlib import Path
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

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

from .design_matrix import build_train_and_future_exog_forecasted
from .feature_loader import TargetSpec, specs_from_selected_feature_ids
from .feature_policy import default_policy


TEMP_DEBUG_LIMIT = None # set to a number to debug; set to 'None' when finished debugging

# ==========================================================
# Helpers
# ==========================================================
def _parse_data_asof(s: str | None):
    if not s:
        return None
    return pd.to_datetime(s).date()

def _load_xgb_selected_feature_ids(
    artifact_root: str,
    xgb_batch_id: str,
    anchor_date: pd.Timestamp,
    top_k: int,
) -> list[str]:
    if not artifact_root:
        raise ValueError("artifact_root is required")
    if not xgb_batch_id:
        raise ValueError("xgb_batch_id is required")

    anchor_key = anchor_date.date().isoformat()
    p = Path(artifact_root) / xgb_batch_id / "xgb" / f"selected_features__anchor={anchor_key}.parquet"
    if not p.exists():
        raise FileNotFoundError(f"Missing XGB shortlist parquet for anchor={anchor_key}: {p}")

    df = pd.read_parquet(p)

    required = {"feature_id", "rank"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Shortlist parquet missing columns {sorted(missing)}: {p}")

    df = df.sort_values("rank", ascending=True).head(int(top_k))
    feats = df["feature_id"].astype(str).tolist()
    if not feats:
        raise ValueError(f"XGB shortlist empty after top_k={top_k} for anchor={anchor_key}: {p}")

    return feats

def _load_target_y(target: "TargetSpec") -> pd.Series:
    con = get_connection()
    pt = target.property_type_id if target.property_type_id is not None else "all"
    df = con.execute(
        """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
        ORDER BY date
        """,
        [target.metric_id, target.geo_id, pt],
    ).fetchdf()
    con.close()

    if df.empty:
        raise ValueError(f"No target data for {target.metric_id}/{target.geo_id}/{pt}")

    y = df.set_index("date")["value"].astype(float)
    y.index = month_end_index(y.index)
    y = y[~y.index.duplicated(keep="last")].sort_index()
    return y

# ==========================================================
# Main backtest entry
# ==========================================================
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

    - SARIMAX-exog uses XGB shortlist artifacts as selector-of-record
    - SARIMAX builds exog matrices from the selected feature ids
    - It forecasts exogs via build_train_and_future_exog_forecasted
    """
    target = TargetSpec(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=property_type_id,
    )

    y_full_for_anchors = _load_target_y(target)
    # batch_id + data_asof
    batch_id = batch_id or new_batch_id()
    if data_asof is None:
        data_asof = y_full_for_anchors.index.max().date()
    else:
        data_asof = _parse_data_asof(data_asof)
    print(f"[backtest_exog] batch_id={batch_id} data_asof={data_asof}")

    
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
    
    # Use policy max, not ad-hoc cap
    policy = default_policy()
    top_k = int(min(sarimax_max_exog, policy.sarimax_max_exog))
    
    for anchor_date in anchors:
        print(f"\n[backtest_exog] Anchor at date={anchor_date.date()}")

        if not artifact_root or not xgb_batch_id:
            raise SystemExit("[backtest_exog] FAIL: require --artifact_root and --xgb_batch_id to load XGB shortlist.")

        feature_ids = _load_xgb_selected_feature_ids(
            artifact_root=artifact_root,
            xgb_batch_id=xgb_batch_id,
            anchor_date=anchor_date,
            top_k=top_k,
        )
        if not feature_ids:
            raise SystemExit("[backtest_exog] FAIL: XGB shortlist returned 0 features.")
        
        selected_specs = specs_from_selected_feature_ids(feature_ids)

        if any(s.source_id is None for s in selected_specs):
            # This means you are still using legacy 3-part XGB artifacts.
            # Not fatal, but it defeats the purpose of source-aware correctness.
            print("[backtest_exog] WARNING: some selected_specs have source_id=None (legacy feature ids).")
      
        print(f"[backtest_exog] Using {len(feature_ids)} exog feature_ids from XGB shortlist.")
        print(f"[backtest_exog] Collapsed to {len(selected_specs)} base series after lag merge.")

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
        # 3) Build SARIMAX exog matrices from XGB-selected feature_ids
        # ------------------------------------------------------------------
        # We expect build_train_and_future_exog_forecasted to produce lagged columns
        # that match the XGB feature_id strings exactly (e.g. "..._lag12").
        missing_cols = [c for c in feature_ids if c not in X_train_raw.columns]
        if missing_cols:
            raise SystemExit(
                f"[backtest_exog] FAIL: {len(missing_cols)} selected feature_ids not present in X_train_raw. "
                f"Example: {missing_cols[:10]}"
            )

        # Select only the lag-level columns chosen by XGB
        X_train_sel = X_train_raw.loc[:anchor_date, feature_ids].copy()
        X_train_sel = X_train_sel.reindex(y_train_raw.index)  # align to target timeline

        # Complete-case on selected exog only
        train_mask = y_train_raw.notna() & X_train_sel.notna().all(axis=1)
        y_train = y_train_raw.loc[train_mask].copy()
        X_train_sel = X_train_sel.loc[train_mask].copy()

        if len(y_train) < min_train_len:
            print("[backtest_exog] Train shorter than min_train_len after exog selection; skipping anchor.")
            continue

        # ---- Collinearity kill-switch (TRAINING ONLY) ----
        # Keep XGB priority order (feature_ids), drop constant cols, then drop cols
        # whose abs corr with any kept col exceeds threshold.
        COLL_THR = 0.98
        
        # 1) Drop constant columns on training window
        std = X_train_sel.std(axis=0, ddof=0)
        nonconst_cols = std[std > 0].index.tolist()
        dropped_const = [c for c in feature_ids if c not in set(nonconst_cols)]
        
        if dropped_const:
            print(f"[backtest_exog] Dropping {len(dropped_const)} constant exog cols (train std=0). Example: {dropped_const[:5]}")
        
        X_train_sel = X_train_sel[nonconst_cols].copy()
        feature_ids_nc = [c for c in feature_ids if c in set(nonconst_cols)]
        
        # 2) Greedy prune by correlation with already-kept columns (training only)
        keep_cols = []
        # precompute corr of all columns (on training rows) for speed + determinism
        corr = X_train_sel.corr().abs()
        
        for c in feature_ids_nc:
            if not keep_cols:
                keep_cols.append(c)
                continue
            # if this col is too correlated with any already-kept col, drop it
            if (corr.loc[c, keep_cols] > COLL_THR).any():
                continue
            keep_cols.append(c)
        
        dropped_collinear = [c for c in feature_ids_nc if c not in set(keep_cols)]
        if dropped_collinear:
            print(
                f"[backtest_exog] Pruned {len(dropped_collinear)} collinear exog cols at |corr|>{COLL_THR}. "
                f"Kept={len(keep_cols)}. Example dropped: {dropped_collinear[:5]}"
            )
        
        # Apply pruning to training matrices and downstream feature id list
        feature_ids = keep_cols
        X_train_sel = X_train_sel[feature_ids].copy()


        train_mask2 = y_train.notna() & X_train_sel.notna().all(axis=1)
        y_train = y_train.loc[train_mask2].copy()
        X_train_sel = X_train_sel.loc[train_mask2].copy()


        # Future exog for the backtest horizon
        X_future_sel = X_future_fc.reindex(test_idx)[feature_ids].copy()

        # With forecasted-exog, future should have no NaNs. If it does, forecaster is broken.
        if X_future_sel.isna().any().any():
            bad_cols = X_future_sel.columns[X_future_sel.isna().any(axis=0)].tolist()
            first_bad_date = X_future_sel.index[X_future_sel.isna().any(axis=1)][0]
            print(
                "[backtest_exog] Missing FUTURE exog after forecasting; skipping anchor.",
                "bad_cols_count=", len(bad_cols),
                "first_bad_date=", first_bad_date.date(),
            )
            continue


        # ------------------------------------------------------------------
        # 6) Fit SARIMAX on integer index; forecast with exog_future
        # ------------------------------------------------------------------
        #endog = pd.Series(y_train.values)  # RangeIndex
        endog = pd.Series(y_train.values, index=np.arange(len(y_train)))

        # ---- Scale exog using TRAINING window only (stabilizes optimizer) ----
        exog_train_df = X_train_sel.astype(float)
        
        mu = exog_train_df.mean(axis=0)
        sd = exog_train_df.std(axis=0, ddof=0).replace(0.0, 1.0)  # avoid divide-by-zero
        exog_train_z = ((exog_train_df - mu) / sd).to_numpy(dtype=float)
        
        # future exog must be present already (you assert no NaNs above)
        exog_future_df = X_future_sel.astype(float)
        exog_future_z = ((exog_future_df - mu) / sd).to_numpy(dtype=float)
        
        assert not X_future_sel.isna().any().any(), "future exog contains NaNs"
        assert X_train_sel.index.is_monotonic_increasing
        assert not X_train_sel.index.duplicated().any()
        assert len(X_train_sel) == len(y_train)
    
        model = SARIMAX(
            endog=endog,
            exog=exog_train_z,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )

        res = model.fit(disp=False)
        mle_ret = getattr(res, "mle_retvals", {}) or {}
        converged = bool(mle_ret.get("converged", True))
        aic = float(getattr(res, "aic", np.nan))
        bic = float(getattr(res, "bic", np.nan))
        
        # ---- Retry ladder if optimizer doesn't converge ----
        retry_used = None
        if not converged:
            # Retry 1: simpler non-seasonal model (often fixes convergence)
            try:
                model2 = SARIMAX(
                    endog=endog,
                    exog=exog_train_z,
                    order=(1, 1, 1),
                    seasonal_order=(0, 0, 0, 0),
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                )
                res2 = model2.fit(disp=False)
                mle_ret2 = getattr(res2, "mle_retvals", {}) or {}
                converged2 = bool(mle_ret2.get("converged", True))
                if converged2:
                    res = res2
                    converged = True
                    retry_used = "retry_nonseasonal_111"
                    aic = float(getattr(res, "aic", np.nan))
                    bic = float(getattr(res, "bic", np.nan))
            except Exception:
                pass
        
            # Retry 2: reduce exog dimensionality (only if still not converged)
            # This is a last resort and should be rare once scaling is in place.
            if not converged and exog_train_z.shape[1] > 10:
                try:
                    k = max(10, exog_train_z.shape[1] // 2)
                    exog_train_z2 = exog_train_z[:, :k]
                    exog_future_z2 = exog_future_z[:, :k]
        
                    model3 = SARIMAX(
                        endog=endog,
                        exog=exog_train_z2,
                        order=(1, 1, 1),
                        seasonal_order=(0, 0, 0, 0),
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    res3 = model3.fit(disp=False)
                    mle_ret3 = getattr(res3, "mle_retvals", {}) or {}
                    converged3 = bool(mle_ret3.get("converged", True))
                    if converged3:
                        res = res3
                        converged = True
                        retry_used = f"retry_nonseasonal_111_reduce_exog_{k}"
                        aic = float(getattr(res, "aic", np.nan))
                        bic = float(getattr(res, "bic", np.nan))
        
                        # IMPORTANT: if we reduced exog, we must also use reduced future exog
                        exog_future_z = exog_future_z2
                except Exception:
                    pass


        exog_future = X_future_sel.to_numpy(dtype=float)
        fc = res.get_forecast(steps=horizon_bt, exog=exog_future_z)
    
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
            "exog_scaled": True,
            "exog_scale_mode": "zscore_train",
            "retry_used": retry_used,
            "mle_converged": converged,   # redundant but explicit
            #"mle_retvals": mle_ret if isinstance(mle_ret, dict) else None,
            "aic": aic,
            "bic": bic,
            "exog_backtest_type": "forecasted_exog_seasonal_naive_else_last",
            "xgb_batch_id": xgb_batch_id,
            "sarimax_max_exog": int(sarimax_max_exog),
        }

        algo_params = store_selected_features_in_params(
            algo_params,
            selected_features=feature_ids,  # lag-level columns actually used
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
