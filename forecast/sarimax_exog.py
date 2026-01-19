# forecast/sarimax_exog.py

import os
import json
from typing import Optional, List, Dict, Tuple
from datetime import date

import duckdb
import numpy as np
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

from .feature_loader import TargetSpec, FeatureSpec, build_design_matrix, specs_from_selected_feature_ids

from .sarimax_univariate import run_sarimax_forecast as run_sarimax_univariate

from .design_matrix import build_train_and_future_exog_forecasted, load_series_from_fact  # if importable; otherwise import at top
from .backtest_utils import month_end_index
from .xgb_shortlist import load_xgb_selected_feature_ids, resolve_anchor_for_live
from .asof_policy import resolve_asof, load_source_max_dates, AsOfPolicy

from .db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
    store_selected_features_in_params,
)


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
    horizon_max_months: int = 12,
    feature_specs: Optional[List[FeatureSpec]] = None,  # still supported, but NOT the main path
    notes: Optional[str] = None,
    *,
    # NEW: shortlist-driven live config
    xgb_batch_id: Optional[str] = None,
    sarimax_max_exog: int = 30,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
    run_kind: str = "live",           # e.g. live_near, live_outlook
    label: Optional[str] = None,      # e.g. "Near-term"
    artifact_root: str = "runs",      # where XGB shortlist artifacts live
) -> int:
    """
    Live SARIMAX(exog).

    Preferred path (Phase B+):
      - Provide xgb_batch_id -> load selected lag-level feature_ids from XGB artifacts/DB,
        build train+future exog via build_train_and_future_exog_forecasted,
        fit SARIMAX, forecast horizon_max_months.

    Legacy/demo path:
      - feature_specs provided explicitly -> build_design_matrix + carry-forward (discouraged).
    """
    pt_id_str = str(property_type_id) if property_type_id is not None else None

    # Batch / asof normalization
    batch_id = batch_id or new_batch_id()

    # ------------------------------------------------------------
    # PATH 1 (preferred): shortlist-driven, forecasted future exog
    # ------------------------------------------------------------
    if xgb_batch_id:
        con = get_connection()

        try:
            # normalize to date (or None)
            data_asof_dt = pd.to_datetime(data_asof).date() if data_asof else None

            target = TargetSpec(
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id_str,
                data_asof=data_asof_dt,
            )
            """
            # If caller passed --data_asof, that is the request.
            # resolve_asof will clamp to month-end and/or to available data as needed.
            data_asof_dt, asof_by_source = resolve_asof(
                con=con,
                target=target,
                feature_specs=None,              # we'll resolve again after shortlist load
                requested_asof=data_asof_dt,     # you may need to add this param to resolve_asof if missing
                mode="global_min",
            )
            """

            # 1) Load target once (defines live train_end)
            y_raw = load_series_from_fact(
                metric_id=target.metric_id,
                geo_id=target.geo_id,
                property_type_id=target.property_type_id,
                data_asof=target.data_asof,
            ).copy()

            y_raw.index = month_end_index(y_raw.index)
            y_raw = y_raw[~y_raw.index.duplicated(keep="last")].sort_index()
    
            if y_raw.empty:
                raise SystemExit("[sarimax_exog] Target series is empty; cannot run live forecast.")
    
            train_end = pd.Timestamp(y_raw.index.max())   # live anchor (month-end)
            anchor_date = train_end                       # use live train_end as anchor for exog builder
    
            # 2) Resolve which backtest anchor's shortlist to use
            anchor_for_shortlist = resolve_anchor_for_live(
                artifact_root=artifact_root,
                xgb_batch_id=xgb_batch_id,
                preferred_anchor=train_end,
            )
    
            print(f"[sarimax_exog] live_train_end={train_end.date()} shortlist_anchor={anchor_for_shortlist.date()}")
    
            # 3) Load lag-level feature_ids from that anchor's shortlist
            feature_ids = load_xgb_selected_feature_ids(
                artifact_root=artifact_root,
                xgb_batch_id=xgb_batch_id,
                anchor_date=anchor_for_shortlist,
                top_k=int(sarimax_max_exog),
            )
            feature_ids_initial = list(feature_ids)  # snapshot before any gating
    
            if not feature_ids:
                raise SystemExit(
                    f"[sarimax_exog] No selected features found for xgb_batch_id={xgb_batch_id} "
                    f"(anchor={anchor_for_shortlist.date().isoformat()})"
                )
    
            # 4) Convert lag-level ids -> FeatureSpecs (base series + lags)
            selected_specs = specs_from_selected_feature_ids(feature_ids)
    
            # 5) Dedupe base series so we don't load/forecast the same base series repeatedly
            seen = set()
            deduped_specs = []
            for s in selected_specs:
                k = (s.metric_id, s.geo_id, s.property_type_id, getattr(s, "source_id", None), s.name)
                if k in seen:
                    continue
                seen.add(k)
                deduped_specs.append(s)
            selected_specs = deduped_specs

            # --------------------------------------------
            # Resolve data_asof AFTER we know the sources
            # --------------------------------------------            
            source_max_dates = load_source_max_dates(con, target=target, feature_specs=selected_specs)
            res = resolve_asof("global_min", source_max_dates)
            
            # clamp requested_asof (target.data_asof) to availability (res.global_asof)
            requested = target.data_asof
            global_max = res.global_asof
            effective = min(requested, global_max) if (requested and global_max) else (requested or global_max)
            
            target.data_asof = effective
            target.asof_by_source = res.asof_by_source  # will be {} in global_min
            print(f"[sarimax_exog] requested_asof={requested} resolved_global_asof={global_max} effective_asof={effective}")            

            # Build lagged train/future exog (raw, NaNs allowed)
            y_full_raw, X_train_raw, X_future_fc, test_idx = build_train_and_future_exog_forecasted(
                target=target,
                feature_specs=selected_specs,
                anchor_date=anchor_date,
                horizon=horizon_max_months,
                method="seasonal_naive_else_last",
                data_asof=target.data_asof,
                asof_by_source=target.asof_by_source,
            )
    
            # Validate shortlist columns exist
            missing_cols = [c for c in feature_ids if c not in X_train_raw.columns]
            if missing_cols:
                raise SystemExit(
                    f"[sarimax_exog] FAIL: shortlist feature_ids not in design matrix. "
                    f"missing_count={len(missing_cols)} example={missing_cols[:10]}"
                )
    
    
            # --- Live viability filter: must have lag features available at anchor_date ---
            if anchor_date not in X_train_raw.index:
                raise SystemExit(f"[sarimax_exog] FAIL: anchor_date {anchor_date.date()} not in X_train_raw index.")
            anchor_row = X_train_raw.loc[anchor_date, feature_ids]
    
            good = anchor_row.notna()
            
            kept_feature_ids = [c for c in feature_ids if c in good.index and bool(good.loc[c])]
            dropped_feature_ids = [c for c in feature_ids if c not in set(kept_feature_ids)]
            
            print(f"[sarimax_exog] shortlist features={len(feature_ids)} kept_at_anchor={len(kept_feature_ids)} dropped_at_anchor={len(dropped_feature_ids)}")
            if dropped_feature_ids:
                print(f"[sarimax_exog] dropped examples: {dropped_feature_ids[:10]}")
            
            feature_ids = kept_feature_ids
            
            # If you drop too many, abort (otherwise you’ll fit garbage)
            MIN_EXOG = 5
            if len(feature_ids) < MIN_EXOG:
                raise SystemExit(f"[sarimax_exog] FAIL: only {len(feature_ids)} usable exog columns at live anchor_date={anchor_date.date()}")
    
    
            # Select train window up to anchor_date
            y_train_full = y_full_raw.loc[:anchor_date].copy()
            X_train_sel = X_train_raw.loc[:anchor_date, feature_ids].copy()
    
            # Align and drop rows with any NA (endog or exog)
            X_train_sel = X_train_sel.reindex(y_train_full.index)
            train_mask = y_train_full.notna() & X_train_sel.notna().all(axis=1)
    
            y_train = y_train_full.loc[train_mask].copy()
            X_train_sel = X_train_sel.loc[train_mask].copy()
    
            if len(y_train) < 60:
                raise SystemExit(f"[sarimax_exog] Too little training history after exog alignment: n={len(y_train)}")
    
            # Select future horizon exog (must be complete)
            X_future_sel = X_future_fc.reindex(test_idx)[feature_ids].copy()
    
    
            # --- Gate: selected features must be feasible for the entire future horizon ---
            bad_future = X_future_sel.columns[X_future_sel.isna().any(axis=0)].tolist()
            
            if bad_future:
                keep = [c for c in feature_ids if c not in bad_future]
            
                print(
                    f"[sarimax_exog] shortlist features={len(feature_ids)} "
                    f"kept_future_feasible={len(keep)} dropped_future_nan={len(bad_future)}"
                )
                print(f"[sarimax_exog] dropped_future examples: {bad_future[:10]}")
            
                # Rebuild train/future matrices using only feasible columns
                feature_ids = keep
                X_train_sel = X_train_raw.loc[:anchor_date, feature_ids].copy()
                X_train_sel = X_train_sel.reindex(y_full_raw.loc[:anchor_date].index)
                
                y_train_full = y_full_raw.loc[:anchor_date]
                train_mask = y_train_full.notna() & X_train_sel.notna().all(axis=1)
                y_train = y_train_full.loc[train_mask].copy()
                
                X_train_sel = X_train_sel.loc[train_mask].copy()
            
                X_future_sel = X_future_fc.reindex(test_idx)[feature_ids].copy()
            
            # Hard fail if we’re left with nothing useful
            if len(feature_ids) == 0:
                raise SystemExit("[sarimax_exog] FAIL: 0 future-feasible features after gating.")
    
    
            # Final hard fail (should be impossible if future-feasibility gate worked)
            if X_future_sel.isna().any().any():
                bad_cols = X_future_sel.columns[X_future_sel.isna().any(axis=0)].tolist()
                first_bad_date = X_future_sel.index[X_future_sel.isna().any(axis=1)][0]
                raise SystemExit(
                    f"[sarimax_exog] FAIL: future exog still has NaNs AFTER gating. "
                    f"bad_cols_count={len(bad_cols)} first_bad_date={first_bad_date.date()} example={bad_cols[:10]}"
                )
    
    
            # 5) Fit SARIMAX (index-safe: use RangeIndex endog)
            endog = pd.Series(y_train.values)  # RangeIndex
            exog_train = X_train_sel.to_numpy(dtype=float)
    
            model = SARIMAX(
                endog=endog,
                exog=exog_train,
                order=(1, 1, 1),
                seasonal_order=(1, 1, 1, 12),
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            fit = model.fit(disp=False)
    
            exog_future = X_future_sel.to_numpy(dtype=float)
            fc = fit.get_forecast(steps=horizon_max_months, exog=exog_future)
    
            # Map forecast to dates
            last_date = pd.Timestamp(y_train.index[-1])
            last_period = last_date.to_period("M")
            future_periods = [last_period + i for i in range(1, horizon_max_months + 1)]
            target_dates = [p.to_timestamp(how="end") for p in future_periods]
    
            mean_fc = np.asarray(fc.predicted_mean, dtype=float)
            ci = np.asarray(fc.conf_int(), dtype=float)  # (h, 2)
    
            # 6) Persist run + preds
            algo_params = {
                "order": (1, 1, 1),
                "seasonal_order": (1, 1, 1, 12),
                "n_obs": int(len(y_train)),
                "xgb_batch_id": xgb_batch_id,
                "sarimax_max_exog": int(sarimax_max_exog),
                "exog_backtest_type": "forecasted_exog",
                "label": label or "",
            }
    
            algo_params["shortlist_info"] = {
                "shortlist_anchor": str(anchor_for_shortlist.date()),
                "n_shortlist": int(len(feature_ids_initial)),
                "n_kept_final": int(len(feature_ids)),
                "dropped_at_anchor": list(dropped_feature_ids),
                "dropped_future_nan": list(bad_future),
            }
    
            algo_params["exog_forecast_method"] = "seasonal_naive_else_last"
            algo_params["data_asof"] = str(target.data_asof) if target.data_asof else None
    
            algo_params = store_selected_features_in_params(
                algo_params,
                selected_features=list(X_train_sel.columns),  # lag-level actually used; identical to 'selected_features=list(feature_ids)'
                selector_meta={
                    "method": "xgb_selected_features",
                    "xgb_batch_id": xgb_batch_id,
                    "sarimax_max_exog": int(sarimax_max_exog),
                },
            )
    
            con = get_connection()
            run_id = insert_run(
                con=con,
                model_name="sarimax_exog",
                model_version="v2_live_shortlist",
                target_metric_id=target.metric_id,
                target_geo_id=target.geo_id,
                target_property_type_id=target.property_type_id,
                freq="M",
                train_start=pd.Timestamp(y_train.index[0]).date(),
                train_end=pd.Timestamp(y_train.index[-1]).date(),
                horizon_max_months=int(horizon_max_months),
                algo_params=algo_params,
                notes=notes or f"SARIMAX(exog) live shortlist ({label or run_kind})",
                is_active=True,
                run_kind=run_kind,
                batch_id=batch_id,
                data_asof=data_asof_dt,
            )
    
            insert_predictions(
                con=con,
                run_id=int(run_id),
                target_dates=[d.date() for d in target_dates],
                y_hat=mean_fc,
                y_hat_lo=ci[:, 0],
                y_hat_hi=ci[:, 1],
            )

        finally:
            con.close()
            
        print(f"[sarimax_exog] Created live run_id={run_id} batch_id={batch_id} label={label or run_kind}")
        return int(run_id)

    # ------------------------------------------------------------
    # PATH 2 (legacy demo): explicit feature_specs + carry-forward
    # ------------------------------------------------------------
    if not feature_specs:
        return run_sarimax_univariate(
            metric_id=metric_id,
            geo_id=geo_id,
            property_type_id=pt_id_str,
            horizon_max_months=horizon_max_months,
            order=(1, 1, 1),
            seasonal_order=(1, 1, 1, 12),
            notes=notes or "SARIMAX (no exog, delegated from sarimax_exog)",
        )

    y, X, _base_series = build_design_matrix(target=target, feature_specs=feature_specs, min_obs=60)

    # carry-forward last row (discouraged; kept only so old CLI usage doesn't break)
    last_exog_row = X.iloc[[-1]]
    exog_future = np.repeat(last_exog_row.values, horizon_max_months, axis=0)

    model = SARIMAX(
        endog=y,
        exog=X,
        order=(1, 1, 1),
        seasonal_order=(1, 1, 1, 12),
        enforce_stationarity=False,
        enforce_invertibility=False,
    )
    fit = model.fit(disp=False)
    fc = fit.get_forecast(steps=horizon_max_months, exog=exog_future)

    last_date = y.index[-1]
    last_period = last_date.to_period("M")
    future_periods = [last_period + i for i in range(1, horizon_max_months + 1)]
    target_dates = [p.to_timestamp(how="end") for p in future_periods]

    mean_fc = np.asarray(fc.predicted_mean, dtype=float)
    ci = np.asarray(fc.conf_int(), dtype=float)

    algo_params = {
        "order": (1, 1, 1),
        "seasonal_order": (1, 1, 1, 12),
        "n_obs": int(len(y)),
        "exog_mode": "carry_forward_last_row",
        "label": label or "",
    }

    con = get_connection()
    try:
        run_id = insert_run(
            con=con,
            model_name="sarimax_exog",
            model_version="v1_demo",
            target_metric_id=target.metric_id,
            target_geo_id=target.geo_id,
            target_property_type_id=target.property_type_id,
            freq="M",
            train_start=pd.Timestamp(y.index[0]).date(),
            train_end=pd.Timestamp(y.index[-1]).date(),
            horizon_max_months=int(horizon_max_months),
            algo_params=algo_params,
            notes=notes or "SARIMAX(exog) demo carry-forward",
            is_active=True,
            run_kind=run_kind,
            batch_id=batch_id,
            data_asof=data_asof_dt,
        )
    
        insert_predictions(
            con=con,
            run_id=int(run_id),
            target_dates=[d.date() for d in target_dates],
            y_hat=mean_fc,
            y_hat_lo=ci[:, 0],
            y_hat_hi=ci[:, 1],
        )
    finally:
        con.close()
        
    print(f"[sarimax_exog] Created demo run_id={run_id} batch_id={batch_id}")
    return int(run_id)

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
    parser.add_argument("--xgb_batch_id", default=None, help="Use XGB shortlist from this batch_id to choose exog.")
    parser.add_argument("--sarimax_max_exog", type=int, default=30)
    parser.add_argument("--batch_id", default=None)
    parser.add_argument("--run_kind", default="live", help="e.g. live_near, live_outlook")

    parser.add_argument("--label", default=None, help="Human label like 'Near-term' or '12-mo outlook'")
    parser.add_argument("--data_asof", type=str, default=None, help="Freeze data reads at month-end <= this date (YYYY-MM-DD).")

    args = parser.parse_args()
    if args.run_kind in ("live_near", "live_outlook") and not args.data_asof:
        raise ValueError("--data_asof is required for live_near/live_outlook to keep runs deterministic.")
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
        horizon_max_months=args.horizon,
        feature_specs=feature_specs_cli,
        notes="CLI SARIMAX exog live run",
        xgb_batch_id=args.xgb_batch_id,
        sarimax_max_exog=args.sarimax_max_exog,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        run_kind=args.run_kind,
        label=args.label,
    )

    print(f"Created SARIMAX(exog) run_id={run_id}")
