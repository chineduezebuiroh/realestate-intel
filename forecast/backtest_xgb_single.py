# forecast/backtest_xgb_single.py

import os
from typing import List, Dict, Optional
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from collections import Counter

from .feature_loader import (
    TargetSpec,
    build_universal_feature_specs,
    build_design_matrix,
    build_design_matrix_incremental,
    load_target_series_for_spec,
)

from .db_forecast import (
    get_connection,
    new_batch_id,
    insert_run,
    insert_predictions,
)

from .backtest_utils import (
    choose_anchor_dates,
    month_end_index,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)

from .feature_catalog import load_catalog, property_type_ids_matching, metric_family
from .feature_policy import default_policy
from .feature_selection import score_candidates, select_scored_candidates, scored_to_feature_specs, default_bucket

TEMP_DEBUG_LIMIT = None  # set to a number to debug; set to 'None' when finished debugging

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

# ==========================================================
# Main backtest entry
# ==========================================================
def run_backtest_xgb_single(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    *,
    min_train_len: int = DEFAULT_MIN_TRAIN_LEN,
    anchor_step_months: int = DEFAULT_ANCHOR_STEP_MONTHS,
    max_anchors: int = DEFAULT_MAX_ANCHORS,
    latest_anchor_offset_months: Optional[int] = None,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD
    seed: int = 1337,
    artifact_root: str = "runs",
    xgb_top_k: int = 100,
    anchors_csv: Optional[str] = None,
):
    """
    Backtest XGBoost for a single target series using a universal feature set.

    For each anchor date:
      - build design matrix up to full history
      - restrict to rows <= anchor_date for training
      - iteratively forecast up to horizon months ahead using carry-forward exogs
      - store as backtest runs (is_active=FALSE)
    """
    # ---- resolve batch + artifact path early (fail fast) ----
    batch_id = batch_id or new_batch_id()
    artifact_root = artifact_root or "runs"

    xgb_out_dir = Path(artifact_root) / batch_id / "xgb"
    xgb_out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[xgb_backtest] batch_id={batch_id} artifact_dir={xgb_out_dir}")

    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)

    catalog = load_catalog()
    policy = default_policy()

    # Policy already owns redfin exclusions
    # (and you already set {"-1"} there)
    # If you want to add "-2", do it *in default_policy()* not here.
    redfin_exclude = policy.exclude_property_type_ids_by_source.get("redfin", set())
    print("[policy] redfin_exclude:", sorted(redfin_exclude))


    candidate_specs = build_universal_feature_specs(target)
    if not candidate_specs:
        print("[xgb_backtest] No candidate features; skipping XGB backtest.")
        return

    # --- scoring window ---
    y_full_for_window = load_target_series_for_spec(target)
    train_end = y_full_for_window.index.max()
    
    scored = score_candidates(
        target=target,
        candidates=candidate_specs,
        train_end=train_end,
        min_eff=60,
        lead_months=(0,1,2,3,4,5,6),
        score_mode="yoy_xcorr",
    )
    
    # --- selection caps: base series BEFORE lagging ---
    policy = default_policy()
    
    # OPTIONAL: ensure key categories appear even if scores are close
    category_minimums = {
        "rates": 5,
        "yields": 5,
        "gdp": 3,
    }
    
    # OPTIONAL: keep geo diversity (avoid 200 zipcodes dominating)
    bucket_caps = {
        "geo:target_equiv": 80,
        "geo:zipcode_dc": 60,
        "geo:county": 40,
        "geo:msa": 40,
        "geo:state": 40,
        "geo:national": 40,
        "geo:other": 40,
    }
    
    picked = select_scored_candidates(
        scored=scored,
        max_base_series=250,  # start here
        category_caps=policy.family_caps,
        category_minimums=category_minimums,
        bucket_caps=bucket_caps,
        bucket_fn=lambda spec: default_bucket(spec, target),
    )
    
    candidate_specs = scored_to_feature_specs(picked)
    
    print("[score] picked_base_series:", len(candidate_specs))
    print("[score] top10:", [(p.spec.name, round(p.score, 3), p.spec.category, p.best_lead) for p in picked[:10]])


    if TEMP_DEBUG_LIMIT is not None:
        candidate_specs = candidate_specs[:TEMP_DEBUG_LIMIT]
        print(f"[xgb_backtest] TEMP: truncating to {len(candidate_specs)} candidates for debugging.")

    # min_train_len is passed in as a function argument
    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    if required_obs < (min_train_len + horizon):
        raise ValueError("required_obs invariant broken")
    print(f"[xgb_backtest] required_obs={required_obs} (min_train_len={min_train_len}, horizon={horizon}, buffer={DEFAULT_ANCHOR_BUFFER_MONTHS})")

    try:
        y_full, X_full, base_series_full, selected_specs = build_design_matrix_incremental(
            target=target,
            candidate_specs=candidate_specs,
            min_obs=required_obs,
            max_features=None,
            load_target_fn=load_target_series_for_spec,
            drop_feature_na=False,   # <-- IMPORTANT for XGB
        )
    except ValueError as e:
        print(f"[xgb_backtest] Incremental design matrix build failed: {e}")
        print("[xgb_backtest] Skipping XGB backtest for this target.")
        return

    print(f"[xgb_backtest] y_full_range={y_full.index.min().date()}..{y_full.index.max().date()} (n={len(y_full)})")

    # sanity: ensure feature ids are 4-part base + _lagK
    sample_cols = list(X_full.columns)[:20]
    bad = [c for c in sample_cols if len(c.rsplit("_lag", 1)[0].split("__")) != 4]
    if bad:
        raise RuntimeError(f"[xgb_backtest] Feature IDs are not 4-part. Example bad: {bad[:5]}")

    y_full = y_full.copy()
    y_full.index = month_end_index(y_full.index)
    y_full = y_full[~y_full.index.duplicated(keep="last")].sort_index()
    
    if len(X_full) != len(y_full):
        raise ValueError(f"X_full and y_full length mismatch: {len(X_full)} vs {len(y_full)}")
    X_full = X_full.copy()
    X_full.index = y_full.index

    # Anchor source-of-truth: MUST share the exact timeline used for training
    # (otherwise anchors can fall off the design-matrix index and you get silent drift)
    y_anchor = y_full.copy()
    y_anchor.index = X_full.index  # force same month-end convention + identical timestamps

    if not y_anchor.index.equals(X_full.index):
        raise ValueError("BUG: y_anchor.index must equal X_full.index")
        
    
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

    print("[debug] raw target max:", load_target_series_for_spec(target).index.max())
    print("[debug] y_full max:", y_full.index.max())
    print("[debug] X_full max:", X_full.index.max())
    print("[debug] X_full nulls last row:", int(X_full.loc[X_full.index.max()].isna().sum()))

    if anchors_csv:
        anchors = [pd.Timestamp(s.strip()) for s in anchors_csv.split(",") if s.strip()]
    else:
        anchors = choose_anchor_dates(
            y_anchor,
            horizon=horizon,
            min_train_len=min_train_len,
            step_months=anchor_step_months,
            max_anchors=max_anchors,
            latest_anchor_offset_months=latest_anchor_offset_months,
        )

    if not anchors:
        print("[xgb_backtest] Not enough history to run backtests.")
        return

    print("[xgb_backtest] anchors:", [a.date().isoformat() for a in anchors])

    # --- Anchor validation against design-matrix timeline ---
    missing = [a for a in anchors if a not in X_full.index]
    if missing:
        print("[xgb_backtest] WARNING: some anchors not in design-matrix timeline:")
        print("  missing:", [a.date().isoformat() for a in missing])
        print("  X_full.index min/max:", X_full.index.min().date(), X_full.index.max().date())
        print("  y_full.index min/max:", y_full.index.min().date(), y_full.index.max().date())
        print("  X_full tail:", [d.date().isoformat() for d in X_full.index[-6:]])
        print("  y_full tail:", [d.date().isoformat() for d in y_full.index[-6:]])
        raise ValueError(f"Anchors not in X_full.index: {[a.date().isoformat() for a in missing]}")
    
    # If we get here, all anchors are in X_full.index
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
            random_state=seed,
        )
        model.fit(X_train, y_train)

        # ---------- Phase B0: emit selected features (XGB is the selector of record) ----------
        importances = getattr(model, "feature_importances_", None)
        if importances is None:
            raise RuntimeError("XGB model missing feature_importances_")

        fi = (
            pd.DataFrame({"feature_id": feature_names, "importance": importances})
              .sort_values("importance", ascending=False)
              .reset_index(drop=True)
        )

        # Keep only non-zero importance, then take top K
        fi = fi[fi["importance"] > 0].copy()
        fi["rank"] = np.arange(1, len(fi) + 1)
        fi_sel = fi.head(int(xgb_top_k)).copy()

        # --- HARD SAFETY CHECKS (do not move this) ---
        assert not fi_sel.empty, "XGB produced empty feature shortlist"
        assert {"feature_id", "rank"}.issubset(fi_sel.columns), (
            f"Invalid XGB shortlist columns: {list(fi_sel.columns)}"
        )
        assert fi_sel["feature_id"].astype(str).str.contains("_lag").all(), (
            "XGB feature_id missing lag suffix — downstream SARIMAX will break"
        )

        # annotate keys for deterministic downstream lookup
        anchor_key = anchor_date.date().isoformat()
        fi_sel["batch_id"] = batch_id
        fi_sel["data_asof"] = str(data_asof)
        fi_sel["geo_id"] = geo_id
        fi_sel["metric_id"] = metric_id
        fi_sel["property_type_id"] = property_type_id
        fi_sel["anchor_date"] = anchor_key
        fi_sel["horizon"] = int(horizon_bt)
        fi_sel["seed"] = int(seed)
        
        if artifact_root:
            #out_dir = Path(artifact_root) / batch_id / "xgb"
            #out_dir.mkdir(parents=True, exist_ok=True)
            #out_dir = xgb_out_dir
        
            anchor_key = anchor_date.date().isoformat()
            out_path = xgb_out_dir / f"selected_features__anchor={anchor_key}.parquet"
            
            if out_path.exists():
                raise SystemExit(
                    f"[xgb_backtest] REFUSING to overwrite existing artifact for anchor={anchor_key}: {out_path}\n"
                    "Use a fresh --batch_id or delete this specific file."
                )
            
            fi_sel.to_parquet(out_path, index=False)

            #fi_sel.to_parquet(out_dir / f"selected_features__anchor={anchor_key}.parquet", index=False)
            #fi_sel.to_parquet(xgb_out_dir / f"selected_features__anchor={anchor_key}.parquet", index=False)

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
    parser.add_argument("--seed", type=int, default=1337)
    parser.add_argument("--artifact_root", type=str, default="runs")
    parser.add_argument("--xgb_top_k", type=int, default=100)
    parser.add_argument(
        "--anchors",
        type=str,
        default=None,
        help="Comma-separated anchor dates YYYY-MM-DD. If provided, overrides internal anchor selection.",
    )
    parser.add_argument(
        "--purpose",
        choices=["forecast", "selector"],
        default="forecast",
        help="forecast = XGB predicts target; selector = produce freshest shortlist for SARIMAX-exog",
    )

    args = parser.parse_args()

    policy = default_policy()

    if args.purpose == "selector":
        # 1) enforce selector horizon (don’t trust CLI)
        if args.horizon != policy.xgb_selector_horizon_months:
            print(
                f"[xgb] selector purpose: overriding --horizon {args.horizon} -> "
                f"{policy.xgb_selector_horizon_months} (policy.xgb_selector_horizon_months)"
            )
        args.horizon = int(policy.xgb_selector_horizon_months)
    
        # 2) enforce selector anchor cadence + count (fresh shortlist)
        args.anchor_step_months = int(policy.xgb_selector_anchor_step_months)
        args.max_anchors = int(policy.xgb_selector_max_anchors)
    
        # 3) enforce "latest backtestable anchor"
        # Must be >= horizon, otherwise the newest anchor has no future y to score against.
        # Default behavior: offset = horizon (best balance of freshness + backtestability)
        if policy.xgb_selector_latest_anchor_offset_months is None:
            args.latest_anchor_offset_months = int(args.horizon)
            print(
                f"[xgb] selector purpose: setting latest_anchor_offset_months="
                f"{args.latest_anchor_offset_months} (default = horizon for backtestability)"
            )
        else:
            args.latest_anchor_offset_months = int(policy.xgb_selector_latest_anchor_offset_months)
            if args.latest_anchor_offset_months < args.horizon:
                raise ValueError(
                    f"[xgb] selector purpose: invalid policy: "
                    f"xgb_selector_latest_anchor_offset_months={args.latest_anchor_offset_months} "
                    f"< horizon={args.horizon} (would create non-backtestable latest anchor)"
                )
            print(
                f"[xgb] selector purpose: setting latest_anchor_offset_months="
                f"{args.latest_anchor_offset_months} (policy override)"
            )

    run_backtest_xgb_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=args.horizon,
        min_train_len=args.min_train_len,
        anchor_step_months=args.anchor_step_months,
        max_anchors=args.max_anchors,
        latest_anchor_offset_months=args.latest_anchor_offset_months,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        seed=args.seed,
        artifact_root=args.artifact_root,
        xgb_top_k=args.xgb_top_k,
        anchors_csv=args.anchors,
    )
