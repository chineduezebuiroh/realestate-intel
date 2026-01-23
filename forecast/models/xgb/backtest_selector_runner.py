from __future__ import annotations
# forecast/models/xgb/backtest_selector_runner.py

import json
import hashlib
from pathlib import Path
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from forecast.db_forecast import new_batch_id
from forecast.feature_loader import (
    TargetSpec,
    build_universal_feature_specs,
    build_design_matrix_incremental,
    load_target_series_for_spec,
)
from forecast.backtest_utils import (
    choose_anchor_dates,
    month_end_index,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)
from forecast.feature_policy import default_policy
from forecast.feature_selection import (
    score_candidates,
    select_scored_candidates,
    scored_to_feature_specs,
    default_bucket,
)

TEMP_DEBUG_LIMIT = None  # set to an int for debugging; None for normal operation


def _parse_data_asof(s: Optional[str]):
    if not s:
        return None
    return pd.to_datetime(s).date()


def _base_id_from_feature_id(feature_id: str) -> str:
    return feature_id.rsplit("_lag", 1)[0]


def run_xgb_selector(
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
    data_asof: Optional[str] = None,  # YYYY-MM-DD requested
    seed: int = 1337,
    artifact_root: str = "runs",
    xgb_top_k: int = 100,
    anchors_csv: Optional[str] = None,
):
    """
    XGB SELECTOR (artifact-only).

    Emits:
      runs/<batch_id>/xgb/selected_features__anchor=YYYY-MM-DD.parquet

    Hard contracts:
      - exactly one anchor per call
      - no DB writes
      - invariant columns present
    """
    policy = default_policy()

    batch_id = batch_id or new_batch_id()
    out_dir = Path(artifact_root or "runs") / batch_id / "xgb"
    out_dir.mkdir(parents=True, exist_ok=True)

    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)

    # 1) build universal candidate specs
    candidate_specs = build_universal_feature_specs(target)
    if not candidate_specs:
        print("[xgb_selector] No candidate features; skipping.")
        return

    if TEMP_DEBUG_LIMIT is not None:
        candidate_specs = candidate_specs[: int(TEMP_DEBUG_LIMIT)]
        print(f"[xgb_selector] TEMP: truncating candidates to {len(candidate_specs)}")

    # 2) scoring window (full target)
    y_full_for_window = load_target_series_for_spec(target)
    train_end = y_full_for_window.index.max()

    scored = score_candidates(
        target=target,
        candidates=candidate_specs,
        train_end=train_end,
        min_eff=60,
        lead_months=(0, 1, 2, 3, 4, 5, 6),
        score_mode="yoy_xcorr",
    )

    # caps/minimums — keep what you had
    category_minimums = {"rates": 5, "yields": 5, "gdp": 3}
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
        max_base_series=250,
        category_caps=policy.family_caps,
        category_minimums=category_minimums,
        bucket_caps=bucket_caps,
        bucket_fn=lambda spec: default_bucket(spec, target),
    )
    candidate_specs = scored_to_feature_specs(picked)

    if not candidate_specs:
        print("[xgb_selector] No picked candidate specs after caps; skipping.")
        return

    # 3) build design matrix incrementally (respecting your DQ choices)
    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    y_full, X_full, _base_series_full, _selected_specs = build_design_matrix_incremental(
        target=target,
        candidate_specs=candidate_specs,
        min_obs=required_obs,
        max_features=None,
        load_target_fn=load_target_series_for_spec,
        drop_feature_na=False,  # keep as you had
    )

    # normalize timeline
    y_full = y_full.copy()
    y_full.index = month_end_index(y_full.index)
    y_full = y_full[~y_full.index.duplicated(keep="last")].sort_index()

    X_full = X_full.copy()
    X_full.index = y_full.index

    # 4) as-of clamp (preserve requested vs effective)
    TAIL_GAP_MONTHS = int(getattr(policy, "tail_gap_months", 3))

    if data_asof is None:
        requested_end = y_full.index.max()
        data_asof_requested = requested_end.date()
    else:
        data_asof_requested = _parse_data_asof(data_asof)
        requested_end = pd.Timestamp(data_asof_requested).to_period("M").to_timestamp(how="end")

    full_idx = pd.date_range(y_full.index.min(), requested_end, freq="ME")
    y_grid = y_full.reindex(full_idx)

    missing_idx = y_grid.index[y_grid.isna()]
    tail_start = (
        requested_end - pd.offsets.MonthEnd(TAIL_GAP_MONTHS - 1)
        if TAIL_GAP_MONTHS > 1 else requested_end
    )
    tail_missing = missing_idx[(missing_idx >= tail_start) & (missing_idx <= requested_end)]

    effective_end = requested_end
    asof_clamp_reason = None

    if len(tail_missing) > 0:
        first_tail_missing = tail_missing.min()
        effective_end = (first_tail_missing - pd.offsets.MonthEnd(1)).to_period("M").to_timestamp(how="end")
        asof_clamp_reason = {
            "policy": "clamp_tail_gap",
            "requested_asof": requested_end.date().isoformat(),
            "tail_window_start": tail_start.date().isoformat(),
            "first_missing_month_in_tail": first_tail_missing.date().isoformat(),
            "effective_asof": effective_end.date().isoformat(),
        }

    data_asof_effective = effective_end.date()

    # clamp y/X to effective_end with consistent mask
    y_grid_eff = y_grid.loc[:effective_end]
    mask = y_grid_eff.notna()

    X_grid_eff = X_full.reindex(y_grid_eff.index)
    y_full = y_grid_eff.loc[mask].copy()
    X_full = X_grid_eff.loc[mask].copy()

    # Drop features missing at effective_asof
    effective_asof_ts = effective_end
    if effective_asof_ts not in X_full.index:
        raise ValueError(f"[xgb_selector] effective_asof_ts not in X_full.index: {effective_asof_ts}")

    na_cols = X_full.loc[effective_asof_ts].isna()
    drop_cols = na_cols[na_cols].index.tolist()
    if drop_cols:
        X_full = X_full.drop(columns=drop_cols)

    # Drop sparse features
    min_non_missing_ratio = float(policy.min_feature_coverage_ratio)
    non_missing_ratio = X_full.notna().mean(axis=0)
    sparse_cols = non_missing_ratio[non_missing_ratio < min_non_missing_ratio].index.tolist()
    if sparse_cols:
        X_full = X_full.drop(columns=sparse_cols)

    if X_full.shape[1] == 0:
        raise SystemExit("[xgb_selector] FAIL: 0 features remain after DQ drops.")

    # 5) anchor selection (must end as exactly one anchor)
    y_anchor = y_full.copy()
    y_anchor.index = X_full.index

    if anchors_csv:
        anchors = [
            pd.Timestamp(s.strip()).to_period("M").to_timestamp(how="end")
            for s in anchors_csv.split(",")
            if s.strip()
        ]
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
        raise ValueError("[xgb_selector] No anchors available.")

    if len(anchors) != 1:
        raise ValueError(f"[xgb_selector] selector batch must have exactly 1 anchor, got {len(anchors)}")

    anchor_date = anchors[0]
    anchor_date = anchor_date.to_period("M").to_timestamp(how="end")

    if anchor_date not in X_full.index:
        # Debug-friendly message
        raise ValueError(
            f"[xgb_selector] anchor_date not in design matrix timeline: {anchor_date} "
            f"(X_full tail={list(X_full.index[-3:])})"
        )

    # 6) train XGB on <= anchor_date and rank importances
    y_train = y_full.loc[:anchor_date]
    X_train = X_full.loc[:anchor_date]
    feature_names = list(X_train.columns)

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

    importances = getattr(model, "feature_importances_", None)
    if importances is None:
        raise RuntimeError("[xgb_selector] XGB model missing feature_importances_")

    fi = (
        pd.DataFrame({"feature_id": feature_names, "importance": importances})
        .sort_values("importance", ascending=False)
        .reset_index(drop=True)
    )
    fi = fi[fi["importance"] > 0].copy()
    if fi.empty:
        raise RuntimeError("[xgb_selector] XGB produced empty importance list (all zero).")

    fi["rank"] = np.arange(1, len(fi) + 1)
    fi_sel = fi.head(int(xgb_top_k)).copy()

    # 7) annotate invariant columns
    fid_list = fi_sel["feature_id"].astype(str).tolist()
    feature_set_sha256 = hashlib.sha256("\n".join(fid_list).encode("utf-8")).hexdigest()

    fi_sel["batch_id"] = batch_id
    fi_sel["metric_id"] = metric_id
    fi_sel["geo_id"] = geo_id
    fi_sel["property_type_id"] = property_type_id
    fi_sel["anchor_date"] = anchor_date.date().isoformat()
    fi_sel["seed"] = int(seed)
    fi_sel["data_asof_requested"] = data_asof_requested.isoformat() if data_asof_requested else None
    fi_sel["data_asof_effective"] = data_asof_effective.isoformat()
    fi_sel["asof_clamp_reason"] = json.dumps(asof_clamp_reason) if asof_clamp_reason else None
    fi_sel["feature_set_sha256"] = feature_set_sha256

    # 8) hard schema checks before writing
    required_cols = [
        "feature_id",
        "rank",
        "feature_set_sha256",
        "data_asof_requested",
        "data_asof_effective",
    ]
    missing = [c for c in required_cols if c not in fi_sel.columns]
    if missing:
        raise ValueError(f"[xgb_selector] missing required cols: {missing}")

    if fi_sel["feature_id"].duplicated().any():
        raise ValueError("[xgb_selector] feature_id not unique in selection")

    ranks = fi_sel["rank"].astype(int).tolist()
    if ranks != list(range(1, len(ranks) + 1)):
        raise ValueError("[xgb_selector] ranks must be 1..N without gaps")

    if fi_sel["feature_set_sha256"].nunique() != 1:
        raise ValueError("[xgb_selector] feature_set_sha256 must be constant across rows")

    # 9) write artifact (refuse overwrite)
    out_path = out_dir / f"selected_features__anchor={anchor_date.date().isoformat()}.parquet"
    if out_path.exists():
        raise SystemExit(
            f"[xgb_selector] REFUSING to overwrite existing artifact: {out_path}\n"
            "Use a fresh --batch_id or delete this specific file."
        )
    fi_sel.to_parquet(out_path, index=False)

    print(
        f"[xgb_selector] wrote {len(fi_sel)} selected features -> {out_path} "
        f"(asof_effective={data_asof_effective.isoformat()}, requested={data_asof_requested})"
    )


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    p = argparse.ArgumentParser(description="XGB selector (artifact-only) for SARIMAX-exog.")
    p.add_argument("--metric_id", default="median_sale_price")
    p.add_argument("--geo_id", default="dc_city")
    p.add_argument("--property_type_id", default="-1")
    p.add_argument("--horizon", type=int, default=12)

    p.add_argument("--min_train_len", type=int, default=DEFAULT_MIN_TRAIN_LEN)
    p.add_argument("--anchor_step_months", type=int, default=DEFAULT_ANCHOR_STEP_MONTHS)
    p.add_argument("--max_anchors", type=int, default=DEFAULT_MAX_ANCHORS)
    p.add_argument("--latest_anchor_offset_months", type=int, default=None)

    p.add_argument("--batch_id", type=str, default=None)
    p.add_argument("--data_asof", type=str, default=None)
    p.add_argument("--seed", type=int, default=1337)
    p.add_argument("--artifact_root", type=str, default="runs")
    p.add_argument("--xgb_top_k", type=int, default=100)
    p.add_argument(
        "--anchors",
        type=str,
        default=None,
        help="Comma-separated anchor dates YYYY-MM-DD. MUST contain exactly one anchor.",
    )

    args = p.parse_args(argv)

    run_xgb_selector(
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())






"""
# forecast/backtest_xgb_single.py

import os
import json
import hashlib
from typing import List, Dict, Optional
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from xgboost import XGBRegressor
from collections import Counter

from forecast.feature_loader import (
    TargetSpec,
    build_universal_feature_specs,
    build_design_matrix,
    build_design_matrix_incremental,
    load_target_series_for_spec,
)

from forecast.backtest_utils import (
    choose_anchor_dates,
    month_end_index,
    month_ends_after,
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)

from forecast.db_forecast import new_batch_id
from forecast.feature_catalog import load_catalog, property_type_ids_matching, metric_family
from forecast.feature_policy import default_policy
from forecast.feature_selection import score_candidates, select_scored_candidates, scored_to_feature_specs, default_bucket

from forecast.artifacts import (

TEMP_DEBUG_LIMIT = None  # set to a number to debug; set to 'None' when finished debugging

# ==========================================================
# Helpers
# ==========================================================
def _parse_data_asof(s: str | None):
    if not s:
        return None
    return pd.to_datetime(s).date()


def _base_id_from_feature_id(feature_id: str) -> str:
    # strips "_lagK"
    return feature_id.rsplit("_lag", 1)[0]



# ==========================================================
# Main backtest entry
# ==========================================================
def run_xgb_selector(
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
"""
    Backtest XGBoost for a single target series using a universal feature set.

    For each anchor date:
      - build design matrix up to full history
      - restrict to rows <= anchor_date for training
      - iteratively forecast up to horizon months ahead using carry-forward exogs
      - store as backtest runs (is_active=FALSE)
"""
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

    # ---------------------------
    # DQ policy: clamp tail gaps
    # ---------------------------
    policy = default_policy()
    TAIL_GAP_MONTHS = int(getattr(policy, "tail_gap_months", 3))
    
    # 1) requested_end (month-end timestamp)
    if data_asof is None:
        requested_end = y_full.index.max()
        requested_asof_dt = requested_end.date()
    else:
        requested_asof_dt = _parse_data_asof(data_asof)  # expects YYYY-MM-DD -> date
        requested_end = pd.Timestamp(requested_asof_dt).to_period("M").to_timestamp(how="end")
    
    # 2) reindex y to full month-end grid THROUGH requested_end so gaps become explicit
    full_idx = pd.date_range(y_full.index.min(), requested_end, freq="ME")
    y_grid = y_full.reindex(full_idx)
    
    missing_idx = y_grid.index[y_grid.isna()]
    
    tail_start = (
        requested_end - pd.offsets.MonthEnd(TAIL_GAP_MONTHS - 1)
        if TAIL_GAP_MONTHS > 1 else requested_end
    )
    tail_missing = missing_idx[(missing_idx >= tail_start) & (missing_idx <= requested_end)]
    
    effective_end = requested_end
    asof_clamp_reason = None
    
    if len(missing_idx) > 0:
        print(
            f"[xgb_backtest] WARNING: missing months on monthly grid: "
            f"n_missing={len(missing_idx)} first={missing_idx[0].date()} last={missing_idx[-1].date()}"
        )
    
    if len(tail_missing) > 0:
        first_tail_missing = tail_missing.min()
        effective_end = (
            first_tail_missing - pd.offsets.MonthEnd(1)
        ).to_period("M").to_timestamp(how="end")
    
        asof_clamp_reason = {
            "policy": "clamp_tail_gap",
            "requested_asof": requested_end.date().isoformat(),
            "tail_window_start": tail_start.date().isoformat(),
            "first_missing_month_in_tail": first_tail_missing.date().isoformat(),
            "effective_asof": effective_end.date().isoformat(),
        }
        print(f"[xgb_backtest] WARNING: clamping data_asof due to tail gap: {asof_clamp_reason}")
        
    
    # 3) set data_asof to effective_end (THIS is what propagates downstream)
    # Preserve requested vs effective explicitly
    data_asof_requested = requested_asof_dt          # <- this must be set earlier when you parse the CLI arg
    data_asof_effective = effective_end.date()       # <- you already computed effective_end
    data_asof = data_asof_effective
    
    # 4) clamp y/X to effective_end using the SAME timeline
    #    NOTE: we also align X to y_grid index so the mask is consistent.
    y_grid_eff = y_grid.loc[:effective_end]
    
    # Keep only rows where y exists (missing months cannot be trained on)
    mask = y_grid_eff.notna()
    
    # Reindex X onto the same monthly grid (it should already be compatible)
    X_grid_eff = X_full.reindex(y_grid_eff.index)
    
    y_full = y_grid_eff.loc[mask].copy()
    X_full = X_grid_eff.loc[mask].copy()


    # --- DQ: base-series coverage on effective training mask ---
    min_base_coverage = float(policy.min_feature_coverage_ratio)  # keep aligned with your sparse feature policy
    
    # coverage per column
    col_coverage = 1.0 - X_full.isna().mean(axis=0)
    
    # group columns by base_id and take MIN coverage across its lag columns
    base_to_cols: Dict[str, List[str]] = {}
    for c in X_full.columns:
        base = _base_id_from_feature_id(str(c))
        base_to_cols.setdefault(base, []).append(c)
    
    base_min_cov = {b: float(col_coverage[cols].min()) for b, cols in base_to_cols.items()}

    
    # y_anchor is the source-of-truth timeline for anchors
    y_anchor = y_full.copy()
    
    print(f"[xgb_backtest] batch_id={batch_id} data_asof_effective={data_asof_effective} (requested={data_asof_requested})")

    
    # --- DQ: drop feature columns missing at effective_asof (selector must not rank unusable features) ---
    effective_asof_ts = effective_end
    
    if effective_asof_ts not in X_full.index:
        raise ValueError(f"[xgb_backtest] effective_asof_ts not in X_full.index: {effective_asof_ts}")
    
    na_cols = X_full.loc[effective_asof_ts].isna()
    drop_cols = na_cols[na_cols].index.tolist()
    
    if drop_cols:
        print(f"[xgb_backtest] WARNING: dropping features missing at effective_asof: n={len(drop_cols)}")
        X_full = X_full.drop(columns=drop_cols)

    # Drop features that are too sparse over the training window (cheap sanity gate)
    min_non_missing_ratio = float(policy.min_feature_coverage_ratio)
    non_missing_ratio = X_full.notna().mean(axis=0)
    sparse_cols = non_missing_ratio[non_missing_ratio < min_non_missing_ratio].index.tolist()
    if sparse_cols:
        print(f"[xgb_backtest] WARNING: dropping sparse features: n={len(sparse_cols)} "
              f"(min_ratio={min_non_missing_ratio})")
        X_full = X_full.drop(columns=sparse_cols)

    # -------------------------
    # HARD GUARD (must be here)
    # -------------------------
    if X_full.shape[1] == 0:
        raise SystemExit(
            "[xgb_backtest] FAIL: 0 feature columns remain after DQ drops. "
            "Disable aggressive base-series dropping and inspect coverage/base-id parsing."
        )

    print(
        f"[xgb_backtest] Final design matrix: "
        f"n_obs={len(y_full)}, n_features={X_full.shape[1]}, "
        f"selected_series={len(selected_specs)}"
    )

    print("[debug] raw target max:", load_target_series_for_spec(target).index.max())
    print("[debug] y_full max:", y_full.index.max())
    print("[debug] X_full max:", X_full.index.max())
    print("[debug] X_full nulls last row (after drop):", int(X_full.loc[effective_asof_ts].isna().sum()))

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

    if len(anchors) != 1:
        raise ValueError(
            f"[xgb_selector] selector batch must have exactly 1 anchor, got {len(anchors)}"
        )
    anchor_date = anchors[0]

    print("[xgb_backtest] anchors:", [a.date().isoformat() for a in anchors])

    # --- selector purpose: only run the latest anchor (speed + relevance) ---
    if purpose == "selector":
        anchors = [anchors[-1]]  # keep newest only
        print(f"[xgb_backtest] selector: restricting to latest anchor={anchors[0].date().isoformat()}")

    # --- Anchor validation against design-matrix timeline ---
    missing = [a for a in anchors if a not in X_full.index]
    if missing:
        print("[xgb_backtest] WARNING: some anchors not in design-matrix timeline (dropping them):")
        print("  missing:", [a.date().isoformat() for a in missing])
        print("  X_full.index min/max:", X_full.index.min().date(), X_full.index.max().date())
        print("  y_full.index min/max:", y_full.index.min().date(), y_full.index.max().date())
        print("  X_full tail:", [d.date().isoformat() for d in X_full.index[-6:]])
        print("  y_full tail:", [d.date().isoformat() for d in y_full.index[-6:]])
    
        anchors = [a for a in anchors if a in X_full.index]
    
    if not anchors:
        raise ValueError("After dropping invalid anchors, 0 anchors remain. Data gap too severe.")

    
    # If we get here, all anchors are in X_full.index
    print(f"[xgb_backtest] Found {len(anchors)} anchors.")

    last_date = y_full.index[-1]
    feature_names = list(X_full.columns)
    results_summary = []
    
    for anchor_date in anchors:
        print(f"\n[xgb_backtest] Anchor at date={anchor_date.date()}")

        # ------------------------------------------------------------
        # Guard: require future y for the full horizon (skip if missing)
        # ------------------------------------------------------------
        test_idx = month_ends_after(anchor_date, steps=horizon)
        y_test = y_full.reindex(test_idx)
    
        if y_test.isna().any():
            missing = [d.date().isoformat() for d in y_test.index[y_test.isna()]]
            print(
                f"[xgb_backtest] SKIP anchor={anchor_date.date().isoformat()} "
                f"missing future y for horizon={horizon}: {missing}"
            )
            continue


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
        
        fi_sel["data_asof"] = str(data_asof_effective) if data_asof_effective else None  # canonical for downstream
        
        fi_sel["geo_id"] = geo_id
        fi_sel["metric_id"] = metric_id
        fi_sel["property_type_id"] = property_type_id
        fi_sel["anchor_date"] = anchor_key
        fi_sel["horizon"] = int(horizon_bt)
        fi_sel["seed"] = int(seed)
        
        fi_sel["data_asof_requested"] = str(data_asof_requested) if data_asof_requested else None
        fi_sel["data_asof_effective"] = str(data_asof_effective) if data_asof_effective else None
        fi_sel["asof_clamp_reason"] = json.dumps(asof_clamp_reason) if asof_clamp_reason else None

        # deterministic checksum so consumers can assert identity
        fid_list = fi_sel["feature_id"].astype(str).tolist()
        fi_sel["feature_set_sha256"] = hashlib.sha256("\n".join(fid_list).encode("utf-8")).hexdigest()


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
        algo_params["data_asof_requested"] = requested_asof_dt.isoformat() if requested_asof_dt else None
        algo_params["data_asof_effective"] = data_asof.isoformat() if data_asof else None
        algo_params["asof_clamp_reason"] = asof_clamp_reason

        
"""
"""
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
"""
"""
        last_period = anchor_date.to_period("M")
        future_periods = [last_period + i for i in range(1, horizon_bt + 1)]
        target_dates = [p.to_timestamp(how="end").date() for p in future_periods]
"""
"""
        insert_predictions(
            con=con,
            run_id=run_id,
            target_dates=target_dates,
            y_hat=preds_array,
            y_hat_lo=None,
            y_hat_hi=None,
        )
"""
"""

        print(f"[xgb_backtest] Created XGB backtest run_id={run_id} for anchor={anchor_date.date()}")
        results_summary.append({"anchor_date": anchor_date, "run_id": run_id})

    print("\n[xgb_backtest] Summary:")
    for r in results_summary:
        print(f"  anchor={r['anchor_date'].date()} -> run_id={r['run_id']}")


def main(argv: Optional[List[str]] = None) -> int:
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
        purpose=args.purpose,
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
"""  
