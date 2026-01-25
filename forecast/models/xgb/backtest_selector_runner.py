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
from forecast.metric_tiers import RedfinTierShareCaps, redfin_metric_tier


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
    
    redfin_caps = RedfinTierShareCaps(
        tier0=0.30, tier1=0.35, tier2=0.25, tier3=0.10,
        redfin_cap_n=int(round(0.70 * 250)),
    )

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


    # 2) pick the single anchor FIRST (selector contract)
    # We only need y to choose anchors; do NOT build the full X yet.
    y_for_anchors = load_target_series_for_spec(target).copy()
    y_for_anchors.index = month_end_index(y_for_anchors.index)
    y_for_anchors = y_for_anchors[~y_for_anchors.index.duplicated(keep="last")].sort_index()

    if anchors_csv:
        anchors = [
            pd.Timestamp(s.strip()).to_period("M").to_timestamp(how="end")
            for s in anchors_csv.split(",")
            if s.strip()
        ]
    else:
        anchors = choose_anchor_dates(
            y_for_anchors,
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

    anchor_date = anchors[0].to_period("M").to_timestamp(how="end")
    anchor_ts = month_end_index(pd.DatetimeIndex([pd.Timestamp(anchor_date)]))[0]


    scored = score_candidates(
        target=target,
        candidates=candidate_specs,
        train_end=anchor_ts,
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
        redfin_tier_caps=redfin_caps,
    )

    print("[xgb_selector] anchor=", anchor_date.date().isoformat())
    print("[xgb_selector] picked_base_series=", len(picked))
    print("[xgb_selector] picked_redfin_tiers=",
          {t: sum(1 for it in picked if (getattr(it.spec, "source_id", "") or "").lower()=="redfin" and redfin_metric_tier(it.spec.metric_id)==t)
           for t in (0,1,2,3)})
    print("[xgb_selector] top20_picked=", [it.spec.name for it in picked[:20]])


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
