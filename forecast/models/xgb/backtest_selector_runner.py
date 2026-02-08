from __future__ import annotations
# forecast/models/xgb/backtest_selector_runner.py

import json
import hashlib
import time
import pickle

from pathlib import Path
from typing import Optional, List, Dict
from collections import Counter
from dataclasses import asdict, is_dataclass

import numpy as np
import pandas as pd
from xgboost import XGBRegressor

from forecast.core.db_forecast import new_batch_id
from forecast.core.backtest_utils import (
    DEFAULT_MIN_TRAIN_LEN,
    DEFAULT_ANCHOR_STEP_MONTHS,
    DEFAULT_MAX_ANCHORS,
    DEFAULT_ANCHOR_BUFFER_MONTHS,
)
from forecast.core.anchors import AnchorPolicy, choose_anchors, month_end_index

from forecast.features.feature_loader import (
    TargetSpec,
    build_universal_feature_specs,
    build_design_matrix_incremental,
    load_target_series_for_spec,
)
from forecast.features.feature_policy import default_policy
from forecast.features.feature_selection import (
    score_candidates,
    select_scored_candidates,
    scored_to_feature_specs,
    default_bucket,
)
from forecast.features.metric_tiers import RedfinTierShareCaps, redfin_metric_tier, canon_geo_id
from forecast.features.feature_loader import specs_from_selected_feature_ids

from forecast.models.xgb.selector_utils import parse_data_asof, _source_from_feature_id, _is_redfin_source
from forecast.models.xgb.selector_reporting import (
    SelectorRunSummary,
    build_stage1_summary,
    build_final_k_summary,
    write_selector_summary,
)

TEMP_DEBUG_LIMIT = None  # set to an int for debugging; None for normal operation
MIN_NON_REDFIN_DEFAULT = 25  # NEW: hard minimum count of non-Redfin features in final top-K
DEFAULT_SELECTOR_MAX_ANCHORS = 1


def _shared_dir(artifact_root: str, batch_id: str) -> Path:
    return Path(artifact_root) / "runs" / batch_id / "xgb" / "_shared"


def _serialize_list_jsonl(items, path: Path) -> bool:
    """
    Try to write list items as JSONL. Returns True if succeeded, else False.
    """
    try:
        with path.open("w", encoding="utf-8") as f:
            for it in items:
                if is_dataclass(it):
                    obj = asdict(it)
                elif isinstance(it, dict):
                    obj = it
                else:
                    # If it's a simple type or has __dict__, attempt to serialize that
                    if hasattr(it, "__dict__"):
                        obj = dict(it.__dict__)
                    else:
                        # not JSON-serializable in a reasonable way
                        return False
                f.write(json.dumps(obj, default=str) + "\n")
        return True
    except Exception:
        return False


def _load_list_jsonl(path: Path):
    out = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def _load_or_build_universal_specs(*, artifact_root: str, batch_id: str, rebuild: bool, **kwargs):
    d = _shared_dir(artifact_root, batch_id)
    d.mkdir(parents=True, exist_ok=True)

    """
    jsonl_path = d / "universal_feature_specs.jsonl"
    pkl_path = d / "universal_feature_specs.pkl"

    if not rebuild:
        if jsonl_path.exists():
            print(f"[cache] HIT universal_feature_specs (jsonl) -> {jsonl_path}")
            return _load_list_jsonl(jsonl_path)
        if pkl_path.exists():
            print(f"[cache] HIT universal_feature_specs (pkl) -> {pkl_path}")
            with pkl_path.open("rb") as f:
                return pickle.load(f)

    print(f"[cache] MISS universal_feature_specs -> building")
    specs = build_universal_feature_specs(**kwargs)
    print(f"[cache] universal_feature_specs_type={type(specs)} item_type={type(specs[0]) if specs else None}")

    # Try JSONL first (auditable). If that fails, use pickle.
    if _serialize_list_jsonl(specs, jsonl_path):
        print(f"[cache] WROTE universal_feature_specs (jsonl) -> {jsonl_path}")
    else:
        with pkl_path.open("wb") as f:
            pickle.dump(specs, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"[cache] WROTE universal_feature_specs (pkl) -> {pkl_path}")
    """

    pkl_path = d / "universal_feature_specs.pkl"
    
    if pkl_path.exists() and not rebuild:
        print(f"[cache] HIT universal_feature_specs (pkl) -> {pkl_path}")
        with pkl_path.open("rb") as f:
            return pickle.load(f)
    
    print(f"[cache] MISS universal_feature_specs -> building")
    specs = build_universal_feature_specs(**kwargs)
    
    with pkl_path.open("wb") as f:
        pickle.dump(specs, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    print(f"[cache] WROTE universal_feature_specs (pkl) -> {pkl_path}")
    return specs


def _load_or_score_candidates(
    *,
    artifact_root: str,
    batch_id: str,
    metric_id: str,
    anchor: pd.Timestamp,
    rebuild: bool,
    score_kwargs: dict,
):
    d = _shared_dir(artifact_root, batch_id)
    d.mkdir(parents=True, exist_ok=True)

    a = anchor.date().isoformat()
    pkl_path = d / f"scored_candidates__metric={metric_id}__anchor={a}.pkl"

    if pkl_path.exists() and not rebuild:
        print(f"[cache] HIT scored_candidates (pkl) -> {pkl_path}")
        with pkl_path.open("rb") as f:
            return pickle.load(f)

    print(f"[cache] MISS scored_candidates -> scoring metric={metric_id} anchor={a}")
    scored = score_candidates(**score_kwargs)

    # sanity: we expect a list-like scored structure
    print(f"[cache] scored_candidates_type={type(scored)} len={len(scored) if hasattr(scored,'__len__') else 'NA'}")

    with pkl_path.open("wb") as f:
        pickle.dump(scored, f, protocol=pickle.HIGHEST_PROTOCOL)

    print(f"[cache] WROTE scored_candidates (pkl) -> {pkl_path}")
    return scored
    
# ==============================
# Main / Primary Code Block
# ==============================
def run_xgb_selector(
    metric_id: str = "median_sale_price",
    geo_id: str = "dc_city",
    property_type_id: str = "-1",
    horizon: int = 12,
    *,
    min_train_len: int = DEFAULT_MIN_TRAIN_LEN,
    anchor_step_months: int = DEFAULT_ANCHOR_STEP_MONTHS,
    max_anchors: int = DEFAULT_SELECTOR_MAX_ANCHORS,
    latest_anchor_offset_months: Optional[int] = None,
    batch_id: Optional[str] = None,
    data_asof: Optional[str] = None,  # YYYY-MM-DD requested
    seed: int = 1337,
    artifact_root: str = "runs",
    xgb_top_k: int = 100,
    anchors_csv: Optional[str] = None,
    metric_pt_cap: int = 10,  # NEW
    min_non_redfin: int = MIN_NON_REDFIN_DEFAULT,  # NEW
    debug: Optional[bool] = False,
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
    t0 = time.time()
    policy = default_policy()
    
    redfin_caps = RedfinTierShareCaps(
        tier0=0.30, tier1=0.35, tier2=0.25, tier3=0.10,
        redfin_cap_n=int(round(0.70 * 250)),
    )

    batch_id = batch_id or new_batch_id()
    #out_dir = Path(artifact_root or "runs") / batch_id / "xgb"
    out_dir = Path(artifact_root) / "runs" / batch_id / "xgb" / metric_id
    out_dir.mkdir(parents=True, exist_ok=True)

    target = TargetSpec(metric_id=metric_id, geo_id=geo_id, property_type_id=property_type_id)

    # 1) build universal candidate specs
    t = time.time()
    #candidate_specs = build_universal_feature_specs(target)
    candidate_specs = _load_or_build_universal_specs(
        artifact_root=artifact_root,
        batch_id=batch_id,
        rebuild=rebuild_cache,   # or a new CLI flag later
        target=target,
    )
    #if not candidate_specs:
    if candidate_specs is None or len(candidate_specs) == 0:
        print("[xgb_selector] No candidate features; skipping.")
        return

    print(f"[timing] build_universal_feature_specs_sec={time.time()-t:.2f}")    

    nrc_count = sum(1 for s in candidate_specs if (s.source_id or "").lower() == "census_nrc_fred")
    print("[xgb_selector] nrc_candidate_count =", nrc_count)

    # --- debug: what redfin metric_ids even exist in candidates? ---
    redfin_mids = sorted({s.metric_id for s in candidate_specs if (s.source_id or "").lower() == "redfin"})
    print("[xgb_selector] unique_redfin_metric_ids (count) =", len(redfin_mids))
    print("[xgb_selector] unique_redfin_metric_ids (sample) =", redfin_mids[:50])
    
    wanted = ["median_sale_price", "median_ppsf", "pending_sales", "new_listings", "price_drops", "sold_above_list", "months_of_supply"]
    c = Counter([s.metric_id for s in candidate_specs if (s.source_id or "").lower() == "redfin"])
    
    if debug:
        print("[xgb_selector] wanted_present =", {w: (w in set(redfin_mids)) for w in wanted})
    
        # optional: show “close matches” so we catch naming mismatches
        import difflib
        for w in wanted:
            close = difflib.get_close_matches(w, redfin_mids, n=5, cutoff=0.6)
            if close:
                print(f"[xgb_selector] close_matches[{w}] =", close)
        
        print("[xgb_selector] redfin_metric_counts_top=", c.most_common(25))
        print("[xgb_selector] has_pending_sales=", "pending_sales" in c)
        print("[xgb_selector] has_new_listings=", "new_listings" in c)
        print("[xgb_selector] has_price_drops=", "price_drops" in c)
        print("[xgb_selector] has_sold_above_list=", "sold_above_list" in c)


    if TEMP_DEBUG_LIMIT is not None:
        candidate_specs = candidate_specs[: int(TEMP_DEBUG_LIMIT)]
        print(f"[xgb_selector] TEMP: truncating candidates to {len(candidate_specs)}")


    # 2) pick the single anchor FIRST (selector contract)
    # We only need y to choose anchors; do NOT build the full X yet.
    y_for_anchors = load_target_series_for_spec(target).copy()
    y_for_anchors.index = month_end_index(y_for_anchors.index)
    y_for_anchors = y_for_anchors[~y_for_anchors.index.duplicated(keep="last")].sort_index()

    # Respect data_asof for anchor selection (no implicit peeking past requested window)
    if data_asof is not None:
        req = parse_data_asof(data_asof)
        req_end = pd.Timestamp(req).to_period("M").to_timestamp(how="end")
        y_for_anchors = y_for_anchors.loc[:req_end]

    if anchors_csv:
        anchors = [
            pd.Timestamp(s.strip()).to_period("M").to_timestamp(how="end")
            for s in anchors_csv.split(",")
            if s.strip()
        ]
    else:
        ap = AnchorPolicy(
            horizon=int(horizon),
            min_train_len=int(min_train_len),
            step_months=int(anchor_step_months),
            max_anchors=int(max_anchors),
            latest_anchor_offset_months=int(latest_anchor_offset_months) if latest_anchor_offset_months is not None else None,
        )
        # Selector: we want anchors that are scorable (full horizon exists) unless you explicitly decide otherwise.
        if not anchors_csv and int(max_anchors) != 1:
            raise SystemExit("[xgb_selector] max_anchors must be 1 unless --anchors is provided (selector contract).")

        anchors = choose_anchors(y_for_anchors, ap, require_full_horizon=True)


    if not anchors:
        raise ValueError("[xgb_selector] No anchors available.")

    if len(anchors) != 1:
        raise ValueError(f"[xgb_selector] selector batch must have exactly 1 anchor, got {len(anchors)}")

    anchor_date = anchors[0].to_period("M").to_timestamp(how="end")
    anchor_ts = month_end_index(pd.DatetimeIndex([pd.Timestamp(anchor_date)]))[0]

    print("[xgb_selector] score_mode=", "combo")
    
    t = time.time()
    """
    scored = score_candidates(
        target=target,
        candidates=candidate_specs,
        train_end=anchor_ts,
        min_eff=60,
        lead_months=(0, 1, 2, 3),
        score_mode="combo",
    )
    """
    scored = _load_or_score_candidates(
        artifact_root=artifact_root,
        batch_id=batch_id,
        metric_id=metric_id,
        anchor=anchor_ts,
        rebuild=rebuild_cache,
        score_kwargs=dict(
            target=target,
            candidates=candidate_specs,
            train_end=anchor_ts,
            min_eff=60,
            lead_months=(0, 1, 2, 3),
            score_mode="combo",
        ),
    )

    print(f"[timing] score_candidates_sec={time.time()-t:.2f}")    

    redfin_scored = [it for it in scored if (it.spec.source_id or "").lower() == "redfin"]
    print("[xgb_selector] n_scored_total=", len(scored), "n_scored_redfin=", len(redfin_scored))
    
    # tier distribution among the TOP 200 scored redfin (this is the real question)
    topN = redfin_scored[:200]
    tier_counts = {t: 0 for t in (0,1,2,3)}
    metric_counts = {}
    for it in topN:
        t = redfin_metric_tier(it.spec.metric_id)
        tier_counts[t] += 1
        metric_counts[it.spec.metric_id] = metric_counts.get(it.spec.metric_id, 0) + 1
    
    print("[xgb_selector] top200_redfin_tier_counts=", tier_counts)
    print("[xgb_selector] top200_redfin_metrics_top=", sorted(metric_counts.items(), key=lambda x: -x[1])[:20])


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

    t = time.time()
    picked = select_scored_candidates(
        scored=scored,
        max_base_series=250,
        category_caps=policy.family_caps,
        category_minimums=category_minimums,
        bucket_caps=bucket_caps,
        bucket_fn=lambda spec: default_bucket(spec, target),
        redfin_tier_caps=redfin_caps,
        metric_pt_cap=int(metric_pt_cap),
    )

    print(f"[timing] select_scored_candidates_sec={time.time()-t:.2f}")

    def _is_redfin_spec(spec) -> bool:
        return (getattr(spec, "source_id", "") or "").lower() == "redfin"
    
    redfin_tier_used = {t: 0 for t in (0,1,2,3)}
    for it in picked:
        if _is_redfin_spec(it.spec):
            redfin_tier_used[redfin_metric_tier(it.spec.metric_id)] += 1
    
    # You already have redfin_caps and the selector prints quota, but quota isn’t returned.
    # Minimal approach: recompute expected quota locally the same way governance does OR store None.
    # For now: store None, and we can patch governance to return it later without breaking API.
    redfin_tier_quota = None


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
    max_features = 300 if debug else None
    required_obs = min_train_len + horizon + DEFAULT_ANCHOR_BUFFER_MONTHS
    t = time.time()
    y_full, X_full, _base_series_full, _selected_specs = build_design_matrix_incremental(
        target=target,
        candidate_specs=candidate_specs,
        min_obs=required_obs,
        max_features=max_features,
        load_target_fn=load_target_series_for_spec,
        drop_feature_na=False,  # keep as you had
    )
    
    print(f"[timing] build_design_matrix_incremental_sec={time.time()-t:.2f}")
    
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
        data_asof_requested = parse_data_asof(data_asof)
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
    t = time.time()
    model.fit(X_train, y_train)

    print(f"[timing] xgb_fit_sec={time.time()-t:.2f}")

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

    # ----------------------------
    # NEW: enforce minimum non-Redfin presence in final top-K
    # ----------------------------
    K = int(xgb_top_k)

    
    fi["_source"] = fi["feature_id"].astype(str).apply(_source_from_feature_id)
    fi["_is_redfin"] = fi["_source"].apply(_is_redfin_source)
    
    fi_non = fi[~fi["_is_redfin"]].copy()
    fi_red = fi[fi["_is_redfin"]].copy()
    
    avail_non = len(fi_non)
    need_non = int(min_non_redfin)
    
    # ---- FAILURE CONDITION (hard) ----
    if need_non > 0 and avail_non < need_non:
        top_sources = fi["_source"].value_counts().head(10).to_dict()
        raise SystemExit(
            "[xgb_selector] FAIL: min_non_redfin cannot be satisfied.\n"
            f"  requested min_non_redfin={need_non}\n"
            f"  available non-redfin (importance>0)={avail_non}\n"
            f"  K={K}\n"
            f"  top_sources={top_sources}\n"
            "Fix by: lowering --min_non_redfin, adding/refreshing non-Redfin sources, "
            "or investigating why non-Redfin importances are zero."
        )
    
    # Choose portfolio:
    # 1) take top 'need_non' non-Redfin
    # 2) fill remaining slots from the rest of fi in original importance order
    #    (this preserves ranking while enforcing composition)
    if need_non > 0:
        take_non = fi_non.head(need_non).copy()
        remaining = K - len(take_non)
    
        # exclude those non-redfin already taken, then fill by importance order
        taken_ids = set(take_non["feature_id"].astype(str).tolist())
        rest = fi[~fi["feature_id"].astype(str).isin(taken_ids)].copy()
        take_rest = rest.head(max(0, remaining)).copy()
    
        fi_sel = pd.concat([take_non, take_rest], ignore_index=True)
    else:
        fi_sel = fi.head(K).copy()
    
    # Re-rank 1..K deterministically in output
    fi_sel = fi_sel.sort_values(["importance", "feature_id"], ascending=[False, True]).reset_index(drop=True)
    fi_sel["rank"] = np.arange(1, len(fi_sel) + 1)
    
    # Drop internal columns
    fi_sel = fi_sel.drop(columns=[c for c in ["_source", "_is_redfin"] if c in fi_sel.columns])


    # ----------------------------
    # NEW: debug prints for source diversity enforcement
    # ----------------------------
    sel_sources = fi_sel["feature_id"].astype(str).apply(_source_from_feature_id)
    sel_is_redfin = sel_sources.apply(_is_redfin_source)
    
    n_sel = len(fi_sel)
    n_sel_red = int(sel_is_redfin.sum())
    n_sel_non = int(n_sel - n_sel_red)
    
    # What got displaced? (only meaningful if need_non > 0)
    if need_non > 0:
        baseline = fi.head(K).copy()
        baseline_ids = set(baseline["feature_id"].astype(str).tolist())
        final_ids = set(fi_sel["feature_id"].astype(str).tolist())
    
        removed = sorted(list(baseline_ids - final_ids))
        added = sorted(list(final_ids - baseline_ids))
    
        removed_sources = [ _source_from_feature_id(x) for x in removed ]
        added_sources = [ _source_from_feature_id(x) for x in added ]
    
        print(
            f"[xgb_selector] min_non_redfin={need_non} K={K} "
            f"final_non_redfin={n_sel_non} final_redfin={n_sel_red} "
            f"displaced_n={len(removed)} added_n={len(added)}"
        )
        print(f"[xgb_selector] displaced_sources_top={Counter([s for s in removed_sources]).most_common(10)}")
        print(f"[xgb_selector] added_sources_top={Counter([s for s in added_sources]).most_common(10)}")
    else:
        print(
            f"[xgb_selector] min_non_redfin=0 K={K} "
            f"final_non_redfin={n_sel_non} final_redfin={n_sel_red}"
        )
    
    print(f"[xgb_selector] final_top_sources={Counter(sel_sources).most_common(12)}")

    
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
    t = time.time()
    out_path = out_dir / f"selected_features__anchor={anchor_date.date().isoformat()}.parquet"
    if out_path.exists():
        raise SystemExit(
            f"[xgb_selector] REFUSING to overwrite existing artifact: {out_path}\n"
            "Use a fresh --batch_id or delete this specific file."
        )
    fi_sel.to_parquet(out_path, index=False)
    

    # 9b) write selector summary JSON (stage1 + finalK)
    out_json = out_dir / f"selector_summary__anchor={anchor_date.date().isoformat()}.json"
    if out_json.exists():
        raise SystemExit(
            f"[xgb_selector] REFUSING to overwrite existing summary: {out_json}\n"
            "Use a fresh --batch_id or delete this specific file."
        )
    
    # stage1 summary (base-series picked BEFORE lag expansion)
    bucket_of = lambda spec: default_bucket(spec, target)
    stage1 = build_stage1_summary(
        anchor=anchor_date.date().isoformat(),
        picked=picked,
        max_base_series=250,
        bucket_of=bucket_of,
        redfin_tier_quota=redfin_tier_quota,
        redfin_tier_used=redfin_tier_used,
    )
    
    # final-K summary (lag features after XGB + min_non_redfin enforcement)
    final_k = build_final_k_summary(
        fi_all=fi,           # the ranked importances DF (importance>0)
        fi_sel=fi_sel,       # your final selection DF
        K_requested=int(xgb_top_k),
        min_non_redfin=int(min_non_redfin),
    )
    
    summary = SelectorRunSummary(
        batch_id=batch_id,
        artifact_root=str(artifact_root or "runs"),
        out_parquet=str(out_path),
        out_json=str(out_json),
        target={"metric_id": metric_id, "geo_id": geo_id, "property_type_id": str(property_type_id)},
        seed=int(seed),
        data_asof_requested=data_asof_requested.isoformat() if data_asof_requested else None,
        data_asof_effective=data_asof_effective.isoformat(),
        asof_clamp_reason=asof_clamp_reason,
        feature_set_sha256=feature_set_sha256,
        stage1=stage1,
        final_k=final_k,
    )
    
    write_selector_summary(out_json, summary)
    print(f"[xgb_selector] wrote selector summary -> {out_json}")

    print(f"[timing] write_artifacts_sec={time.time()-t:.2f}")
    
    print(f"[timing] total_sec={time.time()-t0:.2f}")

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
    p.add_argument(
        "--max_anchors",
        type=int,
        default=DEFAULT_SELECTOR_MAX_ANCHORS,
        help="Selector default is 1. Increase only if you explicitly want multi-anchor selection (will violate selector contract).",
    )

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
    p.add_argument("--metric_pt_cap", type=int, default=10)
    p.add_argument("--min_non_redfin", type=int, default=MIN_NON_REDFIN_DEFAULT)
    p.add_argument("--debug", action="store_true")
    p.add_argument(
        "--rebuild_cache",
        action="store_true",
        help="Force rebuild of shared selector caches (universal specs + scoring).",
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
        metric_pt_cap=args.metric_pt_cap,
        min_non_redfin=args.min_non_redfin,
        debug=args.debug,
        rebuild_cache=args.rebuild_cache,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
