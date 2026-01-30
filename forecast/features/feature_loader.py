# forecast/feature_loader.py

from collections import Counter
from datetime import date
from typing import Optional, List, Dict, Tuple

import pandas as pd

from forecast.core.backtest_utils import month_end_index
from forecast.core.asof import normalize_month_end

from .feature_policy import default_policy
from forecast.features.metric_tiers import canon_geo_id

from .specs import TargetSpec, FeatureSpec
from .fact_loader import get_connection, load_series_from_fact, load_series_from_fact_with_source
from .ids import (
    parse_feature_id_to_spec,
    parse_feature_id,
    parse_base_name,
    specs_from_selected_feature_ids,
)

# ====================================================================
# Constants
# ====================================================================
NRC_SOURCE_ID = "census_nrc_fred"
NRC_PT = "all"

NRC_GEOS = [
    "us_nation",
    "us_region_northeast",
    "us_region_midwest",
    "us_region_south",
    "us_region_west",
]

NRC_METRICS = [
    "census_housing_starts_total_saar",
    "census_housing_completions_total_saar",
]

# ====================================================================
# Orchestrators
# ====================================================================
def _target_expected_buckets(con, target: TargetSpec, data_asof: Optional[date] = None) -> Dict[str, int]:
    """
    Compute how many distinct bucket periods exist in the target timeline:
      - monthly buckets (months)
      - quarterly buckets (quarters)
      - annual buckets (years)

    These are used to compute coverage ratios for features by native frequency.
    """
    pt_id = target.property_type_id if target.property_type_id is not None else "all"
    
    effective_asof = data_asof if data_asof is not None else target.data_asof
    effective_asof = normalize_month_end(effective_asof)

    sql = """
    WITH target_series AS (
      SELECT date
      FROM fact_timeseries
      WHERE metric_id = ?
        AND geo_id = ?
        AND property_type_id = ?
        AND (? IS NULL OR date <= ?)
    )
    SELECT
      COUNT(DISTINCT date_trunc('month', date))   AS n_months,
      COUNT(DISTINCT date_trunc('quarter', date)) AS n_quarters,
      COUNT(DISTINCT date_trunc('year', date))    AS n_years
    FROM target_series
    """
    n_months, n_quarters, n_years = con.execute(
        sql, [target.metric_id, target.geo_id, pt_id, effective_asof, effective_asof]
    ).fetchone()

    # Defensive: never allow 0 denominators
    return {
        "monthly": max(int(n_months or 0), 1),
        "quarterly": max(int(n_quarters or 0), 1),
        "annual": max(int(n_years or 0), 1),
    }


def load_target_series_for_spec(t: TargetSpec) -> pd.Series:
    return load_series_from_fact(
        metric_id=t.metric_id,
        geo_id=t.geo_id,
        property_type_id=t.property_type_id,
        data_asof=t.data_asof,
        asof_by_source=t.asof_by_source,
        source_id=None,
    )

# ====================================================================
# Design matrix builder
# ====================================================================
def build_design_matrix(
    target: TargetSpec,
    feature_specs: List[FeatureSpec],
    min_obs: int = 60,
    drop_feature_na: bool = True,   # NEW
) -> Tuple[pd.Series, pd.DataFrame, Dict[str, pd.Series]]:
    """
    Build a supervised-learning design matrix on the target’s month-end timeline.
    
    Returns:
      y: target series aligned to X (index = dates, name='y')
      X: dataframe of lagged features aligned to the target timeline
      base_series: dict[name -> unlagged series], reindexed to the target timeline
    
    Key behavior:
      - The target defines the canonical timeline (month-end, deduped, sorted).
      - Feature series are reindexed to the target timeline and may contain NaNs.
      - If drop_feature_na=True, rows with any missing lagged feature are dropped
        (complete-case matrix, suitable for models that can’t handle NaNs).
      - If drop_feature_na=False, only rows with missing y are dropped; X may contain NaNs
        (suitable for XGBoost, which can handle missing values).
    """
    # 1) Load target series
    y_raw = load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
        data_asof=target.data_asof,
        asof_by_source=target.asof_by_source,
        source_id=None,  # target load is not source-pinned unless you later choose to pin it
    )
    y_raw.name = "y"

    # Normalize target to month-end, dedupe, sort (target defines the timeline)
    y_raw = y_raw.copy()
    y_raw.index = month_end_index(y_raw.index)
    y_raw = y_raw[~y_raw.index.duplicated(keep="last")].sort_index()

    # 2) Load feature series
    base_series: Dict[str, pd.Series] = {"y": y_raw}  # include target as base for self-lags
    for spec in feature_specs:
        s = load_series_from_fact(
            metric_id=spec.metric_id,
            geo_id=spec.geo_id,
            property_type_id=spec.property_type_id,
            data_asof=target.data_asof,
            asof_by_source=target.asof_by_source,
            source_id=spec.source_id,  # <-- CRITICAL: enables per-source asof later
        )
        s = s.copy()
        s.index = month_end_index(s.index)
        s = s[~s.index.duplicated(keep="last")].sort_index()
        base_series[spec.name] = s

    # 3) Build a base frame on the TARGET index (do NOT inner-join away months)
    df_base = pd.DataFrame(
        {"y": y_raw, **{spec.name: base_series[spec.name].reindex(y_raw.index) for spec in feature_specs}},
        index=y_raw.index,
    )

    # 4) Build lagged features according to specs
    feature_cols = {}
    for spec in feature_specs:
        col_name = spec.name
        for lag in spec.lags:
            lag_col = f"{col_name}_lag{lag}"
            feature_cols[lag_col] = df_base[col_name].shift(lag)

    df_features = pd.DataFrame(feature_cols, index=df_base.index)

    # 5) Combine y and X
    df_all = pd.concat([df_base["y"], df_features], axis=1)
    
    # Always require y
    df_all = df_all.dropna(subset=["y"])
    
    # Only require features if caller asks for complete-case training rows
    if drop_feature_na and not df_features.empty:
        df_all = df_all.dropna(subset=list(df_features.columns))
    
    if len(df_all) < min_obs:
        raise ValueError(
            f"Not enough observations after lagging/alignment: {len(df_all)} < {min_obs}"
        )
    
    y = df_all["y"].copy()
    X = df_all.drop(columns=["y"]).copy()

    # Keep base_series on full target timeline (month-end)
    for k in base_series:
        base_series[k] = base_series[k].reindex(df_base.index)

    return y, X, base_series


def build_design_matrix_incremental(
    target: TargetSpec,
    candidate_specs: List[FeatureSpec],
    min_obs: int = 60,
    max_features: Optional[int] = None,
    load_target_fn=None,
    *,
    drop_feature_na: bool = False,   # NEW: False for XGB
) -> Tuple[pd.Series, pd.DataFrame, Dict[str, pd.Series], List[FeatureSpec]]:
    """
    Incrementally build a design matrix by trying candidate features one-by-one.

    Strategy:
      - Start with no features.
      - For each candidate FeatureSpec in order:
          * Attempt to build a design matrix using currently-selected specs + this one
            (via the existing build_design_matrix).
          * If build_design_matrix fails (e.g. 0 < min_obs or other alignment issue) -> skip this feature.
          * Else, if resulting row count >= min_obs -> accept this feature (update y/X/base/specs).
          * Else -> skip this feature.
      - Stop when we've exhausted candidates or reached max_features (if set).

    Returns:
      y: target series
      X: design matrix with selected features
      base_series: dict[str, pd.Series] from the final build_design_matrix call
      selected_specs: list of FeatureSpec actually used

    Raises:
      ValueError if we cannot get at least min_obs observations even with 0 features
      (which would mean the target itself doesn't have enough history).
    """
    if not candidate_specs:
        # Rely on your existing univariate pipeline to enforce min_obs on target alone.
        raise ValueError("No candidate specs provided to build_design_matrix_incremental.")
    
    print(f"[inc-build] target={target.metric_id}/{target.geo_id}/{target.property_type_id}")
    print(f"[inc-build] candidates={len(candidate_specs)} min_obs={min_obs} max_features={max_features}")

    policy = default_policy()
    # DQ thresholds (policy-owned; fallback if not yet added)
    min_cov = float(getattr(policy, "min_feature_coverage_ratio", 0.95))
    max_consec_missing = int(getattr(policy, "max_consecutive_missing_months", 2))


    selected_specs: List[FeatureSpec] = []
    current_y: Optional[pd.Series] = None
    current_X: Optional[pd.DataFrame] = None
    current_base: Optional[Dict[str, pd.Series]] = None

    
    if load_target_fn is None:
        raise ValueError(
            "build_design_matrix_incremental requires load_target_fn so we can validate "
            "target history without exog contamination."
        )

    def _max_consecutive_missing(mask: pd.Series) -> int:
        # mask=True where missing
        cur = 0
        best = 0
        for v in mask.astype(bool).values:
            if v:
                cur += 1
                if cur > best:
                    best = cur
            else:
                cur = 0
        return int(best)

    def _coverage_ratio(s: pd.Series) -> float:
        if s is None or len(s) == 0:
            return 0.0
        return float(s.notna().sum() / len(s))


    y_raw = load_target_fn(target).copy()
    y_raw.index = month_end_index(y_raw.index)
    y_raw = y_raw[~y_raw.index.duplicated(keep="last")].sort_index()

    if len(y_raw) < min_obs:
        raise ValueError(
            f"Target series does not have enough observations before exogs: "
            f"{len(y_raw)} < {min_obs}"
        )


    # Now incrementally add features
    for i, spec in enumerate(candidate_specs, start=1):
        if max_features is not None and len(selected_specs) >= max_features:
            break

        if i % 50 == 0:
            print(f"[inc-build] tried={i}/{len(candidate_specs)} accepted={len(selected_specs)}")

        trial_specs = selected_specs + [spec]

        try:
            y_trial, X_trial, base_trial = build_design_matrix(
                target=target,
                feature_specs=trial_specs,
                min_obs=1,  # we'll enforce ourselves
                drop_feature_na=drop_feature_na,
            )
        except Exception:
            # This feature makes alignment impossible; skip it.
            continue

        # ----------------------------
        # DQ gate (BEFORE acceptance)
        # ----------------------------
        # Use the base (unlagged) series aligned to the *training* timeline.
        # build_design_matrix already reindexes base_series to the target timeline.
        s_base = base_trial.get(spec.name)
        if s_base is None:
            continue

        # Align to y_trial index (the only months we can train on anyway)
        s_aligned = s_base.reindex(y_trial.index)

        cov = _coverage_ratio(s_aligned)
        max_consec = _max_consecutive_missing(s_aligned.isna())

        if cov < min_cov:
            # Reject early: this series will burn quota + create junk lag columns
            if len(selected_specs) <= 10 or i % 50 == 0:
                print(f"[inc-build] -reject {spec.name} coverage={cov:.3f} < {min_cov:.3f}")
            continue

        if max_consec > max_consec_missing:
            if len(selected_specs) <= 10 or i % 50 == 0:
                print(f"[inc-build] -reject {spec.name} max_consec_missing={max_consec} > {max_consec_missing}")
            continue

        # Effective rows for THIS candidate’s lagged columns (not all columns)
        new_cols = [f"{spec.name}_lag{lag}" for lag in spec.lags]
        df_eff = pd.concat([y_trial, X_trial[new_cols]], axis=1).dropna()
        n_eff = len(df_eff)
        
        if n_eff >= min_obs:
            selected_specs = trial_specs
            current_y, current_X, current_base = y_trial, X_trial, base_trial
        
            if len(selected_specs) <= 10 or len(selected_specs) % 10 == 0:
                print(f"[inc-build] +accept {spec.name} -> y_obs={len(y_trial)} n_eff={n_eff} cov={cov:.3f} max_consec={max_consec} feats={X_trial.shape[1]}")



    if current_y is None or current_X is None or current_base is None:
        # Fallback: try with just the first candidate as a last resort, enforcing min_obs
        try:
            y_last, X_last, base_last = build_design_matrix(
                target=target,
                feature_specs=[candidate_specs[0]],
                min_obs=min_obs,
            )
            selected_specs = [candidate_specs[0]]
            return y_last, X_last, base_last, selected_specs
        except Exception:
            raise ValueError(
                "Could not build a design matrix with any candidate features "
                f"while preserving at least {min_obs} observations."
            )

    print(f"[inc-build] DONE accepted={len(selected_specs)} obs={len(current_y)} feats={current_X.shape[1]}")
    return current_y, current_X, current_base, selected_specs

# -----------------------------------------
# Universal "kitchen sink" feature discovery
# -----------------------------------------
def discover_all_series_for_target(
    target: TargetSpec,
    min_overlap: int = 72,
    exclude_metrics: Optional[List[str]] = None,
    policy=None,
    data_asof: Optional[date] = None,   # NEW
) -> List[Tuple[str, str, str, str, str, str, float, int]]:
    """
    Return governed candidates with metadata:

    (metric_id, geo_id, property_type_id, source_id, category, frequency, coverage_ratio, n_overlap)

    - n_overlap is in MONTHS (raw overlap count on target dates).
    - coverage_ratio is computed on NATIVE frequency buckets:
        monthly -> distinct months / target distinct months
        quarterly -> distinct quarters / target distinct quarters
        annual -> distinct years / target distinct years
    """
    policy = policy or default_policy()
    exclude_metrics_set = set(exclude_metrics or [])

    con = get_connection()

    pt_id = target.property_type_id if target.property_type_id is not None else "all"
    
    effective_asof = data_asof if data_asof is not None else target.data_asof
    effective_asof = normalize_month_end(effective_asof)
    
    expected = _target_expected_buckets(con, target, data_asof=effective_asof)

    sql = """
    WITH target_series AS (
        SELECT date
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
          AND (? IS NULL OR date <= ?)
    ),
    joined AS (
        SELECT
            b.metric_id,
            b.geo_id,
            b.property_type_id,
            b.source_id,
            COALESCE(LOWER(d.category), 'uncategorized') AS category,
            COALESCE(LOWER(d.frequency), 'monthly')      AS frequency,
            t.date                                       AS t_date,
            CASE
              WHEN COALESCE(LOWER(d.frequency), 'monthly') = 'annual' THEN date_trunc('year', t.date)
              WHEN COALESCE(LOWER(d.frequency), 'monthly') = 'quarterly' THEN date_trunc('quarter', t.date)
              ELSE date_trunc('month', t.date)
            END AS bucket
        FROM target_series t
        JOIN fact_timeseries b
          ON t.date = b.date
         AND (? IS NULL OR b.date <= ?)
         AND b.property_type_id IS NOT NULL
        LEFT JOIN dim_metric d
          ON b.metric_id = d.metric_id
    ),
    agg AS (
        SELECT
            metric_id, geo_id, property_type_id, source_id,
            category, frequency,
            COUNT(*) AS n_overlap_months,
            COUNT(DISTINCT bucket) AS n_buckets
        FROM joined
        GROUP BY 1,2,3,4,5,6
    )
    SELECT
        metric_id, geo_id, property_type_id, source_id,
        category, frequency,
        n_overlap_months,
        n_buckets
    FROM agg
    WHERE n_overlap_months >= ?
    ORDER BY metric_id, geo_id, property_type_id, source_id
    """

    rows = con.execute(sql, [target.metric_id, target.geo_id, pt_id, effective_asof, effective_asof, effective_asof, effective_asof, int(min_overlap)]).fetchall()
    con.close()

    t_geo_canon = canon_geo_id(target.geo_id)
    t_metric = target.metric_id
    t_pt = str(target.property_type_id)


    out = []
    for metric_id, geo_id, pt_id, source_id, category, frequency, n_overlap, n_buckets in rows:
        # skip target-equivalent series: same metric + canon-geo-equivalent + same PT
        if (metric_id == t_metric) and (canon_geo_id(geo_id) == t_geo_canon) and (str(pt_id) == t_pt):
            continue

        # explicit metric exclusions
        if metric_id in exclude_metrics_set:
            continue

        # --- Category gating ---
        cat = (category or "uncategorized").lower()
        if policy.include_categories is not None and cat not in policy.include_categories:
            continue
        if policy.exclude_categories and cat in policy.exclude_categories:
            continue

        # --- Source/PT gating (per policy) ---
        ex_ptids = policy.exclude_property_type_ids_by_source.get(source_id, set()) if policy.exclude_property_type_ids_by_source else set()
        if ex_ptids and str(pt_id) in ex_ptids:
            continue

        # --- Hard PT gating (global) ---
        # If you want this to apply only to redfin, keep it inside the source_id == "redfin" condition.
        if source_id == "redfin" and str(pt_id) in {"-1", "-2"}:
            continue

        # --- Coverage gating ---
        freq = (frequency or "monthly").lower()
        denom = expected.get(freq, expected["monthly"])
        cov = float(n_buckets) / float(denom)

        thr = None
        if policy.min_coverage_ratio:
            thr = policy.min_coverage_ratio.get(freq, None)

        if thr is not None and cov < float(thr):
            continue

        out.append((metric_id, geo_id, pt_id, source_id, cat, freq, cov, int(n_overlap)))

    # ------------------------------------------------------------
    # CES: prefer SA over NSA for the same base concept
    # ------------------------------------------------------------
    # If your CES metric ids look like:
    #   ces_construction_sa, ces_construction_nsa
    # then the "base concept" is the prefix without _sa/_nsa.
    # Keep SA if available, otherwise keep NSA.
    def _ces_base(mid: str) -> str:
        if mid.endswith("_sa"):
            return mid[:-3]
        if mid.endswith("_nsa"):
            return mid[:-4]
        return mid

    # Partition into CES vs non-CES
    non_ces = []
    ces_rows = []

    for row in out:
        metric_id, geo_id, pt_id, source_id, cat, freq, cov, n_overlap = row
        if source_id == "ces" and (metric_id.endswith("_sa") or metric_id.endswith("_nsa")):
            ces_rows.append(row)
        else:
            non_ces.append(row)

    # Group CES rows by "same thing" except SA/NSA, and choose SA if present else NSA
    chosen = []
    by_key = {}
    for row in ces_rows:
        metric_id, geo_id, pt_id, source_id, cat, freq, cov, n_overlap = row
        key = (_ces_base(metric_id), geo_id, str(pt_id), source_id)
        by_key.setdefault(key, []).append(row)

    for key, rows_k in by_key.items():
        # choose SA if exists, else NSA; tie-break by higher coverage then higher overlap
        sa = [r for r in rows_k if r[0].endswith("_sa")]
        pool = sa if sa else rows_k
        pool = sorted(pool, key=lambda r: (-float(r[6]), -int(r[7]), r[0]))  # cov desc, overlap desc, metric_id
        chosen.append(pool[0])

    out = non_ces + chosen

    # Optional: deterministic ordering for downstream stability
    out = sorted(out, key=lambda r: (r[4], r[0], r[1], str(r[2]), r[3]))

    return out


def build_universal_feature_specs(
    target: TargetSpec,
    lag_scheme: List[int] = [1, 3, 6, 12],
    min_obs: int = 60,
) -> List[FeatureSpec]:
    max_lag = max(lag_scheme)
    min_overlap = min_obs + max_lag

    policy = default_policy()

    governed = discover_all_series_for_target(
        target=target,
        min_overlap=min_overlap,
        exclude_metrics=[],     # allow same metric other geos
        policy=policy,
    )

    # Debug: distribution in the FULL universe after shared gates (no category caps here)
    print("[governance] category_counts_universe:", Counter([(r[4] or "uncategorized") for r in governed]).most_common(20))
    print("[governance] n_candidates_universe:", len(governed))


    # governed rows: (metric_id, geo_id, pt_id, source_id, category, frequency, coverage_ratio, n_overlap)
    specs: List[FeatureSpec] = []
    for metric_id, geo_id, pt_id, source_id, cat, freq, cov, n_overlap in governed:
        if (source_id or "").lower() == NRC_SOURCE_ID:
            pt_id = NRC_PT
        specs.append(
            FeatureSpec(
                name=f"{metric_id}__{geo_id}__{pt_id}__{source_id}",
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id,
                source_id=source_id,
                category=(cat or "uncategorized").lower(),
                frequency=(freq or "monthly").lower(),
                lags=tuple(lag_scheme),
            )
        )

    # ------------------------------------------------------------
    # Always include NRC (Census new residential construction via FRED)
    # These are macro series (not property-type specific) -> PT = "all"
    # ------------------------------------------------------------
    for geo in NRC_GEOS:
        for mid in NRC_METRICS:
            specs.append(
                FeatureSpec(
                    name=f"{mid}__{geo}__{NRC_PT}__{NRC_SOURCE_ID}",
                    metric_id=mid,
                    geo_id=geo,
                    property_type_id=NRC_PT,
                    source_id=NRC_SOURCE_ID,
                    category="census",
                    frequency="monthly",
                    lags=tuple(lag_scheme),   # ✅ critical
                )
            )

    # deterministic dedupe on base-series identity
    seen = set()
    out = []
    for s in specs:
        k = (s.metric_id, s.geo_id, str(s.property_type_id), str(s.source_id))
        if k in seen:
            continue
        seen.add(k)
        out.append(s)
    specs = out

    nrc = [s for s in specs if (s.source_id or "").lower() == NRC_SOURCE_ID]
    print(f"[governance] nrc_candidates={len(nrc)} example={[(s.metric_id, s.geo_id, s.property_type_id) for s in nrc[:3]]}")

    return specs
