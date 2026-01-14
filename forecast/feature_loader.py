# forecast/feature_loader.py

import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Tuple

import duckdb
import pandas as pd

from .backtest_utils import month_end_index
from .feature_policy import default_policy

# ====================================================================
# Shared types
# ====================================================================
@dataclass
class TargetSpec:
    metric_id: str
    geo_id: str
    # For Redfin, this is '-1', '6', '13', etc. For non-Redfin, use None -> 'all'.
    property_type_id: Optional[str] = None


@dataclass(frozen=True)
class FeatureSpec:
    """
    One base feature series and which lags to create.
    """
    name: str
    metric_id: str
    geo_id: str
    property_type_id: Optional[str]
    source_id: Optional[str] = None   # NEW
    lags: Tuple[int, ...] = field(default_factory=tuple)

# ====================================================================
# Helpers
# ====================================================================
def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


def load_target_series_for_spec(t: TargetSpec) -> pd.Series:
    return load_series_from_fact(
        metric_id=t.metric_id,
        geo_id=t.geo_id,
        property_type_id=t.property_type_id,
    )


def _target_expected_buckets(con, target: TargetSpec) -> Dict[str, int]:
    """
    Compute how many distinct bucket periods exist in the target timeline:
      - monthly buckets (months)
      - quarterly buckets (quarters)
      - annual buckets (years)

    These are used to compute coverage ratios for features by native frequency.
    """
    pt_id = target.property_type_id if target.property_type_id is not None else "all"

    sql = """
    WITH target_series AS (
      SELECT date
      FROM fact_timeseries
      WHERE metric_id = ?
        AND geo_id = ?
        AND property_type_id = ?
    )
    SELECT
      COUNT(DISTINCT date_trunc('month', date))   AS n_months,
      COUNT(DISTINCT date_trunc('quarter', date)) AS n_quarters,
      COUNT(DISTINCT date_trunc('year', date))    AS n_years
    FROM target_series
    """
    n_months, n_quarters, n_years = con.execute(
        sql, [target.metric_id, target.geo_id, pt_id]
    ).fetchone()

    # Defensive: never allow 0 denominators
    return {
        "monthly": max(int(n_months or 0), 1),
        "quarterly": max(int(n_quarters or 0), 1),
        "annual": max(int(n_years or 0), 1),
    }

# ====================================================================
# Load single series from fact_timeseries
# ====================================================================
def load_series_from_fact(
    metric_id: str,
    geo_id: str,
    property_type_id: Optional[str],
) -> pd.Series:
    """
    Load a single series from fact_timeseries for a given (metric, geo, pt_id).

    property_type_id=None -> matches 'all' in fact_timeseries.
    """
    con = get_connection()
    pt_id = property_type_id if property_type_id is not None else "all"

    sql = """
        SELECT date, value
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
        ORDER BY date
    """
    con = get_connection()
    try:
        df = con.execute(sql, [metric_id, geo_id, pt_id]).fetchdf()
    finally:
        con.close()

    if df.empty:
        raise ValueError(
            f"No data for metric={metric_id}, geo={geo_id}, pt={pt_id}"
        )

    s = df.set_index("date")["value"].astype(float)
    return s


# ====================================================================
# Design matrix builder
# ====================================================================
def build_design_matrix(
    target: TargetSpec,
    feature_specs: List[FeatureSpec],
    min_obs: int = 60,
) -> Tuple[pd.Series, pd.DataFrame, Dict[str, pd.Series]]:
    """
    Build a supervised-learning design matrix:

      y_t ~ lagged features (and optionally lagged y)

    Returns:
      y: target series aligned with X (index = dates, name = 'y')
      X: dataframe of lagged features, no NaNs on selected training rows
      base_series: dict name -> base (unlagged) series (on full target timeline)

    Notes:
      - Target defines the timeline (NO inner join with exogs).
      - Feature series are reindexed to target timeline (may contain NaNs).
      - Training rows are those where y and all lagged features exist.
    """
    # 1) Load target series
    y_raw = load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
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

    # 5) Combine y and X, drop rows unusable for training
    df_all = pd.concat([df_base["y"], df_features], axis=1)
    df_all = df_all.dropna(subset=["y"])
    if not df_features.empty:
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

    selected_specs: List[FeatureSpec] = []
    current_y: Optional[pd.Series] = None
    current_X: Optional[pd.DataFrame] = None
    current_base: Optional[Dict[str, pd.Series]] = None

    
    if load_target_fn is None:
        raise ValueError(
            "build_design_matrix_incremental requires load_target_fn so we can validate "
            "target history without exog contamination."
        )

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
                min_obs=1,  # we'll enforce min_obs ourselves
            )
        except Exception:
            # This feature makes alignment impossible; skip it.
            continue

        if len(y_trial) >= min_obs:
            selected_specs = trial_specs
            current_y, current_X, current_base = y_trial, X_trial, base_trial
        
            if len(selected_specs) <= 10 or len(selected_specs) % 10 == 0:
                print(f"[inc-build] +accept {spec.name} -> obs={len(y_trial)} feats={X_trial.shape[1]}")

            """
            # Print first 10 accepts, then every 10 thereafter
            if len(selected_specs) <= 10 or len(selected_specs) % 10 == 0:
                print(f"[inc-build] +accept {spec.name} -> obs={len(y_trial)} feats={X_trial.shape[1]}")
            """

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
    expected = _target_expected_buckets(con, target)

    pt_id = target.property_type_id if target.property_type_id is not None else "all"

    sql = """
    WITH target_series AS (
        SELECT date
        FROM fact_timeseries
        WHERE metric_id = ?
          AND geo_id = ?
          AND property_type_id = ?
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

    rows = con.execute(sql, [target.metric_id, target.geo_id, pt_id, int(min_overlap)]).fetchall()
    con.close()

    out = []
    for metric_id, geo_id, pt_id, source_id, category, frequency, n_overlap, n_buckets in rows:
        # skip exact target triple only
        if metric_id == target.metric_id and geo_id == target.geo_id and str(pt_id) == str(target.property_type_id):
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

    return out


def build_universal_feature_specs(
    target: TargetSpec,
    lag_scheme: List[int] = [1, 2, 3, 6, 12],
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

    # governed rows: (metric_id, geo_id, pt_id, source_id, category, frequency, coverage_ratio, n_overlap)
    specs: List[FeatureSpec] = []
    for metric_id, geo_id, pt_id, source_id, cat, freq, cov, n_overlap in governed:
        specs.append(
            FeatureSpec(
                name=f"{metric_id}__{geo_id}__{pt_id}__{source_id}",
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id,
                source_id=source_id,
                lags=tuple(lag_scheme),
            )
        )

    return specs
