# forecast/feature_loader.py

import os
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple

import duckdb
import pandas as pd


# -----------------------------------------
# Shared types
# -----------------------------------------

@dataclass
class TargetSpec:
    metric_id: str
    geo_id: str
    # For Redfin, this is '-1', '6', '13', etc. For non-Redfin, use None -> 'all'.
    property_type_id: Optional[str] = None


@dataclass
class FeatureSpec:
    """
    Defines one base feature series and which lags to create.

    Example:
      FeatureSpec(
        name="median_dom",
        metric_id="median_dom",
        geo_id="dc_city",
        property_type_id="-1",
        lags=[1, 2, 3]
      )
    """
    name: str
    metric_id: str
    geo_id: str
    property_type_id: Optional[str]
    lags: List[int]


def get_connection():
    db_path = os.getenv("DUCKDB_PATH", "./data/market.duckdb")
    return duckdb.connect(db_path)


# -----------------------------------------
# Load single series from fact_timeseries
# -----------------------------------------

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
    df = con.execute(sql, [metric_id, geo_id, pt_id]).fetchdf()

    if df.empty:
        raise ValueError(
            f"No data for metric={metric_id}, geo={geo_id}, pt={pt_id}"
        )

    s = df.set_index("date")["value"].astype(float)
    return s


# -----------------------------------------
# Design matrix builder
# -----------------------------------------

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
      X: dataframe of lagged features, no NaNs
      base_series: dict name -> base (unlagged) series used to build X

    Notes:
      - We align all series on the intersection of dates.
      - We drop initial rows that don't have all requested lags.
    """
    # 1) Load target series
    y_raw = load_series_from_fact(
        metric_id=target.metric_id,
        geo_id=target.geo_id,
        property_type_id=target.property_type_id,
    )
    y_raw.name = "y"

    # 2) Load feature series
    base_series: Dict[str, pd.Series] = {"y": y_raw}  # include target as base for self-lags
    for spec in feature_specs:
        s = load_series_from_fact(
            metric_id=spec.metric_id,
            geo_id=spec.geo_id,
            property_type_id=spec.property_type_id,
        )
        base_series[spec.name] = s

    # 3) Align all base series on common index (inner join)
    df_base = pd.concat(base_series.values(), axis=1, join="inner")
    df_base.columns = list(base_series.keys())  # ensure names

    # 4) Build lagged features according to specs
    feature_cols = {}

    for spec in feature_specs:
        col_name = spec.name
        for lag in spec.lags:
            lag_col = f"{col_name}_lag{lag}"
            feature_cols[lag_col] = df_base[col_name].shift(lag)

    # Optional: you can also include lagged y here if you want AR terms
    # For now we'll not add them by default; they can be specified as a FeatureSpec
    # with metric_id=target.metric_id, geo_id=target.geo_id, property_type_id=target.property_type_id.

    df_features = pd.DataFrame(feature_cols, index=df_base.index)

    # 5) Combine y and X, drop rows with any NaNs (lag burn-in)
    df_all = pd.concat([df_base["y"], df_features], axis=1)
    df_all = df_all.dropna()

    if len(df_all) < min_obs:
        raise ValueError(
            f"Not enough observations after lagging/alignment: {len(df_all)} < {min_obs}"
        )

    y = df_all["y"].copy()
    X = df_all.drop(columns=["y"]).copy()

    # Update base_series to truncated index (for forecasting alignment convenience)
    for k in base_series:
        base_series[k] = base_series[k].reindex(df_all.index)

    return y, X, base_series


def discover_all_series(exclude_metrics=None, exclude_targets=None):
    """
    Return ALL (metric_id, geo_id, property_type_id) triplets in fact_timeseries,
    minus excluded ones.
    """
    con = get_connection()
    exclude_metrics = exclude_metrics or []
    exclude_targets = exclude_targets or []

    rows = con.execute("""
        SELECT DISTINCT metric_id, geo_id, property_type_id
        FROM fact_timeseries
        ORDER BY metric_id, geo_id, property_type_id
    """).fetchall()

    # Remove any series matching exclusion
    filtered = []
    for m, g, pt in rows:
        if m in exclude_metrics:
            continue
        if (m, g, pt) in exclude_targets:
            continue
        filtered.append((m, g, pt))

    return filtered


def build_universal_feature_specs(
    target: TargetSpec,
    lag_scheme=[1, 2, 3, 6, 12],
) -> List[FeatureSpec]:

    exclude = {(target.metric_id, target.geo_id, target.property_type_id)}
    all_series = discover_all_series(
        exclude_metrics=[target.metric_id],
        exclude_targets=exclude,
    )

    specs = []
    for (metric_id, geo_id, pt_id) in all_series:
        specs.append(
            FeatureSpec(
                name=f"{metric_id}__{geo_id}__{pt_id}",
                metric_id=metric_id,
                geo_id=geo_id,
                property_type_id=pt_id,
                lags=list(lag_scheme),
            )
        )
    return specs

