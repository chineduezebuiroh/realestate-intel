from __future__ import annotations
# regime/asof_aligner.py

import pandas as pd

from regime.metric_scorer import score_metrics


MACRO_GEO_SUFFIXES = ("__county", "__cbsa_metro")

NATIONAL_GEO_ID = "united_states__nation"

CAPITAL_MARKET_METRICS = {
    "fedfunds",
    "mortgage_15y",
    "mortgage_30y",
    "spread_10y_fedfunds",
    "spread_2y10y",
    "treasury_10y",
}


def _is_macro_geo_id(series: pd.Series) -> pd.Series:
    value = series.astype(str)
    mask = False
    for suffix in MACRO_GEO_SUFFIXES:
        mask = mask | value.str.endswith(suffix)
    return mask


def _build_evaluation_calendar(metrics: pd.DataFrame) -> pd.DataFrame:
    """
    Build monthly evaluation dates from observed macro metric dates.

    V1 uses month-end dates only. This avoids mixing month-start permit dates,
    annual ACS dates, quarterly BEA dates, and Redfin month-end dates as separate
    dimension-scoring dates.
    """
    dates = pd.to_datetime(metrics["date"]).dropna()

    month_end = (
        dates
        + pd.offsets.MonthEnd(0)
    ).drop_duplicates().sort_values()

    return pd.DataFrame({"evaluation_date": month_end})


def align_metric_scores_asof(metrics: pd.DataFrame | None = None) -> pd.DataFrame:
    """
    Align metric scores onto a monthly evaluation calendar.

    Input:
      score_metrics() output at each metric's native observation date.

    Output:
      one row per geo_id / evaluation_date / canonical_metric_key where the
      metric has a score available as of that evaluation date.

    The original metric observation date is preserved as metric_date.
    """
    if metrics is None:
        metrics = score_metrics()

    metrics = metrics.copy()
    metrics["date"] = pd.to_datetime(metrics["date"])

    macro_metrics = metrics[_is_macro_geo_id(metrics["geo_id"])].copy()
    
    capital_metrics = metrics[
        (metrics["geo_id"] == NATIONAL_GEO_ID)
        & (metrics["canonical_metric_key"].isin(CAPITAL_MARKET_METRICS))
    ].copy()
    
    if not capital_metrics.empty:
        macro_geos = macro_metrics[["geo_id"]].drop_duplicates()
    
        broadcast_parts = []
        for geo_id in macro_geos["geo_id"]:
            tmp = capital_metrics.copy()
            tmp["geo_id"] = geo_id
            broadcast_parts.append(tmp)
    
        broadcast = pd.concat(broadcast_parts, ignore_index=True)
        metrics = pd.concat([macro_metrics, broadcast], ignore_index=True)
    else:
        metrics = macro_metrics

    if metrics.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "evaluation_date",
                "metric_date",
                "canonical_metric_key",
                "metric_score",
                "feature_count",
                "feature_weight_sum",
                "min_feature_score",
                "max_feature_score",
                "metric_age_days",
            ]
        )

    calendar = _build_evaluation_calendar(metrics)

    pairs = (
        metrics[["geo_id", "canonical_metric_key"]]
        .drop_duplicates()
        .copy()
    )

    grid = pairs.merge(calendar, how="cross")

    source = metrics.rename(columns={"date": "metric_date"}).copy()
    
    aligned_parts = []
    
    for (geo_id, metric_key), source_g in source.groupby(
        ["geo_id", "canonical_metric_key"],
        dropna=False,
    ):
        source_g = source_g.sort_values("metric_date").copy()
    
        grid_g = calendar.copy()
        grid_g["geo_id"] = geo_id
        grid_g["canonical_metric_key"] = metric_key
        grid_g = grid_g.sort_values("evaluation_date")

        source_g = source_g.drop(columns=["geo_id", "canonical_metric_key"])
        
        aligned_g = pd.merge_asof(
            grid_g,
            source_g,
            left_on="evaluation_date",
            right_on="metric_date",
            direction="backward",
            allow_exact_matches=True,
        )
    
        aligned_parts.append(aligned_g)
    
    if aligned_parts:
        out = pd.concat(aligned_parts, ignore_index=True)
    else:
        out = pd.DataFrame()
    
    out = out.dropna(subset=["metric_score"]).copy()

    out["metric_age_days"] = (
        out["evaluation_date"] - out["metric_date"]
    ).dt.days

    return out[
        [
            "geo_id",
            "evaluation_date",
            "metric_date",
            "canonical_metric_key",
            "metric_score",
            "feature_count",
            "feature_weight_sum",
            "min_feature_score",
            "max_feature_score",
            "metric_age_days",
        ]
    ].sort_values(
        ["geo_id", "evaluation_date", "canonical_metric_key"]
    ).reset_index(drop=True)
