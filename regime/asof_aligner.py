from __future__ import annotations
# regime/asof_aligner.py

import pandas as pd

from regime.metric_scorer import score_metrics


MACRO_GEO_SUFFIXES = ("__county", "__cbsa_metro")


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

    metrics = metrics[_is_macro_geo_id(metrics["geo_id"])].copy()

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

    source = metrics.rename(columns={"date": "metric_date"}).sort_values(
        ["geo_id", "canonical_metric_key", "metric_date"]
    )

    grid = grid.sort_values(
        ["geo_id", "canonical_metric_key", "evaluation_date"]
    )

    out = pd.merge_asof(
        grid,
        source,
        by=["geo_id", "canonical_metric_key"],
        left_on="evaluation_date",
        right_on="metric_date",
        direction="backward",
        allow_exact_matches=True,
    )

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
