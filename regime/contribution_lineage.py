"""Deterministic contribution arithmetic for persisted score drilldown."""

from __future__ import annotations

import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._05_dimension_scorer import _build_dimension_weights
from regime._06_axis_engine import _build_axis_weights


def build_contribution_lineage(
    normalized_features: pd.DataFrame,
    aligned_metric_scores: pd.DataFrame,
    dimension_scores: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """Expose scorer inputs and effective weights without recomputing scores."""
    config = load_regime_config(validate=True)

    feature_weights = config.features[["feature_key", "feature_weight"]].drop_duplicates()
    feature_weights["feature_weight"] = pd.to_numeric(feature_weights["feature_weight"])
    feature = normalized_features.merge(feature_weights, on="feature_key", how="left")
    available = feature["feature_score"].notna() & feature["feature_weight"].gt(0)
    totals = feature.loc[available].groupby(
        ["geo_id", "date", "canonical_metric_key"]
    )["feature_weight"].transform("sum")
    feature = feature.loc[available].copy()
    feature["effective_weight"] = feature["feature_weight"] / totals
    feature["weighted_contribution"] = feature["feature_score"] * feature["effective_weight"]
    feature["parent_identifier"] = feature["canonical_metric_key"]
    feature = feature.rename(columns={"feature_weight": "configured_weight", "feature_score": "score"})

    metric_weights = _build_dimension_weights()[
        ["canonical_metric_key", "dimension", "metric_weight"]
    ]
    metric = aligned_metric_scores.merge(metric_weights, on="canonical_metric_key", how="inner")
    metric = metric[metric["metric_score"].notna() & metric["metric_weight"].gt(0)].copy()
    totals = metric.groupby(["geo_id", "evaluation_date", "dimension"])["metric_weight"].transform("sum")
    metric["effective_weight"] = metric["metric_weight"] / totals
    metric["weighted_contribution"] = metric["metric_score"] * metric["effective_weight"]
    metric["parent_identifier"] = metric["dimension"]
    metric = metric.rename(columns={"metric_weight": "configured_weight", "metric_score": "score"})

    axis_weights = _build_axis_weights()
    dimension = dimension_scores.merge(axis_weights, on="dimension", how="inner")
    dimension = dimension[dimension["dimension_score"].notna()].copy()
    totals = dimension.groupby(["geo_id", "date", "axis"])["dimension_weight"].transform("sum")
    dimension["effective_weight"] = dimension["dimension_weight"] / totals
    dimension["weighted_contribution"] = dimension["dimension_score"] * dimension["effective_weight"]
    dimension["parent_identifier"] = dimension["axis"]
    dimension = dimension.rename(columns={"dimension_weight": "configured_weight", "dimension_score": "score"})

    return {
        "feature_contributions": feature.reset_index(drop=True),
        "metric_contributions": metric.reset_index(drop=True),
        "dimension_contributions": dimension.reset_index(drop=True),
    }
