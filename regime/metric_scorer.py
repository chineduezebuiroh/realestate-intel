from __future__ import annotations
# regime/metric_scorer.py

import pandas as pd

from regime.config_loader import load_regime_config
from regime.feature_normalizer import normalize_features


def score_metrics(scores: pd.DataFrame | None = None) -> pd.DataFrame:
    if scores is None:
        scores = normalize_features()

    config = load_regime_config(validate=True)

    feature_weights = (
        config.features[["feature_key", "feature_weight"]]
        .drop_duplicates(subset=["feature_key"])
        .copy()
    )
    feature_weights["feature_weight"] = pd.to_numeric(
        feature_weights["feature_weight"],
        errors="coerce",
    )

    df = scores.merge(feature_weights, on="feature_key", how="left")

    missing = df[df["feature_weight"].isna()][["feature_key"]].drop_duplicates()
    if not missing.empty:
        raise ValueError(
            "Features missing feature_weight:\n"
            + missing.sort_values("feature_key").to_string(index=False)
        )

    df = df.dropna(subset=["feature_score"]).copy()

    grouped = []
    for keys, g in df.groupby(["geo_id", "date", "canonical_metric_key"], dropna=False):
        total_weight = g["feature_weight"].sum()
        if total_weight <= 0:
            continue

        metric_score = (g["feature_score"] * g["feature_weight"]).sum() / total_weight

        grouped.append(
            {
                "geo_id": keys[0],
                "date": keys[1],
                "canonical_metric_key": keys[2],
                "metric_score": metric_score,
                "feature_count": len(g),
                "feature_weight_sum": total_weight,
                "min_feature_score": g["feature_score"].min(),
                "max_feature_score": g["feature_score"].max(),
            }
        )

    out = pd.DataFrame(grouped)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "canonical_metric_key",
                "metric_score",
                "feature_count",
                "feature_weight_sum",
                "min_feature_score",
                "max_feature_score",
            ]
        )

    out["metric_score"] = out["metric_score"].clip(-1.0, 1.0)

    return out.sort_values(["geo_id", "date", "canonical_metric_key"]).reset_index(drop=True)
