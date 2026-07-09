from __future__ import annotations
# regime/dimension_scorer.py

import pandas as pd

from regime.config_loader import load_regime_config
from regime.metric_scorer import score_metrics


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _build_dimension_weights() -> pd.DataFrame:
    config = load_regime_config(validate=True)

    df = config.metric_dimensions.copy()

    df = df[
        _truthy(df["enabled"])
        & ~_truthy(df["diagnostic_only"])
        & _truthy(df["macro_enabled"])
    ].copy()

    df["metric_weight"] = pd.to_numeric(df["metric_weight"], errors="coerce")

    if df["metric_weight"].isna().any():
        bad = df[df["metric_weight"].isna()]
        raise ValueError(
            "Non-numeric metric_weight values:\n"
            + bad.to_string(index=False)
        )

    if (df["metric_weight"] < 0).any():
        bad = df[df["metric_weight"] < 0]
        raise ValueError(
            "Negative metric_weight values:\n"
            + bad.to_string(index=False)
        )

    # Metric scorer outputs canonical_metric_key, not physical metric_key.
    # Multiple physical source metrics may resolve to one canonical metric, so
    # collapse registry rows before joining or canonical metrics get double-counted.
    cols = ["canonical_metric_key", "dimension", "metric_weight"]

    conflicts = (
        df[cols]
        .drop_duplicates()
        .groupby(["canonical_metric_key", "dimension"])
        ["metric_weight"]
        .nunique()
        .reset_index(name="weight_count")
    )

    conflicts = conflicts[conflicts["weight_count"] > 1]
    if not conflicts.empty:
        raise ValueError(
            "Conflicting metric_weight values for canonical metric/dimension pairs:\n"
            + conflicts.to_string(index=False)
        )

    out = (
        df[cols]
        .drop_duplicates(subset=["canonical_metric_key", "dimension"])
        .copy()
    )

    return out


def score_dimensions(metrics: pd.DataFrame | None = None) -> pd.DataFrame:
    if metrics is None:
        metrics = score_metrics()

    dimension_weights = _build_dimension_weights()

    df = metrics.merge(
        dimension_weights,
        on="canonical_metric_key",
        how="left",
    )

    missing = (
        df[df["dimension"].isna() | (df["dimension"].astype(str).str.strip() == "")]
        [["canonical_metric_key"]]
        .drop_duplicates()
    )

    if not missing.empty:
        raise ValueError(
            "Metrics missing dimension mapping:\n"
            + missing.sort_values("canonical_metric_key").to_string(index=False)
        )

    df = df.dropna(subset=["metric_score"]).copy()

    grouped = []
    for keys, g in df.groupby(["geo_id", "date", "dimension"], dropna=False):
        total_weight = g["metric_weight"].sum()
        if total_weight <= 0:
            continue

        dimension_score = (g["metric_score"] * g["metric_weight"]).sum() / total_weight

        grouped.append(
            {
                "geo_id": keys[0],
                "date": keys[1],
                "dimension": keys[2],
                "dimension_score": dimension_score,
                "metric_count": len(g),
                "metric_weight_sum": total_weight,
                "min_metric_score": g["metric_score"].min(),
                "max_metric_score": g["metric_score"].max(),
            }
        )

    out = pd.DataFrame(grouped)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "dimension",
                "dimension_score",
                "metric_count",
                "metric_weight_sum",
                "min_metric_score",
                "max_metric_score",
            ]
        )

    out["dimension_score"] = out["dimension_score"].clip(-1.0, 1.0)

    return out.sort_values(["geo_id", "date", "dimension"]).reset_index(drop=True)
