from __future__ import annotations
# regime/axis_engine.py

import pandas as pd

from regime.config_loader import load_regime_config
from regime.dimension_scorer import score_dimensions


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _build_axis_weights() -> pd.DataFrame:
    config = load_regime_config(validate=True)

    axes = config.axes.copy()
    axes = axes[_truthy(axes["enabled"])].copy()

    axes["dimension_weight"] = pd.to_numeric(
        axes["dimension_weight"],
        errors="coerce",
    )

    if axes["dimension_weight"].isna().any():
        bad = axes[axes["dimension_weight"].isna()]
        raise ValueError(
            "Non-numeric dimension_weight values:\n"
            + bad.to_string(index=False)
        )

    if (axes["dimension_weight"] <= 0).any():
        bad = axes[axes["dimension_weight"] <= 0]
        raise ValueError(
            "Enabled axis rows must have positive dimension_weight:\n"
            + bad.to_string(index=False)
        )

    conflicts = (
        axes[["axis", "dimension", "dimension_weight"]]
        .drop_duplicates()
        .groupby(["axis", "dimension"])["dimension_weight"]
        .nunique()
        .reset_index(name="weight_count")
    )

    conflicts = conflicts[conflicts["weight_count"] > 1]
    if not conflicts.empty:
        raise ValueError(
            "Conflicting dimension_weight values for axis/dimension pairs:\n"
            + conflicts.to_string(index=False)
        )

    axes = axes[
        ["axis", "dimension", "dimension_weight"]
    ].drop_duplicates(
        subset=["axis", "dimension"],
        keep="first",
    )

    weight_sums = (
        axes.groupby("axis")["dimension_weight"]
        .sum()
        .reset_index(name="dimension_weight_sum")
    )

    bad_sums = weight_sums[
        (weight_sums["dimension_weight_sum"] - 1.0).abs() > 0.001
    ]

    if not bad_sums.empty:
        raise ValueError(
            "Active dimension weights must sum to 1.0 by axis:\n"
            + bad_sums.to_string(index=False)
        )

    return axes


def score_axes(dimensions: pd.DataFrame | None = None) -> pd.DataFrame:
    if dimensions is None:
        dimensions = score_dimensions()

    axis_weights = _build_axis_weights()

    df = dimensions.merge(
        axis_weights,
        on="dimension",
        how="inner",
    )

    if df.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "axis",
                "axis_score",
                "dimension_count",
                "dimension_weight_sum",
                "min_dimension_score",
                "max_dimension_score",
                "max_dimension_age_days",
            ]
        )

    df = df.dropna(subset=["dimension_score"]).copy()

    age_col = "max_metric_age_days"
    if age_col not in df.columns:
        df[age_col] = pd.NA

    grouped = []
    for keys, g in df.groupby(["geo_id", "date", "axis"], dropna=False):
        total_weight = g["dimension_weight"].sum()
        if total_weight <= 0:
            continue

        axis_score = (
            g["dimension_score"] * g["dimension_weight"]
        ).sum() / total_weight

        grouped.append(
            {
                "geo_id": keys[0],
                "date": keys[1],
                "axis": keys[2],
                "axis_score": axis_score,
                "dimension_count": len(g),
                "dimension_weight_sum": total_weight,
                "min_dimension_score": g["dimension_score"].min(),
                "max_dimension_score": g["dimension_score"].max(),
                "max_dimension_age_days": g[age_col].max(),
            }
        )

    out = pd.DataFrame(grouped)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "geo_id",
                "date",
                "axis",
                "axis_score",
                "dimension_count",
                "dimension_weight_sum",
                "min_dimension_score",
                "max_dimension_score",
                "max_dimension_age_days",
            ]
        )

    out["axis_score"] = out["axis_score"].clip(-1.0, 1.0)

    return out.sort_values(["geo_id", "date", "axis"]).reset_index(drop=True)
