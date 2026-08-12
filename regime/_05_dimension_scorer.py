from __future__ import annotations
# regime/_05_dimension_scorer.py

from pathlib import Path

import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._04_asof_aligner import align_metric_scores_asof


DEMAND_BLOCK_REGISTRY = Path("config/demand_block_registry.csv")


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
        metrics = align_metric_scores_asof()

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
    for keys, g in df.groupby(["geo_id", "evaluation_date", "dimension"], dropna=False):
        total_weight = g["metric_weight"].sum()
        if total_weight <= 0:
            continue

        if keys[2] == "demand":
            blocks = pd.read_csv(DEMAND_BLOCK_REGISTRY, dtype=str).fillna("")
            blocks = blocks[_truthy(blocks["enabled"])].copy()
            blocks["block_weight"] = pd.to_numeric(blocks["block_weight"], errors="raise")
            if blocks["canonical_metric_key"].duplicated().any():
                raise ValueError("Duplicate Demand block membership")
            block_weights = blocks.groupby("demand_block")["block_weight"].nunique()
            if (block_weights != 1).any() or not set(block_weights.index) == {"structural", "cyclical"}:
                raise ValueError("Demand blocks must define one Structural and Cyclical weight")
            block_weights = blocks.groupby("demand_block")["block_weight"].first()
            if abs(float(block_weights.sum()) - 1.0) > 1e-12:
                raise ValueError("Demand block weights must sum to 1.0")
            demand = g.merge(blocks, on="canonical_metric_key", how="left", validate="many_to_one")
            if demand["demand_block"].eq("").any() or demand["demand_block"].isna().any():
                missing = sorted(demand.loc[demand["demand_block"].isna() | demand["demand_block"].eq(""), "canonical_metric_key"].unique())
                raise ValueError(f"Active Demand metrics missing block membership: {missing}")
            block_scores = []
            for block, members in demand.groupby("demand_block"):
                member_weight = members["metric_weight"].sum()
                if member_weight > 0:
                    block_scores.append((members["metric_score"] * members["metric_weight"]).sum() / member_weight * block_weights[block])
            available = block_weights.loc[demand["demand_block"].unique()].sum()
            dimension_score = sum(block_scores) / available
        else:
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
                "max_metric_age_days": g["metric_age_days"].max(),
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
                "max_metric_age_days",
            ]
        )

    out["dimension_score"] = out["dimension_score"].clip(-1.0, 1.0)

    return out.sort_values(["geo_id", "date", "dimension"]).reset_index(drop=True)
