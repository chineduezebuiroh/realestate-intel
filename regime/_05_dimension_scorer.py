from __future__ import annotations
# regime/_05_dimension_scorer.py

import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._04_asof_aligner import align_metric_scores_asof


_DEMAND_BLOCK_WEIGHTS = {"structural": 0.25, "cyclical": 0.75}


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})


def _build_dimension_weights() -> pd.DataFrame:
    """Load and validate active metric and Demand hierarchy metadata once."""
    config = load_regime_config(validate=True)
    df = config.metric_dimensions.copy()

    df = df[
        _truthy(df["enabled"])
        & ~_truthy(df["diagnostic_only"])
        & _truthy(df["macro_enabled"])
    ].copy()

    df["metric_weight"] = pd.to_numeric(df["metric_weight"], errors="coerce")
    if df["metric_weight"].isna().any():
        raise ValueError(
            "Non-numeric metric_weight values:\n"
            + df[df["metric_weight"].isna()].to_string(index=False)
        )
    if (df["metric_weight"] < 0).any():
        raise ValueError(
            "Negative metric_weight values:\n"
            + df[df["metric_weight"] < 0].to_string(index=False)
        )

    metadata_columns = [
        "canonical_metric_key", "dimension", "metric_weight",
        "demand_block", "block_weight",
    ]
    unique_metadata = df[metadata_columns].drop_duplicates()
    conflicts = (
        unique_metadata.groupby(["canonical_metric_key", "dimension"], dropna=False)
        .size().reset_index(name="metadata_count")
    )
    conflicts = conflicts[conflicts["metadata_count"] > 1]
    if not conflicts.empty:
        raise ValueError(
            "Conflicting active metric governance for canonical metric/dimension pairs:\n"
            + conflicts.to_string(index=False)
        )

    non_demand = df["dimension"].ne("demand")
    has_demand_metadata = (
        df["demand_block"].astype(str).str.strip().ne("")
        | df["block_weight"].astype(str).str.strip().ne("")
    )
    if (non_demand & has_demand_metadata).any():
        raise ValueError(
            "Non-Demand metrics must not define Demand block metadata:\n"
            + df.loc[non_demand & has_demand_metadata, metadata_columns].to_string(index=False)
        )

    demand = df[df["dimension"].eq("demand")].copy()
    if demand.empty:
        raise ValueError("Active production Demand metrics are required")
    if demand["demand_block"].astype(str).str.strip().eq("").any():
        missing = sorted(demand.loc[
            demand["demand_block"].astype(str).str.strip().eq(""),
            "canonical_metric_key",
        ].unique())
        raise ValueError(f"Active Demand metrics missing block membership: {missing}")
    demand["block_weight"] = pd.to_numeric(demand["block_weight"], errors="coerce")
    if demand["block_weight"].isna().any():
        raise ValueError("Active Demand metrics require numeric block_weight values")

    canonical_demand = demand[metadata_columns].drop_duplicates(
        subset=["canonical_metric_key", "dimension"]
    )
    if canonical_demand["canonical_metric_key"].duplicated().any():
        raise ValueError("Active Demand metrics must have exactly one block membership")
    taxonomy = set(canonical_demand["demand_block"])
    if taxonomy != set(_DEMAND_BLOCK_WEIGHTS):
        raise ValueError("Demand block taxonomy must be exactly structural and cyclical")
    for block, required_weight in _DEMAND_BLOCK_WEIGHTS.items():
        weights = canonical_demand.loc[
            canonical_demand["demand_block"].eq(block), "block_weight"
        ].unique()
        if len(weights) != 1 or abs(float(weights[0]) - required_weight) > 1e-12:
            raise ValueError(f"Demand {block} block_weight must be {required_weight}")
    if abs(sum(_DEMAND_BLOCK_WEIGHTS.values()) - 1.0) > 1e-12:
        raise ValueError("Demand block weights must sum to 1.0")

    return unique_metadata.drop_duplicates(
        subset=["canonical_metric_key", "dimension"]
    ).copy()


def score_dimensions(metrics: pd.DataFrame | None = None) -> pd.DataFrame:
    if metrics is None:
        metrics = align_metric_scores_asof()

    # Registry loading and hierarchy validation happen once, before grouping.
    dimension_weights = _build_dimension_weights()
    df = metrics.merge(dimension_weights, on="canonical_metric_key", how="left")

    missing = df[
        df["dimension"].isna() | df["dimension"].astype(str).str.strip().eq("")
    ][["canonical_metric_key"]].drop_duplicates()
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
            block_scores = []
            available_block_weight = 0.0
            for _, members in g.groupby("demand_block"):
                member_weight = members["metric_weight"].sum()
                if member_weight > 0:
                    block_weight = float(members["block_weight"].iloc[0])
                    block_score = (
                        (members["metric_score"] * members["metric_weight"]).sum()
                        / member_weight
                    )
                    block_scores.append(block_score * block_weight)
                    available_block_weight += block_weight
            if available_block_weight <= 0:
                continue
            dimension_score = sum(block_scores) / available_block_weight
        else:
            dimension_score = (g["metric_score"] * g["metric_weight"]).sum() / total_weight

        grouped.append({
            "geo_id": keys[0], "date": keys[1], "dimension": keys[2],
            "dimension_score": dimension_score, "metric_count": len(g),
            "metric_weight_sum": total_weight,
            "min_metric_score": g["metric_score"].min(),
            "max_metric_score": g["metric_score"].max(),
            "max_metric_age_days": g["metric_age_days"].max(),
        })

    out = pd.DataFrame(grouped)
    if out.empty:
        return pd.DataFrame(columns=[
            "geo_id", "date", "dimension", "dimension_score", "metric_count",
            "metric_weight_sum", "min_metric_score", "max_metric_score",
            "max_metric_age_days",
        ])
    out["dimension_score"] = out["dimension_score"].clip(-1.0, 1.0)
    return out.sort_values(["geo_id", "date", "dimension"]).reset_index(drop=True)
