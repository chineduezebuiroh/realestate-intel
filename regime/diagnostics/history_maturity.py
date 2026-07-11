from __future__ import annotations
# regime/diagnostics/history_maturity.py

from pathlib import Path
from typing import Any

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime.config_loader import load_regime_config


DEFAULT_VALIDATION_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

NATIONAL_GEO_ID = "united_states__nation"


def _truthy(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "yes", "y"})
    )


def _numeric(
    series: pd.Series,
    *,
    column_name: str,
) -> pd.Series:
    parsed = pd.to_numeric(series, errors="coerce")

    if parsed.isna().any():
        bad = series[parsed.isna()].drop_duplicates().tolist()
        raise ValueError(
            f"Non-numeric values found in {column_name}: {bad}"
        )

    return parsed


def _build_feature_metadata() -> pd.DataFrame:
    """
    Map each feature to its source metadata, canonical metric, dimension,
    and production axis.

    Capital-markets features appear once for each axis they contribute to.
    """
    config = load_regime_config(validate=True)

    source = config.source_metrics[
        [
            "metric_key",
            "source_id",
            "frequency",
            "seasonality",
        ]
    ].drop_duplicates()

    feature = config.features[
        [
            "feature_key",
            "metric_key",
            "feature_type",
            "transform",
            "feature_weight",
            "feature_window",
        ]
    ].drop_duplicates()

    feature["feature_weight"] = pd.to_numeric(
        feature["feature_weight"],
        errors="coerce",
    )

    dimensions = config.metric_dimensions.copy()
    dimensions = dimensions[
        _truthy(dimensions["enabled"])
        & ~_truthy(dimensions["diagnostic_only"])
        & _truthy(dimensions["macro_enabled"])
    ].copy()

    dimensions = dimensions[
        [
            "metric_key",
            "canonical_metric_key",
            "dimension",
            "metric_weight",
        ]
    ].drop_duplicates()

    dimensions["metric_weight"] = pd.to_numeric(
        dimensions["metric_weight"],
        errors="coerce",
    )

    axes = config.axes.copy()
    axes = axes[_truthy(axes["enabled"])].copy()

    axes = axes[
        [
            "axis",
            "dimension",
            "dimension_weight",
        ]
    ].drop_duplicates()

    axes["dimension_weight"] = pd.to_numeric(
        axes["dimension_weight"],
        errors="coerce",
    )

    metadata = (
        feature
        .merge(source, on="metric_key", how="left")
        .merge(dimensions, on="metric_key", how="left")
        .merge(axes, on="dimension", how="left")
    )

    metadata["axis_metric_feature_weight"] = (
        metadata["dimension_weight"]
        * metadata["metric_weight"]
        * metadata["feature_weight"]
    )

    return metadata


def _build_policy_table(
    normalized_features: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "feature_key",
        "source_family",
        "normalization_method",
        "lookback_periods",
        "min_periods",
        "score_direction",
    }

    missing = required - set(normalized_features.columns)
    if missing:
        raise ValueError(
            "normalized_features artifact is missing policy columns: "
            f"{sorted(missing)}"
        )

    policy = normalized_features[
        [
            "feature_key",
            "source_family",
            "normalization_method",
            "lookback_periods",
            "min_periods",
            "score_direction",
        ]
    ].drop_duplicates()

    conflict_counts = (
        policy.groupby("feature_key")
        .agg(
            source_families=("source_family", "nunique"),
            methods=("normalization_method", "nunique"),
            lookbacks=("lookback_periods", "nunique"),
            minimums=("min_periods", "nunique"),
            directions=("score_direction", "nunique"),
        )
        .reset_index()
    )

    conflicts = conflict_counts[
        (conflict_counts["source_families"] > 1)
        | (conflict_counts["methods"] > 1)
        | (conflict_counts["lookbacks"] > 1)
        | (conflict_counts["minimums"] > 1)
        | (conflict_counts["directions"] > 1)
    ]

    if not conflicts.empty:
        raise ValueError(
            "Conflicting normalization policies found for feature keys:\n"
            + conflicts.to_string(index=False)
        )

    policy = policy.drop_duplicates(
        subset=["feature_key"],
        keep="first",
    ).copy()

    policy["lookback_periods"] = _numeric(
        policy["lookback_periods"],
        column_name="lookback_periods",
    ).astype(int)

    policy["min_periods"] = _numeric(
        policy["min_periods"],
        column_name="min_periods",
    ).astype(int)

    return policy


def _maturity_status(row: pd.Series) -> str:
    observation_count = int(row["observation_count"])
    min_periods = int(row["min_periods"])
    lookback_periods = int(row["lookback_periods"])

    if observation_count < min_periods:
        return "pre_minimum"

    if observation_count < lookback_periods:
        return "minimum_met"

    return "full_window"


def build_history_maturity_audit(
    run_id: str = "macro_regime_v1",
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: list[str] | None = None,
    include_national: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    Measure feature-history maturity using persisted production artifacts.

    Maturity is measured in valid feature observations:

      pre_minimum:
          observation_count < normalization min_periods

      minimum_met:
          min_periods <= observation_count < lookback_periods

      full_window:
          observation_count >= lookback_periods

    Returns:
      feature_history
      feature_summary
      year_end_feature_snapshot
      annual_geo_summary
      annual_axis_summary
      annual_metric_summary
      latest_feature_summary
    """
    if geo_ids is None:
        geo_ids = DEFAULT_VALIDATION_GEOS.copy()

    selected_geos = list(dict.fromkeys(geo_ids))

    if include_national and NATIONAL_GEO_ID not in selected_geos:
        selected_geos.append(NATIONAL_GEO_ID)

    store = RegimeArtifactStore(artifact_root)

    manifest = store.read_manifest(run_id)
    if manifest.get("status") != "complete":
        raise ValueError(
            f"Run {run_id!r} is not complete: "
            f"{manifest.get('status')!r}"
        )

    features = store.read_dataframe(
        run_id,
        "features",
        columns=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
        ],
    )

    normalized = store.read_dataframe(
        run_id,
        "normalized_features",
    )

    features["date"] = pd.to_datetime(features["date"])
    normalized["date"] = pd.to_datetime(normalized["date"])

    features = features[
        features["geo_id"].isin(selected_geos)
    ].copy()

    normalized = normalized[
        normalized["geo_id"].isin(selected_geos)
    ].copy()

    if features.empty:
        raise ValueError(
            f"No persisted feature rows found for selected geographies: "
            f"{selected_geos}"
        )

    policy = _build_policy_table(
        store.read_dataframe(
            run_id,
            "normalized_features",
            columns=[
                "feature_key",
                "source_family",
                "normalization_method",
                "lookback_periods",
                "min_periods",
                "score_direction",
            ],
        )
    )

    metadata = _build_feature_metadata()

    history = features.sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "feature_key",
            "date",
        ]
    ).copy()

    group_cols = [
        "geo_id",
        "canonical_metric_key",
        "feature_key",
    ]

    grouped = history.groupby(
        group_cols,
        group_keys=False,
    )

    history["observation_count"] = grouped.cumcount() + 1
    history["first_feature_date"] = grouped["date"].transform("min")
    history["latest_feature_date"] = grouped["date"].transform("max")

    history = history.merge(
        policy,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    missing_policy = history[
        history["min_periods"].isna()
        | history["lookback_periods"].isna()
    ]

    if not missing_policy.empty:
        missing_keys = sorted(
            missing_policy["feature_key"].dropna().unique()
        )
        raise ValueError(
            "Missing normalization policy for feature keys: "
            f"{missing_keys}"
        )

    history["min_periods"] = history["min_periods"].astype(int)
    history["lookback_periods"] = (
        history["lookback_periods"].astype(int)
    )

    score_keys = normalized[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ]
    ].drop_duplicates()

    score_keys["score_available"] = True

    history = history.merge(
        score_keys,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        how="left",
    )

    history["score_available"] = (
        history["score_available"]
        .fillna(False)
        .astype(bool)
    )

    first_score = (
        normalized.groupby(group_cols)["date"]
        .min()
        .rename("first_score_date")
        .reset_index()
    )

    history = history.merge(
        first_score,
        on=group_cols,
        how="left",
    )

    history["minimum_ratio"] = (
        history["observation_count"]
        / history["min_periods"].replace(0, pd.NA)
    ).clip(upper=1.0)

    history["lookback_ratio"] = (
        history["observation_count"]
        / history["lookback_periods"].replace(0, pd.NA)
    ).clip(upper=1.0)

    history["minimum_met"] = (
        history["observation_count"]
        >= history["min_periods"]
    )

    history["full_window"] = (
        history["observation_count"]
        >= history["lookback_periods"]
    )

    history["maturity_status"] = history.apply(
        _maturity_status,
        axis=1,
    )

    history = history.merge(
        metadata,
        on=[
            "canonical_metric_key",
            "feature_key",
        ],
        how="left",
    )

    feature_summary = (
        history.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
                "canonical_metric_key",
                "feature_key",
                "source_id",
                "source_family",
                "frequency",
                "normalization_method",
                "min_periods",
                "lookback_periods",
            ],
            dropna=False,
        )
        .agg(
            feature_rows=("date", "size"),
            first_feature_date=("date", "min"),
            first_score_date=("first_score_date", "min"),
            latest_feature_date=("date", "max"),
            latest_observation_count=("observation_count", "max"),
            latest_minimum_ratio=("minimum_ratio", "max"),
            latest_lookback_ratio=("lookback_ratio", "max"),
            score_rows=("score_available", "sum"),
        )
        .reset_index()
    )

    feature_summary["latest_minimum_met"] = (
        feature_summary["latest_observation_count"]
        >= feature_summary["min_periods"]
    )

    feature_summary["latest_full_window"] = (
        feature_summary["latest_observation_count"]
        >= feature_summary["lookback_periods"]
    )

    history["year"] = history["date"].dt.year

    year_end_snapshot = (
        history.sort_values(
            [
                "geo_id",
                "axis",
                "canonical_metric_key",
                "feature_key",
                "date",
            ]
        )
        .groupby(
            [
                "geo_id",
                "axis",
                "canonical_metric_key",
                "feature_key",
                "year",
            ],
            dropna=False,
            as_index=False,
        )
        .tail(1)
        .reset_index(drop=True)
    )

    annual_geo_summary = (
        year_end_snapshot.groupby(
            ["geo_id", "year"],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "nunique"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
        )
        .reset_index()
    )

    annual_geo_summary["scored_feature_share"] = (
        annual_geo_summary["scored_feature_count"]
        / annual_geo_summary["feature_count"]
    )

    annual_geo_summary["minimum_met_share"] = (
        annual_geo_summary["minimum_met_count"]
        / annual_geo_summary["feature_count"]
    )

    annual_geo_summary["full_window_share"] = (
        annual_geo_summary["full_window_count"]
        / annual_geo_summary["feature_count"]
    )

    annual_axis_summary = (
        year_end_snapshot.dropna(subset=["axis"])
        .groupby(
            ["geo_id", "year", "axis"],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "nunique"),
            metric_count=("canonical_metric_key", "nunique"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
            weighted_avg_lookback_ratio=(
                "lookback_ratio",
                lambda s: _weighted_mean(
                    s,
                    year_end_snapshot.loc[
                        s.index,
                        "axis_metric_feature_weight",
                    ],
                ),
            ),
        )
        .reset_index()
    )

    annual_axis_summary["scored_feature_share"] = (
        annual_axis_summary["scored_feature_count"]
        / annual_axis_summary["feature_count"]
    )

    annual_axis_summary["minimum_met_share"] = (
        annual_axis_summary["minimum_met_count"]
        / annual_axis_summary["feature_count"]
    )

    annual_axis_summary["full_window_share"] = (
        annual_axis_summary["full_window_count"]
        / annual_axis_summary["feature_count"]
    )

    annual_metric_summary = (
        year_end_snapshot.groupby(
            [
                "geo_id",
                "year",
                "axis",
                "dimension",
                "canonical_metric_key",
            ],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "nunique"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
            min_observation_count=("observation_count", "min"),
            max_observation_count=("observation_count", "max"),
        )
        .reset_index()
    )

    annual_metric_summary["scored_feature_share"] = (
        annual_metric_summary["scored_feature_count"]
        / annual_metric_summary["feature_count"]
    )

    annual_metric_summary["minimum_met_share"] = (
        annual_metric_summary["minimum_met_count"]
        / annual_metric_summary["feature_count"]
    )

    annual_metric_summary["full_window_share"] = (
        annual_metric_summary["full_window_count"]
        / annual_metric_summary["feature_count"]
    )

    latest_feature_summary = (
        history.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "feature_key",
                "date",
            ]
        )
        .groupby(
            group_cols,
            as_index=False,
        )
        .tail(1)
        .sort_values(
            [
                "geo_id",
                "axis",
                "dimension",
                "canonical_metric_key",
                "feature_key",
            ]
        )
        .reset_index(drop=True)
    )

    return {
        "feature_history": history.reset_index(drop=True),
        "feature_summary": feature_summary,
        "year_end_feature_snapshot": year_end_snapshot,
        "annual_geo_summary": annual_geo_summary,
        "annual_axis_summary": annual_axis_summary,
        "annual_metric_summary": annual_metric_summary,
        "latest_feature_summary": latest_feature_summary,
    }


def _weighted_mean(
    values: pd.Series,
    weights: pd.Series,
) -> float:
    valid = values.notna() & weights.notna()

    if not valid.any():
        return float("nan")

    valid_values = values[valid].astype(float)
    valid_weights = weights[valid].astype(float)

    weight_sum = valid_weights.sum()

    if weight_sum <= 0:
        return float(valid_values.mean())

    return float(
        (valid_values * valid_weights).sum()
        / weight_sum
    )
