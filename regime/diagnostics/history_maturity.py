from __future__ import annotations
# regime/diagnostics/history_maturity.py

from pathlib import Path
from typing import Any

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime._00_config_loader import load_regime_config


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
    

def _safe_share(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    denominator = denominator.replace(0, pd.NA)

    result = (
        pd.to_numeric(numerator, errors="coerce")
        / pd.to_numeric(denominator, errors="coerce")
    )

    return result.astype(float)


def _assert_valid_summary_counts(
    df: pd.DataFrame,
    *,
    label: str,
) -> None:
    count_pairs = [
        ("scored_feature_count", "feature_count"),
        ("minimum_met_count", "feature_count"),
        ("full_window_count", "feature_count"),
    ]

    for numerator, denominator in count_pairs:
        if numerator not in df.columns or denominator not in df.columns:
            continue

        invalid = df[
            pd.to_numeric(df[numerator], errors="coerce")
            > pd.to_numeric(df[denominator], errors="coerce")
        ]

        if not invalid.empty:
            raise AssertionError(
                f"{label}: {numerator} exceeds {denominator}:\n"
                + invalid.head(20).to_string(index=False)
            )

    if {
        "full_window_count",
        "minimum_met_count",
    }.issubset(df.columns):
        invalid = df[
            df["full_window_count"] > df["minimum_met_count"]
        ]

        if not invalid.empty:
            raise AssertionError(
                f"{label}: full_window_count exceeds "
                "minimum_met_count:\n"
                + invalid.head(20).to_string(index=False)
            )

    share_columns = [
        column
        for column in df.columns
        if column.endswith("_share")
    ]

    for column in share_columns:
        values = pd.to_numeric(df[column], errors="coerce")
        invalid = df[
            values.notna()
            & ((values < -1e-12) | (values > 1.0 + 1e-12))
        ]

        if not invalid.empty:
            raise AssertionError(
                f"{label}: {column} lies outside [0, 1]:\n"
                + invalid.head(20).to_string(index=False)
            )


def _add_feature_shares(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["scored_feature_share"] = _safe_share(
        out["scored_feature_count"],
        out["feature_count"],
    )

    out["minimum_met_share"] = _safe_share(
        out["minimum_met_count"],
        out["feature_count"],
    )

    out["full_window_share"] = _safe_share(
        out["full_window_count"],
        out["feature_count"],
    )

    return out


def _build_axis_metric_metadata(
    feature_metadata: pd.DataFrame,
) -> pd.DataFrame:
    """
    Produce exactly one row per axis × dimension × canonical metric.

    Feature rows must not leak into metric-level or axis-level counts.
    """
    columns = [
        "axis",
        "dimension",
        "canonical_metric_key",
        "metric_weight",
        "dimension_weight",
    ]

    out = (
        feature_metadata[columns]
        .dropna(subset=["axis", "canonical_metric_key"])
        .drop_duplicates()
        .copy()
    )

    out["axis_metric_weight"] = (
        pd.to_numeric(out["metric_weight"], errors="coerce")
        * pd.to_numeric(out["dimension_weight"], errors="coerce")
    )

    duplicates = out.duplicated(
        subset=["axis", "canonical_metric_key"],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Conflicting axis metric metadata:\n"
            + out[duplicates]
            .sort_values(["axis", "canonical_metric_key"])
            .to_string(index=False)
        )

    return out


def _last_row_per_group(
    df: pd.DataFrame,
    group_columns: list[str],
    *,
    date_column: str,
) -> pd.DataFrame:
    return (
        df.sort_values(group_columns + [date_column])
        .groupby(
            group_columns,
            dropna=False,
            as_index=False,
        )
        .tail(1)
        .reset_index(drop=True)
    )


def build_history_maturity_audit(
    run_id: str = "macro_regime_v1",
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: list[str] | None = None,
    include_national: bool = True,
) -> dict[str, pd.DataFrame]:
    """
    History Maturity Audit v2.

    This audit intentionally exposes two calendars:

    1. Source-history calendar
       Uses feature observation dates and answers:
       "How much history existed when this feature was calculated?"

    2. Evaluation calendar
       Uses aligned monthly regime evaluation dates and answers:
       "How mature and how old were the metric scores actually consumed
       by the regime engine on this date?"

    This audit does not yet measure the ages of individual inputs used
    inside derived metrics. That requires derived-input lineage.
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

    normalized_policy_source = store.read_dataframe(
        run_id,
        "normalized_features",
        columns=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "source_family",
            "normalization_method",
            "lookback_periods",
            "min_periods",
            "score_direction",
        ],
    )

    aligned = store.read_dataframe(
        run_id,
        "aligned_metric_scores",
    )

    if "feature_count" in aligned.columns:
        aligned = aligned.rename(
            columns={
                "feature_count": (
                    "scored_feature_count_at_metric_date"
                )
            }
        )

    features["date"] = pd.to_datetime(features["date"])
    normalized_policy_source["date"] = pd.to_datetime(
        normalized_policy_source["date"]
    )
    aligned["evaluation_date"] = pd.to_datetime(
        aligned["evaluation_date"]
    )
    aligned["metric_date"] = pd.to_datetime(
        aligned["metric_date"]
    )

    features = features[
        features["geo_id"].isin(selected_geos)
    ].copy()

    normalized = normalized_policy_source[
        normalized_policy_source["geo_id"].isin(selected_geos)
    ].copy()

    aligned = aligned[
        aligned["geo_id"].isin(geo_ids)
    ].copy()

    if features.empty:
        raise ValueError(
            "No feature rows found for selected geographies: "
            f"{selected_geos}"
        )

    if aligned.empty:
        raise ValueError(
            "No aligned metric rows found for local validation "
            f"geographies: {geo_ids}"
        )

    policy = _build_policy_table(normalized_policy_source)
    feature_metadata = _build_feature_metadata()
    axis_metric_metadata = _build_axis_metric_metadata(
        feature_metadata
    )

    # ------------------------------------------------------------------
    # A. Source-history maturity at the unique feature grain
    # ------------------------------------------------------------------

    source_history = features.sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "feature_key",
            "date",
        ]
    ).copy()

    feature_group = [
        "geo_id",
        "canonical_metric_key",
        "feature_key",
    ]

    grouped = source_history.groupby(
        feature_group,
        group_keys=False,
    )

    source_history["observation_count"] = grouped.cumcount() + 1
    source_history["first_feature_date"] = grouped["date"].transform(
        "min"
    )
    source_history["latest_feature_date"] = grouped["date"].transform(
        "max"
    )

    source_history = source_history.merge(
        policy,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    missing_policy = source_history[
        source_history["min_periods"].isna()
        | source_history["lookback_periods"].isna()
    ]

    if not missing_policy.empty:
        missing_keys = sorted(
            missing_policy["feature_key"].dropna().unique()
        )
        raise ValueError(
            "Missing normalization policy for feature keys: "
            f"{missing_keys}"
        )

    source_history["min_periods"] = (
        source_history["min_periods"].astype(int)
    )
    source_history["lookback_periods"] = (
        source_history["lookback_periods"].astype(int)
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

    source_history = source_history.merge(
        score_keys,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        how="left",
        validate="one_to_one",
    )

    source_history["score_available"] = (
        source_history["score_available"]
        .fillna(False)
        .astype(bool)
    )

    first_score = (
        normalized.groupby(feature_group)["date"]
        .min()
        .rename("first_score_date")
        .reset_index()
    )

    source_history = source_history.merge(
        first_score,
        on=feature_group,
        how="left",
        validate="many_to_one",
    )

    source_history["minimum_ratio"] = (
        source_history["observation_count"]
        / source_history["min_periods"].replace(0, pd.NA)
    ).clip(upper=1.0)

    source_history["lookback_ratio"] = (
        source_history["observation_count"]
        / source_history["lookback_periods"].replace(0, pd.NA)
    ).clip(upper=1.0)

    source_history["minimum_met"] = (
        source_history["observation_count"]
        >= source_history["min_periods"]
    )

    source_history["full_window"] = (
        source_history["observation_count"]
        >= source_history["lookback_periods"]
    )

    source_history["maturity_status"] = source_history.apply(
        _maturity_status,
        axis=1,
    )

    # Add non-axis metadata without multiplying rows.
    unique_feature_metadata = (
        feature_metadata[
            [
                "canonical_metric_key",
                "feature_key",
                "dimension",
                "source_id",
                "frequency",
                "seasonality",
                "feature_type",
                "transform",
                "feature_weight",
                "metric_weight",
            ]
        ]
        .drop_duplicates()
    )

    duplicate_feature_metadata = unique_feature_metadata.duplicated(
        subset=["canonical_metric_key", "feature_key"],
        keep=False,
    )

    if duplicate_feature_metadata.any():
        raise ValueError(
            "Feature metadata is not unique before axis expansion:\n"
            + unique_feature_metadata[
                duplicate_feature_metadata
            ].to_string(index=False)
        )

    source_history = source_history.merge(
        unique_feature_metadata,
        on=["canonical_metric_key", "feature_key"],
        how="left",
        validate="many_to_one",
    )

    source_history["source_year"] = source_history["date"].dt.year

    source_year_end = _last_row_per_group(
        source_history,
        [
            "geo_id",
            "canonical_metric_key",
            "feature_key",
            "source_year",
        ],
        date_column="date",
    )

    annual_source_history_summary = (
        source_year_end.groupby(
            ["geo_id", "source_year"],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "size"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
        )
        .reset_index()
    )

    annual_source_history_summary = _add_feature_shares(
        annual_source_history_summary
    )

    # ------------------------------------------------------------------
    # B. Axis-expanded source-history maturity
    # ------------------------------------------------------------------

    axis_feature_metadata = (
        feature_metadata[
            [
                "axis",
                "dimension",
                "canonical_metric_key",
                "feature_key",
                "dimension_weight",
                "metric_weight",
                "feature_weight",
                "axis_metric_feature_weight",
            ]
        ]
        .dropna(subset=["axis"])
        .drop_duplicates()
    )

    axis_source_history = source_history.merge(
        axis_feature_metadata,
        on=[
            "dimension",
            "canonical_metric_key",
            "feature_key",
            "metric_weight",
            "feature_weight",
        ],
        how="inner",
        validate="many_to_many",
    )

    axis_source_year_end = _last_row_per_group(
        axis_source_history,
        [
            "geo_id",
            "axis",
            "canonical_metric_key",
            "feature_key",
            "source_year",
        ],
        date_column="date",
    )

    annual_axis_source_history_summary = (
        axis_source_year_end.groupby(
            ["geo_id", "source_year", "axis"],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "size"),
            metric_count=("canonical_metric_key", "nunique"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
            weighted_avg_lookback_ratio=(
                "lookback_ratio",
                lambda values: _weighted_mean(
                    values,
                    axis_source_year_end.loc[
                        values.index,
                        "axis_metric_feature_weight",
                    ],
                ),
            ),
        )
        .reset_index()
    )

    annual_axis_source_history_summary = _add_feature_shares(
        annual_axis_source_history_summary
    )

    annual_metric_source_history_summary = (
        axis_source_year_end.groupby(
            [
                "geo_id",
                "source_year",
                "axis",
                "dimension",
                "canonical_metric_key",
            ],
            dropna=False,
        )
        .agg(
            feature_count=("feature_key", "size"),
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

    annual_metric_source_history_summary = _add_feature_shares(
        annual_metric_source_history_summary
    )

    # ------------------------------------------------------------------
    # C. Metric-date maturity
    # ------------------------------------------------------------------

    metric_date_maturity = (
        source_history.groupby(
            [
                "geo_id",
                "date",
                "canonical_metric_key",
            ],
            dropna=False,
        )
        .agg(
            source_feature_count=("feature_key", "size"),
            scored_feature_count=("score_available", "sum"),
            minimum_met_count=("minimum_met", "sum"),
            full_window_count=("full_window", "sum"),
            avg_minimum_ratio=("minimum_ratio", "mean"),
            avg_lookback_ratio=("lookback_ratio", "mean"),
            min_observation_count=("observation_count", "min"),
            max_observation_count=("observation_count", "max"),
        )
        .reset_index()
        .rename(columns={"date": "metric_date"})
    )

    metric_date_maturity["scored_feature_share"] = _safe_share(
        metric_date_maturity["scored_feature_count"],
        metric_date_maturity["source_feature_count"],
    )

    metric_date_maturity["minimum_met_share"] = _safe_share(
        metric_date_maturity["minimum_met_count"],
        metric_date_maturity["source_feature_count"],
    )

    metric_date_maturity["full_window_share"] = _safe_share(
        metric_date_maturity["full_window_count"],
        metric_date_maturity["source_feature_count"],
    )

    # ------------------------------------------------------------------
    # D. Evaluation-calendar maturity
    # ------------------------------------------------------------------

    # Capital-market metric scores are sourced nationally and then
    # broadcast onto local macro geographies by the as-of aligner.
    #
    # The aligned artifact therefore contains:
    #
    #   geo_id = local evaluation geography
    #
    # while the source feature history contains:
    #
    #   geo_id = united_states__nation
    #
    # Preserve the local geography as evaluation_geo_id and resolve the
    # correct source geography before joining maturity metadata.

    capital_market_metrics = set(
        axis_metric_metadata.loc[
            axis_metric_metadata["dimension"].eq("capital_markets"),
            "canonical_metric_key",
        ]
        .dropna()
        .astype(str)
    )

    aligned_for_maturity = aligned.copy()

    aligned_for_maturity = aligned_for_maturity.rename(
        columns={"geo_id": "evaluation_geo_id"}
    )

    aligned_for_maturity["source_geo_id"] = (
        aligned_for_maturity["evaluation_geo_id"]
    )

    national_mask = aligned_for_maturity[
        "canonical_metric_key"
    ].isin(capital_market_metrics)

    aligned_for_maturity.loc[
        national_mask,
        "source_geo_id",
    ] = NATIONAL_GEO_ID

    invalid_national_source = aligned_for_maturity[
        national_mask
        & ~aligned_for_maturity["source_geo_id"].eq(
            NATIONAL_GEO_ID
        )
    ]

    if not invalid_national_source.empty:
        raise AssertionError(
            "Capital-market rows were not mapped to the national "
            "source geography:\n"
            + invalid_national_source[
                [
                    "evaluation_geo_id",
                    "source_geo_id",
                    "canonical_metric_key",
                ]
            ]
            .drop_duplicates()
            .head(20)
            .to_string(index=False)
        )

    metric_date_maturity_for_join = (
        metric_date_maturity.rename(
            columns={"geo_id": "source_geo_id"}
        )
    )

    evaluation_metric_maturity = aligned_for_maturity.merge(
        metric_date_maturity_for_join,
        on=[
            "source_geo_id",
            "metric_date",
            "canonical_metric_key",
        ],
        how="left",
        validate="many_to_one",
    )

    evaluation_metric_maturity = (
        evaluation_metric_maturity.rename(
            columns={"evaluation_geo_id": "geo_id"}
        )
    )

    evaluation_metric_maturity = evaluation_metric_maturity.merge(
        axis_metric_metadata,
        on="canonical_metric_key",
        how="inner",
        validate="many_to_many",
    )

    local_capital_rows = evaluation_metric_maturity[
        evaluation_metric_maturity["canonical_metric_key"].isin(
            capital_market_metrics
        )
    ]

    if not local_capital_rows.empty:
        invalid = local_capital_rows[
            ~local_capital_rows["source_geo_id"].eq(
                NATIONAL_GEO_ID
            )
        ]

        if not invalid.empty:
            raise AssertionError(
                "Broadcast capital-market rows have an invalid "
                "source geography:\n"
                + invalid[
                    [
                        "geo_id",
                        "source_geo_id",
                        "canonical_metric_key",
                    ]
                ]
                .drop_duplicates()
                .head(20)
                .to_string(index=False)
            )
    
    missing_maturity = evaluation_metric_maturity[
        evaluation_metric_maturity["avg_lookback_ratio"].isna()
    ]

    if not missing_maturity.empty:
        missing = (
            missing_maturity[
                [
                    "geo_id",
                    "source_geo_id",
                    "evaluation_date",
                    "metric_date",
                    "canonical_metric_key",
                ]
            ]
            .drop_duplicates()
            .head(30)
        )

        raise ValueError(
            "Aligned metric scores could not be matched to source "
            "history maturity:\n"
            + missing.to_string(index=False)
        )

    evaluation_metric_maturity["evaluation_year"] = (
        evaluation_metric_maturity["evaluation_date"].dt.year
    )

    evaluation_axis_maturity = (
        evaluation_metric_maturity.groupby(
            ["geo_id", "evaluation_date", "axis"],
            dropna=False,
        )
        .agg(
            metric_count=("canonical_metric_key", "nunique"),
            avg_metric_age_days=("metric_age_days", "mean"),
            max_metric_age_days=("metric_age_days", "max"),
            avg_minimum_ratio=("avg_minimum_ratio", "mean"),
            avg_lookback_ratio=("avg_lookback_ratio", "mean"),
            minimum_met_metric_share=(
                "minimum_met_share",
                "mean",
            ),
            full_window_metric_share=(
                "full_window_share",
                "mean",
            ),
            weighted_avg_lookback_ratio=(
                "avg_lookback_ratio",
                lambda values: _weighted_mean(
                    values,
                    evaluation_metric_maturity.loc[
                        values.index,
                        "axis_metric_weight",
                    ],
                ),
            ),
            weighted_avg_metric_age_days=(
                "metric_age_days",
                lambda values: _weighted_mean(
                    values,
                    evaluation_metric_maturity.loc[
                        values.index,
                        "axis_metric_weight",
                    ],
                ),
            ),
        )
        .reset_index()
    )

    evaluation_axis_maturity["evaluation_year"] = (
        evaluation_axis_maturity["evaluation_date"].dt.year
    )

    annual_evaluation_axis_snapshot = _last_row_per_group(
        evaluation_axis_maturity,
        [
            "geo_id",
            "axis",
            "evaluation_year",
        ],
        date_column="evaluation_date",
    )

    annual_evaluation_metric_snapshot = _last_row_per_group(
        evaluation_metric_maturity,
        [
            "geo_id",
            "axis",
            "canonical_metric_key",
            "evaluation_year",
        ],
        date_column="evaluation_date",
    )

    latest_source_feature_summary = _last_row_per_group(
        source_history,
        feature_group,
        date_column="date",
    ).sort_values(
        [
            "geo_id",
            "dimension",
            "canonical_metric_key",
            "feature_key",
        ]
    )

    # ------------------------------------------------------------------
    # E. Assertions
    # ------------------------------------------------------------------

    _assert_valid_summary_counts(
        annual_source_history_summary,
        label="annual_source_history_summary",
    )

    _assert_valid_summary_counts(
        annual_axis_source_history_summary,
        label="annual_axis_source_history_summary",
    )

    _assert_valid_summary_counts(
        annual_metric_source_history_summary,
        label="annual_metric_source_history_summary",
    )

    for label, frame in {
        "metric_date_maturity": metric_date_maturity,
        "evaluation_metric_maturity": evaluation_metric_maturity,
        "evaluation_axis_maturity": evaluation_axis_maturity,
    }.items():
        share_columns = [
            column
            for column in frame.columns
            if column.endswith("_share")
        ]

        for column in share_columns:
            values = pd.to_numeric(
                frame[column],
                errors="coerce",
            )

            invalid = frame[
                values.notna()
                & (
                    (values < -1e-12)
                    | (values > 1.0 + 1e-12)
                )
            ]

            if not invalid.empty:
                raise AssertionError(
                    f"{label}: {column} outside [0,1]:\n"
                    + invalid.head(20).to_string(index=False)
                )

    return {
        "source_feature_history": (
            source_history.reset_index(drop=True)
        ),
        "source_feature_summary": (
            latest_source_feature_summary.reset_index(drop=True)
        ),
        "source_year_end_feature_snapshot": (
            source_year_end.reset_index(drop=True)
        ),
        "annual_source_history_summary": (
            annual_source_history_summary.reset_index(drop=True)
        ),
        "annual_axis_source_history_summary": (
            annual_axis_source_history_summary.reset_index(drop=True)
        ),
        "annual_metric_source_history_summary": (
            annual_metric_source_history_summary.reset_index(drop=True)
        ),
        "metric_date_maturity": (
            metric_date_maturity.reset_index(drop=True)
        ),
        "evaluation_metric_maturity": (
            evaluation_metric_maturity.reset_index(drop=True)
        ),
        "evaluation_axis_maturity": (
            evaluation_axis_maturity.reset_index(drop=True)
        ),
        "annual_evaluation_axis_snapshot": (
            annual_evaluation_axis_snapshot.reset_index(drop=True)
        ),
        "annual_evaluation_metric_snapshot": (
            annual_evaluation_metric_snapshot.reset_index(drop=True)
        ),
        "latest_source_feature_summary": (
            latest_source_feature_summary.reset_index(drop=True)
        ),
    }
