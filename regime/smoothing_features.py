from __future__ import annotations
# regime/experiments/smoothing_features.py

from collections.abc import Sequence

import numpy as np
import pandas as pd

from regime.smoothing_policy import (
    SmoothingMetricPolicy,
)


REQUIRED_ID_COLUMNS = (
    "geo_id",
    "date",
    "canonical_metric_key",
)

OUTPUT_FEATURE_SUFFIXES = (
    "level",
    "short",
    "long",
)


def _validate_input_frame(
    frame: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    """
    Validate and standardize a canonical long-form metric frame.

    Expected grain:
        geo_id
        date
        canonical_metric_key

    The supplied value column must contain the raw metric observation.
    """
    required_columns = {
        *REQUIRED_ID_COLUMNS,
        value_column,
    }

    missing = (
        required_columns
        - set(frame.columns)
    )

    if missing:
        raise ValueError(
            "Smoothing feature input is missing "
            f"required columns: {sorted(missing)}"
        )

    work = frame.copy()

    work["geo_id"] = (
        work["geo_id"]
        .astype(str)
        .str.strip()
    )

    work["canonical_metric_key"] = (
        work["canonical_metric_key"]
        .astype(str)
        .str.strip()
    )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    invalid_ids = work[
        work["geo_id"].eq("")
        | work[
            "canonical_metric_key"
        ].eq("")
        | work["date"].isna()
    ]

    if not invalid_ids.empty:
        raise ValueError(
            "Smoothing feature input contains "
            "invalid identifiers or dates:\n"
            + invalid_ids.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = work.duplicated(
        subset=list(
            REQUIRED_ID_COLUMNS
        ),
        keep=False,
    )

    if duplicate_keys.any():
        raise ValueError(
            "Smoothing feature input contains "
            "duplicate canonical observations:\n"
            + work.loc[
                duplicate_keys
            ]
            .sort_values(
                list(
                    REQUIRED_ID_COLUMNS
                )
            )
            .head(30)
            .to_string(index=False)
        )

    return (
        work.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _safe_ratio_minus_one(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    """
    Calculate numerator / denominator - 1 without returning infinity.

    A zero, missing, or non-finite denominator produces NaN.
    """
    numerator_values = pd.to_numeric(
        numerator,
        errors="coerce",
    )

    denominator_values = pd.to_numeric(
        denominator,
        errors="coerce",
    )

    valid = (
        numerator_values.notna()
        & denominator_values.notna()
        & np.isfinite(
            numerator_values
        )
        & np.isfinite(
            denominator_values
        )
        & denominator_values.ne(0)
    )

    output = pd.Series(
        np.nan,
        index=numerator.index,
        dtype="float64",
    )

    output.loc[valid] = (
        numerator_values.loc[valid]
        / denominator_values.loc[valid]
        - 1.0
    )

    output = output.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    return output


def _rolling_mean(
    series: pd.Series,
    *,
    window: int,
) -> pd.Series:
    """
    Full-window trailing moving average.

    No partial-window values are emitted.
    """
    return (
        pd.to_numeric(
            series,
            errors="coerce",
        )
        .rolling(
            window=window,
            min_periods=window,
        )
        .mean()
    )


def _feature_key(
    metric_key: str,
    suffix: str,
) -> str:
    if suffix not in OUTPUT_FEATURE_SUFFIXES:
        raise ValueError(
            f"Unsupported feature suffix: {suffix!r}"
        )

    return (
        f"{metric_key}_{suffix}"
    )


def _validate_policy_for_metric(
    policy: SmoothingMetricPolicy,
    *,
    metric_key: str,
) -> None:
    policy.validate()

    if policy.is_baseline:
        raise ValueError(
            "Baseline/current policies do not "
            "generate challenger smoothing features"
        )

    if (
        policy.metric_key
        != metric_key
    ):
        raise ValueError(
            "Smoothing policy metric does not match "
            "the requested metric. "
            f"Policy={policy.metric_key!r}, "
            f"requested={metric_key!r}"
        )


def _build_group_features(
    group: pd.DataFrame,
    *,
    policy: SmoothingMetricPolicy,
    value_column: str,
) -> pd.DataFrame:
    """
    Generate level, short, and long values for one geo/metric series.
    """
    work = group.sort_values(
        "date"
    ).copy()

    raw = work[
        value_column
    ].astype("float64")

    level_ma = _rolling_mean(
        raw,
        window=policy.level_window,
    )

    short_ma = _rolling_mean(
        raw,
        window=policy.short_window,
    )

    long_ma = _rolling_mean(
        raw,
        window=policy.long_window,
    )

    if policy.transform_strategy in {
        "ma_momentum",
        "ma_structural",
    }:
        short_reference = (
            short_ma.shift(
                policy.short_lag_periods
            )
        )

        short_feature = (
            _safe_ratio_minus_one(
                short_ma,
                short_reference,
            )
        )

    elif (
        policy.transform_strategy
        == "ma_deviation"
    ):
        short_reference = short_ma

        short_feature = (
            _safe_ratio_minus_one(
                raw,
                short_reference,
            )
        )

    else:
        raise ValueError(
            "Unsupported smoothing transform "
            f"strategy: "
            f"{policy.transform_strategy!r}"
        )

    long_reference = long_ma.shift(
        policy.long_lag_periods
    )

    long_feature = _safe_ratio_minus_one(
        long_ma,
        long_reference,
    )

    work[
        "smoothed_level_value"
    ] = level_ma

    work[
        "smoothed_short_value"
    ] = short_feature

    work[
        "smoothed_long_value"
    ] = long_feature

    work[
        "short_reference_value"
    ] = short_reference

    work[
        "long_reference_value"
    ] = long_reference

    work[
        "level_ma_value"
    ] = level_ma

    work[
        "short_ma_value"
    ] = short_ma

    work[
        "long_ma_value"
    ] = long_ma

    return work


def build_smoothed_metric_features_wide(
    observations: pd.DataFrame,
    *,
    policy: SmoothingMetricPolicy,
    value_column: str = "raw_value",
) -> pd.DataFrame:
    """
    Generate wide smoothing features for one canonical metric.

    Output grain:
        geo_id
        date
        canonical_metric_key

    Output feature columns:
        smoothed_level_value
        smoothed_short_value
        smoothed_long_value
    """
    work = _validate_input_frame(
        observations,
        value_column=value_column,
    )

    metric_keys = set(
        work[
            "canonical_metric_key"
        ].unique()
    )

    if len(metric_keys) != 1:
        raise ValueError(
            "build_smoothed_metric_features_wide() "
            "requires exactly one canonical metric. "
            f"Found {sorted(metric_keys)}"
        )

    metric_key = next(
        iter(metric_keys)
    )

    _validate_policy_for_metric(
        policy,
        metric_key=metric_key,
    )

    result = (
        work.groupby(
            [
                "geo_id",
                "canonical_metric_key",
            ],
            group_keys=False,
            sort=False,
        )
        .apply(
            _build_group_features,
            policy=policy,
            value_column=value_column,
        )
        .reset_index(drop=True)
    )

    generated_columns = [
        "smoothed_level_value",
        "smoothed_short_value",
        "smoothed_long_value",
    ]

    for column in generated_columns:
        result[column] = (
            pd.to_numeric(
                result[column],
                errors="coerce",
            )
            .replace(
                [
                    np.inf,
                    -np.inf,
                ],
                np.nan,
            )
        )

    result[
        "smoothing_experiment_id"
    ] = policy.experiment_id

    result[
        "smoothing_strategy"
    ] = policy.transform_strategy

    result[
        "smoothing_level_window"
    ] = policy.level_window

    result[
        "smoothing_short_window"
    ] = policy.short_window

    result[
        "smoothing_short_lag_periods"
    ] = policy.short_lag_periods

    result[
        "smoothing_long_window"
    ] = policy.long_window

    result[
        "smoothing_long_lag_periods"
    ] = policy.long_lag_periods

    result[
        "smoothing_policy_role"
    ] = policy.policy_role

    return result


def build_smoothed_metric_features(
    observations: pd.DataFrame,
    *,
    policy: SmoothingMetricPolicy,
    value_column: str = "raw_value",
    preserve_columns: Sequence[str] = (),
) -> pd.DataFrame:
    """
    Generate long-form canonical feature rows.

    Output grain:
        geo_id
        date
        canonical_metric_key
        feature_key

    The raw observation is retained for lineage.
    """
    wide = (
        build_smoothed_metric_features_wide(
            observations,
            policy=policy,
            value_column=value_column,
        )
    )

    missing_preserve_columns = (
        set(preserve_columns)
        - set(wide.columns)
    )

    if missing_preserve_columns:
        raise ValueError(
            "Requested preserve_columns are missing: "
            f"{sorted(missing_preserve_columns)}"
        )

    id_columns = [
        "geo_id",
        "date",
        "canonical_metric_key",
        value_column,
        *preserve_columns,
        "smoothing_experiment_id",
        "smoothing_strategy",
        "smoothing_level_window",
        "smoothing_short_window",
        "smoothing_short_lag_periods",
        "smoothing_long_window",
        "smoothing_long_lag_periods",
        "smoothing_policy_role",
    ]

    id_columns = list(
        dict.fromkeys(
            id_columns
        )
    )

    feature_map = {
        "smoothed_level_value": "level",
        "smoothed_short_value": "short",
        "smoothed_long_value": "long",
    }

    long = wide.melt(
        id_vars=id_columns,
        value_vars=list(
            feature_map
        ),
        var_name=(
            "smoothed_feature_component"
        ),
        value_name="raw_feature_value",
    )

    long["feature_component"] = (
        long[
            "smoothed_feature_component"
        ].map(feature_map)
    )

    long["feature_key"] = [
        _feature_key(
            metric_key,
            component,
        )
        for metric_key, component
        in zip(
            long[
                "canonical_metric_key"
            ],
            long[
                "feature_component"
            ],
            strict=True,
        )
    ]

    long[
        "feature_observation_available"
    ] = long[
        "raw_feature_value"
    ].notna()

    duplicate_features = long.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicate_features.any():
        raise AssertionError(
            "Generated smoothing features contain "
            "duplicate rows:\n"
            + long.loc[
                duplicate_features
            ]
            .head(30)
            .to_string(index=False)
        )

    return (
        long.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
                "feature_component",
            ]
        )
        .reset_index(drop=True)
    )
