from __future__ import annotations
# regime/experiments/linked_price_family_comparison.py

from pathlib import Path

import numpy as np
import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)
from regime._02_feature_normalizer import (
    normalize_features,
)
from regime._03_metric_scorer import (
    score_metrics,
)
from regime._04_asof_aligner import (
    align_metric_scores_asof,
)
from regime._05_dimension_scorer import (
    score_dimensions,
)
from regime._06_axis_engine import (
    score_axes,
)
from regime.experiments.linked_price_family_features import (
    PRICE_FAMILY_METRICS,
    build_linked_price_family_features,
)


BASELINE_RUN_ID = (
    "macro_regime_v1_bps120_sources"
)

CHALLENGER_ID = (
    "price_family_ma12_momentum_lag3"
)

FOCUS_GEOS = (
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
)

TARGET_METRICS = set(
    PRICE_FAMILY_METRICS
)

TARGET_DIMENSIONS = {
    "price",
    "affordability",
}

TARGET_AXIS = "demand"

FEATURE_KEY_MAP = {
    "median_sale_price": {
        "level": (
            "redfin_median_sale_price_level"
        ),
        "short": (
            "redfin_median_sale_price_short"
        ),
        "long": (
            "redfin_median_sale_price_long"
        ),
    },
    "median_ppsf": {
        "level": (
            "redfin_median_ppsf_level"
        ),
        "short": (
            "redfin_median_ppsf_short"
        ),
        "long": (
            "redfin_median_ppsf_long"
        ),
    },
    "price_to_income": {
        "level": "price_to_income_level",
        "short": "price_to_income_short",
        "long": "price_to_income_long",
    },
    "payment_burden": {
        "level": "payment_burden_level",
        "short": "payment_burden_short",
        "long": "payment_burden_long",
    },
}

TARGET_FEATURE_KEYS = {
    feature_key
    for metric_map in FEATURE_KEY_MAP.values()
    for feature_key in metric_map.values()
}

FEATURE_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "feature_key",
    "raw_feature_value",
]


def _standardize_date(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    work = frame.copy()

    if "date" in work.columns:
        source_date = "date"
    elif "evaluation_date" in work.columns:
        source_date = "evaluation_date"
    elif "metric_date" in work.columns:
        source_date = "metric_date"
    else:
        raise ValueError(
            "Could not resolve a date column from "
            f"{list(work.columns)}"
        )

    if source_date != "date":
        work = work.rename(
            columns={
                source_date: "date",
            }
        )

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    if work["date"].isna().any():
        raise ValueError(
            "Comparison frame contains invalid dates"
        )

    return work


def _safe_correlation(
    left: pd.Series,
    right: pd.Series,
) -> float:
    work = pd.concat(
        [
            pd.to_numeric(
                left,
                errors="coerce",
            ),
            pd.to_numeric(
                right,
                errors="coerce",
            ),
        ],
        axis=1,
    ).dropna()

    if len(work) < 3:
        return np.nan

    if (
        work.iloc[:, 0].nunique() <= 1
        or work.iloc[:, 1].nunique() <= 1
    ):
        return np.nan

    return float(
        work.iloc[:, 0].corr(
            work.iloc[:, 1]
        )
    )


def _sort_exact(
    frame: pd.DataFrame,
    keys: list[str],
) -> pd.DataFrame:
    valid_keys = [
        key
        for key in keys
        if key in frame.columns
    ]

    if not valid_keys:
        raise ValueError(
            "No valid exact-comparison sort keys"
        )

    return (
        frame.sort_values(
            valid_keys,
            kind="mergesort",
        )
        .reset_index(drop=True)
    )


def _exact_parity_result(
    baseline: pd.DataFrame,
    challenger: pd.DataFrame,
    *,
    artifact_name: str,
    comparison_scope: str,
    sort_keys: list[str],
) -> dict[str, object]:
    baseline_sorted = _sort_exact(
        baseline,
        sort_keys,
    )

    challenger_sorted = _sort_exact(
        challenger,
        sort_keys,
    )

    error_message = ""

    try:
        pd.testing.assert_frame_equal(
            baseline_sorted,
            challenger_sorted,
            check_dtype=True,
            check_exact=True,
            check_like=False,
        )
        exact_match = True
    except AssertionError as exc:
        exact_match = False
        error_message = str(exc)[:4000]

    return {
        "artifact_name": artifact_name,
        "comparison_scope": comparison_scope,
        "baseline_rows": len(
            baseline_sorted
        ),
        "challenger_rows": len(
            challenger_sorted
        ),
        "exact_match": exact_match,
        "error_message": error_message,
    }


def _build_feature_override(
    feature_history: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_component",
        "raw_feature_value",
    }

    missing = required - set(
        feature_history.columns
    )

    if missing:
        raise ValueError(
            "Linked price feature output is missing "
            f"columns: {sorted(missing)}"
        )

    override = feature_history.copy()

    override["feature_key"] = [
        FEATURE_KEY_MAP[
            metric_key
        ][
            feature_component
        ]
        for metric_key, feature_component
        in zip(
            override[
                "canonical_metric_key"
            ],
            override[
                "feature_component"
            ],
            strict=True,
        )
    ]

    override = override.dropna(
        subset=[
            "raw_feature_value",
        ]
    )

    nonfinite = override[
        ~np.isfinite(
            pd.to_numeric(
                override[
                    "raw_feature_value"
                ],
                errors="coerce",
            )
        )
    ]

    if not nonfinite.empty:
        raise ValueError(
            "Linked price feature override "
            "contains non-finite values:\n"
            + nonfinite.head(30).to_string(
                index=False
            )
        )

    override = override[
        FEATURE_COLUMNS
    ].copy()

    duplicates = override.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Linked price feature override "
            "contains duplicate production keys:\n"
            + override.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return override


def _replace_target_features(
    baseline_features: pd.DataFrame,
    override: pd.DataFrame,
) -> pd.DataFrame:
    missing = (
        set(FEATURE_COLUMNS)
        - set(baseline_features.columns)
    )

    if missing:
        raise ValueError(
            "Baseline features are missing "
            f"columns: {sorted(missing)}"
        )

    baseline = baseline_features[
        FEATURE_COLUMNS
    ].copy()

    target_mask = (
        baseline[
            "canonical_metric_key"
        ].isin(TARGET_METRICS)
        & baseline[
            "feature_key"
        ].isin(TARGET_FEATURE_KEYS)
    )

    retained = baseline[
        ~target_mask
    ].copy()

    combined = pd.concat(
        [
            retained,
            override,
        ],
        ignore_index=True,
    )

    duplicates = combined.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Linked price feature replacement "
            "created duplicate rows:\n"
            + combined.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    feature_order = (
        baseline[
            [
                "feature_key",
            ]
        ]
        .drop_duplicates()
        .reset_index()
        .rename(
            columns={
                "index": "_feature_order",
            }
        )
    )

    combined = combined.merge(
        feature_order,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    missing_order = combined[
        combined[
            "_feature_order"
        ].isna()
    ]

    if not missing_order.empty:
        raise AssertionError(
            "Linked price challenger introduced "
            "unknown production feature keys:\n"
            + missing_order[
                [
                    "canonical_metric_key",
                    "feature_key",
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

    return (
        combined.sort_values(
            [
                "_feature_order",
                "geo_id",
                "canonical_metric_key",
                "date",
            ],
            kind="mergesort",
        )
        .drop(
            columns=[
                "_feature_order",
            ]
        )
        .reset_index(drop=True)
    )


def _add_change_diagnostics(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    work = _standardize_date(
        frame
    )

    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    work = work.sort_values(
        [
            *group_columns,
            "date",
        ]
    ).reset_index(drop=True)

    grouped = work.groupby(
        group_columns,
        group_keys=False,
    )

    work[
        f"{value_column}_change_1m"
    ] = grouped[
        value_column
    ].diff()

    work[
        f"absolute_{value_column}_change_1m"
    ] = work[
        f"{value_column}_change_1m"
    ].abs()

    previous = grouped[
        value_column
    ].shift(1)

    work[
        f"{value_column}_sign_flip"
    ] = (
        work[value_column].notna()
        & previous.notna()
        & np.sign(
            work[value_column]
        ).ne(
            np.sign(previous)
        )
    )

    work["calendar_month"] = (
        work["date"].dt.month
    )

    return work


def _stability_summary(
    frame: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    absolute_column = (
        f"absolute_{value_column}_change_1m"
    )

    sign_flip_column = (
        f"{value_column}_sign_flip"
    )

    return (
        frame.groupby(
            group_columns,
            dropna=False,
        )
        .agg(
            rows=(value_column, "count"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_value=(
                value_column,
                "mean",
            ),
            value_std=(
                value_column,
                "std",
            ),
            mean_absolute_change_1m=(
                absolute_column,
                "mean",
            ),
            p90_absolute_change_1m=(
                absolute_column,
                lambda values: values.quantile(
                    0.90
                ),
            ),
            maximum_absolute_change_1m=(
                absolute_column,
                "max",
            ),
            sign_flip_rate=(
                sign_flip_column,
                "mean",
            ),
            near_zero_rate=(
                value_column,
                lambda values: values.abs().lt(
                    0.10
                ).mean(),
            ),
        )
        .reset_index()
    )


def _comparison_vs_baseline(
    summary: pd.DataFrame,
    *,
    identity_columns: list[str],
) -> pd.DataFrame:
    baseline = summary[
        summary[
            "run_role"
        ].eq("baseline")
    ].copy()

    challenger = summary[
        summary[
            "run_role"
        ].eq("challenger")
    ].copy()

    baseline = baseline.drop(
        columns=[
            "run_role",
        ]
    )

    baseline = baseline.rename(
        columns={
            column: f"baseline_{column}"
            for column in baseline.columns
            if column not in identity_columns
        }
    )

    output = challenger.merge(
        baseline,
        on=identity_columns,
        how="left",
        validate="one_to_one",
    )

    for column in (
        "value_std",
        "mean_absolute_change_1m",
        "p90_absolute_change_1m",
        "maximum_absolute_change_1m",
    ):
        baseline_column = (
            f"baseline_{column}"
        )

        output[
            f"{column}_pct_vs_baseline"
        ] = np.where(
            output[
                baseline_column
            ].ne(0),
            (
                output[column]
                / output[
                    baseline_column
                ]
                - 1.0
            ),
            np.nan,
        )

    output[
        "sign_flip_rate_delta_vs_baseline"
    ] = (
        output["sign_flip_rate"]
        - output[
            "baseline_sign_flip_rate"
        ]
    )

    output[
        "near_zero_rate_delta_vs_baseline"
    ] = (
        output["near_zero_rate"]
        - output[
            "baseline_near_zero_rate"
        ]
    )

    return output


def _baseline_correlation(
    history: pd.DataFrame,
    *,
    value_column: str,
    identity_columns: list[str],
) -> pd.DataFrame:
    baseline = history[
        history[
            "run_role"
        ].eq("baseline")
    ][
        [
            *identity_columns,
            "date",
            value_column,
        ]
    ].rename(
        columns={
            value_column: (
                f"baseline_{value_column}"
            ),
        }
    )

    challenger = history[
        history[
            "run_role"
        ].eq("challenger")
    ]

    rows: list[dict[str, object]] = []

    for keys, frame in challenger.groupby(
        identity_columns,
        dropna=False,
    ):
        if not isinstance(
            keys,
            tuple,
        ):
            keys = (
                keys,
            )

        merged = frame.merge(
            baseline,
            on=[
                *identity_columns,
                "date",
            ],
            how="inner",
            validate="one_to_one",
        )

        row = {
            column: value
            for column, value
            in zip(
                identity_columns,
                keys,
                strict=True,
            )
        }

        row["overlap_rows"] = int(
            merged[
                [
                    value_column,
                    f"baseline_{value_column}",
                ]
            ]
            .dropna()
            .shape[0]
        )

        row["correlation"] = (
            _safe_correlation(
                merged[value_column],
                merged[
                    f"baseline_{value_column}"
                ],
            )
        )

        row[
            "mean_absolute_difference"
        ] = (
            (
                merged[value_column]
                - merged[
                    f"baseline_{value_column}"
                ]
            )
            .abs()
            .mean()
        )

        rows.append(row)

    return pd.DataFrame(rows)


def _seasonality_summary(
    history: pd.DataFrame,
    *,
    value_column: str,
    group_columns: list[str],
) -> pd.DataFrame:
    absolute_column = (
        f"absolute_{value_column}_change_1m"
    )

    return (
        history.groupby(
            [
                *group_columns,
                "calendar_month",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            mean_absolute_change_1m=(
                absolute_column,
                "mean",
            ),
        )
        .reset_index()
    )


def _build_isolation_audit(
    *,
    baseline_normalized: pd.DataFrame,
    challenger_normalized: pd.DataFrame,
    baseline_metrics: pd.DataFrame,
    challenger_metrics: pd.DataFrame,
    baseline_dimensions: pd.DataFrame,
    challenger_dimensions: pd.DataFrame,
    baseline_axes: pd.DataFrame,
    challenger_axes: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []

    rows.append(
        _exact_parity_result(
            baseline_normalized[
                ~baseline_normalized[
                    "canonical_metric_key"
                ].isin(TARGET_METRICS)
            ].copy(),
            challenger_normalized[
                ~challenger_normalized[
                    "canonical_metric_key"
                ].isin(TARGET_METRICS)
            ].copy(),
            artifact_name=(
                "normalized_features"
            ),
            comparison_scope=(
                "all_non_price_family_metrics"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "canonical_metric_key",
                "feature_key",
            ],
        )
    )

    rows.append(
        _exact_parity_result(
            baseline_metrics[
                ~baseline_metrics[
                    "canonical_metric_key"
                ].isin(TARGET_METRICS)
            ].copy(),
            challenger_metrics[
                ~challenger_metrics[
                    "canonical_metric_key"
                ].isin(TARGET_METRICS)
            ].copy(),
            artifact_name="metric_scores",
            comparison_scope=(
                "all_non_price_family_metrics"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "canonical_metric_key",
            ],
        )
    )

    baseline_dimensions = _standardize_date(
        baseline_dimensions
    )

    challenger_dimensions = _standardize_date(
        challenger_dimensions
    )

    rows.append(
        _exact_parity_result(
            baseline_dimensions[
                ~baseline_dimensions[
                    "dimension"
                ].isin(TARGET_DIMENSIONS)
            ].copy(),
            challenger_dimensions[
                ~challenger_dimensions[
                    "dimension"
                ].isin(TARGET_DIMENSIONS)
            ].copy(),
            artifact_name="dimension_scores",
            comparison_scope=(
                "all_non_price_affordability_dimensions"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "dimension",
            ],
        )
    )

    baseline_axes = _standardize_date(
        baseline_axes
    )

    challenger_axes = _standardize_date(
        challenger_axes
    )

    rows.append(
        _exact_parity_result(
            baseline_axes[
                ~baseline_axes[
                    "axis"
                ].eq(TARGET_AXIS)
            ].copy(),
            challenger_axes[
                ~challenger_axes[
                    "axis"
                ].eq(TARGET_AXIS)
            ].copy(),
            artifact_name="axis_scores",
            comparison_scope=(
                "all_non_demand_axes"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "axis",
            ],
        )
    )

    supply_baseline = baseline_axes[
        baseline_axes[
            "axis"
        ].eq("supply")
    ].copy()

    supply_challenger = challenger_axes[
        challenger_axes[
            "axis"
        ].eq("supply")
    ].copy()

    rows.append(
        _exact_parity_result(
            supply_baseline,
            supply_challenger,
            artifact_name="axis_scores",
            comparison_scope=(
                "supply_axis_exact"
            ),
            sort_keys=[
                "geo_id",
                "date",
                "axis",
            ],
        )
    )

    return pd.DataFrame(rows)


def build_linked_price_family_comparison(
    *,
    baseline_run_id: str = (
        BASELINE_RUN_ID
    ),
    challenger_id: str = (
        CHALLENGER_ID
    ),
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: tuple[str, ...] = (
        FOCUS_GEOS
    ),
) -> dict[str, pd.DataFrame]:
    store = RegimeArtifactStore(
        artifact_root
    )

    source_metrics = store.read_dataframe(
        baseline_run_id,
        "source_metrics",
    )

    baseline_features = store.read_dataframe(
        baseline_run_id,
        "features",
    )

    baseline_normalized = (
        store.read_dataframe(
            baseline_run_id,
            "normalized_features",
        )
    )

    baseline_metrics = store.read_dataframe(
        baseline_run_id,
        "metric_scores",
    )

    baseline_dimensions = (
        store.read_dataframe(
            baseline_run_id,
            "dimension_scores",
        )
    )

    baseline_axes = store.read_dataframe(
        baseline_run_id,
        "axis_scores",
    )

    linked = build_linked_price_family_features(
        source_metrics,
        experiment_id=challenger_id,
    )

    override = _build_feature_override(
        linked.feature_history
    )

    challenger_features = (
        _replace_target_features(
            baseline_features,
            override,
        )
    )

    challenger_normalized = normalize_features(
        challenger_features
    )

    challenger_metrics = score_metrics(
        challenger_normalized
    )

    challenger_aligned = (
        align_metric_scores_asof(
            challenger_metrics
        )
    )

    challenger_dimensions = score_dimensions(
        challenger_aligned
    )

    challenger_axes = score_axes(
        challenger_dimensions
    )

    isolation_audit = _build_isolation_audit(
        baseline_normalized=(
            baseline_normalized
        ),
        challenger_normalized=(
            challenger_normalized
        ),
        baseline_metrics=baseline_metrics,
        challenger_metrics=(
            challenger_metrics
        ),
        baseline_dimensions=(
            baseline_dimensions
        ),
        challenger_dimensions=(
            challenger_dimensions
        ),
        baseline_axes=baseline_axes,
        challenger_axes=challenger_axes,
    )

    normalized_frames: list[
        pd.DataFrame
    ] = []

    metric_frames: list[
        pd.DataFrame
    ] = []

    dimension_frames: list[
        pd.DataFrame
    ] = []

    axis_frames: list[
        pd.DataFrame
    ] = []

    for run_role, normalized, metrics, dimensions, axes in (
        (
            "baseline",
            baseline_normalized,
            baseline_metrics,
            baseline_dimensions,
            baseline_axes,
        ),
        (
            "challenger",
            challenger_normalized,
            challenger_metrics,
            challenger_dimensions,
            challenger_axes,
        ),
    ):
        normalized_focus = normalized[
            normalized[
                "canonical_metric_key"
            ].isin(TARGET_METRICS)
            & normalized[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        normalized_focus[
            "feature_component"
        ] = (
            normalized_focus[
                "feature_key"
            ]
            .str.rsplit(
                "_",
                n=1,
            )
            .str[-1]
        )

        normalized_focus[
            "run_role"
        ] = run_role

        normalized_frames.append(
            normalized_focus
        )

        metric_focus = metrics[
            metrics[
                "canonical_metric_key"
            ].isin(TARGET_METRICS)
            & metrics[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        metric_focus["run_role"] = (
            run_role
        )

        metric_frames.append(
            metric_focus
        )

        dimension_focus = (
            _standardize_date(
                dimensions
            )
        )

        dimension_focus = (
            dimension_focus[
                dimension_focus[
                    "dimension"
                ].isin(
                    TARGET_DIMENSIONS
                )
                & dimension_focus[
                    "geo_id"
                ].isin(geo_ids)
            ].copy()
        )

        dimension_focus[
            "run_role"
        ] = run_role

        dimension_frames.append(
            dimension_focus
        )

        axis_focus = _standardize_date(
            axes
        )

        axis_focus = axis_focus[
            axis_focus[
                "axis"
            ].eq(TARGET_AXIS)
            & axis_focus[
                "geo_id"
            ].isin(geo_ids)
        ].copy()

        axis_focus["run_role"] = (
            run_role
        )

        axis_frames.append(
            axis_focus
        )

    normalized_history = pd.concat(
        normalized_frames,
        ignore_index=True,
    )

    metric_history = pd.concat(
        metric_frames,
        ignore_index=True,
    )

    dimension_history = pd.concat(
        dimension_frames,
        ignore_index=True,
    )

    axis_history = pd.concat(
        axis_frames,
        ignore_index=True,
    )

    normalized_history = (
        _add_change_diagnostics(
            normalized_history,
            value_column="feature_score",
            group_columns=[
                "run_role",
                "geo_id",
                "canonical_metric_key",
                "feature_key",
            ],
        )
    )

    metric_history = (
        _add_change_diagnostics(
            metric_history,
            value_column="metric_score",
            group_columns=[
                "run_role",
                "geo_id",
                "canonical_metric_key",
            ],
        )
    )

    dimension_history = (
        _add_change_diagnostics(
            dimension_history,
            value_column="dimension_score",
            group_columns=[
                "run_role",
                "geo_id",
                "dimension",
            ],
        )
    )

    axis_history = (
        _add_change_diagnostics(
            axis_history,
            value_column="axis_score",
            group_columns=[
                "run_role",
                "geo_id",
                "axis",
            ],
        )
    )

    feature_summary = _stability_summary(
        normalized_history,
        value_column="feature_score",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
            "feature_component",
        ],
    )

    metric_summary = _stability_summary(
        metric_history,
        value_column="metric_score",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
        ],
    )

    dimension_summary = (
        _stability_summary(
            dimension_history,
            value_column="dimension_score",
            group_columns=[
                "run_role",
                "geo_id",
                "dimension",
            ],
        )
    )

    axis_summary = _stability_summary(
        axis_history,
        value_column="axis_score",
        group_columns=[
            "run_role",
            "geo_id",
            "axis",
        ],
    )

    feature_comparison = (
        _comparison_vs_baseline(
            feature_summary,
            identity_columns=[
                "geo_id",
                "canonical_metric_key",
                "feature_component",
            ],
        )
    )

    metric_comparison = (
        _comparison_vs_baseline(
            metric_summary,
            identity_columns=[
                "geo_id",
                "canonical_metric_key",
            ],
        )
    )

    dimension_comparison = (
        _comparison_vs_baseline(
            dimension_summary,
            identity_columns=[
                "geo_id",
                "dimension",
            ],
        )
    )

    axis_comparison = (
        _comparison_vs_baseline(
            axis_summary,
            identity_columns=[
                "geo_id",
                "axis",
            ],
        )
    )

    feature_correlations = (
        _baseline_correlation(
            normalized_history,
            value_column="feature_score",
            identity_columns=[
                "geo_id",
                "canonical_metric_key",
                "feature_component",
            ],
        )
    )

    metric_correlations = (
        _baseline_correlation(
            metric_history,
            value_column="metric_score",
            identity_columns=[
                "geo_id",
                "canonical_metric_key",
            ],
        )
    )

    dimension_correlations = (
        _baseline_correlation(
            dimension_history,
            value_column="dimension_score",
            identity_columns=[
                "geo_id",
                "dimension",
            ],
        )
    )

    axis_correlations = (
        _baseline_correlation(
            axis_history,
            value_column="axis_score",
            identity_columns=[
                "geo_id",
                "axis",
            ],
        )
    )

    feature_seasonality = (
        _seasonality_summary(
            normalized_history,
            value_column="feature_score",
            group_columns=[
                "run_role",
                "geo_id",
                "canonical_metric_key",
                "feature_component",
            ],
        )
    )

    demand_conviction = (
        axis_history.groupby(
            [
                "run_role",
                "geo_id",
            ]
        )
        .agg(
            rows=("axis_score", "count"),
            mean_axis_score=(
                "axis_score",
                "mean",
            ),
            mean_absolute_axis_score=(
                "axis_score",
                lambda values: values.abs().mean(),
            ),
            median_absolute_axis_score=(
                "axis_score",
                lambda values: values.abs().median(),
            ),
            near_origin_rate_005=(
                "axis_score",
                lambda values: values.abs().lt(
                    0.05
                ).mean(),
            ),
            near_origin_rate_010=(
                "axis_score",
                lambda values: values.abs().lt(
                    0.10
                ).mean(),
            ),
            strong_conviction_rate_025=(
                "axis_score",
                lambda values: values.abs().ge(
                    0.25
                ).mean(),
            ),
        )
        .reset_index()
    )

    return {
        "challenger_features": (
            challenger_features
        ),
        "challenger_normalized_features": (
            challenger_normalized
        ),
        "challenger_metric_scores": (
            challenger_metrics
        ),
        "challenger_aligned_metric_scores": (
            challenger_aligned
        ),
        "challenger_dimension_scores": (
            challenger_dimensions
        ),
        "challenger_axis_scores": (
            challenger_axes
        ),
        "source_substitution_lineage": (
            linked.source_substitution_lineage
        ),
        "derived_lineage": (
            linked.derived_lineage
        ),
        "price_family_feature_lineage": (
            linked.feature_history
        ),
        "isolation_audit": (
            isolation_audit
        ),
        "normalized_feature_history": (
            normalized_history
        ),
        "metric_score_history": (
            metric_history
        ),
        "dimension_score_history": (
            dimension_history
        ),
        "axis_score_history": (
            axis_history
        ),
        "feature_stability_summary": (
            feature_summary
        ),
        "metric_stability_summary": (
            metric_summary
        ),
        "dimension_stability_summary": (
            dimension_summary
        ),
        "axis_stability_summary": (
            axis_summary
        ),
        "feature_comparison_vs_baseline": (
            feature_comparison
        ),
        "metric_comparison_vs_baseline": (
            metric_comparison
        ),
        "dimension_comparison_vs_baseline": (
            dimension_comparison
        ),
        "axis_comparison_vs_baseline": (
            axis_comparison
        ),
        "feature_baseline_correlations": (
            feature_correlations
        ),
        "metric_baseline_correlations": (
            metric_correlations
        ),
        "dimension_baseline_correlations": (
            dimension_correlations
        ),
        "axis_baseline_correlations": (
            axis_correlations
        ),
        "feature_seasonality": (
            feature_seasonality
        ),
        "demand_conviction": (
            demand_conviction
        ),
    }
