from __future__ import annotations
# regime/diagnostics/axis_contribution.py

from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from regime._00_config_loader import (
    load_regime_config,
)
from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)


DEFAULT_RUN_ID = "macro_regime_v1_freshness"

DEFAULT_AUDIT_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

PRODUCTION_AXES = (
    "demand",
    "supply",
)

FOCUS_METRICS = {
    "active_inventory",
    "median_sale_price",
    "median_ppsf",
    "price_to_income",
    "payment_burden",
    "permit_activity",
    "permit_intensity",
}


def _truthy(
    values: pd.Series,
) -> pd.Series:
    return (
        values.astype(str)
        .str.strip()
        .str.lower()
        .isin(
            {
                "true",
                "1",
                "yes",
                "y",
            }
        )
    )


def _resolve_column(
    dataframe: pd.DataFrame,
    candidates: Iterable[str],
    *,
    label: str,
) -> str:
    for candidate in candidates:
        if candidate in dataframe.columns:
            return candidate

    raise ValueError(
        f"Could not resolve {label}. "
        f"Expected one of {list(candidates)}, "
        f"found {list(dataframe.columns)}"
    )


def _prepare_metric_scores(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    date_column = _resolve_column(
        dataframe,
        [
            "evaluation_date",
            "date",
        ],
        label="aligned metric evaluation date",
    )

    required = {
        "geo_id",
        "canonical_metric_key",
        "metric_score",
    }

    missing = required - set(
        dataframe.columns
    )

    if missing:
        raise ValueError(
            "aligned_metric_scores is missing columns: "
            f"{sorted(missing)}"
        )

    columns = [
        "geo_id",
        date_column,
        "canonical_metric_key",
        "metric_score",
    ]

    optional_columns = [
        "metric_date",
        "metric_age_days",
        "feature_count",
        "feature_weight_sum",
        "min_feature_score",
        "max_feature_score",
    ]

    columns.extend(
        column
        for column in optional_columns
        if column in dataframe.columns
    )

    out = dataframe[
        columns
    ].copy()

    out = out.rename(
        columns={
            date_column: "date",
        }
    )

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["metric_score"] = pd.to_numeric(
        out["metric_score"],
        errors="coerce",
    )

    invalid = out[
        out["date"].isna()
        | out["metric_score"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "aligned metric scores contain invalid "
            "dates or scores:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate aligned metric-score rows:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_dimension_scores(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    date_column = _resolve_column(
        dataframe,
        [
            "evaluation_date",
            "date",
        ],
        label="dimension-score date",
    )

    required = {
        "geo_id",
        "dimension",
        "dimension_score",
    }

    missing = required - set(
        dataframe.columns
    )

    if missing:
        raise ValueError(
            "dimension_scores is missing columns: "
            f"{sorted(missing)}"
        )

    out = dataframe[
        [
            "geo_id",
            date_column,
            "dimension",
            "dimension_score",
        ]
    ].copy()

    out = out.rename(
        columns={
            date_column: "date",
        }
    )

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["dimension_score"] = pd.to_numeric(
        out["dimension_score"],
        errors="coerce",
    )

    invalid = out[
        out["date"].isna()
        | out["dimension_score"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "dimension scores contain invalid dates "
            "or scores:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "dimension",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate dimension-score rows:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_axis_scores(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    date_column = _resolve_column(
        dataframe,
        [
            "evaluation_date",
            "date",
        ],
        label="axis-score date",
    )

    required = {
        "geo_id",
        "axis",
        "axis_score",
    }

    missing = required - set(
        dataframe.columns
    )

    if missing:
        raise ValueError(
            "axis_scores is missing columns: "
            f"{sorted(missing)}"
        )

    out = dataframe[
        [
            "geo_id",
            date_column,
            "axis",
            "axis_score",
        ]
    ].copy()

    out = out.rename(
        columns={
            date_column: "date",
        }
    )

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["axis_score"] = pd.to_numeric(
        out["axis_score"],
        errors="coerce",
    )

    out = out[
        out["axis"].isin(
            PRODUCTION_AXES
        )
    ].copy()

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "axis",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate axis-score rows:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _prepare_metric_registry(
    config,
) -> pd.DataFrame:
    registry = (
        config.metric_dimensions.copy()
    )

    required = {
        "canonical_metric_key",
        "dimension",
        "metric_weight",
    }

    missing = required - set(
        registry.columns
    )

    if missing:
        raise ValueError(
            "metric_dimension_registry is missing: "
            f"{sorted(missing)}"
        )

    if "enabled" in registry.columns:
        registry = registry[
            _truthy(
                registry["enabled"]
            )
        ].copy()

    if (
        "diagnostic_only"
        in registry.columns
    ):
        registry = registry[
            ~_truthy(
                registry["diagnostic_only"]
            )
        ].copy()

    registry["metric_weight"] = (
        pd.to_numeric(
            registry["metric_weight"],
            errors="coerce",
        )
    )

    invalid = registry[
        registry["metric_weight"].isna()
        | (
            registry["metric_weight"]
            < 0
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Invalid active metric weights:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    registry = registry[
        registry["metric_weight"] > 0
    ].copy()

    canonical_policy_check = (
        registry.groupby(
            "canonical_metric_key",
            dropna=False,
        )
        .agg(
            dimension_count=(
                "dimension",
                "nunique",
            ),
            metric_weight_count=(
                "metric_weight",
                "nunique",
            ),
            dimensions=(
                "dimension",
                lambda values: sorted(
                    set(values.dropna())
                ),
            ),
            metric_weights=(
                "metric_weight",
                lambda values: sorted(
                    set(values.dropna())
                ),
            ),
            source_row_count=(
                "canonical_metric_key",
                "size",
            ),
        )
        .reset_index()
    )

    conflicting_policies = (
        canonical_policy_check[
            (
                canonical_policy_check[
                    "dimension_count"
                ] != 1
            )
            | (
                canonical_policy_check[
                    "metric_weight_count"
                ] != 1
            )
        ]
    )

    if not conflicting_policies.empty:
        raise ValueError(
            "Source rows resolving to the same canonical "
            "metric disagree on dimension or metric weight:\n"
            + conflicting_policies.to_string(
                index=False
            )
        )

    canonical_registry = (
        registry[
            [
                "dimension",
                "canonical_metric_key",
                "metric_weight",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "dimension",
                "canonical_metric_key",
            ]
        )
        .reset_index(drop=True)
    )

    duplicate_canonical_keys = (
        canonical_registry.duplicated(
            subset=[
                "canonical_metric_key",
            ],
            keep=False,
        )
    )

    if duplicate_canonical_keys.any():
        raise ValueError(
            "Canonical metrics remain duplicated after "
            "source-policy consolidation:\n"
            + canonical_registry.loc[
                duplicate_canonical_keys
            ].to_string(index=False)
        )

    return canonical_registry


def _prepare_axis_registry(
    config,
) -> pd.DataFrame:
    registry = config.axes.copy()

    axis_column = _resolve_column(
        registry,
        [
            "axis",
            "axis_name",
        ],
        label="axis registry axis",
    )

    required = {
        "dimension",
        "dimension_weight",
    }

    missing = required - set(
        registry.columns
    )

    if missing:
        raise ValueError(
            "axis_registry is missing columns: "
            f"{sorted(missing)}"
        )

    if "enabled" in registry.columns:
        registry = registry[
            _truthy(
                registry["enabled"]
            )
        ].copy()

    registry = registry.rename(
        columns={
            axis_column: "axis",
        }
    )

    registry = registry[
        registry["axis"].isin(
            PRODUCTION_AXES
        )
    ].copy()

    registry["dimension_weight"] = (
        pd.to_numeric(
            registry["dimension_weight"],
            errors="coerce",
        )
    )

    invalid = registry[
        registry["dimension_weight"].isna()
        | (
            registry[
                "dimension_weight"
            ] < 0
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Invalid active axis dimension weights:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    registry = registry[
        registry["dimension_weight"] > 0
    ].copy()

    duplicate_keys = registry.duplicated(
        subset=[
            "axis",
            "dimension",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise ValueError(
            "Duplicate axis-dimension policies:\n"
            + registry.loc[
                duplicate_keys
            ].to_string(index=False)
        )

    return registry[
        [
            "axis",
            "dimension",
            "dimension_weight",
        ]
    ].copy()


def _build_metric_contributions(
    metric_scores: pd.DataFrame,
    metric_registry: pd.DataFrame,
    dimension_scores: pd.DataFrame,
) -> pd.DataFrame:
    contributions = metric_scores.merge(
        metric_registry,
        on="canonical_metric_key",
        how="inner",
        validate="many_to_one",
    )

    scored_metrics = set(
        metric_scores[
            "canonical_metric_key"
        ].unique()
    )

    governed_metrics = set(
        metric_registry[
            "canonical_metric_key"
        ].unique()
    )

    missing_registry_metrics = (
        scored_metrics - governed_metrics
    )

    if missing_registry_metrics:
        raise ValueError(
            "Aligned metric scores contain canonical metrics "
            "without active contribution policies: "
            f"{sorted(missing_registry_metrics)}"
        )

    group_keys = [
        "geo_id",
        "date",
        "dimension",
    ]

    contributions[
        "available_metric_weight_sum"
    ] = (
        contributions.groupby(
            group_keys
        )["metric_weight"]
        .transform("sum")
    )

    if (
        contributions[
            "available_metric_weight_sum"
        ] <= 0
    ).any():
        raise AssertionError(
            "Metric contribution groups contain "
            "non-positive available weight sums"
        )

    contributions[
        "effective_metric_weight"
    ] = (
        contributions["metric_weight"]
        / contributions[
            "available_metric_weight_sum"
        ]
    )

    contributions[
        "metric_dimension_contribution"
    ] = (
        contributions["metric_score"]
        * contributions[
            "effective_metric_weight"
        ]
    )

    reconciled = (
        contributions.groupby(
            group_keys,
            dropna=False,
        )
        .agg(
            reconstructed_dimension_score=(
                "metric_dimension_contribution",
                "sum",
            ),
            available_metric_count=(
                "canonical_metric_key",
                "nunique",
            ),
            available_metric_weight_sum=(
                "metric_weight",
                "sum",
            ),
        )
        .reset_index()
        .merge(
            dimension_scores,
            on=group_keys,
            how="inner",
            validate="one_to_one",
        )
    )

    reconciled[
        "dimension_reconciliation_error"
    ] = (
        reconciled[
            "reconstructed_dimension_score"
        ]
        - reconciled["dimension_score"]
    )

    maximum_error = reconciled[
        "dimension_reconciliation_error"
    ].abs().max()

    if (
        pd.notna(maximum_error)
        and maximum_error > 1e-9
    ):
        worst = (
            reconciled.assign(
                absolute_error=lambda frame: (
                    frame[
                        "dimension_reconciliation_error"
                    ].abs()
                )
            )
            .sort_values(
                "absolute_error",
                ascending=False,
            )
            .head(30)
        )

        raise AssertionError(
            "Metric contributions do not reconcile "
            "to persisted dimension scores. "
            f"Maximum error: {maximum_error}\n"
            + worst.to_string(index=False)
        )

    contributions = contributions.merge(
        reconciled[
            group_keys
            + [
                "dimension_score",
                "dimension_reconciliation_error",
                "available_metric_count",
            ]
        ],
        on=group_keys,
        how="left",
        validate="many_to_one",
    )

    contributions = contributions.sort_values(
        [
            "geo_id",
            "dimension",
            "canonical_metric_key",
            "date",
        ]
    ).reset_index(drop=True)

    contribution_group = (
        contributions.groupby(
            [
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
            group_keys=False,
        )
    )

    contributions[
        "metric_score_change_1m"
    ] = contribution_group[
        "metric_score"
    ].diff()

    contributions[
        "metric_dimension_contribution_change_1m"
    ] = contribution_group[
        "metric_dimension_contribution"
    ].diff()

    contributions[
        "metric_dimension_contribution_absolute_change_1m"
    ] = contributions[
        "metric_dimension_contribution_change_1m"
    ].abs()

    return contributions


def _build_axis_contributions(
    dimension_scores: pd.DataFrame,
    axis_registry: pd.DataFrame,
    axis_scores: pd.DataFrame,
) -> pd.DataFrame:
    contributions = dimension_scores.merge(
        axis_registry,
        on="dimension",
        how="inner",
        validate="many_to_many",
    )

    group_keys = [
        "geo_id",
        "date",
        "axis",
    ]

    contributions[
        "available_dimension_weight_sum"
    ] = (
        contributions.groupby(
            group_keys
        )["dimension_weight"]
        .transform("sum")
    )

    contributions[
        "effective_dimension_weight"
    ] = (
        contributions["dimension_weight"]
        / contributions[
            "available_dimension_weight_sum"
        ]
    )

    contributions[
        "dimension_axis_contribution"
    ] = (
        contributions["dimension_score"]
        * contributions[
            "effective_dimension_weight"
        ]
    )

    reconciled = (
        contributions.groupby(
            group_keys,
            dropna=False,
        )
        .agg(
            reconstructed_axis_score=(
                "dimension_axis_contribution",
                "sum",
            ),
            available_dimension_count=(
                "dimension",
                "nunique",
            ),
            available_dimension_weight_sum=(
                "dimension_weight",
                "sum",
            ),
        )
        .reset_index()
        .merge(
            axis_scores,
            on=group_keys,
            how="inner",
            validate="one_to_one",
        )
    )

    reconciled[
        "axis_reconciliation_error"
    ] = (
        reconciled[
            "reconstructed_axis_score"
        ]
        - reconciled["axis_score"]
    )

    maximum_error = reconciled[
        "axis_reconciliation_error"
    ].abs().max()

    if (
        pd.notna(maximum_error)
        and maximum_error > 1e-9
    ):
        worst = (
            reconciled.assign(
                absolute_error=lambda frame: (
                    frame[
                        "axis_reconciliation_error"
                    ].abs()
                )
            )
            .sort_values(
                "absolute_error",
                ascending=False,
            )
            .head(30)
        )

        raise AssertionError(
            "Dimension contributions do not reconcile "
            "to persisted axis scores. "
            f"Maximum error: {maximum_error}\n"
            + worst.to_string(index=False)
        )

    contributions = contributions.merge(
        reconciled[
            group_keys
            + [
                "axis_score",
                "axis_reconciliation_error",
                "available_dimension_count",
            ]
        ],
        on=group_keys,
        how="left",
        validate="many_to_one",
    )

    contributions = contributions.sort_values(
        [
            "geo_id",
            "axis",
            "date",
            "dimension",
        ]
    ).reset_index(drop=True)

    # Dimension-score change remains defined only where the
    # dimension exists in consecutive observations.
    contributions[
        "dimension_score_change_1m"
    ] = (
        contributions.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
            ],
            group_keys=False,
        )["dimension_score"]
        .diff()
    )

    # Contribution-change attribution requires a dense panel.
    #
    # When a dimension first becomes available, its prior axis
    # contribution was zero—not unknown. The axis also reweights
    # all previously available dimensions at that date. Their
    # contribution changes are already captured by differencing
    # the effective weighted contributions.
    contribution_wide = (
        contributions.pivot(
            index=[
                "geo_id",
                "axis",
                "date",
            ],
            columns="dimension",
            values="dimension_axis_contribution",
        )
        .sort_index()
    )

    contribution_present = (
        contribution_wide.notna()
    )

    presence_int = (
        contribution_present.astype(int)
    )

    presence_change = (
        presence_int.groupby(
            level=[
                "geo_id",
                "axis",
            ],
            group_keys=False,
        )
        .diff()
    )

    disappearing_dimensions = (
        presence_change.eq(-1)
    )

    if disappearing_dimensions.any().any():
        disappearing = (
            disappearing_dimensions
            .stack()
            .rename(
                "dimension_disappeared"
            )
            .reset_index()
        )

        disappearing = disappearing[
            disappearing[
                "dimension_disappeared"
            ]
        ]

        raise AssertionError(
            "A previously available dimension disappeared. "
            "Axis change attribution requires explicit exit "
            "contribution rows:\n"
            + disappearing.head(30).to_string(
                index=False
            )
        )

    # Missing dimensions contributed zero to the axis at that date.
    contribution_wide = (
        contribution_wide.fillna(0.0)
    )

    contribution_change_wide = (
        contribution_wide.groupby(
            level=[
                "geo_id",
                "axis",
            ],
            group_keys=False,
        )
        .diff()
    )

    contribution_change_long = (
        contribution_change_wide
        .stack(
            dropna=False
        )
        .rename(
            "dimension_axis_contribution_change_1m"
        )
        .reset_index()
    )

    # Keep only rows where the dimension exists at the current
    # date. A disappearing dimension would require a separate
    # exit row; production dimensions currently enter and then
    # remain available.
    contribution_change_long = (
        contribution_change_long.merge(
            contribution_present
            .stack(
                dropna=False
            )
            .rename(
                "dimension_present"
            )
            .reset_index(),
            on=[
                "geo_id",
                "axis",
                "date",
                "dimension",
            ],
            how="left",
            validate="one_to_one",
        )
    )

    contribution_change_long = (
        contribution_change_long[
            contribution_change_long[
                "dimension_present"
            ]
        ]
        .drop(
            columns=[
                "dimension_present",
            ]
        )
    )

    contributions = contributions.merge(
        contribution_change_long,
        on=[
            "geo_id",
            "axis",
            "date",
            "dimension",
        ],
        how="left",
        validate="one_to_one",
    )

    contributions[
        "dimension_axis_contribution_absolute_change_1m"
    ] = contributions[
        "dimension_axis_contribution_change_1m"
    ].abs()

    return contributions


def _build_axis_change_attribution(
    axis_contributions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reconcile month-over-month axis movements to changes in weighted
    dimension contributions.

    Axis changes are calculated once at the axis grain. Dimension
    contribution changes are aggregated independently and then joined.
    """
    group_keys = [
        "geo_id",
        "date",
        "axis",
    ]

    required_columns = {
        "geo_id",
        "date",
        "axis",
        "dimension",
        "axis_score",
        "dimension_axis_contribution_change_1m",
        (
            "dimension_axis_contribution_"
            "absolute_change_1m"
        ),
    }

    missing = (
        required_columns
        - set(axis_contributions.columns)
    )

    if missing:
        raise ValueError(
            "Axis contributions are missing columns "
            "required for change attribution: "
            f"{sorted(missing)}"
        )

    # Build the actual persisted axis history at one row per axis date.
    axis_history = (
        axis_contributions[
            [
                "geo_id",
                "date",
                "axis",
                "axis_score",
            ]
        ]
        .drop_duplicates()
        .sort_values(
            [
                "geo_id",
                "axis",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    duplicate_axis_rows = axis_history.duplicated(
        subset=[
            "geo_id",
            "date",
            "axis",
        ],
        keep=False,
    )

    if duplicate_axis_rows.any():
        raise AssertionError(
            "Axis history contains duplicate axis-date rows:\n"
            + axis_history.loc[
                duplicate_axis_rows
            ].head(30).to_string(index=False)
        )

    axis_history[
        "axis_score_change_1m"
    ] = (
        axis_history.groupby(
            [
                "geo_id",
                "axis",
            ],
            group_keys=False,
        )["axis_score"]
        .diff()
    )

    # Sum all weighted dimension-contribution changes at the axis grain.
    contribution_change = (
        axis_contributions.groupby(
            group_keys,
            dropna=False,
        )
        .agg(
            reconstructed_axis_change_1m=(
                (
                    "dimension_axis_"
                    "contribution_change_1m"
                ),
                lambda values: values.sum(
                    min_count=1
                ),
            ),
            gross_dimension_contribution_change_1m=(
                (
                    "dimension_axis_"
                    "contribution_absolute_change_1m"
                ),
                lambda values: values.sum(
                    min_count=1
                ),
            ),
            contributing_dimension_count=(
                "dimension",
                "nunique",
            ),
        )
        .reset_index()
    )

    attribution = contribution_change.merge(
        axis_history,
        on=group_keys,
        how="left",
        validate="one_to_one",
    )

    required_result_columns = {
        "axis_score",
        "axis_score_change_1m",
        "reconstructed_axis_change_1m",
        "gross_dimension_contribution_change_1m",
    }

    missing_result_columns = (
        required_result_columns
        - set(attribution.columns)
    )

    if missing_result_columns:
        raise AssertionError(
            "Axis change attribution merge did not "
            "produce required columns: "
            f"{sorted(missing_result_columns)}. "
            f"Actual columns: "
            f"{sorted(attribution.columns)}"
        )

    attribution[
        "axis_change_reconciliation_error"
    ] = (
        attribution[
            "reconstructed_axis_change_1m"
        ]
        - attribution[
            "axis_score_change_1m"
        ]
    )

    gross = pd.to_numeric(
        attribution[
            "gross_dimension_contribution_change_1m"
        ],
        errors="coerce",
    )

    net = pd.to_numeric(
        attribution[
            "axis_score_change_1m"
        ],
        errors="coerce",
    ).abs()

    attribution[
        "offsetting_dimension_change_1m"
    ] = (
        gross - net
    ).clip(lower=0.0)

    attribution[
        "dimension_cancellation_ratio"
    ] = np.where(
        gross > 0,
        1.0 - (net / gross),
        np.nan,
    )

    # Floating-point noise can put ratios infinitesimally outside
    # the theoretical [0, 1] range.
    attribution[
        "dimension_cancellation_ratio"
    ] = attribution[
        "dimension_cancellation_ratio"
    ].clip(
        lower=0.0,
        upper=1.0,
    )

    return (
        attribution.sort_values(
            [
                "geo_id",
                "axis",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_dimension_contribution_summary(
    contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        contributions.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            configured_weight=(
                "dimension_weight",
                "first",
            ),
            average_effective_weight=(
                "effective_dimension_weight",
                "mean",
            ),
            mean_contribution=(
                "dimension_axis_contribution",
                "mean",
            ),
            mean_absolute_contribution=(
                "dimension_axis_contribution",
                lambda values: values.abs().mean(),
            ),
            mean_absolute_contribution_change_1m=(
                "dimension_axis_contribution_change_1m",
                lambda values: values.abs().mean(),
            ),
            p90_absolute_contribution_change_1m=(
                "dimension_axis_contribution_change_1m",
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_contribution_change_1m=(
                "dimension_axis_contribution_change_1m",
                lambda values: values.abs().max(),
            ),
        )
        .reset_index()
        .sort_values(
            [
                "geo_id",
                "axis",
                (
                    "mean_absolute_"
                    "contribution_change_1m"
                ),
            ],
            ascending=[
                True,
                True,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def _build_metric_contribution_summary(
    contributions: pd.DataFrame,
) -> pd.DataFrame:
    return (
        contributions.groupby(
            [
                "geo_id",
                "dimension",
                "canonical_metric_key",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            configured_metric_weight=(
                "metric_weight",
                "first",
            ),
            average_effective_metric_weight=(
                "effective_metric_weight",
                "mean",
            ),
            mean_metric_score=(
                "metric_score",
                "mean",
            ),
            metric_score_std=(
                "metric_score",
                "std",
            ),
            mean_absolute_metric_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().mean(),
            ),
            p90_absolute_metric_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_metric_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().max(),
            ),
            mean_absolute_dimension_contribution=(
                "metric_dimension_contribution",
                lambda values: values.abs().mean(),
            ),
            mean_absolute_dimension_contribution_change_1m=(
                "metric_dimension_contribution_change_1m",
                lambda values: values.abs().mean(),
            ),
            p90_absolute_dimension_contribution_change_1m=(
                "metric_dimension_contribution_change_1m",
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_dimension_contribution_change_1m=(
                "metric_dimension_contribution_change_1m",
                lambda values: values.abs().max(),
            ),
        )
        .reset_index()
        .sort_values(
            [
                "geo_id",
                "dimension",
                (
                    "mean_absolute_dimension_"
                    "contribution_change_1m"
                ),
            ],
            ascending=[
                True,
                True,
                False,
            ],
        )
        .reset_index(drop=True)
    )


def build_axis_contribution_audit(
    run_id: str = DEFAULT_RUN_ID,
    *,
    artifact_root: str | Path = (
        DEFAULT_ARTIFACT_ROOT
    ),
    geo_ids: list[str] | None = None,
) -> dict[str, pd.DataFrame]:
    """
    Reconstruct metric-to-dimension and dimension-to-axis
    contributions from persisted artifacts and frozen registries.

    No pipeline stages are recomputed.
    """
    store = RegimeArtifactStore(
        artifact_root
    )

    manifest = store.read_manifest(
        run_id
    )

    if manifest.get("status") != "complete":
        raise ValueError(
            f"Run {run_id!r} is not complete: "
            f"{manifest.get('status')!r}"
        )

    if geo_ids is None:
        geo_ids = DEFAULT_AUDIT_GEOS.copy()

    config = load_regime_config(
        validate=True
    )

    metric_registry = (
        _prepare_metric_registry(config)
    )

    axis_registry = (
        _prepare_axis_registry(config)
    )

    metric_scores = _prepare_metric_scores(
        store.read_dataframe(
            run_id,
            "aligned_metric_scores",
        )
    )

    dimension_scores = (
        _prepare_dimension_scores(
            store.read_dataframe(
                run_id,
                "dimension_scores",
            )
        )
    )

    axis_scores = _prepare_axis_scores(
        store.read_dataframe(
            run_id,
            "axis_scores",
        )
    )

    metric_scores = metric_scores[
        metric_scores["geo_id"].isin(
            geo_ids
        )
    ].copy()

    dimension_scores = dimension_scores[
        dimension_scores["geo_id"].isin(
            geo_ids
        )
    ].copy()

    axis_scores = axis_scores[
        axis_scores["geo_id"].isin(
            geo_ids
        )
    ].copy()

    metric_contributions = (
        _build_metric_contributions(
            metric_scores,
            metric_registry,
            dimension_scores,
        )
    )

    axis_contributions = (
        _build_axis_contributions(
            dimension_scores,
            axis_registry,
            axis_scores,
        )
    )

    axis_change_attribution = (
        _build_axis_change_attribution(
            axis_contributions
        )
    )

    dimension_summary = (
        _build_dimension_contribution_summary(
            axis_contributions
        )
    )

    metric_summary = (
        _build_metric_contribution_summary(
            metric_contributions
        )
    )

    focus_metric_summary = metric_summary[
        metric_summary[
            "canonical_metric_key"
        ].isin(FOCUS_METRICS)
    ].copy()

    active_inventory_history = (
        metric_contributions[
            metric_contributions[
                "canonical_metric_key"
            ].eq("active_inventory")
        ]
        .sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    highest_cancellation_months = (
        axis_change_attribution
        .dropna(
            subset=[
                "dimension_cancellation_ratio"
            ]
        )
        .sort_values(
            [
                "dimension_cancellation_ratio",
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                ),
            ],
            ascending=[
                False,
                False,
            ],
        )
        .head(100)
        .reset_index(drop=True)
    )

    return {
        "metric_contributions": (
            metric_contributions
        ),
        "axis_contributions": (
            axis_contributions
        ),
        "axis_change_attribution": (
            axis_change_attribution
        ),
        "dimension_contribution_summary": (
            dimension_summary
        ),
        "metric_contribution_summary": (
            metric_summary
        ),
        "focus_metric_summary": (
            focus_metric_summary
        ),
        "active_inventory_history": (
            active_inventory_history
        ),
        "highest_cancellation_months": (
            highest_cancellation_months
        ),
    }
