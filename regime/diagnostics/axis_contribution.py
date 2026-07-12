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

    duplicate_keys = registry.duplicated(
        subset=[
            "dimension",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise ValueError(
            "Duplicate metric-to-dimension policies:\n"
            + registry.loc[
                duplicate_keys
            ].to_string(index=False)
        )

    return registry[
        [
            "dimension",
            "canonical_metric_key",
            "metric_weight",
        ]
    ].copy()


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
        validate="many_to_many",
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
            "dimension",
            "date",
        ]
    ).reset_index(drop=True)

    contribution_group = (
        contributions.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
            ],
            group_keys=False,
        )
    )

    contributions[
        "dimension_score_change_1m"
    ] = contribution_group[
        "dimension_score"
    ].diff()

    contributions[
        "dimension_axis_contribution_change_1m"
    ] = contribution_group[
        "dimension_axis_contribution"
    ].diff()

    contributions[
        "dimension_axis_contribution_absolute_change_1m"
    ] = contributions[
        "dimension_axis_contribution_change_1m"
    ].abs()

    return contributions


def _build_axis_change_attribution(
    axis_contributions: pd.DataFrame,
) -> pd.DataFrame:
    group_keys = [
        "geo_id",
        "date",
        "axis",
    ]

    work = axis_contributions.copy()

    work["axis_score_change_1m"] = (
        work.groupby(
            [
                "geo_id",
                "axis",
                "dimension",
            ],
            group_keys=False,
        )["axis_score"]
        .diff()
    )

    attribution = (
        work.groupby(
            group_keys,
            dropna=False,
        )
        .agg(
            axis_score=(
                "axis_score",
                "first",
            ),
            axis_score_change_1m=(
                "axis_score_change_1m",
                "first",
            ),
            reconstructed_axis_change_1m=(
                "dimension_axis_contribution_change_1m",
                "sum",
            ),
            gross_dimension_contribution_change_1m=(
                "dimension_axis_contribution_absolute_change_1m",
                "sum",
            ),
        )
        .reset_index()
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

    gross = attribution[
        "gross_dimension_contribution_change_1m"
    ]

    net = attribution[
        "axis_score_change_1m"
    ].abs()

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

    return attribution


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
