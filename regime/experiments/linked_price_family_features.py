from __future__ import annotations
# regime/experiments/linked_price_family_features.py

from dataclasses import dataclass

import numpy as np
import pandas as pd

from regime.derived_metrics import (
    build_derived_metrics_with_lineage,
)
from regime.experiments.source_substitution import (
    SourceSubstitutionResult,
    apply_metric_source_substitution,
)


PRICE_SOURCE_METRICS = (
    "median_sale_price",
    "median_ppsf",
)

DERIVED_PRICE_METRICS = (
    "price_to_income",
    "payment_burden",
)

PRICE_FAMILY_METRICS = (
    *PRICE_SOURCE_METRICS,
    *DERIVED_PRICE_METRICS,
)

LEVEL_WINDOW = 12
SHORT_LAG_PERIODS = 3
LONG_LAG_PERIODS = 12

FEATURE_COMPONENTS = (
    "level",
    "short",
    "long",
)

FEATURE_OUTPUT_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "feature_component",
    "raw_feature_value",
    "source_level_value",
    "reference_value",
    "price_family_experiment_id",
    "level_window",
    "lag_periods",
    "feature_origin",
]


@dataclass(frozen=True)
class LinkedPriceFamilyResult:
    substituted_sources: pd.DataFrame
    source_substitution_lineage: pd.DataFrame
    derived_metrics: pd.DataFrame
    derived_lineage: pd.DataFrame
    level_history: pd.DataFrame
    feature_history: pd.DataFrame


def _safe_ratio_minus_one(
    numerator: pd.Series,
    denominator: pd.Series,
) -> pd.Series:
    output = (
        pd.to_numeric(
            numerator,
            errors="coerce",
        )
        / pd.to_numeric(
            denominator,
            errors="coerce",
        )
        - 1.0
    )

    return output.replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )


def _validate_metric_frame(
    frame: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        value_column,
    }

    missing = required - set(
        frame.columns
    )

    if missing:
        raise ValueError(
            "Price-family input is missing "
            f"required columns: {sorted(missing)}"
        )

    work = frame.copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work[value_column] = pd.to_numeric(
        work[value_column],
        errors="coerce",
    )

    if work["date"].isna().any():
        raise ValueError(
            "Price-family input contains invalid dates"
        )

    duplicate_mask = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicate_mask.any():
        raise ValueError(
            "Price-family input is not unique by "
            "geo/date/metric:\n"
            + work.loc[
                duplicate_mask
            ].head(30).to_string(
                index=False
            )
        )

    return work.sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "date",
        ]
    ).reset_index(
        drop=True
    )


def build_ma12_level(
    observations: pd.DataFrame,
    *,
    value_column: str = "value",
) -> pd.DataFrame:
    """
    Build the full-window MA12 level for one or more canonical metrics.
    No partial-window values are emitted.
    """
    work = _validate_metric_frame(
        observations,
        value_column=value_column,
    )

    grouped = work.groupby(
        [
            "geo_id",
            "canonical_metric_key",
        ],
        group_keys=False,
        sort=False,
    )

    work["structural_level_value"] = (
        grouped[value_column]
        .rolling(
            window=LEVEL_WINDOW,
            min_periods=LEVEL_WINDOW,
        )
        .mean()
        .reset_index(
            level=[
                0,
                1,
            ],
            drop=True,
        )
    )

    return work


def build_same_state_features(
    level_history: pd.DataFrame,
    *,
    level_value_column: str = (
        "structural_level_value"
    ),
    experiment_id: str,
    feature_origin: str,
) -> pd.DataFrame:
    """
    Build same-state structural features:

        level = structural level
        short = level / lag3(level) - 1
        long  = level / lag12(level) - 1
    """
    work = _validate_metric_frame(
        level_history,
        value_column=level_value_column,
    )

    grouped = work.groupby(
        [
            "geo_id",
            "canonical_metric_key",
        ],
        group_keys=False,
        sort=False,
    )

    work["short_reference_value"] = (
        grouped[
            level_value_column
        ].shift(
            SHORT_LAG_PERIODS
        )
    )

    work["long_reference_value"] = (
        grouped[
            level_value_column
        ].shift(
            LONG_LAG_PERIODS
        )
    )

    work["short_feature_value"] = (
        _safe_ratio_minus_one(
            work[
                level_value_column
            ],
            work[
                "short_reference_value"
            ],
        )
    )

    work["long_feature_value"] = (
        _safe_ratio_minus_one(
            work[
                level_value_column
            ],
            work[
                "long_reference_value"
            ],
        )
    )

    frames: list[pd.DataFrame] = []

    for (
        component,
        value_column,
        reference_column,
        lag_periods,
    ) in (
        (
            "level",
            level_value_column,
            None,
            0,
        ),
        (
            "short",
            "short_feature_value",
            "short_reference_value",
            SHORT_LAG_PERIODS,
        ),
        (
            "long",
            "long_feature_value",
            "long_reference_value",
            LONG_LAG_PERIODS,
        ),
    ):
        output = work[
            [
                "geo_id",
                "date",
                "canonical_metric_key",
                level_value_column,
            ]
        ].copy()

        output["feature_component"] = (
            component
        )

        output["raw_feature_value"] = (
            work[value_column]
        )

        output["source_level_value"] = (
            work[level_value_column]
        )

        if reference_column is None:
            output["reference_value"] = (
                np.nan
            )
        else:
            output["reference_value"] = (
                work[reference_column]
            )

        output[
            "price_family_experiment_id"
        ] = experiment_id

        output["level_window"] = (
            LEVEL_WINDOW
        )

        output["lag_periods"] = (
            lag_periods
        )

        output["feature_origin"] = (
            feature_origin
        )

        frames.append(
            output[
                FEATURE_OUTPUT_COLUMNS
            ]
        )

    return pd.concat(
        frames,
        ignore_index=True,
    ).sort_values(
        [
            "canonical_metric_key",
            "geo_id",
            "date",
            "feature_component",
        ]
    ).reset_index(
        drop=True
    )


def _source_metric_rows(
    source_metrics: pd.DataFrame,
    *,
    metric_key: str,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
    }

    missing = required - set(
        source_metrics.columns
    )

    if missing:
        raise ValueError(
            "Canonical source panel is missing "
            f"required columns: {sorted(missing)}"
        )

    rows = source_metrics[
        source_metrics[
            "canonical_metric_key"
        ].eq(metric_key)
    ].copy()

    if rows.empty:
        raise ValueError(
            "Canonical source panel contains no "
            f"{metric_key!r} rows"
        )

    return rows


def _derived_level_history(
    derived_metrics: pd.DataFrame,
) -> pd.DataFrame:
    frame = derived_metrics[
        derived_metrics[
            "canonical_metric_key"
        ].isin(
            DERIVED_PRICE_METRICS
        )
    ].copy()

    if frame.empty:
        raise ValueError(
            "No linked derived price metrics "
            "were produced"
        )

    frame = frame.rename(
        columns={
            "value": "structural_level_value",
        }
    )

    return frame


def build_linked_price_family_features(
    source_metrics: pd.DataFrame,
    *,
    experiment_id: str = (
        "price_family_ma12_momentum_lag3"
    ),
) -> LinkedPriceFamilyResult:
    """
    Build the linked MA12 price-family experiment.

    Contract
    --------
    median_sale_price:
        level = MA12(raw price)
        short = level / lag3(level) - 1
        long = level / lag12(level) - 1

    median_ppsf:
        same transform, calculated independently

    price_to_income:
        recompute level from substituted MA12 price and preserved income,
        then calculate same-state lag3/lag12 features

    payment_burden:
        recompute level from substituted MA12 price, preserved income,
        preserved mortgage rates, and the canonical payment formula,
        then calculate same-state lag3/lag12 features
    """
    if not experiment_id.strip():
        raise ValueError(
            "experiment_id must be non-empty"
        )

    price_rows = _source_metric_rows(
        source_metrics,
        metric_key="median_sale_price",
    )

    ppsf_rows = _source_metric_rows(
        source_metrics,
        metric_key="median_ppsf",
    )

    price_level = build_ma12_level(
        price_rows,
        value_column="value",
    )

    ppsf_level = build_ma12_level(
        ppsf_rows,
        value_column="value",
    )

    substitution: SourceSubstitutionResult = (
        apply_metric_source_substitution(
            source_metrics,
            price_level[
                [
                    "geo_id",
                    "date",
                    "structural_level_value",
                ]
            ],
            metric_key="median_sale_price",
            substitution_id=experiment_id,
            replacement_value_column=(
                "structural_level_value"
            ),
            missing_policy="null",
        )
    )

    (
        derived_metrics,
        derived_lineage,
    ) = build_derived_metrics_with_lineage(
        substitution.source_metrics
    )

    derived_level = (
        _derived_level_history(
            derived_metrics
        )
    )

    source_level_history = pd.concat(
        [
            price_level[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "structural_level_value",
                ]
            ],
            ppsf_level[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "structural_level_value",
                ]
            ],
        ],
        ignore_index=True,
    )

    level_history = pd.concat(
        [
            source_level_history,
            derived_level[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "structural_level_value",
                ]
            ],
        ],
        ignore_index=True,
    ).sort_values(
        [
            "canonical_metric_key",
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )

    source_features = (
        build_same_state_features(
            source_level_history,
            experiment_id=experiment_id,
            feature_origin=(
                "smoothed_source"
            ),
        )
    )

    derived_features = (
        build_same_state_features(
            derived_level[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "structural_level_value",
                ]
            ],
            experiment_id=experiment_id,
            feature_origin=(
                "recomputed_derived"
            ),
        )
    )

    feature_history = pd.concat(
        [
            source_features,
            derived_features,
        ],
        ignore_index=True,
    ).sort_values(
        [
            "canonical_metric_key",
            "geo_id",
            "date",
            "feature_component",
        ]
    ).reset_index(
        drop=True
    )

    actual_metrics = set(
        feature_history[
            "canonical_metric_key"
        ].unique()
    )

    expected_metrics = set(
        PRICE_FAMILY_METRICS
    )

    if actual_metrics != expected_metrics:
        raise AssertionError(
            "Linked price-family feature metrics "
            "do not match the contract. "
            f"Expected {sorted(expected_metrics)}, "
            f"found {sorted(actual_metrics)}"
        )

    nonfinite = feature_history[
        "raw_feature_value"
    ].replace(
        [
            np.inf,
            -np.inf,
        ],
        np.nan,
    )

    if (
        nonfinite.notna().sum()
        != feature_history[
            "raw_feature_value"
        ].notna().sum()
    ):
        raise AssertionError(
            "Linked price-family features "
            "contain infinity"
        )

    return LinkedPriceFamilyResult(
        substituted_sources=(
            substitution.source_metrics
        ),
        source_substitution_lineage=(
            substitution.substitution_lineage
        ),
        derived_metrics=derived_metrics,
        derived_lineage=derived_lineage,
        level_history=level_history,
        feature_history=feature_history,
    )
