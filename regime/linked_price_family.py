from __future__ import annotations
# regime/linked_price_family.py

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

SHORT_LAG_PERIODS = 3
LONG_LAG_PERIODS = 12

LINKED_PRICE_FAMILY_DERIVED_METRICS = (
    "price_to_income",
    "payment_burden",
)


@dataclass(frozen=True)
class PriceFamilyStructuralCandidate:
    experiment_id: str
    level_window: int
    short_lag_periods: int = SHORT_LAG_PERIODS
    long_lag_periods: int = LONG_LAG_PERIODS
    description: str = ""


PRICE_FAMILY_STRUCTURAL_CANDIDATES: dict[str, PriceFamilyStructuralCandidate] = {
    "price_family_ma6_structural_linked": PriceFamilyStructuralCandidate(
        experiment_id="price_family_ma6_structural_linked",
        level_window=6,
        description="Price family MA6 structural linked candidate",
    ),
    "price_family_ma9_structural_linked": PriceFamilyStructuralCandidate(
        experiment_id="price_family_ma9_structural_linked",
        level_window=9,
        description="Price family MA9 structural linked candidate",
    ),
    "price_family_ma12_structural_linked": PriceFamilyStructuralCandidate(
        experiment_id="price_family_ma12_structural_linked",
        level_window=12,
        description="Price family MA12 structural linked candidate",
    ),
}

PRICE_FAMILY_EXPERIMENT_ALIASES = {
    "price_family_ma12_momentum_lag3": "price_family_ma12_structural_linked",
}

DEFAULT_PRICE_FAMILY_EXPERIMENT_ID = "price_family_ma12_momentum_lag3"

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


def get_price_family_structural_candidate(
    experiment_id: str,
) -> PriceFamilyStructuralCandidate:
    if not experiment_id.strip():
        raise ValueError(
            "experiment_id must be non-empty"
        )

    canonical_id = PRICE_FAMILY_EXPERIMENT_ALIASES.get(
        experiment_id,
        experiment_id,
    )

    try:
        candidate = PRICE_FAMILY_STRUCTURAL_CANDIDATES[canonical_id]
    except KeyError as exc:
        raise ValueError(
            "Unknown price-family structural experiment_id "
            f"{experiment_id!r}; expected one of "
            f"{sorted(PRICE_FAMILY_STRUCTURAL_CANDIDATES)} "
            f"or aliases {sorted(PRICE_FAMILY_EXPERIMENT_ALIASES)}"
        ) from exc

    if candidate.level_window <= 0:
        raise ValueError(
            "Price-family level_window must be positive"
        )
    if candidate.short_lag_periods <= 0 or candidate.long_lag_periods <= 0:
        raise ValueError(
            "Price-family lag periods must be positive"
        )

    return candidate


@dataclass(frozen=True)
class LinkedPriceFamilyObservationResult:
    substituted_sources: pd.DataFrame
    source_substitution_lineage: pd.DataFrame
    derived_metrics: pd.DataFrame
    derived_lineage: pd.DataFrame
    source_level_history: pd.DataFrame
    derived_level_history: pd.DataFrame


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


def build_structural_level(
    observations: pd.DataFrame,
    *,
    level_window: int,
    value_column: str = "value",
) -> pd.DataFrame:
    """
    Build a full-window trailing structural level for one or more metrics.
    No partial-window values are emitted.
    """
    if level_window <= 0:
        raise ValueError("level_window must be positive")
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
            window=level_window,
            min_periods=level_window,
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


def build_ma12_level(
    observations: pd.DataFrame,
    *,
    value_column: str = "value",
) -> pd.DataFrame:
    """Backward-compatible wrapper for the MA12 structural level."""
    return build_structural_level(
        observations,
        level_window=12,
        value_column=value_column,
    )


def build_same_state_features(
    level_history: pd.DataFrame,
    *,
    level_value_column: str = (
        "structural_level_value"
    ),
    experiment_id: str,
    feature_origin: str,
    level_window: int,
    short_lag_periods: int = SHORT_LAG_PERIODS,
    long_lag_periods: int = LONG_LAG_PERIODS,
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
            short_lag_periods
        )
    )

    work["long_reference_value"] = (
        grouped[
            level_value_column
        ].shift(
            long_lag_periods
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
            short_lag_periods,
        ),
        (
            "long",
            "long_feature_value",
            "long_reference_value",
            long_lag_periods,
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
            level_window
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


def build_linked_price_family_observations(
    source_metrics: pd.DataFrame,
    *,
    experiment_id: str = (
        DEFAULT_PRICE_FAMILY_EXPERIMENT_ID
    ),
) -> LinkedPriceFamilyObservationResult:
    """
    Build linked structural Price-family observations.

    The source panel returned here contains a temporary structural
    median-sale-price substitution used for derived-metric
    recalculation. It is not intended to replace the canonical raw
    source panel used for direct source-metric features.
    """
    candidate = get_price_family_structural_candidate(
        experiment_id
    )

    price_rows = _source_metric_rows(
        source_metrics,
        metric_key="median_sale_price",
    )

    ppsf_rows = _source_metric_rows(
        source_metrics,
        metric_key="median_ppsf",
    )

    price_level = build_structural_level(
        price_rows,
        level_window=candidate.level_window,
        value_column="value",
    )

    ppsf_level = build_structural_level(
        ppsf_rows,
        level_window=candidate.level_window,
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

    derived_level = _derived_level_history(
        derived_metrics
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
    ).sort_values(
        [
            "canonical_metric_key",
            "geo_id",
            "date",
        ]
    ).reset_index(
        drop=True
    )

    return LinkedPriceFamilyObservationResult(
        substituted_sources=(
            substitution.source_metrics
        ),
        source_substitution_lineage=(
            substitution.substitution_lineage
        ),
        derived_metrics=derived_metrics,
        derived_lineage=derived_lineage,
        source_level_history=source_level_history,
        derived_level_history=(
            derived_level[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "structural_level_value",
                ]
            ]
            .sort_values(
                [
                    "canonical_metric_key",
                    "geo_id",
                    "date",
                ]
            )
            .reset_index(drop=True)
        ),
    )


def apply_linked_price_family_augmentation(
    observations: pd.DataFrame,
    derived_lineage: pd.DataFrame,
    *,
    experiment_id: str = (
        DEFAULT_PRICE_FAMILY_EXPERIMENT_ID
    ),
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Replace canonical Price/Affordability derived observations with
    linked structural versions recomputed from a temporary smoothed
    median-sale-price source.

    The canonical source observations are not modified. Only
    price_to_income and payment_burden observations and their
    corresponding lineage rows are replaced.
    """
    required_observation_columns = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "metric_origin",
    }

    missing_observation_columns = (
        required_observation_columns
        - set(observations.columns)
    )

    if missing_observation_columns:
        raise ValueError(
            "Linked Price-family augmentation observations are "
            "missing required columns: "
            f"{sorted(missing_observation_columns)}"
        )

    required_lineage_columns = {
        "geo_id",
        "date",
        "derived_metric_key",
        "component_metric_key",
        "component_value",
        "component_source_date",
        "component_source_geo_id",
        "component_age_days",
        "component_age_months",
        "was_carried_forward",
    }

    missing_lineage_columns = (
        required_lineage_columns
        - set(derived_lineage.columns)
    )

    if missing_lineage_columns:
        raise ValueError(
            "Linked Price-family augmentation lineage is missing "
            "required columns: "
            f"{sorted(missing_lineage_columns)}"
        )

    canonical_observations = observations.copy()
    canonical_lineage = derived_lineage.copy()

    source_metrics = canonical_observations[
        ~canonical_observations[
            "metric_origin"
        ].eq("derived")
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
        ]
    ].copy()

    linked = build_linked_price_family_observations(
        source_metrics,
        experiment_id=experiment_id,
    )

    replacement_metrics = linked.derived_metrics[
        linked.derived_metrics[
            "canonical_metric_key"
        ].isin(
            LINKED_PRICE_FAMILY_DERIVED_METRICS
        )
    ].copy()

    replacement_lineage = linked.derived_lineage[
        linked.derived_lineage[
            "derived_metric_key"
        ].isin(
            LINKED_PRICE_FAMILY_DERIVED_METRICS
        )
    ].copy()

    replacement_metrics["metric_origin"] = "derived"

    retained_observations = canonical_observations[
        ~canonical_observations[
            "canonical_metric_key"
        ].isin(
            LINKED_PRICE_FAMILY_DERIVED_METRICS
        )
    ].copy()

    augmented_observations = pd.concat(
        [
            retained_observations,
            replacement_metrics[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "value",
                    "metric_origin",
                ]
            ],
        ],
        ignore_index=True,
    )

    retained_lineage = canonical_lineage[
        ~canonical_lineage[
            "derived_metric_key"
        ].isin(
            LINKED_PRICE_FAMILY_DERIVED_METRICS
        )
    ].copy()

    augmented_lineage = pd.concat(
        [
            retained_lineage,
            replacement_lineage[
                list(canonical_lineage.columns)
            ],
        ],
        ignore_index=True,
    )

    duplicate_observations = (
        augmented_observations.duplicated(
            subset=[
                "geo_id",
                "date",
                "canonical_metric_key",
            ],
            keep=False,
        )
    )

    if duplicate_observations.any():
        raise AssertionError(
            "Linked Price-family augmentation produced duplicate "
            "observations:\n"
            + augmented_observations.loc[
                duplicate_observations
            ]
            .sort_values(
                [
                    "geo_id",
                    "canonical_metric_key",
                    "date",
                ]
            )
            .head(50)
            .to_string(index=False)
        )

    duplicate_lineage = augmented_lineage.duplicated(
        subset=[
            "geo_id",
            "date",
            "derived_metric_key",
            "component_metric_key",
        ],
        keep=False,
    )

    if duplicate_lineage.any():
        raise AssertionError(
            "Linked Price-family augmentation produced duplicate "
            "derived lineage:\n"
            + augmented_lineage.loc[
                duplicate_lineage
            ]
            .sort_values(
                [
                    "geo_id",
                    "derived_metric_key",
                    "date",
                    "component_metric_key",
                ]
            )
            .head(50)
            .to_string(index=False)
        )

    augmented_observations = (
        augmented_observations.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    augmented_lineage = (
        augmented_lineage.sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "date",
                "component_metric_key",
            ]
        )
        .reset_index(drop=True)
    )

    return (
        augmented_observations,
        augmented_lineage,
    )


def build_linked_price_family_features(
    source_metrics: pd.DataFrame,
    *,
    experiment_id: str = (
        DEFAULT_PRICE_FAMILY_EXPERIMENT_ID
    ),
) -> LinkedPriceFamilyResult:
    """
    Build a linked structural price-family experiment.

    Contract
    --------
    median_sale_price:
        level = MA(window)(raw price)
        short = level / lag3(level) - 1
        long = level / lag12(level) - 1

    median_ppsf:
        same transform, calculated independently

    price_to_income:
        recompute level from substituted MA(window) price and preserved income,
        then calculate same-state lag3/lag12 features

    payment_burden:
        recompute level from substituted MA(window) price, preserved income,
        preserved mortgage rates, and the canonical payment formula,
        then calculate same-state lag3/lag12 features
    """

    candidate = get_price_family_structural_candidate(
        experiment_id
    )

    observation_result = (
        build_linked_price_family_observations(
            source_metrics,
            experiment_id=experiment_id,
        )
    )

    source_level_history = (
        observation_result.source_level_history
    )

    derived_level = (
        observation_result.derived_level_history
    )

    level_history = pd.concat(
        [
            source_level_history,
            derived_level,
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
            level_window=candidate.level_window,
            short_lag_periods=candidate.short_lag_periods,
            long_lag_periods=candidate.long_lag_periods,
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
            level_window=candidate.level_window,
            short_lag_periods=candidate.short_lag_periods,
            long_lag_periods=candidate.long_lag_periods,
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
        substituted_sources=(observation_result.substituted_sources),
        source_substitution_lineage=(observation_result.source_substitution_lineage),
        derived_metrics=(observation_result.derived_metrics),
        derived_lineage=(observation_result.derived_lineage),
        level_history=level_history,
        feature_history=feature_history,
    )
