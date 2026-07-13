from __future__ import annotations
# regime/experiments/smoothing_run.py

import numpy as np
import pandas as pd

from regime.experiments.smoothing_features import (
    build_smoothed_metric_features,
)
from regime.experiments.smoothing_policy import (
    SmoothingExperiment,
    load_smoothing_experiments,
)


SUPPORTED_PIPELINE_EXPERIMENTS = {
    "inventory_ma3_deviation",
}

TARGET_METRIC = "active_inventory"

PRODUCTION_FEATURE_KEY_MAP = {
    "level": "redfin_inventory_level",
    "short": "redfin_inventory_short",
    "long": "redfin_inventory_long",
}

TARGET_FEATURE_KEYS = set(
    PRODUCTION_FEATURE_KEY_MAP.values()
)

FEATURE_COLUMNS = [
    "geo_id",
    "date",
    "canonical_metric_key",
    "feature_key",
    "raw_feature_value",
]

LINEAGE_COLUMNS = [
    "experiment_id",
    "experiment_name",
    "parent_run",
    "geo_id",
    "date",
    "canonical_metric_key",
    "feature_key",
    "feature_component",
    "transform_strategy",
    "policy_role",
    "level_window",
    "short_window",
    "short_lag_periods",
    "long_window",
    "long_lag_periods",
    "source_metric_origin",
    "source_value",
    "challenger_feature_value",
]


def _validate_feature_frame(
    features: pd.DataFrame,
) -> pd.DataFrame:
    missing = (
        set(FEATURE_COLUMNS)
        - set(features.columns)
    )

    if missing:
        raise ValueError(
            "Feature frame is missing required columns: "
            f"{sorted(missing)}"
        )

    work = features[
        FEATURE_COLUMNS
    ].copy()

    work["date"] = pd.to_datetime(
        work["date"],
        errors="coerce",
    )

    work["raw_feature_value"] = pd.to_numeric(
        work["raw_feature_value"],
        errors="coerce",
    )

    invalid_keys = work[
        work["geo_id"].isna()
        | work["date"].isna()
        | work[
            "canonical_metric_key"
        ].isna()
        | work["feature_key"].isna()
    ]

    if not invalid_keys.empty:
        raise ValueError(
            "Feature frame contains invalid "
            "keys or dates:\n"
            + invalid_keys.head(
                30
            ).to_string(
                index=False
            )
        )

    non_numeric_values = work[
        work["raw_feature_value"].isna()
    ]

    if not non_numeric_values.empty:
        raise ValueError(
            "Feature frame contains missing or "
            "non-numeric feature values:\n"
            + non_numeric_values.head(
                30
            ).to_string(
                index=False
            )
        )

    duplicates = work.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Feature frame contains duplicate keys:\n"
            + work.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    return work


def _load_experiment(
    experiment_id: str,
) -> SmoothingExperiment:
    experiments = (
        load_smoothing_experiments(
            validate=True
        )
    )

    if (
        experiment_id
        not in experiments
    ):
        raise KeyError(
            "Unknown smoothing experiment: "
            f"{experiment_id}"
        )

    if (
        experiment_id
        not in SUPPORTED_PIPELINE_EXPERIMENTS
    ):
        raise ValueError(
            "Smoothing experiment is not yet "
            "approved for full pipeline execution: "
            f"{experiment_id}"
        )

    experiment = experiments[
        experiment_id
    ]

    if experiment.is_baseline:
        raise ValueError(
            "Baseline policy cannot be applied "
            "as a challenger override"
        )

    return experiment


def _extract_target_source_metric(
    source_metrics: pd.DataFrame,
) -> pd.DataFrame:
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "value",
        "metric_origin",
    }

    missing = (
        required
        - set(source_metrics.columns)
    )

    if missing:
        raise ValueError(
            "source_metrics is missing required columns: "
            f"{sorted(missing)}"
        )

    target = source_metrics[
        source_metrics[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "value",
            "metric_origin",
        ]
    ].copy()

    if target.empty:
        raise ValueError(
            "source_metrics contains no "
            "active_inventory observations"
        )

    target["date"] = pd.to_datetime(
        target["date"],
        errors="coerce",
    )

    target["value"] = pd.to_numeric(
        target["value"],
        errors="coerce",
    )

    invalid = target[
        target["date"].isna()
        | target["value"].isna()
        | ~np.isfinite(
            target["value"]
        )
    ]

    if not invalid.empty:
        raise ValueError(
            "Active-inventory source observations "
            "contain invalid rows:\n"
            + invalid.head(30).to_string(
                index=False
            )
        )

    duplicates = target.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise ValueError(
            "Active-inventory source observations "
            "contain duplicate keys:\n"
            + target.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    target = target.rename(
        columns={
            "value": "raw_value",
        }
    )

    return (
        target.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .reset_index(drop=True)
    )


def _build_inventory_override(
    source_metrics: pd.DataFrame,
    *,
    experiment: SmoothingExperiment,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    policy = experiment.policy_for(
        TARGET_METRIC
    )

    if policy is None:
        raise AssertionError(
            f"{experiment.experiment_id} has no "
            "active_inventory policy"
        )

    if (
        policy.transform_strategy
        != "ma_deviation"
    ):
        raise ValueError(
            "The approved inventory finalist must "
            "use ma_deviation"
        )

    raw_inventory = (
        _extract_target_source_metric(
            source_metrics
        )
    )

    challenger = (
        build_smoothed_metric_features(
            raw_inventory,
            policy=policy,
            value_column="raw_value",
            preserve_columns=[
                "metric_origin",
            ],
        )
    )

    challenger[
        "production_feature_key"
    ] = challenger[
        "feature_component"
    ].map(
        PRODUCTION_FEATURE_KEY_MAP
    )

    unmapped = challenger[
        challenger[
            "production_feature_key"
        ].isna()
    ]

    if not unmapped.empty:
        raise AssertionError(
            "A challenger feature component did "
            "not map to a production feature key:\n"
            + unmapped.head(30).to_string(
                index=False
            )
        )

    override = challenger[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "production_feature_key",
            "raw_feature_value",
        ]
    ].rename(
        columns={
            "production_feature_key": (
                "feature_key"
            ),
        }
    )

    override = override.dropna(
        subset=[
            "raw_feature_value",
        ]
    )

    non_finite_override = override[
        ~np.isfinite(
            override[
                "raw_feature_value"
            ]
        )
    ]

    if not non_finite_override.empty:
        raise ValueError(
            "Inventory smoothing override "
            "contains non-finite values:\n"
            + non_finite_override.head(
                30
            ).to_string(
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
            "Inventory smoothing override contains "
            "duplicate production feature keys:\n"
            + override.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    lineage = challenger[
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_component",
            "smoothing_strategy",
            "smoothing_policy_role",
            "smoothing_level_window",
            "smoothing_short_window",
            "smoothing_short_lag_periods",
            "smoothing_long_window",
            "smoothing_long_lag_periods",
            "metric_origin",
            "raw_value",
            "raw_feature_value",
        ]
    ].copy()

    lineage[
        "experiment_id"
    ] = experiment.experiment_id

    lineage[
        "experiment_name"
    ] = experiment.experiment_name

    lineage[
        "parent_run"
    ] = experiment.parent_run

    lineage["feature_key"] = lineage[
        "feature_component"
    ].map(
        PRODUCTION_FEATURE_KEY_MAP
    )

    lineage = lineage.rename(
        columns={
            "smoothing_strategy": (
                "transform_strategy"
            ),
            "smoothing_policy_role": (
                "policy_role"
            ),
            "smoothing_level_window": (
                "level_window"
            ),
            "smoothing_short_window": (
                "short_window"
            ),
            (
                "smoothing_short_lag_"
                "periods"
            ): (
                "short_lag_periods"
            ),
            "smoothing_long_window": (
                "long_window"
            ),
            (
                "smoothing_long_lag_"
                "periods"
            ): (
                "long_lag_periods"
            ),
            "metric_origin": (
                "source_metric_origin"
            ),
            "raw_value": (
                "source_value"
            ),
            "raw_feature_value": (
                "challenger_feature_value"
            ),
        }
    )

    lineage = lineage[
        LINEAGE_COLUMNS
    ].copy()

    return (
        override,
        lineage,
    )


def apply_smoothing_experiment(
    *,
    features: pd.DataFrame,
    source_metrics: pd.DataFrame,
    experiment_id: str | None,
) -> tuple[
    pd.DataFrame,
    pd.DataFrame,
]:
    """
    Apply an approved smoothing experiment to the baseline feature
    matrix.

    When experiment_id is None, the feature matrix is returned
    unchanged and the lineage output is empty.
    """
    baseline = _validate_feature_frame(
        features
    )

    if experiment_id is None:
        return (
            baseline,
            pd.DataFrame(
                columns=LINEAGE_COLUMNS
            ),
        )

    experiment = _load_experiment(
        experiment_id
    )

    (
        override,
        lineage,
    ) = _build_inventory_override(
        source_metrics,
        experiment=experiment,
    )

    target_mask = (
        baseline[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & baseline[
            "feature_key"
        ].isin(
            TARGET_FEATURE_KEYS
        )
    )

    retained = baseline[
        ~target_mask
    ].copy()

    output = pd.concat(
        [
            retained,
            override,
        ],
        ignore_index=True,
    )

    duplicates = output.duplicated(
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
            "Smoothing experiment produced duplicate "
            "feature rows:\n"
            + output.loc[
                duplicates
            ].head(30).to_string(
                index=False
            )
        )

    # Reapply the production feature-definition order so downstream
    # normalization receives a deterministic frame.
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

    output = output.merge(
        feature_order,
        on="feature_key",
        how="left",
        validate="many_to_one",
    )

    missing_order = output[
        output[
            "_feature_order"
        ].isna()
    ]

    if not missing_order.empty:
        raise AssertionError(
            "Smoothing experiment introduced unknown "
            "feature keys:\n"
            + missing_order[
                [
                    "feature_key",
                ]
            ]
            .drop_duplicates()
            .to_string(index=False)
        )

    output = (
        output.sort_values(
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

    return (
        output,
        lineage,
    )
