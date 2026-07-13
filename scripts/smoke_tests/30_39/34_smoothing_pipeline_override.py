from __future__ import annotations
# scripts/smoke_tests/30_39/34_smoothing_pipeline_override.py

import numpy as np
import pandas as pd

from regime.artifacts import (
    RegimeArtifactStore,
)
from regime.experiments.smoothing_run import (
    TARGET_FEATURE_KEYS,
    TARGET_METRIC,
    apply_smoothing_experiment,
)


BASELINE_RUN = (
    "macro_regime_v1_bps120_sources"
)

EXPERIMENT_ID = (
    "inventory_ma3_deviation"
)


def main() -> int:
    store = RegimeArtifactStore()

    source_metrics = store.read_dataframe(
        BASELINE_RUN,
        "source_metrics",
    )

    baseline_features = store.read_dataframe(
        BASELINE_RUN,
        "features",
    )

    (
        challenger_features,
        smoothing_lineage,
    ) = apply_smoothing_experiment(
        features=baseline_features,
        source_metrics=source_metrics,
        experiment_id=EXPERIMENT_ID,
    )

    print(
        "[smoothing_override] baseline rows:",
        len(baseline_features),
    )

    print(
        "[smoothing_override] challenger rows:",
        len(challenger_features),
    )

    print(
        "[smoothing_override] lineage rows:",
        len(smoothing_lineage),
    )

    baseline_non_target = baseline_features[
        ~(
            baseline_features[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
            & baseline_features[
                "feature_key"
            ].isin(
                TARGET_FEATURE_KEYS
            )
        )
    ].sort_values(
        [
            "feature_key",
            "geo_id",
            "date",
        ],
        kind="mergesort",
    ).reset_index(drop=True)

    challenger_non_target = challenger_features[
        ~(
            challenger_features[
                "canonical_metric_key"
            ].eq(TARGET_METRIC)
            & challenger_features[
                "feature_key"
            ].isin(
                TARGET_FEATURE_KEYS
            )
        )
    ].sort_values(
        [
            "feature_key",
            "geo_id",
            "date",
        ],
        kind="mergesort",
    ).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        baseline_non_target,
        challenger_non_target,
        check_exact=True,
        check_dtype=True,
    )

    baseline_non_target_inf = (
        np.isinf(
            baseline_non_target[
                "raw_feature_value"
            ]
        ).sum()
    )

    challenger_non_target_inf = (
        np.isinf(
            challenger_non_target[
                "raw_feature_value"
            ]
        ).sum()
    )

    if (
        baseline_non_target_inf
        != challenger_non_target_inf
    ):
        raise AssertionError(
            "The smoothing override changed "
            "non-target infinity preservation"
        )

    baseline_target = baseline_features[
        baseline_features[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & baseline_features[
            "feature_key"
        ].isin(
            TARGET_FEATURE_KEYS
        )
    ]

    challenger_target = challenger_features[
        challenger_features[
            "canonical_metric_key"
        ].eq(TARGET_METRIC)
        & challenger_features[
            "feature_key"
        ].isin(
            TARGET_FEATURE_KEYS
        )
    ]

    if baseline_target.empty:
        raise AssertionError(
            "Baseline target features are empty"
        )

    if challenger_target.empty:
        raise AssertionError(
            "Challenger target features are empty"
        )

    if not np.isfinite(
        challenger_target[
            "raw_feature_value"
        ]
    ).all():
        raise AssertionError(
            "Challenger active-inventory "
            "features contain non-finite values"
        )

    if smoothing_lineage.empty:
        raise AssertionError(
            "Smoothing lineage is empty"
        )

    if set(
        smoothing_lineage[
            "feature_key"
        ]
    ) != TARGET_FEATURE_KEYS:
        raise AssertionError(
            "Smoothing lineage feature-key "
            "contract mismatch"
        )

    if set(
        smoothing_lineage[
            "experiment_id"
        ]
    ) != {
        EXPERIMENT_ID,
    }:
        raise AssertionError(
            "Unexpected smoothing experiment ID"
        )

    duplicates = challenger_features.duplicated(
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
            "Challenger feature matrix contains "
            "duplicate keys"
        )

    comparison = baseline_target.merge(
        challenger_target,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        how="inner",
        suffixes=(
            "_baseline",
            "_challenger",
        ),
        validate="one_to_one",
    )

    comparison[
        "absolute_delta"
    ] = (
        comparison[
            "raw_feature_value_challenger"
        ]
        - comparison[
            "raw_feature_value_baseline"
        ]
    ).abs()

    changed = comparison[
        comparison[
            "absolute_delta"
        ].gt(1e-12)
    ]

    if changed.empty:
        raise AssertionError(
            "The challenger changed no "
            "active-inventory feature values"
        )

    print(
        "\n[smoothing_override] target coverage:"
    )

    print(
        challenger_target.groupby(
            "feature_key",
            as_index=False,
        )
        .agg(
            rows=(
                "raw_feature_value",
                "size",
            ),
            geographies=(
                "geo_id",
                "nunique",
            ),
            first_date=(
                "date",
                "min",
            ),
            last_date=(
                "date",
                "max",
            ),
        )
        .to_string(index=False)
    )

    print(
        "\n[smoothing_override] target changes:"
    )

    print(
        comparison.groupby(
            "feature_key",
            as_index=False,
        )
        .agg(
            overlap_rows=(
                "absolute_delta",
                "size",
            ),
            changed_rows=(
                "absolute_delta",
                lambda values: (
                    values.gt(
                        1e-12
                    ).sum()
                ),
            ),
            mean_absolute_delta=(
                "absolute_delta",
                "mean",
            ),
            maximum_absolute_delta=(
                "absolute_delta",
                "max",
            ),
        )
        .to_string(index=False)
    )

    print(
        "\n[smoothing_override] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
