from __future__ import annotations
# scripts/smoke_tests/32_active_inventory_comparison.py

import numpy as np

from regime.experiments.active_inventory_comparison import (
    CHALLENGER_IDS,
    CURRENT_FEATURE_MAP,
    DEFAULT_RUN_ID,
    FOCUS_GEOS,
    build_active_inventory_comparison,
)


def main() -> int:
    audit = build_active_inventory_comparison(
        run_id=DEFAULT_RUN_ID,
        geo_ids=FOCUS_GEOS,
    )

    raw_inventory = audit[
        "raw_inventory"
    ]

    feature_history = audit[
        "feature_history"
    ]

    coverage = audit[
        "coverage_summary"
    ]

    volatility = audit[
        "volatility_summary"
    ]

    seasonality = audit[
        "seasonality_summary"
    ]

    correlations = audit[
        "baseline_correlations"
    ]

    comparison = audit[
        "policy_comparison"
    ]

    print(
        "[active_inventory_comparison] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "[active_inventory_comparison] "
        "raw rows:",
        len(raw_inventory),
    )

    print(
        "[active_inventory_comparison] "
        "geographies:",
        raw_inventory[
            "geo_id"
        ].nunique(),
    )

    print(
        "\n[active_inventory_comparison] "
        "coverage:"
    )

    print(
        coverage.to_string(
            index=False
        )
    )

    print(
        "\n[active_inventory_comparison] "
        "volatility:"
    )

    print(
        volatility.sort_values(
            [
                "geo_id",
                "feature_component",
                "policy_id",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[active_inventory_comparison] "
        "challenger vs baseline:"
    )

    print(
        comparison[
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "rows",
                "mean_absolute_change_1m",
                (
                    "baseline_mean_absolute_"
                    "change_1m"
                ),
                (
                    "mean_absolute_change_"
                    "pct_vs_baseline"
                ),
                "p90_absolute_change_1m",
                (
                    "baseline_p90_absolute_"
                    "change_1m"
                ),
                (
                    "p90_absolute_change_"
                    "pct_vs_baseline"
                ),
                "sign_flip_rate",
                "baseline_sign_flip_rate",
                (
                    "sign_flip_rate_delta_"
                    "vs_baseline"
                ),
            ]
        ].to_string(
            index=False
        )
    )

    print(
        "\n[active_inventory_comparison] "
        "correlation with baseline:"
    )

    print(
        correlations.to_string(
            index=False
        )
    )

    seasonal_extremes = (
        seasonality.sort_values(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "mean_absolute_change_1m",
            ],
            ascending=[
                True,
                True,
                True,
                False,
            ],
        )
        .groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
            ],
            as_index=False,
        )
        .head(3)
    )

    print(
        "\n[active_inventory_comparison] "
        "top seasonal volatility months:"
    )

    print(
        seasonal_extremes.to_string(
            index=False
        )
    )

    latest = (
        feature_history.sort_values(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                "date",
            ]
        )
        .groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
            ],
            as_index=False,
        )
        .tail(6)
    )

    print(
        "\n[active_inventory_comparison] "
        "latest feature values:"
    )

    print(
        latest[
            [
                "policy_id",
                "geo_id",
                "date",
                "feature_component",
                "feature_value",
                "feature_change_1m",
                "sign_flip_flag",
            ]
        ].to_string(
            index=False
        )
    )

    required_outputs = [
        "raw_inventory",
        "feature_history",
        "coverage_summary",
        "volatility_summary",
        "seasonality_summary",
        "baseline_correlations",
        "policy_comparison",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    actual_geos = set(
        raw_inventory["geo_id"]
    )

    if actual_geos != set(
        FOCUS_GEOS
    ):
        raise AssertionError(
            "Unexpected focus geographies. "
            f"Expected {sorted(FOCUS_GEOS)}, "
            f"found {sorted(actual_geos)}"
        )

    expected_policies = {
        "baseline_current",
        *CHALLENGER_IDS,
    }

    actual_policies = set(
        feature_history["policy_id"]
    )

    if actual_policies != expected_policies:
        raise AssertionError(
            "Unexpected comparison policies. "
            f"Expected {sorted(expected_policies)}, "
            f"found {sorted(actual_policies)}"
        )

    expected_components = {
        "level",
        "short",
        "long",
    }

    actual_components = set(
        feature_history[
            "feature_component"
        ]
    )

    if actual_components != expected_components:
        raise AssertionError(
            "Unexpected feature components. "
            f"Expected {sorted(expected_components)}, "
            f"found {sorted(actual_components)}"
        )

    baseline_features = feature_history[
        feature_history[
            "policy_id"
        ].eq("baseline_current")
    ]

    expected_baseline_feature_keys = set(
        CURRENT_FEATURE_MAP
    )

    actual_baseline_feature_keys = set(
        baseline_features[
            "feature_key"
        ]
    )

    if (
        actual_baseline_feature_keys
        != expected_baseline_feature_keys
    ):
        raise AssertionError(
            "Baseline feature-key contract mismatch. "
            f"Expected "
            f"{sorted(expected_baseline_feature_keys)}, "
            f"found "
            f"{sorted(actual_baseline_feature_keys)}"
        )

    invalid_values = feature_history[
        "feature_value"
    ].dropna()

    if not np.isfinite(
        invalid_values
    ).all():
        raise AssertionError(
            "Comparison contains non-finite "
            "feature values"
        )

    rate_columns = [
        "sign_flip_rate",
        "baseline_sign_flip_rate",
    ]

    for column in rate_columns:
        if not (
            comparison[column]
            .dropna()
            .between(
                0.0,
                1.0,
                inclusive="both",
            )
            .all()
        ):
            raise AssertionError(
                f"{column} must remain between "
                "zero and one"
            )

    challenger_coverage = coverage[
        coverage[
            "policy_id"
        ].isin(
            CHALLENGER_IDS
        )
    ]

    if (
        challenger_coverage[
            "valid_rows"
        ] <= 0
    ).any():
        raise AssertionError(
            "A challenger produced no valid "
            "feature observations"
        )

    momentum_short = coverage[
        coverage[
            "policy_id"
        ].eq(
            "inventory_ma3_momentum"
        )
        & coverage[
            "feature_component"
        ].eq("short")
    ]

    deviation_short = coverage[
        coverage[
            "policy_id"
        ].eq(
            "inventory_ma3_deviation"
        )
        & coverage[
            "feature_component"
        ].eq("short")
    ]

    coverage_check = momentum_short.merge(
        deviation_short,
        on="geo_id",
        suffixes=(
            "_momentum",
            "_deviation",
        ),
        validate="one_to_one",
    )

    if not (
        coverage_check[
            "first_valid_date_momentum"
        ]
        > coverage_check[
            "first_valid_date_deviation"
        ]
    ).all():
        raise AssertionError(
            "Momentum short must begin later "
            "than deviation short"
        )

    comparison_keys = comparison[
        [
            "policy_id",
            "geo_id",
            "feature_component",
        ]
    ]

    if comparison_keys.duplicated().any():
        raise AssertionError(
            "Policy comparison contains "
            "duplicate rows"
        )

    print(
        "\n[active_inventory_comparison] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
