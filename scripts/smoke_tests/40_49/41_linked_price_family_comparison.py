from __future__ import annotations
# scripts/smoke_tests/40_49/41_linked_price_family_comparison.py

import numpy as np

from regime.experiments.linked_price_family_comparison import (
    TARGET_DIMENSIONS,
    TARGET_METRICS,
    build_linked_price_family_comparison,
)


def main() -> int:
    print(
        "[linked_price_comparison] "
        "building real-data comparison..."
    )

    result = (
        build_linked_price_family_comparison()
    )

    isolation = result[
        "isolation_audit"
    ]

    metric_comparison = result[
        "metric_comparison_vs_baseline"
    ]

    dimension_comparison = result[
        "dimension_comparison_vs_baseline"
    ]

    axis_comparison = result[
        "axis_comparison_vs_baseline"
    ]

    metric_correlations = result[
        "metric_baseline_correlations"
    ]

    dimension_correlations = result[
        "dimension_baseline_correlations"
    ]

    axis_correlations = result[
        "axis_baseline_correlations"
    ]

    demand_conviction = result[
        "demand_conviction"
    ]

    feature_seasonality = result[
        "feature_seasonality"
    ]

    failed_isolation = isolation[
        ~isolation[
            "exact_match"
        ]
    ]

    if not failed_isolation.empty:
        raise AssertionError(
            "Linked price-family isolation "
            "failed:\n"
            + failed_isolation[
                [
                    "artifact_name",
                    "comparison_scope",
                    "baseline_rows",
                    "challenger_rows",
                    "error_message",
                ]
            ].to_string(
                index=False
            )
        )

    actual_metrics = set(
        metric_comparison[
            "canonical_metric_key"
        ].unique()
    )

    if actual_metrics != TARGET_METRICS:
        raise AssertionError(
            "Metric comparison does not contain "
            "the complete price family. "
            f"Expected {sorted(TARGET_METRICS)}, "
            f"found {sorted(actual_metrics)}"
        )

    actual_dimensions = set(
        dimension_comparison[
            "dimension"
        ].unique()
    )

    if actual_dimensions != TARGET_DIMENSIONS:
        raise AssertionError(
            "Dimension comparison does not contain "
            "Price and Affordability. "
            f"Expected {sorted(TARGET_DIMENSIONS)}, "
            f"found {sorted(actual_dimensions)}"
        )

    if set(
        axis_comparison[
            "axis"
        ].unique()
    ) != {
        "demand",
    }:
        raise AssertionError(
            "Axis comparison is not Demand-only"
        )

    for frame_name, frame in (
        (
            "metric comparison",
            metric_comparison,
        ),
        (
            "dimension comparison",
            dimension_comparison,
        ),
        (
            "axis comparison",
            axis_comparison,
        ),
    ):
        if frame.empty:
            raise AssertionError(
                f"{frame_name} is empty"
            )

    if metric_correlations.empty:
        raise AssertionError(
            "Metric baseline correlations are empty"
        )

    if dimension_correlations.empty:
        raise AssertionError(
            "Dimension baseline correlations are empty"
        )

    if axis_correlations.empty:
        raise AssertionError(
            "Axis baseline correlations are empty"
        )

    numeric_checks = [
        (
            metric_comparison,
            "mean_absolute_change_1m",
        ),
        (
            dimension_comparison,
            "mean_absolute_change_1m",
        ),
        (
            axis_comparison,
            "mean_absolute_change_1m",
        ),
    ]

    for frame, column in numeric_checks:
        if not np.isfinite(
            frame[column]
        ).all():
            raise AssertionError(
                f"{column} contains non-finite values"
            )

    expected_roles = {
        "baseline",
        "challenger",
    }

    if set(
        demand_conviction[
            "run_role"
        ].unique()
    ) != expected_roles:
        raise AssertionError(
            "Demand conviction is missing a run role"
        )

    if feature_seasonality.empty:
        raise AssertionError(
            "Feature seasonality output is empty"
        )

    print(
        "\n[linked_price_comparison] "
        "isolation audit:"
    )

    print(
        isolation.to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "metric comparison:"
    )

    print(
        metric_comparison.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "metric correlations:"
    )

    print(
        metric_correlations.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "dimension comparison:"
    )

    print(
        dimension_comparison.sort_values(
            [
                "geo_id",
                "dimension",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "dimension correlations:"
    )

    print(
        dimension_correlations.sort_values(
            [
                "geo_id",
                "dimension",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "Demand-axis comparison:"
    )

    print(
        axis_comparison.sort_values(
            [
                "geo_id",
                "axis",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "Demand-axis correlations:"
    )

    print(
        axis_correlations.sort_values(
            [
                "geo_id",
                "axis",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "Demand conviction:"
    )

    print(
        demand_conviction.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] "
        "short-feature seasonality:"
    )

    print(
        feature_seasonality[
            feature_seasonality[
                "feature_component"
            ].eq("short")
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "run_role",
                "calendar_month",
            ]
        )
        .to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_comparison] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
