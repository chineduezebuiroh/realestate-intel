from __future__ import annotations
# scripts/smoke_tests/40_49/46_labor_demand_comparison.py

import numpy as np

from regime.experiments.labor_demand_comparison import (
    LABOR_METRICS,
    LABOR_POLICIES,
    build_labor_demand_comparison,
)


def main() -> int:
    print(
        "[labor_demand_comparison] "
        "building comparison..."
    )

    result = build_labor_demand_comparison()

    isolation = result[
        "isolation_audit"
    ]

    failed = isolation[
        ~isolation[
            "exact_match"
        ]
    ]

    if not failed.empty:
        raise AssertionError(
            "Labor challenger isolation failed:\n"
            + failed[
                [
                    "artifact_name",
                    "scope",
                    "baseline_rows",
                    "challenger_rows",
                    "error_message",
                ]
            ].to_string(index=False)
        )

    feature_summary = result[
        "feature_stability_summary"
    ]

    metric_summary = result[
        "metric_stability_summary"
    ]

    dimension_summary = result[
        "dimension_stability_summary"
    ]

    axis_summary = result[
        "axis_stability_summary"
    ]

    cancellation_summary = result[
        "cancellation_summary"
    ]

    expected_roles = {
        "baseline",
        *LABOR_POLICIES.keys(),
    }

    for frame_name, frame in (
        ("feature summary", feature_summary),
        ("metric summary", metric_summary),
        ("dimension summary", dimension_summary),
        ("axis summary", axis_summary),
        ("cancellation summary", cancellation_summary),
    ):
        if frame.empty:
            raise AssertionError(
                f"{frame_name} is empty"
            )

        if set(
            frame[
                "run_role"
            ].unique()
        ) != expected_roles:
            raise AssertionError(
                f"{frame_name} has unexpected roles"
            )

    if set(
        metric_summary[
            "canonical_metric_key"
        ].unique()
    ) != set(LABOR_METRICS):
        raise AssertionError(
            "Metric summary does not contain "
            "all three labor metrics"
        )

    numeric_checks = [
        (
            feature_summary,
            "mean_absolute_change_1m",
        ),
        (
            metric_summary,
            "mean_absolute_change_1m",
        ),
        (
            dimension_summary,
            "mean_absolute_change_1m",
        ),
        (
            axis_summary,
            "mean_absolute_change_1m",
        ),
        (
            cancellation_summary,
            "mean_cancellation_rate",
        ),
    ]

    for frame, column in numeric_checks:
        values = frame[
            column
        ].dropna()

        if not np.isfinite(
            values
        ).all():
            raise AssertionError(
                f"{column} contains non-finite values"
            )

    rates = cancellation_summary[
        "mean_cancellation_rate"
    ]

    if (
        rates.lt(0).any()
        or rates.gt(1).any()
    ):
        raise AssertionError(
            "Cancellation rate fell outside [0, 1]"
        )

    print(
        "\n[labor_demand_comparison] "
        "isolation audit:"
    )

    print(
        isolation.to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "short-feature stability:"
    )

    print(
        feature_summary[
            feature_summary[
                "feature_component"
            ].eq("short")
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "run_role",
            ]
        )
        .to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "metric stability:"
    )

    print(
        metric_summary.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "core Demand dimension:"
    )

    print(
        dimension_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "Demand axis:"
    )

    print(
        axis_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "core Demand cancellation:"
    )

    print(
        cancellation_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
