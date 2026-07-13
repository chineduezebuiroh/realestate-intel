from __future__ import annotations
# scripts/smoke_tests/30_39/35_inventory_finalist_comparison.py

import numpy as np

from regime.experiments.inventory_finalist_comparison import (
    BASELINE_RUN_ID,
    CHALLENGER_RUN_ID,
    FOCUS_GEOS,
    build_inventory_finalist_comparison,
)


def main() -> int:
    audit = (
        build_inventory_finalist_comparison(
            baseline_run_id=(
                BASELINE_RUN_ID
            ),
            challenger_run_id=(
                CHALLENGER_RUN_ID
            ),
            geo_ids=FOCUS_GEOS,
        )
    )

    isolation = audit[
        "isolation_audit"
    ]

    metric_comparison = audit[
        "metric_comparison"
    ]

    dimension_comparison = audit[
        "dimension_comparison"
    ]

    axis_comparison = audit[
        "axis_comparison"
    ]

    transition_summary = audit[
        "transition_summary"
    ]

    assignment_change_summary = audit[
        "assignment_change_summary"
    ]

    persistence_summary = audit[
        "persistence_summary"
    ]

    dwell_summary = audit[
        "dwell_summary"
    ]

    flips = audit[
        "recovery_hypersupply_flips"
    ]

    historical = audit[
        "historical_period_summary"
    ]

    sensitivity = audit[
        "sensitivity_comparison"
    ]

    print(
        "[inventory_finalist] baseline:",
        BASELINE_RUN_ID,
    )

    print(
        "[inventory_finalist] challenger:",
        CHALLENGER_RUN_ID,
    )

    print(
        "\n[inventory_finalist] "
        "isolation audit:"
    )

    print(
        isolation.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "active-inventory metric comparison:"
    )

    print(
        metric_comparison.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "Supply dimension comparison:"
    )

    print(
        dimension_comparison.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "Supply axis comparison:"
    )

    print(
        axis_comparison.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "transition counts:"
    )

    print(
        transition_summary.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "assignment changes:"
    )

    print(
        assignment_change_summary.to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "continuous persistence:"
    )

    print(
        persistence_summary.sort_values(
            [
                "geo_id",
                "regime_level",
                "horizon_months",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "dwell summary:"
    )

    print(
        dwell_summary.sort_values(
            [
                "geo_id",
                "regime_level",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "Recovery/Hyper Supply flips:"
    )

    print(
        flips.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "historical periods:"
    )

    print(
        historical.sort_values(
            [
                "geo_id",
                "period",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[inventory_finalist] "
        "transition sensitivity comparison:"
    )

    print(
        sensitivity.to_string(
            index=False
        )
    )

    required_outputs = [
        "isolation_audit",
        "metric_history",
        "metric_comparison",
        "dimension_history",
        "dimension_comparison",
        "axis_history",
        "axis_comparison",
        "assignment_history",
        "transition_summary",
        "assignment_comparison",
        "assignment_change_summary",
        "persistence_events",
        "persistence_summary",
        "dwell_episodes",
        "dwell_summary",
        "recovery_hypersupply_flips",
        "historical_period_summary",
        "sensitivity_summary",
        "sensitivity_comparison",
    ]

    for output_name in required_outputs:
        if audit[
            output_name
        ].empty:
            raise AssertionError(
                "Expected non-empty output: "
                f"{output_name}"
            )

    failed_isolation = isolation[
        ~isolation[
            "exact_match"
        ]
    ]

    if not failed_isolation.empty:
        raise AssertionError(
            "Experiment isolation failed:\n"
            + failed_isolation.to_string(
                index=False
            )
        )

    expected_isolation_artifacts = {
        "normalized_features",
        "metric_scores",
        "dimension_scores",
        "axis_scores",
    }

    if set(
        isolation[
            "artifact_name"
        ]
    ) != expected_isolation_artifacts:
        raise AssertionError(
            "Isolation audit artifact set mismatch"
        )

    expected_geos = set(
        FOCUS_GEOS
    )

    for output_name in (
        "metric_comparison",
        "dimension_comparison",
        "axis_comparison",
        "assignment_change_summary",
    ):
        actual_geos = set(
            audit[
                output_name
            ]["geo_id"]
        )

        if actual_geos != expected_geos:
            raise AssertionError(
                f"{output_name} geography mismatch. "
                f"Expected {sorted(expected_geos)}, "
                f"found {sorted(actual_geos)}"
            )

    assignment_comparison = audit[
        "assignment_comparison"
    ]

    if not assignment_comparison[
        "_merge"
    ].eq("both").all():
        raise AssertionError(
            "Baseline and challenger assignment "
            "calendars do not match exactly"
        )

    numeric_rate_columns = [
        (
            "major_assignment_"
            "change_rate"
        ),
        (
            "minor_assignment_"
            "change_rate"
        ),
    ]

    for column in numeric_rate_columns:
        if not (
            assignment_change_summary[
                column
            ]
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

    persistence_rates = (
        persistence_summary[
            (
                "continuous_"
                "persistence_rate"
            )
        ].dropna()
    )

    if not persistence_rates.between(
        0.0,
        1.0,
        inclusive="both",
    ).all():
        raise AssertionError(
            "Continuous persistence rates must "
            "remain between zero and one"
        )

    if (
        dwell_summary[
            "mean_duration_months"
        ] <= 0
    ).any():
        raise AssertionError(
            "Mean dwell duration must be positive"
        )

    comparison_percent_columns = [
        column
        for frame in (
            metric_comparison,
            dimension_comparison,
            axis_comparison,
        )
        for column in frame.columns
        if column.endswith(
            "_pct_vs_baseline"
        )
    ]

    if not comparison_percent_columns:
        raise AssertionError(
            "No baseline-relative volatility "
            "comparison columns were generated"
        )

    for frame in (
        metric_comparison,
        dimension_comparison,
        axis_comparison,
    ):
        numeric = frame.select_dtypes(
            include=[
                "number",
            ]
        )

        values = numeric.to_numpy()

        if np.isinf(
            values
        ).any():
            raise AssertionError(
                "A comparison output contains "
                "infinite numeric values"
            )

    print(
        "\n[inventory_finalist] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
