from __future__ import annotations
# scripts/smoke_tests/25_axis_contribution_audit.py

from regime.diagnostics.axis_contribution import (
    DEFAULT_AUDIT_GEOS,
    DEFAULT_RUN_ID,
    build_axis_contribution_audit,
)


def main() -> int:
    audit = build_axis_contribution_audit(
        run_id=DEFAULT_RUN_ID,
        geo_ids=DEFAULT_AUDIT_GEOS,
    )

    dimension_summary = audit[
        "dimension_contribution_summary"
    ]

    metric_summary = audit[
        "metric_contribution_summary"
    ]

    focus_metrics = audit[
        "focus_metric_summary"
    ]

    cancellation = audit[
        "highest_cancellation_months"
    ]

    active_inventory = audit[
        "active_inventory_history"
    ]

    print(
        "[axis_contribution] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "\n[axis_contribution] "
        "dimension contribution summary:"
    )
    print(
        dimension_summary.to_string(
            index=False
        )
    )

    print(
        "\n[axis_contribution] "
        "focus metric summary:"
    )
    print(
        focus_metrics.to_string(
            index=False
        )
    )

    print(
        "\n[axis_contribution] "
        "active inventory summary:"
    )
    print(
        active_inventory.groupby(
            "geo_id"
        )
        .agg(
            rows=("date", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            mean_metric_score=(
                "metric_score",
                "mean",
            ),
            metric_score_std=(
                "metric_score",
                "std",
            ),
            mean_absolute_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().mean(),
            ),
            p90_absolute_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().quantile(
                    0.90
                ),
            ),
            maximum_absolute_score_change_1m=(
                "metric_score_change_1m",
                lambda values: values.abs().max(),
            ),
            mean_absolute_dimension_contribution_change_1m=(
                (
                    "metric_dimension_"
                    "contribution_change_1m"
                ),
                lambda values: values.abs().mean(),
            ),
        )
        .reset_index()
        .to_string(index=False)
    )

    print(
        "\n[axis_contribution] "
        "highest cancellation months:"
    )
    print(
        cancellation[
            [
                "geo_id",
                "date",
                "axis",
                "axis_score",
                "axis_score_change_1m",
                (
                    "gross_dimension_"
                    "contribution_change_1m"
                ),
                (
                    "offsetting_dimension_"
                    "change_1m"
                ),
                "dimension_cancellation_ratio",
                (
                    "axis_change_"
                    "reconciliation_error"
                ),
            ]
        ]
        .head(100)
        .to_string(index=False)
    )

    required_outputs = [
        "metric_contributions",
        "axis_contributions",
        "axis_change_attribution",
        "dimension_contribution_summary",
        "metric_contribution_summary",
        "focus_metric_summary",
        "active_inventory_history",
        "highest_cancellation_months",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    maximum_dimension_error = (
        audit["metric_contributions"][
            "dimension_reconciliation_error"
        ]
        .abs()
        .max()
    )

    maximum_axis_error = (
        audit["axis_contributions"][
            "axis_reconciliation_error"
        ]
        .abs()
        .max()
    )

    maximum_axis_change_error = (
        audit["axis_change_attribution"][
            "axis_change_reconciliation_error"
        ]
        .abs()
        .max()
    )

    print(
        "\n[axis_contribution] "
        "maximum reconciliation errors:"
    )
    print(
        "dimension:",
        maximum_dimension_error,
    )
    print(
        "axis:",
        maximum_axis_error,
    )
    print(
        "axis change:",
        maximum_axis_change_error,
    )

    tolerance = 1e-9

    if maximum_dimension_error > tolerance:
        raise AssertionError(
            "Metric contributions failed "
            "dimension reconciliation"
        )

    if maximum_axis_error > tolerance:
        raise AssertionError(
            "Dimension contributions failed "
            "axis reconciliation"
        )

    if maximum_axis_change_error > tolerance:
        raise AssertionError(
            "Contribution changes failed "
            "axis-change reconciliation"
        )

    expected_focus_metrics = {
        "active_inventory",
        "median_sale_price",
        "median_ppsf",
        "price_to_income",
        "payment_burden",
        "permit_activity",
        "permit_intensity",
    }

    actual_focus_metrics = set(
        focus_metrics[
            "canonical_metric_key"
        ]
    )

    missing_focus_metrics = (
        expected_focus_metrics
        - actual_focus_metrics
    )

    if missing_focus_metrics:
        raise AssertionError(
            "Missing expected focus metrics: "
            f"{sorted(missing_focus_metrics)}"
        )

    if not (
        cancellation[
            "dimension_cancellation_ratio"
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
            "Cancellation ratios must be "
            "between zero and one"
        )

    print(
        "\n[axis_contribution] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
