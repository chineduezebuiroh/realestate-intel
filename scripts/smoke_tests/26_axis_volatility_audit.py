from __future__ import annotations
# scripts/smoke_tests/26_axis_volatility_audit.py

from regime.diagnostics.axis_volatility import (
    DEFAULT_AUDIT_GEOS,
    DEFAULT_NEAR_ORIGIN_THRESHOLD,
    DEFAULT_ROLLING_WINDOW,
    DEFAULT_RUN_ID,
    REDFIN_FOCUS_METRICS,
    build_axis_volatility_audit,
)


def main() -> int:
    audit = build_axis_volatility_audit(
        run_id=DEFAULT_RUN_ID,
        geo_ids=DEFAULT_AUDIT_GEOS,
        near_origin_threshold=(
            DEFAULT_NEAR_ORIGIN_THRESHOLD
        ),
        rolling_window=(
            DEFAULT_ROLLING_WINDOW
        ),
    )

    metric_summary = audit[
        "metric_volatility_summary"
    ]

    dimension_summary = audit[
        "dimension_volatility_summary"
    ]

    axis_summary = audit[
        "axis_volatility_summary"
    ]

    focus_metrics = audit[
        "focus_metric_volatility"
    ]

    hidden_summary = audit[
        "hidden_volatility_summary"
    ]

    hidden_events = audit[
        "hidden_volatility_events"
    ]

    transition_volatility = audit[
        "transition_volatility"
    ]

    metric_seasonality = audit[
        "metric_seasonality"
    ]

    print(
        "[axis_volatility] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "[axis_volatility] near-origin threshold:",
        DEFAULT_NEAR_ORIGIN_THRESHOLD,
    )

    print(
        "[axis_volatility] rolling window:",
        DEFAULT_ROLLING_WINDOW,
    )

    print(
        "\n[axis_volatility] "
        "axis volatility summary:"
    )
    print(
        axis_summary.to_string(
            index=False
        )
    )

    print(
        "\n[axis_volatility] "
        "dimension volatility summary:"
    )
    print(
        dimension_summary[
            [
                "geo_id",
                "axis",
                "dimension",
                "rows",
                "first_date",
                "last_date",
                "mean_absolute_change_1m",
                "p90_absolute_change_1m",
                "maximum_absolute_change_1m",
                "sign_flip_rate",
                "large_jump_rate",
                "average_rolling_change_std",
                (
                    "mean_absolute_axis_"
                    "contribution_change_1m"
                ),
                (
                    "p90_absolute_axis_"
                    "contribution_change_1m"
                ),
            ]
        ]
        .sort_values(
            [
                "geo_id",
                "axis",
                (
                    "mean_absolute_axis_"
                    "contribution_change_1m"
                ),
            ],
            ascending=[
                True,
                True,
                False,
            ],
        )
        .to_string(index=False)
    )

    print(
        "\n[axis_volatility] "
        "focus metric volatility:"
    )
    print(
        focus_metrics[
            [
                "geo_id",
                "dimension",
                "canonical_metric_key",
                "redfin_focus_flag",
                "rows",
                "first_date",
                "last_date",
                "mean_absolute_change_1m",
                "p90_absolute_change_1m",
                "maximum_absolute_change_1m",
                "sign_flip_rate",
                "large_jump_rate",
                "average_rolling_change_std",
                (
                    "mean_absolute_dimension_"
                    "contribution_change_1m"
                ),
                (
                    "p90_absolute_dimension_"
                    "contribution_change_1m"
                ),
            ]
        ]
        .to_string(index=False)
    )

    print(
        "\n[axis_volatility] "
        "hidden volatility summary:"
    )
    print(
        hidden_summary.to_string(
            index=False
        )
    )

    print(
        "\n[axis_volatility] "
        "largest hidden-volatility events:"
    )
    print(
        hidden_events[
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
                    "dimension_cancellation_ratio"
                ),
                "hidden_volatility_ratio",
                "transition_month_flag",
                "near_origin_flag",
                "major_regime",
                "minor_regime",
            ]
        ]
        .head(100)
        .to_string(index=False)
    )

    print(
        "\n[axis_volatility] "
        "largest transition-month axis moves:"
    )
    print(
        transition_volatility[
            [
                "geo_id",
                "date",
                "axis",
                "axis_score",
                "axis_score_change_1m",
                "absolute_change_1m",
                "major_regime",
                "minor_regime",
                "regime_strength",
                "near_origin_flag",
                "sign_flip_flag",
                "large_jump_flag",
            ]
        ]
        .head(100)
        .to_string(index=False)
    )

    redfin_monthly = (
        metric_seasonality[
            metric_seasonality[
                "canonical_metric_key"
            ].isin(
                REDFIN_FOCUS_METRICS
            )
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "mean_absolute_change_1m",
            ],
            ascending=[
                True,
                True,
                False,
            ],
        )
    )

    print(
        "\n[axis_volatility] "
        "Redfin-focus seasonality:"
    )
    print(
        redfin_monthly.to_string(
            index=False
        )
    )

    required_outputs = [
        "metric_volatility_history",
        "metric_volatility_summary",
        "dimension_volatility_history",
        "dimension_volatility_summary",
        "axis_volatility_history",
        "axis_volatility_summary",
        "metric_seasonality",
        "dimension_seasonality",
        "axis_seasonality",
        "hidden_volatility_summary",
        "hidden_volatility_events",
        "transition_volatility",
        "focus_metric_volatility",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    expected_axes = {
        "demand",
        "supply",
    }

    actual_axes = set(
        axis_summary["axis"]
    )

    if actual_axes != expected_axes:
        raise AssertionError(
            "Axis volatility summary mismatch. "
            f"Expected {sorted(expected_axes)}, "
            f"found {sorted(actual_axes)}"
        )

    if not (
        hidden_summary[
            "mean_cancellation_ratio"
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
            "Mean cancellation ratios must be "
            "between zero and one"
        )

    if not (
        hidden_events[
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
            "Event cancellation ratios must be "
            "between zero and one"
        )

    if (
        focus_metrics[
            focus_metrics[
                "canonical_metric_key"
            ].eq("active_inventory")
        ].empty
    ):
        raise AssertionError(
            "Active inventory is missing from "
            "focus metric volatility"
        )

    if (
        focus_metrics[
            focus_metrics[
                "canonical_metric_key"
            ].isin(
                REDFIN_FOCUS_METRICS
            )
        ].empty
    ):
        raise AssertionError(
            "Redfin-focus metrics are missing"
        )

    invalid_sign_flip_rates = (
        metric_summary[
            "sign_flip_rate"
        ]
        .dropna()
        .between(
            0.0,
            1.0,
            inclusive="both",
        )
        .eq(False)
    )

    if invalid_sign_flip_rates.any():
        raise AssertionError(
            "Metric sign-flip rates must be "
            "between zero and one"
        )

    print(
        "\n[axis_volatility] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
