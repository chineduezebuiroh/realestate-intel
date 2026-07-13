from __future__ import annotations
# scripts/smoke_tests/20_29/22_derived_input_freshness.py

from regime.diagnostics.derived_input_freshness import (
    DEFAULT_RUN_ID,
    DEFAULT_VALIDATION_GEOS,
    build_derived_input_freshness_audit,
)


def main() -> int:
    audit = build_derived_input_freshness_audit(
        run_id=DEFAULT_RUN_ID,
        geo_ids=DEFAULT_VALIDATION_GEOS,
        top_n=50,
    )

    component_summary = audit[
        "component_age_summary"
    ]
    latest = audit["latest_selected_geos"]
    distribution = audit["age_distribution"]
    worst_events = audit["worst_component_events"]
    worst_derived = audit[
        "worst_derived_observations"
    ]
    streaks = audit["carry_forward_streaks"]
    geo_extremes = audit["geo_component_extremes"]

    print(
        "[derived_input_freshness] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "\n[derived_input_freshness] "
        "component age summary:"
    )
    print(component_summary.to_string(index=False))

    print(
        "\n[derived_input_freshness] "
        "latest component status for DC and Alameda:"
    )
    print(
        latest[
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "component_metric_key",
                "component_source_geo_id",
                "component_source_date",
                "component_age_days",
                "component_age_months",
                "was_carried_forward",
            ]
        ]
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .to_string(index=False)
    )

    print(
        "\n[derived_input_freshness] "
        "age distributions:"
    )
    print(
        distribution[
            distribution["rows"] > 0
        ].to_string(index=False)
    )

    print(
        "\n[derived_input_freshness] "
        "50 oldest component events:"
    )
    print(
        worst_events[
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "component_metric_key",
                "component_source_geo_id",
                "component_source_date",
                "component_age_days",
                "component_age_months",
                "was_carried_forward",
            ]
        ].to_string(index=False)
    )

    print(
        "\n[derived_input_freshness] "
        "50 oldest derived observations:"
    )
    print(
        worst_derived[
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "oldest_component_metric_key",
                "oldest_component_source_geo_id",
                "oldest_component_source_date",
                "oldest_component_age_days",
                "oldest_component_age_months",
                "component_count",
                "carried_forward_component_count",
                "carried_forward_component_share",
            ]
        ].to_string(index=False)
    )

    print(
        "\n[derived_input_freshness] "
        "50 longest carry-forward streaks:"
    )
    print(
        streaks.head(50)[
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
                "component_source_geo_id",
                "component_source_date",
                "first_derived_date",
                "last_derived_date",
                "derived_observation_count",
                "calendar_span_days",
                "first_age_days",
                "last_age_days",
                "maximum_age_days",
            ]
        ].to_string(index=False)
    )

    print(
        "\n[derived_input_freshness] "
        "50 worst geo/component combinations:"
    )
    print(
        geo_extremes.head(50).to_string(index=False)
    )

    required_outputs = [
        "component_age_summary",
        "latest_component_status",
        "derived_observation_summary",
        "age_distribution",
        "carry_forward_streaks",
        "geo_component_extremes",
        "worst_component_events",
        "worst_derived_observations",
        "latest_selected_geos",
        "oldest_selected_geos",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty audit output: {key}"
            )

    expected_metrics = {
        "permit_intensity",
        "price_to_income",
        "payment_burden",
    }

    actual_metrics = set(
        component_summary["derived_metric_key"]
    )

    if actual_metrics != expected_metrics:
        raise AssertionError(
            f"Expected derived metrics "
            f"{sorted(expected_metrics)}, "
            f"found {sorted(actual_metrics)}"
        )

    if (
        latest["component_age_days"] < 0
    ).any():
        raise AssertionError(
            "Latest component status contains negative ages"
        )

    if not (
        worst_events["component_age_days"]
        .is_monotonic_decreasing
    ):
        raise AssertionError(
            "Worst component events are not sorted by age"
        )

    print(
        "\n[derived_input_freshness] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
