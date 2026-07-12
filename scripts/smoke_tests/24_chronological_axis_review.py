from __future__ import annotations
# scripts/smoke_tests/24_chronological_axis_review.py

from regime.diagnostics.chronological_axis_review import (
    DEFAULT_REVIEW_GEOS,
    DEFAULT_RUN_ID,
    build_chronological_axis_review,
)


def main() -> int:
    review = build_chronological_axis_review(
        run_id=DEFAULT_RUN_ID,
        geo_ids=DEFAULT_REVIEW_GEOS,
        top_n_axis_events=15,
    )

    timeline = review["monthly_timeline"]
    axis_events = review["axis_events"]
    transitions = review[
        "transition_timeline"
    ]
    axis_summary = review["axis_summary"]

    actual_axes = set(
        axis_summary["axis"]
    )

    expected_axes = {
        "demand",
        "supply",
    }

    if actual_axes != expected_axes:
        raise AssertionError(
            "Axis summary contains unexpected axes. "
            f"Expected {sorted(expected_axes)}, "
            f"found {sorted(actual_axes)}"
        )

    event_axes = set(
        axis_events["axis"]
    )

    if not event_axes.issubset(
        expected_axes
    ):
        raise AssertionError(
            "Axis events contain non-production axes: "
            f"{sorted(event_axes - expected_axes)}"
        )
    
    latest = review["latest_snapshot"]

    print(
        "[chronological_axis_review] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "[chronological_axis_review] rows:",
        len(timeline),
    )

    print(
        "[chronological_axis_review] geos:",
        timeline["geo_id"].nunique(),
    )

    print(
        "[chronological_axis_review] date range:",
        timeline["date"].min(),
        "→",
        timeline["date"].max(),
    )

    print(
        "\n[chronological_axis_review] "
        "axis summary:"
    )
    print(
        axis_summary.to_string(
            index=False
        )
    )

    print(
        "\n[chronological_axis_review] "
        "largest axis moves:"
    )
    print(
        axis_events[
            [
                "geo_id",
                "date",
                "axis",
                "axis_score",
                "axis_score_change_1m",
                "axis_score_absolute_change_1m",
                "major_regime",
                "minor_regime",
                "major_changed",
                "minor_changed",
            ]
            + [
                column
                for column in [
                    "angle_degrees",
                    "regime_strength",
                    "distance_to_boundary_degrees",
                    "derived_freshness_status",
                    "any_stale_derived_input",
                    "any_exceeded_derived_horizon",
                ]
                if column
                in axis_events.columns
            ]
        ]
        .head(60)
        .to_string(index=False)
    )

    print(
        "\n[chronological_axis_review] "
        "regime transitions:"
    )
    print(
        transitions[
            [
                "geo_id",
                "date",
                "previous_major_regime",
                "major_regime",
                "previous_minor_regime",
                "minor_regime",
                "major_changed",
                "minor_changed",
                "demand_axis_score",
                "demand_axis_score_change_1m",
                "supply_axis_score",
                "supply_axis_score_change_1m",
                "dominant_axis",
                "axis_dominance_margin",
                "axis_dominance_ratio",
            ]
            + [
                column
                for column in [
                    "angle_degrees",
                    "regime_strength",
                    "distance_to_boundary_degrees",
                    "derived_freshness_status",
                    "any_stale_derived_input",
                    "any_exceeded_derived_horizon",
                ]
                if column
                in transitions.columns
            ]
        ]
        .tail(100)
        .to_string(index=False)
    )

    print(
        "\n[chronological_axis_review] "
        "latest DC and Alameda:"
    )
    print(
        latest[
            [
                "geo_id",
                "date",
                "major_regime",
                "minor_regime",
                "demand_axis_score",
                "supply_axis_score",
                "dominant_axis",
                "axis_dominance_margin",
                "axis_dominance_ratio",
            ]
            + [
                column
                for column in [
                    "angle_degrees",
                    "regime_strength",
                    "distance_to_boundary_degrees",
                    "demand_dimension_score",
                    "supply_dimension_score",
                    "affordability_dimension_score",
                    "price_dimension_score",
                    "capital_markets_dimension_score",
                    "derived_freshness_status",
                    "maximum_derived_component_age_days",
                    "any_stale_derived_input",
                    "any_exceeded_derived_horizon",
                ]
                if column
                in latest.columns
            ]
        ]
        .sort_values(
            "geo_id"
        )
        .to_string(index=False)
    )

    required_outputs = [
        "monthly_timeline",
        "axis_events",
        "transition_timeline",
        "axis_summary",
        "latest_snapshot",
    ]

    for key in required_outputs:
        if review[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    required_timeline_columns = {
        "geo_id",
        "date",
        "major_regime",
        "minor_regime",
        "demand_axis_score",
        "supply_axis_score",
        "dominant_axis",
        "axis_dominance_margin",
        "major_changed",
        "minor_changed",
    }

    missing = (
        required_timeline_columns
        - set(timeline.columns)
    )

    if missing:
        raise AssertionError(
            "Timeline is missing columns: "
            f"{sorted(missing)}"
        )

    if set(
        timeline["geo_id"].unique()
    ) != set(DEFAULT_REVIEW_GEOS):
        raise AssertionError(
            "Chronological review did not return "
            "the expected geographies"
        )

    if timeline[
        [
            "demand_axis_score",
            "supply_axis_score",
        ]
    ].isna().all(axis=None):
        raise AssertionError(
            "All axis scores are missing"
        )

    print(
        "\n[chronological_axis_review] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
