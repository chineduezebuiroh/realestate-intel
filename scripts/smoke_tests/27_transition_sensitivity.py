from __future__ import annotations
# scripts/smoke_tests/27_transition_sensitivity.py

from regime.diagnostics.transition_sensitivity import (
    DEFAULT_AUDIT_GEOS,
    DEFAULT_RUN_ID,
    DEFAULT_WEIGHT_MULTIPLIER,
    TARGET_DIMENSIONS,
    TARGET_METRICS,
    build_transition_sensitivity_audit,
)


def main() -> int:
    audit = build_transition_sensitivity_audit(
        run_id=DEFAULT_RUN_ID,
        geo_ids=DEFAULT_AUDIT_GEOS,
        weight_multiplier=(
            DEFAULT_WEIGHT_MULTIPLIER
        ),
    )

    transitions = audit[
        "transition_events"
    ]

    sensitivity = audit[
        "transition_sensitivity"
    ]

    summary = audit[
        "sensitivity_summary"
    ]

    persistence = audit[
        "transition_persistence"
    ]

    price_affordability = audit[
        "price_affordability_sensitivity"
    ]

    print(
        "[transition_sensitivity] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "[transition_sensitivity] transitions:",
        transitions["transition_id"].nunique(),
    )

    print(
        "[transition_sensitivity] "
        "counterfactual rows:",
        len(sensitivity),
    )

    print(
        "\n[transition_sensitivity] "
        "sensitivity summary:"
    )
    print(
        summary.to_string(index=False)
    )

    print(
        "\n[transition_sensitivity] "
        "Price/Affordability counterfactuals "
        "that changed an assignment:"
    )
    print(
        price_affordability[
            price_affordability[
                "major_assignment_changed"
            ]
            | price_affordability[
                "minor_assignment_changed"
            ]
        ][
            [
                "geo_id",
                "date",
                "target_level",
                "target_key",
                "scenario",
                "previous_major_regime",
                "major_regime",
                "counterfactual_major_regime",
                "previous_minor_regime",
                "minor_regime",
                "counterfactual_minor_regime",
                "major_transition_prevented",
                "minor_transition_prevented",
                "regime_strength",
            ]
        ]
        .head(200)
        .to_string(index=False)
    )

    print(
        "\n[transition_sensitivity] "
        "transition persistence summary:"
    )
    print(
        persistence[
            [
                "geo_id",
                "date",
                "previous_major_regime",
                "major_regime",
                "previous_minor_regime",
                "minor_regime",
                "major_persists_1m",
                "major_persists_3m",
                "major_persists_6m",
                "major_reverses_1m",
                "major_reverses_3m",
                "major_reverses_6m",
                "minor_persists_1m",
                "minor_persists_3m",
                "minor_persists_6m",
            ]
        ]
        .tail(150)
        .to_string(index=False)
    )

    required_outputs = [
        "transition_events",
        "counterfactual_coordinates",
        "transition_sensitivity",
        "sensitivity_summary",
        "transition_persistence",
        "price_affordability_sensitivity",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    expected_scenarios = {
        "freeze",
        "remove",
        "reweight",
    }

    actual_scenarios = set(
        sensitivity["scenario"]
    )

    if actual_scenarios != expected_scenarios:
        raise AssertionError(
            "Scenario mismatch. "
            f"Expected {sorted(expected_scenarios)}, "
            f"found {sorted(actual_scenarios)}"
        )

    actual_dimensions = set(
        sensitivity.loc[
            sensitivity[
                "target_level"
            ].eq("dimension"),
            "target_key",
        ]
    )

    missing_dimensions = (
        set(TARGET_DIMENSIONS)
        - actual_dimensions
    )

    if missing_dimensions:
        raise AssertionError(
            "Missing target dimensions: "
            f"{sorted(missing_dimensions)}"
        )

    actual_metrics = set(
        sensitivity.loc[
            sensitivity[
                "target_level"
            ].eq("metric"),
            "target_key",
        ]
    )

    missing_metrics = (
        set(TARGET_METRICS)
        - actual_metrics
    )

    if missing_metrics:
        raise AssertionError(
            "Missing target metrics: "
            f"{sorted(missing_metrics)}"
        )

    rate_columns = [
        "major_transition_prevented_rate",
        "minor_transition_prevented_rate",
        "major_assignment_changed_rate",
        "minor_assignment_changed_rate",
    ]

    for column in rate_columns:
        if not (
            summary[column]
            .dropna()
            .between(
                0.0,
                1.0,
                inclusive="both",
            )
            .all()
        ):
            raise AssertionError(
                f"{column} must be between zero and one"
            )

    if not {
        "price",
        "affordability",
    }.issubset(
        set(
            price_affordability[
                "target_key"
            ]
        )
    ):
        raise AssertionError(
            "Price/Affordability dimension "
            "counterfactuals are missing"
        )

    impossible_major = persistence[
        persistence["major_persists_1m"]
        & persistence["major_reverses_1m"]
    ]

    if not impossible_major.empty:
        raise AssertionError(
            "A major transition cannot both persist "
            "and reverse at the same horizon"
        )

    impossible_minor = persistence[
        persistence["minor_persists_1m"]
        & persistence["minor_reverses_1m"]
    ]

    if not impossible_minor.empty:
        raise AssertionError(
            "A minor transition cannot both persist "
            "and reverse at the same horizon"
        )

    unavailable_effects = sensitivity[
        ~sensitivity["target_available"]
        & (
            sensitivity[
                "major_assignment_changed"
            ]
            | sensitivity[
                "minor_assignment_changed"
            ]
        )
    ]

    if not unavailable_effects.empty:
        raise AssertionError(
            "Unavailable targets changed a "
            "counterfactual assignment"
        )

    if not (
        sensitivity[
            "eligible_major_transition"
        ]
        <= sensitivity[
            "target_available"
        ]
    ).all():
        raise AssertionError(
            "Major eligibility cannot be true when "
            "the target is unavailable"
        )

    if not (
        sensitivity[
            "eligible_minor_transition"
        ]
        <= sensitivity[
            "target_available"
        ]
    ).all():
        raise AssertionError(
            "Minor eligibility cannot be true when "
            "the target is unavailable"
        )

    denominator_columns = {
        "evaluated_transition_count",
        "available_transition_count",
        "eligible_major_transition_count",
        "eligible_minor_transition_count",
    }

    missing_denominator_columns = (
        denominator_columns
        - set(summary.columns)
    )

    if missing_denominator_columns:
        raise AssertionError(
            "Sensitivity summary is missing corrected "
            "denominator columns: "
            f"{sorted(missing_denominator_columns)}"
        )

    invalid_major_counts = summary[
        summary[
            "major_transition_prevented_count"
        ]
        > summary[
            "eligible_major_transition_count"
        ]
    ]

    if not invalid_major_counts.empty:
        raise AssertionError(
            "Prevented major-transition count exceeds "
            "eligible major-transition count"
        )

    invalid_minor_counts = summary[
        summary[
            "minor_transition_prevented_count"
        ]
        > summary[
            "eligible_minor_transition_count"
        ]
    ]

    if not invalid_minor_counts.empty:
        raise AssertionError(
            "Prevented minor-transition count exceeds "
            "eligible minor-transition count"
        )

    print(
        "\n[transition_sensitivity] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
