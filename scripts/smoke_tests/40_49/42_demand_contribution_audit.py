from __future__ import annotations
# scripts/smoke_tests/40_49/42_demand_contribution_audit.py

import numpy as np

from regime.experiments.demand_contribution_audit import (
    TARGET_DIMENSIONS,
    build_demand_contribution_audit,
)


def main() -> int:
    print(
        "[demand_contribution] "
        "building Demand contribution audit..."
    )

    result = (
        build_demand_contribution_audit()
    )

    weights = result[
        "demand_weights"
    ]

    contributions = result[
        "dimension_contributions"
    ]

    monthly_panel = result[
        "monthly_contribution_panel"
    ]

    run_comparison = result[
        "run_comparison"
    ]

    contribution_summary = result[
        "contribution_summary"
    ]

    cancellation_summary = result[
        "cancellation_summary"
    ]

    near_zero_summary = result[
        "near_zero_summary"
    ]

    dominant_summary = result[
        "dominant_dimension_summary"
    ]

    historical_summary = result[
        "historical_summary"
    ]

    largest_changes = result[
        "largest_change_months"
    ]

    if weights.empty:
        raise AssertionError(
            "Demand weights are empty"
        )

    if not np.isclose(
        weights[
            "dimension_weight"
        ].sum(),
        1.0,
        rtol=0.0,
        atol=1e-12,
    ):
        raise AssertionError(
            "Demand weights do not sum to 1"
        )

    if not TARGET_DIMENSIONS.issubset(
        set(
            weights[
                "dimension"
            ]
        )
    ):
        raise AssertionError(
            "Price/Affordability are missing "
            "from Demand weights"
        )

    expected_roles = {
        "baseline",
        "challenger",
    }

    if set(
        contributions[
            "run_role"
        ].unique()
    ) != expected_roles:
        raise AssertionError(
            "Contribution history is missing "
            "a run role"
        )

    if (
        monthly_panel[
            "reconstruction_error"
        ].abs().max()
        > 1e-12
    ):
        raise AssertionError(
            "Demand reconstruction is not exact"
        )

    if (
        run_comparison[
            "unexplained_axis_delta"
        ].abs().max()
        > 1e-12
    ):
        raise AssertionError(
            "Demand delta contains unexplained "
            "movement"
        )

    for frame_name, frame in (
        (
            "contribution summary",
            contribution_summary,
        ),
        (
            "cancellation summary",
            cancellation_summary,
        ),
        (
            "near-zero summary",
            near_zero_summary,
        ),
        (
            "dominant dimension summary",
            dominant_summary,
        ),
        (
            "historical summary",
            historical_summary,
        ),
        (
            "largest change months",
            largest_changes,
        ),
    ):
        if frame.empty:
            raise AssertionError(
                f"{frame_name} is empty"
            )

    cancellation_rates = monthly_panel[
        "price_affordability_cancellation_rate"
    ]

    if (
        cancellation_rates.lt(0).any()
        or cancellation_rates.gt(1).any()
    ):
        raise AssertionError(
            "Cancellation rate fell outside [0, 1]"
        )

    print(
        "\n[demand_contribution] "
        "Demand weights:"
    )

    print(
        weights.sort_values(
            "dimension_weight",
            ascending=False,
        ).to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] "
        "contribution summary:"
    )

    print(
        contribution_summary.sort_values(
            [
                "geo_id",
                "run_role",
                "mean_absolute_weighted_contribution",
            ],
            ascending=[
                True,
                True,
                False,
            ],
        ).to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] "
        "Price/Affordability cancellation:"
    )

    print(
        cancellation_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] "
        "near-zero Demand summary:"
    )

    print(
        near_zero_summary.sort_values(
            [
                "geo_id",
                "threshold",
                "run_role",
            ]
        ).to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] "
        "dominant dimensions during "
        "|Demand| < 0.10:"
    )

    print(
        dominant_summary.to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] "
        "historical-period summary:"
    )

    print(
        historical_summary.sort_values(
            [
                "geo_id",
                "period",
            ]
        ).to_string(
            index=False
        )
    )

    display_columns = [
        column
        for column in [
            "geo_id",
            "date",
            "axis_score_baseline",
            "axis_score_challenger",
            "axis_score_delta",
            "price_baseline",
            "price_challenger",
            "price_delta",
            "affordability_baseline",
            "affordability_challenger",
            "affordability_delta",
            (
                "price_affordability_"
                "cancellation_rate_baseline"
            ),
            (
                "price_affordability_"
                "cancellation_rate_challenger"
            ),
            "other_demand_contribution_baseline",
            "other_demand_contribution_challenger",
        ]
        if column in largest_changes.columns
    ]

    print(
        "\n[demand_contribution] "
        "largest Demand changes:"
    )

    print(
        largest_changes[
            display_columns
        ].to_string(
            index=False
        )
    )

    print(
        "\n[demand_contribution] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
