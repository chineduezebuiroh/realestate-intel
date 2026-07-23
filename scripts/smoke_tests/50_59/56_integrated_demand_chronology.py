from __future__ import annotations
# scripts/smoke_tests/50_59/56_integrated_demand_chronology.py

from regime.experiments.integrated_demand_chronology import (
    EXPECTED_DIMENSIONS,
    build_integrated_demand_chronology,
)


def main() -> None:
    result = build_integrated_demand_chronology()

    monthly = result[
        "monthly_integrated_demand_axis"
    ]
    
    delta_overlap = monthly.dropna(
        subset=[
            "integrated_minus_incumbent_axis",
            "price_family_axis_delta",
        ]
    )

    assert not delta_overlap.empty

    delta_residual = (
        delta_overlap[
            "integrated_minus_incumbent_axis"
        ]
        - delta_overlap[
            "price_family_axis_delta"
        ]
    ).abs()

    assert delta_residual.max() <= 1e-12
    
    axis_impact = result[
        "axis_impact_summary"
    ]

    complete = monthly[
        monthly["complete_dimension_coverage"]
    ].copy()
    
    expected_geographies = {
        "alameda_county_ca__county",
        "district_of_columbia_dc__county",
    }

    actual_geographies = set(
        monthly["geo_id"].unique()
    )

    assert actual_geographies == expected_geographies, (
        "Unexpected integrated geography universe: "
        f"{sorted(actual_geographies)}"
    )

    assert (
        monthly["complete_dimension_coverage"]
        .all()
    ), (
        "Phase D1 output contains incomplete "
        "geography-month rows."
    )

    assert len(monthly) == len(complete)

    assert (
        monthly["available_dimension_count"]
        .eq(len(EXPECTED_DIMENSIONS))
        .all()
    )

    assert not monthly.empty
    assert not complete.empty

    assert set(EXPECTED_DIMENSIONS).issubset(
        monthly.columns
    )

    assert (
        complete["available_dimension_count"]
        .eq(len(EXPECTED_DIMENSIONS))
        .all()
    )

    assert (
        complete["integrated_demand_axis"]
        .notna()
        .all()
    )

    expected_axis = sum(
        complete[
            f"{dimension}_weighted_contribution"
        ]
        for dimension in EXPECTED_DIMENSIONS
    )

    residual = (
        complete["integrated_demand_axis"]
        - expected_axis
    ).abs()

    assert residual.max() <= 1e-12
        
    for dimension in (
        "price",
        "affordability",
    ):
        source_roles = set(
            complete[
                f"{dimension}_source_role"
            ].dropna()
        )

        assert source_roles == {
            "ma12_price_family_challenger"
        }, (
            f"Unexpected source roles for {dimension}: "
            f"{sorted(source_roles)}"
        )
        
    for dimension in (
        "demand",
        "capital_markets",
    ):
        source_roles = set(
            complete[
                f"{dimension}_source_role"
            ].dropna()
        )

        assert source_roles == {
            "incumbent_dimension_history"
        }, (
            f"Unexpected source roles for {dimension}: "
            f"{sorted(source_roles)}"
        )

    print(
        "\n"
        + "=" * 100
    )
    
    assert (
        monthly["incumbent_axis_score"]
        .notna()
        .any()
    )

    overlap = monthly.dropna(
        subset=[
            "integrated_demand_axis",
            "incumbent_axis_score",
        ]
    )

    assert not overlap.empty
    assert not axis_impact.empty

    assert (
        overlap[
            "absolute_integrated_minus_incumbent_axis"
        ]
        .notna()
        .all()
    )
    
    print(
        "incumbent_axis_overlap_rows="
        f"{len(overlap):,}"
    )

    print(
        "mean_absolute_integrated_minus_incumbent="
        f"{overlap['absolute_integrated_minus_incumbent_axis'].mean():.6f}"
    )
    
    print(
        "SMOKE TEST 56 — INTEGRATED DEMAND "
        "CHRONOLOGY: PASS"
    )
    print(
        "=" * 100
    )

    print(
        f"rows={len(monthly):,}"
    )
    print(
        f"complete_rows={len(complete):,}"
    )
    print(
        "geographies="
        f"{complete['geo_id'].nunique():,}"
    )
    print(
        "first_complete_date="
        f"{complete['date'].min().date()}"
    )
    print(
        "last_complete_date="
        f"{complete['date'].max().date()}"
    )
    print(
        "max_axis_reconstruction_residual="
        f"{residual.max():.12g}"
    )
    print(
        "max_price_family_delta_reconstruction_residual="
        f"{delta_residual.max():.12g}"
    )


if __name__ == "__main__":
    main()
