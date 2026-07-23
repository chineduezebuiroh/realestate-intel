from __future__ import annotations

import numpy as np

from regime.experiments.demand_axis_attribution import (
    DIMENSIONS,
    run_demand_axis_attribution,
)


def main() -> None:
    result = run_demand_axis_attribution()

    monthly = result[
        "monthly_axis_attribution"
    ]
    
    effective = monthly[
        "effective_component_count"
    ].dropna()

    assert not effective.empty

    assert effective.ge(
        1.0 - 1e-12
    ).all()

    assert effective.le(
        len(DIMENSIONS) + 1e-12
    ).all()

    long_attribution = result[
        "monthly_dimension_contributions"
    ]

    diagnostics = result[
        "axis_reconstruction_diagnostics"
    ]

    absolute_share = result[
        "dimension_share_of_absolute_change"
    ]

    candidate_summary = result[
        "candidate_summary"
    ]

    assert not monthly.empty
    assert not long_attribution.empty
    assert not diagnostics.empty
    assert not absolute_share.empty
    assert not candidate_summary.empty

    assert set(
        monthly["geo_id"].unique()
    ) == {
        "alameda_county_ca__county",
        "district_of_columbia_dc__county",
    }

    expected_change_rows = (
        len(monthly)
        - monthly["geo_id"].nunique()
    )

    actual_change_rows = int(
        monthly["axis_change"]
        .notna()
        .sum()
    )

    assert actual_change_rows == (
        expected_change_rows
    )

    compared = monthly.dropna(
        subset=[
            "axis_change",
            "reconstructed_axis_change",
        ]
    )

    assert not compared.empty

    residual = (
        compared[
            "axis_change_reconstruction_residual"
        ].abs()
    )

    assert residual.max() <= 1e-12

    share_columns = [
        f"{dimension}_share_of_absolute_change"
        for dimension in DIMENSIONS
    ]

    share_rows = monthly[
        monthly["gross_component_activity"]
        .gt(1e-12)
    ].copy()

    assert not share_rows.empty

    absolute_share_sum = (
        share_rows[
            share_columns
        ].sum(axis=1)
    )

    assert np.allclose(
        absolute_share_sum,
        1.0,
        atol=1e-12,
        rtol=0.0,
    )

    expected_long_rows = (
        len(monthly)
        * len(DIMENSIONS)
    )

    assert len(long_attribution) == (
        expected_long_rows
    )

    assert (
        diagnostics[
            "max_absolute_residual"
        ]
        .fillna(0)
        .le(1e-12)
        .all()
    )

    required_monthly_columns = {
        "aligned_component_activity",
        "opposing_component_activity",
        "gross_component_activity",
        "component_cancellation",
        "component_cancellation_rate",
        "net_to_gross_ratio",
    }

    assert required_monthly_columns.issubset(
        monthly.columns
    )
    
    required_long_columns = {
        "aligned_contribution_activity",
        "opposing_contribution_activity",
        "share_of_axis_change",
        "share_of_absolute_change",
    }

    assert required_long_columns.issubset(
        long_attribution.columns
    )

    print(
        "\n"
        + "=" * 100
    )

    print(
        "SMOKE TEST 57 — DEMAND AXIS "
        "ATTRIBUTION: PASS"
    )

    print(
        "=" * 100
    )

    print(
        f"monthly_rows={len(monthly):,}"
    )

    print(
        f"change_rows={actual_change_rows:,}"
    )

    print(
        "long_attribution_rows="
        f"{len(long_attribution):,}"
    )

    print(
        "geographies="
        f"{monthly['geo_id'].nunique():,}"
    )

    print(
        "first_date="
        f"{monthly['date'].min().date()}"
    )

    print(
        "last_date="
        f"{monthly['date'].max().date()}"
    )

    print(
        "max_axis_change_reconstruction_residual="
        f"{residual.max():.12g}"
    )

    print(
        "max_absolute_share_sum_residual="
        f"{(absolute_share_sum - 1.0).abs().max():.12g}"
    )
    
    print(
        "effective_component_count_range="
        f"{effective.min():.6f} "
        f"to {effective.max():.6f}"
    )


if __name__ == "__main__":
    main()
