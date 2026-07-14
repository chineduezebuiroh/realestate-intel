from __future__ import annotations
# scripts/smoke_tests/30_39/37_inventory_structural_window_diagnostic.py

import numpy as np

from regime.experiments.inventory_structural_window_diagnostic import (
    build_inventory_structural_window_diagnostic,
)


EXPECTED_POLICIES = {
    "inventory_ma3_momentum",
    "inventory_ma3_deviation",
    "inventory_ma6_structural",
    "inventory_ma12_structural",
    "inventory_ma6_momentum_lag1",
    "inventory_ma6_momentum_lag3",
    "inventory_ma12_momentum_lag1",
    "inventory_ma12_momentum_lag3",
}

EXPECTED_COMPONENTS = {"level", "short", "long"}

FOCUS_POLICIES = {
    "inventory_ma3_deviation",
    "inventory_ma6_momentum_lag1",
    "inventory_ma6_momentum_lag3",
    "inventory_ma12_momentum_lag1",
    "inventory_ma12_momentum_lag3",
}


def main() -> int:
    print("[inventory_structural] building diagnostic...")
    diagnostic = build_inventory_structural_window_diagnostic()

    feature_history = diagnostic["feature_history"]
    coverage = diagnostic["coverage_summary"]
    stability = diagnostic["stability_summary"]
    seasonal = diagnostic["seasonal_dependence_summary"]
    redundancy = diagnostic["feature_redundancy_summary"]
    equivalence = diagnostic["ma6_momentum_equivalence"]
    turning_points = diagnostic["turning_point_summary"]
    shock_summary = diagnostic["shock_summary"]

    actual_policies = set(feature_history["policy_id"].unique())
    if actual_policies != EXPECTED_POLICIES:
        raise AssertionError(
            "Unexpected policies. "
            f"Expected {sorted(EXPECTED_POLICIES)}, "
            f"found {sorted(actual_policies)}"
        )

    actual_components = set(coverage["feature_component"].unique())
    if actual_components != EXPECTED_COMPONENTS:
        raise AssertionError(
            "Unexpected feature components. "
            f"Expected {sorted(EXPECTED_COMPONENTS)}, "
            f"found {sorted(actual_components)}"
        )

    if equivalence.empty or not equivalence["rank_equivalent"].all():
        raise AssertionError(
            "MA6 structural short and MA3 momentum short "
            "must be rank-equivalent"
        )

    short_seasonal = seasonal[
        seasonal["policy_id"].isin(FOCUS_POLICIES)
        & seasonal["feature_component"].eq("short")
    ].copy()

    if short_seasonal.empty:
        raise AssertionError("Short-feature seasonality output is empty")

    variance_share = short_seasonal["calendar_month_variance_share"]
    if not np.isfinite(variance_share).all():
        raise AssertionError(
            "Short-feature calendar-month variance share is non-finite"
        )

    if variance_share.lt(0).any() or variance_share.gt(1).any():
        raise AssertionError(
            "Calendar-month variance share fell outside [0, 1]"
        )

    print("[inventory_structural] feature rows:", len(feature_history))
    print("[inventory_structural] coverage rows:", len(coverage))

    print("\n[inventory_structural] coverage summary:")
    print(
        coverage[coverage["policy_id"].isin(FOCUS_POLICIES)]
        .sort_values(["geo_id", "policy_id", "feature_component"])
        .to_string(index=False)
    )

    print("\n[inventory_structural] short-feature stability:")
    print(
        stability[
            stability["policy_id"].isin(FOCUS_POLICIES)
            & stability["feature_component"].eq("short")
        ]
        .sort_values(["geo_id", "policy_id"])
        .to_string(index=False)
    )

    print("\n[inventory_structural] short-feature seasonal dependence:")
    print(
        short_seasonal.sort_values(["geo_id", "policy_id"]).to_string(
            index=False
        )
    )

    print("\n[inventory_structural] within-policy redundancy:")
    print(
        redundancy[redundancy["policy_id"].isin(FOCUS_POLICIES)]
        .sort_values(
            ["geo_id", "policy_id", "left_component", "right_component"]
        )
        .to_string(index=False)
    )

    print("\n[inventory_structural] MA6 short / momentum equivalence:")
    print(equivalence.sort_values("geo_id").to_string(index=False))

    print("\n[inventory_structural] turning-point lag summary:")
    print(
        turning_points[
            turning_points["policy_id"].isin(
                {
                    "inventory_ma6_momentum_lag1",
                    "inventory_ma6_momentum_lag3",
                    "inventory_ma12_momentum_lag1",
                    "inventory_ma12_momentum_lag3",
                }
            )
        ]
        .sort_values(["geo_id", "policy_id", "feature_component"])
        .to_string(index=False)
    )

    print("\n[inventory_structural] shock summary:")
    print(
        shock_summary[shock_summary["policy_id"].isin(FOCUS_POLICIES)]
        .sort_values(["geo_id", "policy_id"])
        .to_string(index=False)
    )

    print("\n[inventory_structural] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
