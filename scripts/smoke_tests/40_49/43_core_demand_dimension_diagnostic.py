from __future__ import annotations
# scripts/smoke_tests/40_49/43_core_demand_dimension_diagnostic.py

import numpy as np

from regime.experiments.core_demand_dimension_diagnostic import (
    build_core_demand_dimension_diagnostic,
)


def main() -> int:
    print("[core_demand] building diagnostic...")

    result = build_core_demand_dimension_diagnostic()

    metric_registry = result["metric_registry"]
    feature_registry = result["feature_registry"]
    metric_summary = result["metric_summary"]
    feature_summary = result["feature_summary"]
    contribution_summary = result["contribution_summary"]
    cancellation_summary = result["cancellation_summary"]
    dominant_summary = result["dominant_metric_summary"]
    correlations = result["pairwise_metric_correlations"]
    latest_panel = result["latest_monthly_panel"]
    monthly_panel = result["monthly_contribution_panel"]

    for frame_name, frame in (
        ("metric registry", metric_registry),
        ("feature registry", feature_registry),
        ("metric summary", metric_summary),
        ("feature summary", feature_summary),
        ("contribution summary", contribution_summary),
        ("cancellation summary", cancellation_summary),
        ("dominant metric summary", dominant_summary),
        ("pairwise correlations", correlations),
        ("latest panel", latest_panel),
    ):
        if frame.empty:
            raise AssertionError(f"{frame_name} is empty")

    maximum_error = monthly_panel["reconstruction_error"].abs().max()

    if (
        not np.isfinite(maximum_error)
        or maximum_error > 1e-12
    ):
        raise AssertionError(
            "Core Demand dimension did not reconstruct exactly"
        )

    cancellation_rates = monthly_panel["cancellation_rate"]

    if (
        cancellation_rates.lt(0).any()
        or cancellation_rates.gt(1).any()
    ):
        raise AssertionError(
            "Core Demand cancellation rate fell outside [0, 1]"
        )

    print("\n[core_demand] metric registry:")
    print(metric_registry.to_string(index=False))

    print("\n[core_demand] feature registry:")
    print(feature_registry.to_string(index=False))

    print("\n[core_demand] metric summary:")
    print(
        metric_summary.sort_values(
            ["geo_id", "mean_absolute_change_1m"],
            ascending=[True, False],
        ).to_string(index=False)
    )

    print("\n[core_demand] feature summary:")
    print(
        feature_summary.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "mean_absolute_change_1m",
            ],
            ascending=[True, True, False],
        ).to_string(index=False)
    )

    print("\n[core_demand] contribution summary:")
    print(
        contribution_summary.sort_values(
            [
                "geo_id",
                "mean_absolute_weighted_contribution",
            ],
            ascending=[True, False],
        ).to_string(index=False)
    )

    print("\n[core_demand] cancellation summary:")
    print(cancellation_summary.to_string(index=False))

    print(
        "\n[core_demand] dominant metrics during "
        "|Demand dimension| < 0.10:"
    )
    print(dominant_summary.to_string(index=False))

    print("\n[core_demand] pairwise metric correlations:")
    print(
        correlations.sort_values(
            ["geo_id", "pearson_correlation"]
        ).to_string(index=False)
    )

    print("\n[core_demand] latest monthly panel:")
    print(latest_panel.to_string(index=False))

    print("\n[core_demand] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
