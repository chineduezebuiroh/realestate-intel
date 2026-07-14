from __future__ import annotations
# scripts/smoke_tests/30_39/38_price_family_source_diagnostic.py

import numpy as np

from regime.experiments.price_family_source_diagnostic import (
    PRICE_FAMILY_METRICS,
    build_price_family_source_diagnostic,
)


def main() -> int:
    print("[price_family_source] building diagnostic...")

    diagnostic = build_price_family_source_diagnostic()

    panel = diagnostic["source_panel"]
    summary = diagnostic["source_summary"]
    windows = diagnostic["window_summary"]
    correlations = diagnostic["cross_metric_correlations"]
    latest = diagnostic["latest_panel"]

    expected_metrics = set(PRICE_FAMILY_METRICS)
    actual_metrics = set(panel["canonical_metric_key"].unique())

    if actual_metrics != expected_metrics:
        raise AssertionError(
            f"Expected {sorted(expected_metrics)}, found {sorted(actual_metrics)}"
        )

    if summary.empty or windows.empty or correlations.empty or latest.empty:
        raise AssertionError("One or more price-family outputs are empty")

    if set(windows["window"].unique()) != {3, 6, 9, 12}:
        raise AssertionError("Unexpected MA window matrix")

    if set(windows["lag_months"].unique()) != {1, 3, 12}:
        raise AssertionError("Unexpected lag matrix")

    share_columns = [
        column
        for column in summary.columns
        if column.endswith("calendar_variance_share")
    ]

    for column in share_columns:
        values = summary[column].dropna()
        if values.lt(0).any() or values.gt(1).any():
            raise AssertionError(f"{column} fell outside [0, 1]")

    numeric_columns = [
        "total_change",
        "log_trend_slope_per_month",
        "log_trend_r_squared",
    ]

    for column in numeric_columns:
        if not np.isfinite(summary[column]).all():
            raise AssertionError(f"{column} contains non-finite values")

    print("[price_family_source] panel rows:", len(panel))
    print("[price_family_source] geographies:", panel["geo_id"].nunique())
    print("[price_family_source] metrics:", panel["canonical_metric_key"].nunique())

    print("\n[price_family_source] source summary:")
    print(
        summary.sort_values(
            ["geo_id", "canonical_metric_key"]
        ).to_string(index=False)
    )

    print("\n[price_family_source] MA window / lag summary:")
    print(
        windows[
            windows["lag_months"].isin({3, 12})
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "window",
                "lag_months",
            ]
        )
        .to_string(index=False)
    )

    print("\n[price_family_source] cross-metric correlations:")
    print(
        correlations[
            correlations["measure"].isin(
                {"value", "raw_change_3m", "raw_change_12m"}
            )
        ]
        .sort_values(
            ["geo_id", "measure", "left_metric", "right_metric"]
        )
        .to_string(index=False)
    )

    print("\n[price_family_source] latest observations:")
    print(
        latest.sort_values(
            ["geo_id", "canonical_metric_key", "date"]
        ).to_string(index=False)
    )

    print("\n[price_family_source] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
