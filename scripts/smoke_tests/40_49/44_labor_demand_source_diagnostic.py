from __future__ import annotations
# scripts/smoke_tests/40_49/44_labor_demand_source_diagnostic.py

import numpy as np

from regime.experiments.labor_demand_source_diagnostic import (
    LAUS_METRICS,
    ROLLING_WINDOWS,
    build_labor_demand_source_diagnostic,
)


def main() -> int:
    print("[labor_demand_source] building diagnostic...")
    result = build_labor_demand_source_diagnostic()

    source_panel = result["source_panel"]
    raw_summary = result["raw_summary"]
    candidate_summary = result["candidate_summary"]
    current_summary = result["current_feature_summary"]
    month_summary = result["month_of_year_summary"]

    expected_metrics = set(LAUS_METRICS)
    actual_metrics = set(source_panel["canonical_metric_key"].unique())
    if actual_metrics != expected_metrics:
        raise AssertionError(
            "Unexpected LAUS metrics. "
            f"Expected {sorted(expected_metrics)}, "
            f"found {sorted(actual_metrics)}"
        )

    for frame_name, frame in (
        ("raw summary", raw_summary),
        ("candidate summary", candidate_summary),
        ("current feature summary", current_summary),
        ("month-of-year summary", month_summary),
    ):
        if frame.empty:
            raise AssertionError(f"{frame_name} is empty")

    if set(candidate_summary["window"].unique()) != set(ROLLING_WINDOWS):
        raise AssertionError("Unexpected LAUS moving-average windows")

    if set(candidate_summary["lag_periods"].unique()) != {1, 3, 12}:
        raise AssertionError("Unexpected LAUS lag matrix")

    variance_checks = (
        (raw_summary, "raw_change_1m_calendar_month_variance_share"),
        (raw_summary, "raw_change_3m_calendar_month_variance_share"),
        (candidate_summary, "calendar_month_variance_share"),
        (current_summary, "raw_feature_calendar_month_variance_share"),
        (current_summary, "feature_score_calendar_month_variance_share"),
    )
    for frame, column in variance_checks:
        values = frame[column].dropna()
        if values.lt(0).any() or values.gt(1).any():
            raise AssertionError(f"{column} fell outside [0, 1]")

    for column in ("mean_abs_movement", "p90_abs_movement"):
        values = candidate_summary[column].dropna()
        if not np.isfinite(values).all():
            raise AssertionError(f"{column} contains non-finite values")

    print("\n[labor_demand_source] raw summary:")
    print(
        raw_summary.sort_values(
            ["geo_id", "canonical_metric_key"]
        ).to_string(index=False)
    )

    print("\n[labor_demand_source] current production features:")
    print(
        current_summary.sort_values(
            ["geo_id", "canonical_metric_key", "feature_component"]
        ).to_string(index=False)
    )

    print("\n[labor_demand_source] candidate matrix:")
    focus = candidate_summary[
        candidate_summary["lag_periods"].isin({3, 12})
    ]
    print(
        focus.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "window",
                "lag_periods",
            ]
        ).to_string(index=False)
    )

    print("\n[labor_demand_source] month-of-year raw changes:")
    print(
        month_summary.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "measure",
                "calendar_month",
            ]
        ).to_string(index=False)
    )

    print("\n[labor_demand_source] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
