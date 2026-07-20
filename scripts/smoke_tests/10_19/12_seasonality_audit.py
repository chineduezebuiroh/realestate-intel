from __future__ import annotations
# scripts/smoke_tests/10_19/12_seasonality_audit.py

from regime.validation import DEFAULT_VALIDATION_GEOS, build_seasonality_audit


def main() -> int:
    audit = build_seasonality_audit()

    counts = audit["transition_counts_by_month"]
    calendar = audit["transition_calendar"]
    movement = audit["monthly_movement"]
    diagnostics = audit["monthly_diagnostics"]
    events = audit["transition_events"]

    print("[seasonality_audit] transition rows:", len(events))
    print("[seasonality_audit] geos:", events["geo_id"].nunique())

    print("\n[seasonality_audit] transition counts by month:")
    print(
        counts[
            [
                "geo_id",
                "month",
                "month_name",
                "transitions",
                "major_transitions",
                "minor_transitions",
                "transition_share",
                "avg_regime_strength",
                "avg_boundary_distance",
                "avg_abs_supply_delta",
                "avg_abs_demand_delta",
                "avg_abs_angle_delta",
                "avg_axis_age_days",
            ]
        ].to_string(index=False)
    )

    print("\n[seasonality_audit] transition calendar:")
    print(calendar.to_string(index=False))

    print("\n[seasonality_audit] monthly movement:")
    print(
        movement[
            [
                "geo_id",
                "month",
                "month_name",
                "observations",
                "transition_rate",
                "avg_abs_supply_delta",
                "median_abs_supply_delta",
                "avg_abs_demand_delta",
                "median_abs_demand_delta",
                "avg_abs_radius_delta",
                "median_abs_radius_delta",
                "avg_abs_angle_delta",
                "median_abs_angle_delta",
            ]
        ].to_string(index=False)
    )

    print("\n[seasonality_audit] monthly diagnostics:")
    print(
        diagnostics[
            [
                "geo_id",
                "month",
                "month_name",
                "avg_regime_strength",
                "median_regime_strength",
                "avg_boundary_distance",
                "median_boundary_distance",
                "avg_axis_age_days",
                "median_axis_age_days",
                "max_axis_age_days",
            ]
        ].to_string(index=False)
    )

    for geo_id in DEFAULT_VALIDATION_GEOS:
        geo_events = events[events["geo_id"] == geo_id].sort_values("date")

        print(f"\n[seasonality_audit] last 36 transition events: {geo_id}")
        if geo_events.empty:
            print("  NONE")
        else:
            print(
                geo_events.tail(36)[
                    [
                        "geo_id",
                        "date",
                        "month_name",
                        "major_regime",
                        "minor_regime",
                        "previous_major_regime",
                        "previous_minor_regime",
                        "regime_strength",
                        "distance_to_boundary_degrees",
                        "delta_supply_pressure_score",
                        "delta_demand_strength_score",
                        "delta_regime_strength",
                        "delta_angle_degrees",
                        "max_axis_age_days",
                    ]
                ].to_string(index=False)
            )

    print("\n[seasonality_audit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
