from __future__ import annotations
# scripts/smoke_tests/13_metric_contribution_audit.py

from regime.validation import (
    DEFAULT_VALIDATION_GEOS,
    build_metric_contribution_audit,
)


def main() -> int:
    audit = build_metric_contribution_audit(axis="supply")

    summary = audit["metric_summary"]
    by_geo = audit["by_geo_metric"]
    events = audit["transition_metric_events"]
    top_events = audit["top_metric_events"]

    print("[metric_contribution_audit] transition metric rows:", len(events))
    print("[metric_contribution_audit] geos:", events["geo_id"].nunique())
    print("[metric_contribution_audit] metrics:", events["canonical_metric_key"].nunique())

    print("\n[metric_contribution_audit] supply metric summary:")
    print(summary.to_string(index=False))

    print("\n[metric_contribution_audit] supply metric summary by geo:")
    print(by_geo.to_string(index=False))

    for geo_id in DEFAULT_VALIDATION_GEOS:
        sample = top_events[top_events["geo_id"] == geo_id].copy()

        print(f"\n[metric_contribution_audit] largest supply metric contribution events: {geo_id}")
        if sample.empty:
            print("  NONE")
            continue

        print(
            sample[
                [
                    "geo_id",
                    "date",
                    "major_regime",
                    "minor_regime",
                    "previous_major_regime",
                    "previous_minor_regime",
                    "canonical_metric_key",
                    "dimension",
                    "metric_score",
                    "previous_metric_score",
                    "delta_metric_score",
                    "axis_metric_weight",
                    "delta_weighted_metric_contribution",
                    "metric_age_days",
                    "supply_pressure_score",
                    "delta_supply_pressure_score",
                    "demand_strength_score",
                    "delta_demand_strength_score",
                    "regime_strength",
                    "delta_regime_strength",
                    "angle_degrees",
                    "delta_angle_degrees",
                ]
            ]
            .head(30)
            .to_string(index=False)
        )

        print(f"\n[metric_contribution_audit] latest transition metric events: {geo_id}")
        latest = (
            events[events["geo_id"] == geo_id]
            .sort_values(["date", "canonical_metric_key"])
            .tail(45)
        )

        print(
            latest[
                [
                    "geo_id",
                    "date",
                    "canonical_metric_key",
                    "metric_score",
                    "previous_metric_score",
                    "delta_metric_score",
                    "axis_metric_weight",
                    "delta_weighted_metric_contribution",
                    "metric_age_days",
                    "major_regime",
                    "minor_regime",
                    "previous_major_regime",
                    "previous_minor_regime",
                    "supply_pressure_score",
                    "delta_supply_pressure_score",
                ]
            ].to_string(index=False)
        )

    print("\n[metric_contribution_audit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
