from __future__ import annotations
# scripts/smoke_tests/10_historical_trajectory.py

from regime.validation import DEFAULT_VALIDATION_GEOS, build_historical_trajectory


def main() -> int:
    trajectory = build_historical_trajectory()

    print("[historical_trajectory] rows:", len(trajectory))
    print("[historical_trajectory] geos:", trajectory["geo_id"].nunique())
    print("[historical_trajectory] date range:", trajectory["date"].min(), "→", trajectory["date"].max())

    print("\n[historical_trajectory] rows by geo:")
    print(
        trajectory.groupby("geo_id")
        .size()
        .reset_index(name="rows")
        .sort_values("geo_id")
        .to_string(index=False)
    )

    print("\n[historical_trajectory] transition counts by geo:")
    print(
        trajectory.groupby("geo_id")
        .agg(
            major_transitions=("major_changed", "sum"),
            minor_transitions=("minor_changed", "sum"),
            avg_regime_strength=("regime_strength", "mean"),
            median_regime_strength=("regime_strength", "median"),
            avg_boundary_distance=("distance_to_boundary_degrees", "mean"),
            median_boundary_distance=("distance_to_boundary_degrees", "median"),
            max_axis_age_days=("max_axis_age_days", "max"),
        )
        .reset_index()
        .sort_values("geo_id")
        .to_string(index=False)
    )

    for geo_id in DEFAULT_VALIDATION_GEOS:
        sample = trajectory[trajectory["geo_id"] == geo_id].sort_values("date")

        print(f"\n[historical_trajectory] latest history: {geo_id}")
        if sample.empty:
            print("  MISSING")
            continue

        print(
            sample.tail(24)
            .to_string(index=False)
        )

        transitions = sample[sample["major_changed"] | sample["minor_changed"]].copy()

        print(f"\n[historical_trajectory] recent transitions: {geo_id}")
        if transitions.empty:
            print("  NONE")
        else:
            print(
                transitions.tail(24)
                .to_string(index=False)
            )

    print("\n[historical_trajectory] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
