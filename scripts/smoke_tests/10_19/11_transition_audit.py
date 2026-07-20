from __future__ import annotations
# scripts/smoke_tests/10_19/11_transition_audit.py

from regime.validation import (
    DEFAULT_VALIDATION_GEOS,
    build_transition_audit,
    build_transition_events,
)


def main() -> int:
    audit = build_transition_audit()
    events = build_transition_events()

    print("[transition_audit] summary rows:", len(audit))
    print("[transition_audit] event rows:", len(events))
    print("[transition_audit] geos:", events["geo_id"].nunique())

    print("\n[transition_audit] summary:")
    print(audit.to_string(index=False))

    print("\n[transition_audit] transition type counts:")
    print(
        events.groupby(["geo_id", "transition_type"])
        .size()
        .reset_index(name="rows")
        .sort_values(["geo_id", "transition_type"])
        .to_string(index=False)
    )

    print("\n[transition_audit] low-radius transitions:")
    low_radius = events[events["regime_strength"] < 0.20].copy()
    if low_radius.empty:
        print("  NONE")
    else:
        total_by_geo = events.groupby("geo_id").size().rename("total")
        low_by_geo = (
            low_radius.groupby("geo_id")
            .agg(
                rows=("date", "size"),
                avg_boundary_distance=("distance_to_boundary_degrees", "mean"),
                avg_abs_angle_delta=("delta_angle_degrees", lambda s: s.abs().mean()),
            )
            .join(total_by_geo)
            .reset_index()
        )
        low_by_geo["share_of_transitions"] = low_by_geo["rows"] / low_by_geo["total"]
        print(low_by_geo.to_string(index=False))

    print("\n[transition_audit] near-boundary transitions:")
    near_boundary = events[events["distance_to_boundary_degrees"] < 5.0].copy()
    if near_boundary.empty:
        print("  NONE")
    else:
        print(
            near_boundary.groupby("geo_id")
            .agg(
                rows=("date", "size"),
                avg_regime_strength=("regime_strength", "mean"),
                avg_abs_angle_delta=("delta_angle_degrees", lambda s: s.abs().mean()),
            )
            .reset_index()
            .to_string(index=False)
        )

    for geo_id in DEFAULT_VALIDATION_GEOS:
        sample = events[events["geo_id"] == geo_id].sort_values("date")

        print(f"\n[transition_audit] largest angle transitions: {geo_id}")
        if sample.empty:
            print("  NONE")
            continue

        print(
            sample.assign(abs_angle_delta=sample["delta_angle_degrees"].abs())
            .sort_values("abs_angle_delta", ascending=False)
            .head(15)
            .to_string(index=False)
        )

        print(f"\n[transition_audit] latest transitions: {geo_id}")
        print(sample.tail(20).to_string(index=False))

    print("\n[transition_audit] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
