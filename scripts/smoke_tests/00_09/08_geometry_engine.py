from __future__ import annotations
# scripts/smoke_tests/00_09/08_geometry_engine.py

from regime._08_geometry_engine import assign_geometry


SAMPLE_GEO = "alameda_county_ca__county"


def main() -> int:
    regimes = assign_geometry()

    print("[geometry_engine] rows:", len(regimes))
    print("[geometry_engine] geos:", regimes["geo_id"].nunique())
    print("[geometry_engine] date range:", regimes["date"].min(), "→", regimes["date"].max())

    print("\n[geometry_engine] major regime counts:")
    print(
        regimes.groupby("major_regime")
        .size()
        .reset_index(name="rows")
        .sort_values("major_regime")
        .to_string(index=False)
    )

    print("\n[geometry_engine] minor regime counts:")
    print(
        regimes.groupby("minor_regime")
        .size()
        .reset_index(name="rows")
        .sort_values("minor_regime")
        .to_string(index=False)
    )

    latest_date = regimes[regimes["geo_id"] == SAMPLE_GEO]["date"].max()
    sample = regimes[
        (regimes["geo_id"] == SAMPLE_GEO)
        & (regimes["date"] == latest_date)
    ]

    print(f"\n[geometry_engine] latest sample for {SAMPLE_GEO} at {latest_date}:")
    print(sample.to_string(index=False))

    print("\n[geometry_engine] sample history:")
    print(
        regimes[regimes["geo_id"] == SAMPLE_GEO]
        .sort_values("date")
        .tail(18)
        .to_string(index=False)
    )

    print("\n[geometry_engine] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
