from __future__ import annotations
# scripts/smoke_tests/09_regime_assignment.py

from regime._09_regime_assignment import assign_regimes


SAMPLE_GEO = "alameda_county_ca__county"


def main() -> int:
    regimes = assign_regimes()

    print("[regime_assignment] rows:", len(regimes))
    print("[regime_assignment] geos:", regimes["geo_id"].nunique())
    print("[regime_assignment] date range:", regimes["date"].min(), "→", regimes["date"].max())

    print("\n[regime_assignment] major regime counts:")
    print(
        regimes.groupby("major_regime")
        .size()
        .reset_index(name="rows")
        .sort_values("major_regime")
        .to_string(index=False)
    )

    print("\n[regime_assignment] minor regime counts:")
    print(
        regimes.groupby("minor_regime")
        .size()
        .reset_index(name="rows")
        .sort_values("minor_regime")
        .to_string(index=False)
    )

    latest_date = regimes[regimes["geo_id"] == SAMPLE_GEO]["date"].max()

    print(f"\n[regime_assignment] latest sample for {SAMPLE_GEO} at {latest_date}:")
    print(
        regimes[
            (regimes["geo_id"] == SAMPLE_GEO)
            & (regimes["date"] == latest_date)
        ].to_string(index=False)
    )

    print("\n[regime_assignment] sample history:")
    print(
        regimes[regimes["geo_id"] == SAMPLE_GEO]
        .sort_values("date")
        .tail(18)
        .to_string(index=False)
    )

    print("\n[regime_assignment] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
