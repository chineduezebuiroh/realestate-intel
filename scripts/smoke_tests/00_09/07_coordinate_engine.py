from __future__ import annotations
# scripts/smoke_tests/00_09/07_coordinate_engine.py

from regime._07_coordinate_engine import build_coordinates


SAMPLE_GEO = "alameda_county_ca__county"


def main() -> int:
    coords = build_coordinates()

    print("[coordinate_engine] rows:", len(coords))
    print("[coordinate_engine] geos:", coords["geo_id"].nunique())
    print("[coordinate_engine] date range:", coords["date"].min(), "→", coords["date"].max())

    print("\n[coordinate_engine] radius summary:")
    print(coords["radius"].describe().to_string())

    print("\n[coordinate_engine] angle summary:")
    print(coords["angle_degrees"].describe().to_string())

    latest_date = coords[coords["geo_id"] == SAMPLE_GEO]["date"].max()
    sample = coords[
        (coords["geo_id"] == SAMPLE_GEO)
        & (coords["date"] == latest_date)
    ]

    print(f"\n[coordinate_engine] latest sample for {SAMPLE_GEO} at {latest_date}:")
    print(sample.to_string(index=False))

    print("\n[coordinate_engine] sample history:")
    print(
        coords[coords["geo_id"] == SAMPLE_GEO]
        .sort_values("date")
        .tail(12)
        .to_string(index=False)
    )

    print("\n[coordinate_engine] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
