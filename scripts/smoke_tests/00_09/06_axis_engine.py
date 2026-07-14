from __future__ import annotations
# scripts/smoke_tests/00_09/06_axis_engine.py

from regime._06_axis_engine import score_axes


SAMPLE_GEO = "alameda_county_ca__county"


def main() -> int:
    axes = score_axes()

    print("[axis_engine] rows:", len(axes))
    print("[axis_engine] geos:", axes["geo_id"].nunique())
    print("[axis_engine] axes:", axes["axis"].nunique())
    print("[axis_engine] date range:", axes["date"].min(), "→", axes["date"].max())

    print("\n[axis_engine] axis_score summary:")
    print(axes["axis_score"].describe().to_string())

    print("\n[axis_engine] axis counts:")
    print(
        axes.groupby("axis")
        .size()
        .reset_index(name="rows")
        .sort_values("axis")
        .to_string(index=False)
    )

    print("\n[axis_engine] recent axis coverage:")
    recent = axes[
        axes["date"] >= axes["date"].max() - __import__("pandas").Timedelta(days=730)
    ].copy()

    print(
        recent.groupby("axis")
        .agg(
            rows=("axis_score", "size"),
            avg_dimension_count=("dimension_count", "mean"),
            min_dimension_count=("dimension_count", "min"),
            max_dimension_count=("dimension_count", "max"),
            avg_dimension_weight_sum=("dimension_weight_sum", "mean"),
            min_axis_score=("axis_score", "min"),
            max_axis_score=("axis_score", "max"),
            mean_axis_score=("axis_score", "mean"),
            max_dimension_age_days=("max_dimension_age_days", "max"),
        )
        .reset_index()
        .sort_values("axis")
        .to_string(index=False)
    )

    latest_date = axes[axes["geo_id"] == SAMPLE_GEO]["date"].max()

    sample = (
        axes[
            (axes["geo_id"] == SAMPLE_GEO)
            & (axes["date"] == latest_date)
        ]
        .sort_values("axis")
    )

    print(f"\n[axis_engine] latest sample for {SAMPLE_GEO} at {latest_date}:")
    print(sample.to_string(index=False))

    print("\n[axis_engine] sample history:")
    print(
        axes[axes["geo_id"] == SAMPLE_GEO]
        .sort_values(["date", "axis"])
        .tail(24)
        .to_string(index=False)
    )

    print("\n[axis_engine] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
