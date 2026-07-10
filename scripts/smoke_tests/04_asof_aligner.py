from __future__ import annotations
# scripts/smoke_tests/04_asof_aligner.py

from regime._04_asof_aligner import align_metric_scores_asof


SAMPLE_GEO = "alameda_county_ca__county"


def main() -> int:
    aligned = align_metric_scores_asof()

    print("[asof_aligner] rows:", len(aligned))
    print("[asof_aligner] geos:", aligned["geo_id"].nunique())
    print("[asof_aligner] metrics:", aligned["canonical_metric_key"].nunique())
    print("[asof_aligner] evaluation date range:", aligned["evaluation_date"].min(), "→", aligned["evaluation_date"].max())

    print("\n[asof_aligner] metric age summary:")
    print(aligned["metric_age_days"].describe().to_string())

    latest_date = aligned[aligned["geo_id"] == SAMPLE_GEO]["evaluation_date"].max()

    sample = (
        aligned[
            (aligned["geo_id"] == SAMPLE_GEO)
            & (aligned["evaluation_date"] == latest_date)
        ]
        .sort_values("canonical_metric_key")
    )

    print(f"\n[asof_aligner] latest sample for {SAMPLE_GEO} at {latest_date}:")
    print(sample.to_string(index=False))

    print("\n[asof_aligner] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
