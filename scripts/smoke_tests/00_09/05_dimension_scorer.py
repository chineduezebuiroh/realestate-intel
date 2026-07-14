from __future__ import annotations
# scripts/smoke_tests/00_09/05_dimension_scorer.py

from regime._05_dimension_scorer import score_dimensions


SAMPLES = [
    "demand",
    "supply",
    "affordability",
    "transaction_activity",
    "price",
    "capital_markets",
    "liquidity",
]


def main() -> int:
    dimensions = score_dimensions()

    print("[dimension_scorer] rows:", len(dimensions))
    print("[dimension_scorer] geos:", dimensions["geo_id"].nunique())
    print("[dimension_scorer] dimensions:", dimensions["dimension"].nunique())
    print("[dimension_scorer] date range:", dimensions["date"].min(), "→", dimensions["date"].max())

    print("\n[dimension_scorer] dimension_score summary:")
    print(dimensions["dimension_score"].describe().to_string())

    print("\n[dimension_scorer] dimension counts:")
    print(
        dimensions.groupby("dimension")
        .size()
        .reset_index(name="rows")
        .sort_values("dimension")
        .to_string(index=False)
    )

    print("\n[dimension_scorer] recent metric coverage:")
    recent = dimensions[
        dimensions["date"] >= dimensions["date"].max() - __import__("pandas").Timedelta(days=730)
    ].copy()

    print(
        recent.groupby("dimension")
        .agg(
            rows=("dimension_score", "size"),
            avg_metric_count=("metric_count", "mean"),
            min_metric_count=("metric_count", "min"),
            max_metric_count=("metric_count", "max"),
            avg_metric_weight_sum=("metric_weight_sum", "mean"),
            min_score=("dimension_score", "min"),
            max_score=("dimension_score", "max"),
            mean_score=("dimension_score", "mean"),
        )
        .reset_index()
        .sort_values("dimension")
        .to_string(index=False)
    )

    print("\n[dimension_scorer] sample dimension summaries:")
    print(
        dimensions[dimensions["dimension"].isin(SAMPLES)]
        .groupby("dimension")
        .agg(
            rows=("dimension_score", "size"),
            min_score=("dimension_score", "min"),
            max_score=("dimension_score", "max"),
            mean_score=("dimension_score", "mean"),
            avg_metric_count=("metric_count", "mean"),
        )
        .reset_index()
        .sort_values("dimension")
        .to_string(index=False)
    )

    for dimension in SAMPLES:
        sample = (
            dimensions[dimensions["dimension"] == dimension]
            .sort_values(["geo_id", "date"])
            .head(12)
        )

        print(f"\n[dimension_scorer] sample: {dimension}")
        if sample.empty:
            print("  MISSING")
        else:
            print(sample.to_string(index=False))

    print("\n[dimension_scorer] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
