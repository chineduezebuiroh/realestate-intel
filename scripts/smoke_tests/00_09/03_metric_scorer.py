from __future__ import annotations
# scripts/smoke_tests/00_09/03_metric_scorer.py

from regime._03_metric_scorer import score_metrics


SAMPLES = [
    "median_sale_price",
    "price_to_income",
    "payment_burden",
    "permit_intensity",
    "days_on_market",
    "mortgage_30y",
]


def main() -> int:
    metrics = score_metrics()

    print("[metric_scorer] rows:", len(metrics))
    print("[metric_scorer] geos:", metrics["geo_id"].nunique())
    print("[metric_scorer] metrics:", metrics["canonical_metric_key"].nunique())
    print("[metric_scorer] date range:", metrics["date"].min(), "→", metrics["date"].max())

    print("\n[metric_scorer] metric_score summary:")
    print(metrics["metric_score"].describe().to_string())

    print("\n[metric_scorer] metric counts:")
    print(
        metrics.groupby("canonical_metric_key")
        .size()
        .reset_index(name="rows")
        .sort_values("canonical_metric_key")
        .to_string(index=False)
    )

    print("\n[metric_scorer] sample metric summaries:")

    print("\n[metric_scorer] recent feature coverage:")
    recent = metrics[metrics["date"] >= metrics["date"].max() - __import__("pandas").Timedelta(days=730)].copy()
    
    print(
        recent.groupby("canonical_metric_key")
        .agg(
            rows=("metric_score", "size"),
            avg_feature_count=("feature_count", "mean"),
            pct_full_3_feature=("feature_count", lambda s: (s >= 3).mean()),
            min_feature_count=("feature_count", "min"),
            max_feature_count=("feature_count", "max"),
        )
        .reset_index()
        .sort_values("canonical_metric_key")
        .to_string(index=False)
    )
    
    print(
        metrics[metrics["canonical_metric_key"].isin(SAMPLES)]
        .groupby("canonical_metric_key")
        .agg(
            rows=("metric_score", "size"),
            min_score=("metric_score", "min"),
            max_score=("metric_score", "max"),
            mean_score=("metric_score", "mean"),
            avg_feature_count=("feature_count", "mean"),
        )
        .reset_index()
        .sort_values("canonical_metric_key")
        .to_string(index=False)
    )

    for metric_key in SAMPLES:
        sample = (
            metrics[metrics["canonical_metric_key"] == metric_key]
            .sort_values(["geo_id", "date"])
            .head(12)
        )

        print(f"\n[metric_scorer] sample: {metric_key}")
        if sample.empty:
            print("  MISSING")
        else:
            print(sample.to_string(index=False))

    print("\n[metric_scorer] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
