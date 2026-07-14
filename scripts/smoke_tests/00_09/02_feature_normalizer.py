from __future__ import annotations
# scripts/smoke_tests/00_09/02_feature_normalizer.py

from regime._02_feature_normalizer import normalize_features


SAMPLES = [
    "redfin_median_sale_price_level",
    "redfin_median_sale_price_short",
    "price_to_income_level",
    "payment_burden_level",
    "permit_intensity_level",
    "redfin_dom_level",
    "fred_mortgage_30y_level",
]


def main() -> int:
    scores = normalize_features()

    print("[feature_normalizer] rows:", len(scores))
    print("[feature_normalizer] geos:", scores["geo_id"].nunique())
    print("[feature_normalizer] feature_keys:", scores["feature_key"].nunique())
    print("[feature_normalizer] date range:", scores["date"].min(), "→", scores["date"].max())

    print("\n[feature_normalizer] score summary:")
    print(
        scores["feature_score"]
        .describe()
        .to_string()
    )

    print("\n[feature_normalizer] min/max by sample feature:")
    print(
        scores[scores["feature_key"].isin(SAMPLES)]
        .groupby("feature_key")
        .agg(
            rows=("feature_score", "size"),
            min_score=("feature_score", "min"),
            max_score=("feature_score", "max"),
            mean_score=("feature_score", "mean"),
        )
        .reset_index()
        .sort_values("feature_key")
        .to_string(index=False)
    )

    for feature_key in SAMPLES:
        sample = (
            scores[scores["feature_key"] == feature_key]
            .sort_values(["geo_id", "date"])
            .head(12)
        )

        print(f"\n[feature_normalizer] sample: {feature_key}")
        if sample.empty:
            print("  MISSING")
        else:
            print(sample.to_string(index=False))

    print("\n[feature_normalizer] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
