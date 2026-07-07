from __future__ import annotations
# scripts/smoke_test_feature_engine.py

from regime.feature_engine import build_feature_matrix


SAMPLES = [
    "redfin_median_sale_price_level",
    "redfin_median_sale_price_short",
    "redfin_median_sale_price_long",
    "bea_quarterly_gdp_short",
    "bea_quarterly_gdp_long",
    "population_short",
    "population_long",
    "median_household_income_short",
    "median_household_income_long",
]


def main() -> int:
    features = build_feature_matrix()

    print("[feature_engine] rows:", len(features))
    print("[feature_engine] geos:", features["geo_id"].nunique())
    print("[feature_engine] feature_keys:", features["feature_key"].nunique())
    print("[feature_engine] date range:", features["date"].min(), "→", features["date"].max())

    print("\n[feature_engine] feature counts:")
    print(
        features.groupby("feature_key")
        .size()
        .reset_index(name="rows")
        .sort_values("feature_key")
        .to_string(index=False)
    )

    for feature_key in SAMPLES:
        sample = (
            features[features["feature_key"] == feature_key]
            .sort_values(["geo_id", "date"])
            .head(12)
        )

        print(f"\n[feature_engine] sample: {feature_key}")
        if sample.empty:
            print("  MISSING")
        else:
            print(sample.to_string(index=False))

    print("\n[feature_engine] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
