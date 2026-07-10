from __future__ import annotations
# scripts/smoke_tests/17_permit_ma_features.py

import pandas as pd

from regime._01_feature_engine import build_feature_matrix


SAMPLE_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

PERMIT_FEATURES = [
    "bps_total_units_level",
    "bps_total_units_short",
    "bps_total_units_long",
    "permit_intensity_level",
    "permit_intensity_short",
    "permit_intensity_long",
]


def main() -> int:
    features = build_feature_matrix()

    sample = features[
        features["geo_id"].isin(SAMPLE_GEOS)
        & features["feature_key"].isin(PERMIT_FEATURES)
    ].copy()

    if sample.empty:
        raise AssertionError("No MA permit features were produced")

    print("[permit_ma_features] rows:", len(sample))
    print("[permit_ma_features] geos:", sample["geo_id"].nunique())
    print("[permit_ma_features] features:", sample["feature_key"].nunique())

    print("\n[permit_ma_features] coverage:")
    print(
        sample.groupby(["geo_id", "feature_key"])
        .agg(
            rows=("raw_feature_value", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            min_value=("raw_feature_value", "min"),
            max_value=("raw_feature_value", "max"),
        )
        .reset_index()
        .sort_values(["geo_id", "feature_key"])
        .to_string(index=False)
    )

    print("\n[permit_ma_features] latest values:")
    print(
        sample.sort_values(["geo_id", "feature_key", "date"])
        .groupby(["geo_id", "feature_key"], as_index=False)
        .tail(6)
        .sort_values(["geo_id", "date", "feature_key"])
        .to_string(index=False)
    )

    invalid = sample[
        ~pd.to_numeric(
            sample["raw_feature_value"],
            errors="coerce",
        ).notna()
    ]

    if not invalid.empty:
        raise AssertionError(
            "MA permit features contain non-numeric values"
        )

    print("\n[permit_ma_features] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
