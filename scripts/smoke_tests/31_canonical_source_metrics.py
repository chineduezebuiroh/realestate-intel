from __future__ import annotations
# scripts/smoke_tests/31_canonical_source_metrics.py

import pandas as pd

from regime._00_config_loader import (
    load_regime_config,
)
from regime._01_feature_engine import (
    CANONICAL_SOURCE_METRIC_COLUMNS,
    build_canonical_source_metrics_with_lineage,
    build_feature_matrix_with_lineage,
)


FOCUS_GEOS = {
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
}

FOCUS_METRICS = {
    "active_inventory",
    "median_sale_price",
    "median_ppsf",
    "price_to_income",
    "payment_burden",
    "permit_activity",
    "permit_intensity",
}


def main() -> int:
    config = load_regime_config(
        validate=True
    )

    (
        source_metrics,
        derived_metric_lineage,
    ) = (
        build_canonical_source_metrics_with_lineage(
            config=config
        )
    )

    (
        features_from_persistable_input,
        lineage_from_persistable_input,
    ) = build_feature_matrix_with_lineage(
        config=config,
        canonical_observations=(
            source_metrics
        ),
        derived_metric_lineage=(
            derived_metric_lineage
        ),
    )

    (
        features_from_default_path,
        lineage_from_default_path,
    ) = build_feature_matrix_with_lineage(
        config=config
    )
  
    print(
        "[canonical_source_metrics] rows:",
        len(source_metrics),
    )

    print(
        "[canonical_source_metrics] geographies:",
        source_metrics[
            "geo_id"
        ].nunique(),
    )

    print(
        "[canonical_source_metrics] metrics:",
        source_metrics[
            "canonical_metric_key"
        ].nunique(),
    )

    print(
        "[canonical_source_metrics] date range:",
        source_metrics["date"].min(),
        "→",
        source_metrics["date"].max(),
    )

    print(
        "\n[canonical_source_metrics] "
        "origin summary:"
    )

    print(
        source_metrics.groupby(
            "metric_origin",
            as_index=False,
        )
        .agg(
            rows=("value", "size"),
            geographies=(
                "geo_id",
                "nunique",
            ),
            metrics=(
                "canonical_metric_key",
                "nunique",
            ),
            first_date=("date", "min"),
            last_date=("date", "max"),
        )
        .to_string(index=False)
    )

    focus = source_metrics[
        source_metrics[
            "geo_id"
        ].isin(FOCUS_GEOS)
        & source_metrics[
            "canonical_metric_key"
        ].isin(FOCUS_METRICS)
    ].copy()

    print(
        "\n[canonical_source_metrics] "
        "focus coverage:"
    )

    print(
        focus.groupby(
            [
                "geo_id",
                "canonical_metric_key",
                "metric_origin",
            ],
            as_index=False,
        )
        .agg(
            rows=("value", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
            minimum_value=("value", "min"),
            maximum_value=("value", "max"),
        )
        .to_string(index=False)
    )

    active_inventory = focus[
        focus[
            "canonical_metric_key"
        ].eq("active_inventory")
    ].copy()

    print(
        "\n[canonical_source_metrics] "
        "latest active inventory:"
    )

    print(
        active_inventory.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .tail(12)
        .to_string(index=False)
    )

    actual_columns = list(
        source_metrics.columns
    )

    if (
        actual_columns
        != CANONICAL_SOURCE_METRIC_COLUMNS
    ):
        raise AssertionError(
            "Canonical source metric column "
            "contract mismatch. "
            f"Expected "
            f"{CANONICAL_SOURCE_METRIC_COLUMNS}, "
            f"found {actual_columns}"
        )

    duplicates = source_metrics.duplicated(
        subset=[
            "geo_id",
            "date",
            "canonical_metric_key",
        ],
        keep=False,
    )

    if duplicates.any():
        raise AssertionError(
            "Canonical source metrics contain "
            "duplicate observation keys"
        )

    if set(
        source_metrics[
            "metric_origin"
        ].unique()
    ) != {
        "canonical_source",
        "derived",
    }:
        raise AssertionError(
            "Expected both canonical_source and "
            "derived metric origins"
        )

    missing_focus_metrics = (
        FOCUS_METRICS
        - set(
            focus[
                "canonical_metric_key"
            ].unique()
        )
    )

    if missing_focus_metrics:
        raise AssertionError(
            "Missing expected focus metrics: "
            f"{sorted(missing_focus_metrics)}"
        )

    missing_focus_geos = (
        FOCUS_GEOS
        - set(
            active_inventory[
                "geo_id"
            ].unique()
        )
    )

    if missing_focus_geos:
        raise AssertionError(
            "Missing active inventory for: "
            f"{sorted(missing_focus_geos)}"
        )

    if (
        active_inventory[
            "metric_origin"
        ]
        .ne("canonical_source")
        .any()
    ):
        raise AssertionError(
            "Active inventory must be a "
            "canonical source metric"
        )

    if (
        source_metrics[
            "value"
        ].isna().any()
    ):
        raise AssertionError(
            "Canonical source metrics contain "
            "missing values"
        )

    sort_columns = [
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
    ]

    comparison_columns = [
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_key",
        "raw_feature_value",
    ]

    left = (
        features_from_default_path[
            comparison_columns
        ]
        .sort_values(sort_columns)
        .reset_index(drop=True)
    )

    right = (
        features_from_persistable_input[
            comparison_columns
        ]
        .sort_values(sort_columns)
        .reset_index(drop=True)
    )

    try:
        pd.testing.assert_frame_equal(
            features_from_default_path,
            features_from_persistable_input,
            check_dtype=True,
            check_exact=True,
        )
    
        pd.testing.assert_frame_equal(
            lineage_from_default_path,
            lineage_from_persistable_input,
            check_dtype=True,
            check_exact=True,
        )
    except AssertionError as exc:
        raise AssertionError(
            "Feature generation differs between "
            "the default source-loading path and "
            "the persisted canonical-input path"
        ) from exc

    print(
        "\n[canonical_source_metrics] "
        "feature parity rows:",
        len(left),
    )

    print(
        "[canonical_source_metrics] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
