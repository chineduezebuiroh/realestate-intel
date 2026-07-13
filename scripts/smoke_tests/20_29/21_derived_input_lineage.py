from __future__ import annotations
# scripts/smoke_tests/20_29/21_derived_input_lineage.py

import pandas as pd

from regime._00_config_loader import (
    load_regime_config,
)
from regime._01_feature_engine import (
    load_raw_metric_series,
)
from regime.canonical_metrics import (
    resolve_canonical_metrics,
)
from regime.derived_metrics import (
    DERIVED_METRIC_COMPONENTS,
    build_derived_metrics,
    build_derived_metrics_with_lineage,
)


CHECK_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

DERIVED_METRICS = [
    "permit_intensity",
    "price_to_income",
    "payment_burden",
]


def main() -> int:
    config = load_regime_config(validate=True)

    raw_source = load_raw_metric_series(config)

    canonical = resolve_canonical_metrics(
        raw_source,
        config,
    )

    legacy_output = build_derived_metrics(
        canonical
    )

    derived, lineage = (
        build_derived_metrics_with_lineage(
            canonical
        )
    )

    pd.testing.assert_frame_equal(
        legacy_output.reset_index(drop=True),
        derived.reset_index(drop=True),
        check_dtype=True,
    )

    expected_components = {
        (
            derived_metric,
            component_metric,
        )
        for derived_metric, components
        in DERIVED_METRIC_COMPONENTS.items()
        for component_metric in components
    }

    actual_components = set(
        lineage[
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        ]
        .drop_duplicates()
        .itertuples(
            index=False,
            name=None,
        )
    )

    if expected_components != actual_components:
        raise AssertionError(
            "Derived lineage component mismatch.\n"
            f"Expected: {sorted(expected_components)}\n"
            f"Actual: {sorted(actual_components)}"
        )

    if lineage["component_source_date"].isna().any():
        raise AssertionError(
            "Lineage contains missing component source dates"
        )

    if lineage["component_source_geo_id"].isna().any():
        raise AssertionError(
            "Lineage contains missing component source geographies"
        )

    if (lineage["component_age_days"] < 0).any():
        raise AssertionError(
            "Lineage contains negative component ages"
        )

    print(
        "[derived_input_lineage] derived rows:",
        len(derived),
    )
    print(
        "[derived_input_lineage] lineage rows:",
        len(lineage),
    )

    print(
        "\n[derived_input_lineage] component summary:"
    )
    print(
        lineage.groupby(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .agg(
            rows=("date", "size"),
            first_derived_date=("date", "min"),
            latest_derived_date=("date", "max"),
            average_age_days=(
                "component_age_days",
                "mean",
            ),
            p90_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.90),
            ),
            maximum_age_days=(
                "component_age_days",
                "max",
            ),
            carried_forward_share=(
                "was_carried_forward",
                "mean",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .to_string(index=False)
    )

    latest = (
        lineage[
            lineage["geo_id"].isin(CHECK_GEOS)
            & lineage[
                "derived_metric_key"
            ].isin(DERIVED_METRICS)
        ]
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "date",
                "component_metric_key",
            ]
        )
        .groupby(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ],
            as_index=False,
        )
        .tail(1)
    )

    print(
        "\n[derived_input_lineage] latest component "
        "lineage for DC and Alameda:"
    )
    print(
        latest[
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "component_metric_key",
                "component_source_geo_id",
                "component_source_date",
                "component_age_days",
                "component_age_months",
                "was_carried_forward",
            ]
        ]
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .to_string(index=False)
    )

    print("\n[derived_input_lineage] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
