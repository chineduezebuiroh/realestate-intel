from __future__ import annotations
# scripts/smoke_tests/40_49/40_linked_price_family_features.py

import math

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.experiments.linked_price_family_features import (
    PRICE_FAMILY_METRICS,
    build_linked_price_family_features,
)


def _assert_close(
    actual: float,
    expected: float,
    *,
    label: str,
) -> None:
    if not math.isclose(
        float(actual),
        float(expected),
        rel_tol=0.0,
        abs_tol=1e-12,
    ):
        raise AssertionError(
            f"{label}: expected {expected}, "
            f"found {actual}"
        )


def _build_fixture() -> pd.DataFrame:
    dates = pd.date_range(
        "2020-01-31",
        periods=30,
        freq=MONTH_END,
    )

    rows: list[
        dict[str, object]
    ] = []

    for geo_index, geo_id in enumerate(
        (
            "geo_alpha",
            "geo_beta",
        )
    ):
        for index, date in enumerate(
            dates
        ):
            price = (
                300_000.0
                + geo_index
                * 100_000.0
                + index
                * 5_000.0
            )

            ppsf = (
                250.0
                + geo_index
                * 50.0
                + index
                * 2.0
            )

            for metric_key, value in (
                (
                    "median_sale_price",
                    price,
                ),
                (
                    "median_ppsf",
                    ppsf,
                ),
            ):
                rows.append(
                    {
                        "geo_id": geo_id,
                        "date": date,
                        "canonical_metric_key": (
                            metric_key
                        ),
                        "value": value,
                        "metric_origin": "local",
                    }
                )

        for annual_date, income in (
            (
                pd.Timestamp(
                    "2020-01-31"
                ),
                100_000.0
                + geo_index
                * 20_000.0,
            ),
            (
                pd.Timestamp(
                    "2021-01-31"
                ),
                105_000.0
                + geo_index
                * 20_000.0,
            ),
            (
                pd.Timestamp(
                    "2022-01-31"
                ),
                110_000.0
                + geo_index
                * 20_000.0,
            ),
        ):
            rows.append(
                {
                    "geo_id": geo_id,
                    "date": annual_date,
                    "canonical_metric_key": (
                        "median_household_income"
                    ),
                    "value": income,
                    "metric_origin": "local",
                }
            )

    for index, date in enumerate(
        dates
    ):
        rows.append(
            {
                "geo_id": "national",
                "date": date,
                "canonical_metric_key": (
                    "mortgage_30y"
                ),
                "value": 3.0 + index * 0.05,
                "metric_origin": "national",
            }
        )

    return pd.DataFrame(rows)


def main() -> int:
    source = _build_fixture()

    result = (
        build_linked_price_family_features(
            source
        )
    )

    features = result.feature_history
    levels = result.level_history

    print(
        "[linked_price_features] source rows:",
        len(source),
    )

    print(
        "[linked_price_features] substituted rows:",
        len(
            result.substituted_sources
        ),
    )

    print(
        "[linked_price_features] level rows:",
        len(levels),
    )

    print(
        "[linked_price_features] feature rows:",
        len(features),
    )

    actual_metrics = set(
        features[
            "canonical_metric_key"
        ].unique()
    )

    expected_metrics = set(
        PRICE_FAMILY_METRICS
    )

    if actual_metrics != expected_metrics:
        raise AssertionError(
            "Unexpected linked price-family metrics"
        )

    expected_components = {
        "level",
        "short",
        "long",
    }

    if set(
        features[
            "feature_component"
        ].unique()
    ) != expected_components:
        raise AssertionError(
            "Unexpected feature components"
        )

    if not features[
        "price_family_experiment_id"
    ].eq(
        "price_family_ma12_momentum_lag3"
    ).all():
        raise AssertionError(
            "Experiment ID was not preserved"
        )

    source_origins = set(
        features[
            features[
                "canonical_metric_key"
            ].isin(
                {
                    "median_sale_price",
                    "median_ppsf",
                }
            )
        ][
            "feature_origin"
        ].unique()
    )

    if source_origins != {
        "smoothed_source"
    }:
        raise AssertionError(
            "Source feature origin mismatch"
        )

    derived_origins = set(
        features[
            features[
                "canonical_metric_key"
            ].isin(
                {
                    "price_to_income",
                    "payment_burden",
                }
            )
        ][
            "feature_origin"
        ].unique()
    )

    if derived_origins != {
        "recomputed_derived"
    }:
        raise AssertionError(
            "Derived feature origin mismatch"
        )

    test_geo = "geo_alpha"

    price_source = source[
        source[
            "geo_id"
        ].eq(test_geo)
        & source[
            "canonical_metric_key"
        ].eq(
            "median_sale_price"
        )
    ].sort_values(
        "date"
    ).reset_index(
        drop=True
    )

    ppsf_source = source[
        source[
            "geo_id"
        ].eq(test_geo)
        & source[
            "canonical_metric_key"
        ].eq(
            "median_ppsf"
        )
    ].sort_values(
        "date"
    ).reset_index(
        drop=True
    )

    first_level_date = pd.Timestamp(
        "2020-12-31"
    )

    first_short_date = pd.Timestamp(
        "2021-03-31"
    )

    first_long_date = pd.Timestamp(
        "2021-12-31"
    )

    expected_price_level = (
        price_source.iloc[
            0:12
        ]["value"].mean()
    )

    actual_price_level = float(
        features[
            features[
                "geo_id"
            ].eq(test_geo)
            & features[
                "date"
            ].eq(
                first_level_date
            )
            & features[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
            & features[
                "feature_component"
            ].eq("level")
        ][
            "raw_feature_value"
        ].iloc[0]
    )

    _assert_close(
        actual_price_level,
        expected_price_level,
        label="First price level",
    )

    expected_ppsf_level = (
        ppsf_source.iloc[
            0:12
        ]["value"].mean()
    )

    actual_ppsf_level = float(
        features[
            features[
                "geo_id"
            ].eq(test_geo)
            & features[
                "date"
            ].eq(
                first_level_date
            )
            & features[
                "canonical_metric_key"
            ].eq(
                "median_ppsf"
            )
            & features[
                "feature_component"
            ].eq("level")
        ][
            "raw_feature_value"
        ].iloc[0]
    )

    _assert_close(
        actual_ppsf_level,
        expected_ppsf_level,
        label="First PPSF level",
    )

    current_price_level = (
        price_source.iloc[
            3:15
        ]["value"].mean()
    )

    lagged_price_level = (
        price_source.iloc[
            0:12
        ]["value"].mean()
    )

    expected_price_short = (
        current_price_level
        / lagged_price_level
        - 1.0
    )

    actual_price_short = float(
        features[
            features[
                "geo_id"
            ].eq(test_geo)
            & features[
                "date"
            ].eq(
                first_short_date
            )
            & features[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
            & features[
                "feature_component"
            ].eq("short")
        ][
            "raw_feature_value"
        ].iloc[0]
    )

    _assert_close(
        actual_price_short,
        expected_price_short,
        label="First price short",
    )

    current_long_level = (
        price_source.iloc[
            12:24
        ]["value"].mean()
    )

    lagged_long_level = (
        price_source.iloc[
            0:12
        ]["value"].mean()
    )

    expected_price_long = (
        current_long_level
        / lagged_long_level
        - 1.0
    )

    actual_price_long = float(
        features[
            features[
                "geo_id"
            ].eq(test_geo)
            & features[
                "date"
            ].eq(
                first_long_date
            )
            & features[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
            & features[
                "feature_component"
            ].eq("long")
        ][
            "raw_feature_value"
        ].iloc[0]
    )

    _assert_close(
        actual_price_long,
        expected_price_long,
        label="First price long",
    )

    pti_level = features[
        features[
            "geo_id"
        ].eq(test_geo)
        & features[
            "date"
        ].eq(
            first_level_date
        )
        & features[
            "canonical_metric_key"
        ].eq(
            "price_to_income"
        )
        & features[
            "feature_component"
        ].eq("level")
    ]

    if len(pti_level) != 1:
        raise AssertionError(
            "Missing first recomputed PTI level"
        )

    expected_pti_level = (
        expected_price_level
        / 100_000.0
    )

    _assert_close(
        float(
            pti_level[
                "raw_feature_value"
            ].iloc[0]
        ),
        expected_pti_level,
        label="First recomputed PTI level",
    )

    burden_level = features[
        features[
            "geo_id"
        ].eq(test_geo)
        & features[
            "date"
        ].eq(
            first_level_date
        )
        & features[
            "canonical_metric_key"
        ].eq(
            "payment_burden"
        )
        & features[
            "feature_component"
        ].eq("level")
    ]

    if len(burden_level) != 1:
        raise AssertionError(
            "Missing first recomputed burden level"
        )

    substitution = (
        result.source_substitution_lineage
    )

    if int(
        substitution[
            "row_action"
        ].eq("null").sum()
    ) != 22:
        raise AssertionError(
            "Unexpected price warm-up null count"
        )

    if int(
        substitution[
            "row_action"
        ].eq("replace").sum()
    ) != 38:
        raise AssertionError(
            "Unexpected price replacement count"
        )

    unrelated_before = source[
        ~source[
            "canonical_metric_key"
        ].eq(
            "median_sale_price"
        )
    ].sort_values(
        [
            "geo_id",
            "canonical_metric_key",
            "date",
        ]
    ).reset_index(
        drop=True
    )

    unrelated_after = (
        result.substituted_sources[
            ~result.substituted_sources[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(
            drop=True
        )
    )

    pd.testing.assert_frame_equal(
        unrelated_before,
        unrelated_after,
        check_exact=True,
        check_dtype=True,
    )

    if not np.isfinite(
        result.derived_metrics[
            "value"
        ]
    ).all():
        raise AssertionError(
            "Derived metrics contain infinity"
        )

    valid_features = features[
        "raw_feature_value"
    ].dropna()

    if not np.isfinite(
        valid_features
    ).all():
        raise AssertionError(
            "Feature output contains infinity"
        )

    print(
        "\n[linked_price_features] "
        "feature coverage:"
    )

    print(
        features.groupby(
            [
                "canonical_metric_key",
                "feature_component",
                "feature_origin",
            ],
            as_index=False,
        )
        .agg(
            rows=(
                "raw_feature_value",
                "size",
            ),
            valid_rows=(
                "raw_feature_value",
                "count",
            ),
            first_valid_date=(
                "date",
                lambda values: values[
                    features.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].min(),
            ),
            last_valid_date=(
                "date",
                lambda values: values[
                    features.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].max(),
            ),
        )
        .sort_values(
            [
                "canonical_metric_key",
                "feature_component",
            ]
        )
        .to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_features] "
        "substitution summary:"
    )

    print(
        substitution.groupby(
            "row_action",
            as_index=False,
        )
        .size()
        .to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_features] "
        "latest levels:"
    )

    print(
        levels.sort_values(
            [
                "canonical_metric_key",
                "geo_id",
                "date",
            ]
        )
        .groupby(
            [
                "canonical_metric_key",
                "geo_id",
            ],
            as_index=False,
        )
        .tail(3)
        .to_string(
            index=False
        )
    )

    print(
        "\n[linked_price_features] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
