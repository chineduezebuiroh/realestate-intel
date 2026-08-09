from __future__ import annotations
# scripts/smoke_tests/30_39/39_linked_price_family_recalculation.py

import math

import numpy as np
import pandas as pd

from regime.pandas_compat import MONTH_END

from regime.derived_metrics import (
    build_derived_metrics_with_lineage,
)
from regime.experiments.source_substitution import (
    apply_metric_source_substitution,
)


SUBSTITUTION_ID = (
    "price_family_ma12_momentum_lag3"
)


def _build_source_fixture() -> pd.DataFrame:
    dates = pd.date_range(
        "2020-01-31",
        periods=18,
        freq=MONTH_END,
    )

    rows: list[
        dict[str, object]
    ] = []

    geographies = (
        "geo_alpha",
        "geo_beta",
    )

    for geo_index, geo_id in enumerate(
        geographies
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

            rows.append(
                {
                    "geo_id": geo_id,
                    "date": date,
                    "canonical_metric_key": (
                        "median_sale_price"
                    ),
                    "value": price,
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


def _build_replacement_fixture(
    source: pd.DataFrame,
) -> pd.DataFrame:
    price = source[
        source[
            "canonical_metric_key"
        ].eq(
            "median_sale_price"
        )
    ][
        [
            "geo_id",
            "date",
            "value",
        ]
    ].copy()

    price = price.sort_values(
        [
            "geo_id",
            "date",
        ]
    )

    price[
        "smoothed_price"
    ] = (
        price.groupby(
            "geo_id"
        )[
            "value"
        ]
        .rolling(
            window=12,
            min_periods=12,
        )
        .mean()
        .reset_index(
            level=0,
            drop=True,
        )
    )

    return price[
        [
            "geo_id",
            "date",
            "smoothed_price",
        ]
    ]


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


def _expected_payment_burden(
    *,
    price: float,
    annual_income: float,
    annual_rate_percent: float,
) -> float:
    annual_rate = (
        annual_rate_percent / 100.0
    )

    monthly_rate = (
        annual_rate / 12.0
    )

    term_months = 360

    principal = price * 0.80

    growth_factor = (
        1.0 + monthly_rate
    ) ** term_months

    payment = principal * (
        monthly_rate
        * growth_factor
    ) / (
        growth_factor
        - 1.0
    )

    monthly_income = (
        annual_income / 12.0
    )

    return (
        payment / monthly_income
    )


def main() -> int:
    source = _build_source_fixture()

    replacement = (
        _build_replacement_fixture(
            source
        )
    )

    result = (
        apply_metric_source_substitution(
            source,
            replacement,
            metric_key=(
                "median_sale_price"
            ),
            substitution_id=(
                SUBSTITUTION_ID
            ),
            replacement_value_column=(
                "smoothed_price"
            ),
            missing_policy="null",
        )
    )

    substituted = result.source_metrics
    substitution_lineage = (
        result.substitution_lineage
    )

    print(
        "[linked_price_family] source rows:",
        len(source),
    )

    print(
        "[linked_price_family] substituted rows:",
        len(substituted),
    )

    print(
        "[linked_price_family] substitution rows:",
        len(substitution_lineage),
    )

    target_lineage = substitution_lineage[
        substitution_lineage[
            "canonical_metric_key"
        ].eq(
            "median_sale_price"
        )
    ]

    expected_replaced = 2 * 7
    expected_null = 2 * 11

    replaced_count = int(
        target_lineage[
            "row_action"
        ].eq("replace").sum()
    )

    null_count = int(
        target_lineage[
            "row_action"
        ].eq("null").sum()
    )

    if replaced_count != expected_replaced:
        raise AssertionError(
            "Unexpected replaced-row count: "
            f"{replaced_count}"
        )

    if null_count != expected_null:
        raise AssertionError(
            "Unexpected null-row count: "
            f"{null_count}"
        )

    unrelated_keys = {
        "median_household_income",
        "mortgage_30y",
    }

    before_unrelated = (
        source[
            source[
                "canonical_metric_key"
            ].isin(
                unrelated_keys
            )
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    after_unrelated = (
        substituted[
            substituted[
                "canonical_metric_key"
            ].isin(
                unrelated_keys
            )
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "date",
            ]
        )
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(
        before_unrelated,
        after_unrelated,
        check_exact=True,
        check_dtype=True,
    )

    (
        derived,
        derived_lineage,
    ) = build_derived_metrics_with_lineage(
        substituted
    )

    expected_derived_keys = {
        "price_to_income",
        "payment_burden",
    }

    actual_derived_keys = set(
        derived[
            "canonical_metric_key"
        ].unique()
    )

    if (
        actual_derived_keys
        != expected_derived_keys
    ):
        raise AssertionError(
            "Unexpected derived metrics: "
            f"{sorted(actual_derived_keys)}"
        )

    expected_dates = set(
        pd.date_range(
            "2020-12-31",
            periods=7,
            freq=MONTH_END,
        )
    )

    for geo_id in (
        "geo_alpha",
        "geo_beta",
    ):
        for metric_key in (
            "price_to_income",
            "payment_burden",
        ):
            dates = set(
                derived[
                    derived[
                        "geo_id"
                    ].eq(geo_id)
                    & derived[
                        "canonical_metric_key"
                    ].eq(metric_key)
                ][
                    "date"
                ]
            )

            if dates != expected_dates:
                raise AssertionError(
                    f"{geo_id}/{metric_key}: "
                    "unexpected derived coverage"
                )

    test_geo = "geo_alpha"
    test_date = pd.Timestamp(
        "2021-06-30"
    )

    smoothed_price = float(
        substituted[
            substituted[
                "geo_id"
            ].eq(test_geo)
            & substituted[
                "date"
            ].eq(test_date)
            & substituted[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
        ][
            "value"
        ].iloc[0]
    )

    expected_smoothed_price = float(
        source[
            source[
                "geo_id"
            ].eq(test_geo)
            & source[
                "canonical_metric_key"
            ].eq(
                "median_sale_price"
            )
        ]
        .sort_values("date")
        .tail(12)[
            "value"
        ]
        .mean()
    )

    _assert_close(
        smoothed_price,
        expected_smoothed_price,
        label="Smoothed price substitution",
    )

    annual_income = 105_000.0
    mortgage_rate = 3.0 + 17 * 0.05

    expected_pti = (
        smoothed_price
        / annual_income
    )

    actual_pti = float(
        derived[
            derived[
                "geo_id"
            ].eq(test_geo)
            & derived[
                "date"
            ].eq(test_date)
            & derived[
                "canonical_metric_key"
            ].eq(
                "price_to_income"
            )
        ][
            "value"
        ].iloc[0]
    )

    _assert_close(
        actual_pti,
        expected_pti,
        label="Recomputed price-to-income",
    )

    expected_burden = (
        _expected_payment_burden(
            price=smoothed_price,
            annual_income=annual_income,
            annual_rate_percent=(
                mortgage_rate
            ),
        )
    )

    actual_burden = float(
        derived[
            derived[
                "geo_id"
            ].eq(test_geo)
            & derived[
                "date"
            ].eq(test_date)
            & derived[
                "canonical_metric_key"
            ].eq(
                "payment_burden"
            )
        ][
            "value"
        ].iloc[0]
    )

    _assert_close(
        actual_burden,
        expected_burden,
        label="Recomputed payment burden",
    )

    income_lineage = derived_lineage[
        derived_lineage[
            "geo_id"
        ].eq(test_geo)
        & derived_lineage[
            "date"
        ].eq(test_date)
        & derived_lineage[
            "derived_metric_key"
        ].eq(
            "price_to_income"
        )
        & derived_lineage[
            "component_metric_key"
        ].eq(
            "median_household_income"
        )
    ].iloc[0]

    if (
        income_lineage[
            "component_source_date"
        ]
        != pd.Timestamp(
            "2021-01-31"
        )
    ):
        raise AssertionError(
            "Income lineage source date changed"
        )

    if not bool(
        income_lineage[
            "was_carried_forward"
        ]
    ):
        raise AssertionError(
            "Expected carried-forward income lineage"
        )

    mortgage_lineage = derived_lineage[
        derived_lineage[
            "geo_id"
        ].eq(test_geo)
        & derived_lineage[
            "date"
        ].eq(test_date)
        & derived_lineage[
            "derived_metric_key"
        ].eq(
            "payment_burden"
        )
        & derived_lineage[
            "component_metric_key"
        ].eq(
            "mortgage_30y"
        )
    ].iloc[0]

    if (
        mortgage_lineage[
            "component_source_geo_id"
        ]
        != "national"
    ):
        raise AssertionError(
            "Mortgage lineage source geography "
            "was not preserved"
        )

    if bool(
        mortgage_lineage[
            "was_carried_forward"
        ]
    ):
        raise AssertionError(
            "Monthly mortgage lineage should not "
            "be carried forward"
        )

    price_lineage = derived_lineage[
        derived_lineage[
            "component_metric_key"
        ].eq(
            "median_sale_price"
        )
    ]

    if not (
        price_lineage[
            "component_source_date"
        ]
        == price_lineage[
            "date"
        ]
    ).all():
        raise AssertionError(
            "Substituted price lineage date mismatch"
        )

    if not np.isfinite(
        derived["value"]
    ).all():
        raise AssertionError(
            "Derived output contains non-finite values"
        )

    print(
        "\n[linked_price_family] "
        "substitution lineage:"
    )

    print(
        substitution_lineage.groupby(
            [
                "canonical_metric_key",
                "row_action",
            ],
            as_index=False,
        )
        .size()
        .to_string(index=False)
    )

    print(
        "\n[linked_price_family] "
        "derived coverage:"
    )

    print(
        derived.groupby(
            [
                "geo_id",
                "canonical_metric_key",
            ],
            as_index=False,
        )
        .agg(
            rows=("value", "size"),
            first_date=("date", "min"),
            last_date=("date", "max"),
        )
        .to_string(index=False)
    )

    print(
        "\n[linked_price_family] "
        "lineage sample:"
    )

    print(
        derived_lineage[
            derived_lineage[
                "geo_id"
            ].eq(test_geo)
            & derived_lineage[
                "date"
            ].eq(test_date)
        ]
        .sort_values(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .to_string(index=False)
    )

    print(
        "\n[linked_price_family] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
