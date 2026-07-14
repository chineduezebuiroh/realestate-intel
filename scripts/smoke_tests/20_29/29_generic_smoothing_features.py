from __future__ import annotations
# scripts/smoke_tests/20_29/29_generic_smoothing_features.py

import numpy as np
import pandas as pd

from regime.experiments.smoothing_features import (
    build_smoothed_metric_features,
    build_smoothed_metric_features_wide,
)
from regime.experiments.smoothing_policy import (
    load_smoothing_experiments,
)


def _build_test_observations() -> pd.DataFrame:
    """
    Create two deterministic geography series.

    geo_steady:
        Smooth monotonic growth.

    geo_seasonal:
        Seasonal/lumpy monthly observations with an upward trend.
    """
    dates = pd.date_range(
        "2020-01-31",
        periods=30,
        freq="M",
    )

    steady_values = [
        100.0 + 5.0 * index
        for index in range(
            len(dates)
        )
    ]

    seasonal_pattern = [
        0.80,
        0.85,
        0.95,
        1.10,
        1.25,
        1.35,
        1.40,
        1.30,
        1.15,
        1.00,
        0.90,
        0.82,
    ]

    seasonal_values = [
        (
            200.0
            + 3.0 * index
        )
        * seasonal_pattern[
            index % 12
        ]
        for index in range(
            len(dates)
        )
    ]

    frames = []

    for geo_id, values in [
        (
            "geo_steady",
            steady_values,
        ),
        (
            "geo_seasonal",
            seasonal_values,
        ),
    ]:
        frames.append(
            pd.DataFrame(
                {
                    "geo_id": geo_id,
                    "date": dates,
                    "canonical_metric_key": (
                        "active_inventory"
                    ),
                    "raw_value": values,
                    "source_observation_date": (
                        dates
                    ),
                }
            )
        )

    return pd.concat(
        frames,
        ignore_index=True,
    )


def _assert_close(
    actual: float,
    expected: float,
    *,
    label: str,
    tolerance: float = 1e-12,
) -> None:
    if not np.isclose(
        actual,
        expected,
        rtol=0.0,
        atol=tolerance,
        equal_nan=True,
    ):
        raise AssertionError(
            f"{label}: expected {expected}, "
            f"found {actual}"
        )


def main() -> int:
    experiments = (
        load_smoothing_experiments(
            validate=True
        )
    )

    momentum_policy = experiments[
        "inventory_ma3_momentum"
    ].policy_for(
        "active_inventory"
    )

    deviation_policy = experiments[
        "inventory_ma3_deviation"
    ].policy_for(
        "active_inventory"
    )

    ma6_structural_policy = experiments[
        "inventory_ma6_structural"
    ].policy_for(
        "active_inventory"
    )
    
    ma12_structural_policy = experiments[
        "inventory_ma12_structural"
    ].policy_for(
        "active_inventory"
    )

    if momentum_policy is None:
        raise AssertionError(
            "Momentum policy not found"
        )

    if deviation_policy is None:
        raise AssertionError(
            "Deviation policy not found"
        )
    
    if ma6_structural_policy is None:
        raise AssertionError(
            "MA6 structural policy not found"
        )
    
    if ma12_structural_policy is None:
        raise AssertionError(
            "MA12 structural policy not found"
        )

    observations = (
        _build_test_observations()
    )

    momentum_wide = (
        build_smoothed_metric_features_wide(
            observations,
            policy=momentum_policy,
        )
    )

    deviation_wide = (
        build_smoothed_metric_features_wide(
            observations,
            policy=deviation_policy,
        )
    )

    ma6_structural_wide = (
        build_smoothed_metric_features_wide(
            observations,
            policy=ma6_structural_policy,
        )
    )
    
    ma12_structural_wide = (
        build_smoothed_metric_features_wide(
            observations,
            policy=ma12_structural_policy,
        )
    )

    momentum_long = (
        build_smoothed_metric_features(
            observations,
            policy=momentum_policy,
            preserve_columns=[
                "source_observation_date",
            ],
        )
    )

    deviation_long = (
        build_smoothed_metric_features(
            observations,
            policy=deviation_policy,
            preserve_columns=[
                "source_observation_date",
            ],
        )
    )

    ma6_structural_long = (
        build_smoothed_metric_features(
            observations,
            policy=ma6_structural_policy,
            preserve_columns=[
                "source_observation_date",
            ],
        )
    )
    
    ma12_structural_long = (
        build_smoothed_metric_features(
            observations,
            policy=ma12_structural_policy,
            preserve_columns=[
                "source_observation_date",
            ],
        )
    )

    print(
        "[generic_smoothing] observations:",
        len(observations),
    )

    print(
        "[generic_smoothing] geographies:",
        observations["geo_id"].nunique(),
    )

    print(
        "[generic_smoothing] momentum rows:",
        len(momentum_long),
    )

    print(
        "[generic_smoothing] deviation rows:",
        len(deviation_long),
    )

    print(
        "[generic_smoothing] MA6 structural rows:",
        len(ma6_structural_long),
    )
    
    print(
        "[generic_smoothing] MA12 structural rows:",
        len(ma12_structural_long),
    )

    print(
        "\n[generic_smoothing] momentum coverage:"
    )

    print(
        momentum_long.groupby(
            [
                "geo_id",
                "feature_component",
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
                    momentum_long.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].min(),
            ),
            last_valid_date=(
                "date",
                lambda values: values[
                    momentum_long.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].max(),
            ),
        )
        .to_string(index=False)
    )

    print(
        "\n[generic_smoothing] deviation coverage:"
    )

    print(
        deviation_long.groupby(
            [
                "geo_id",
                "feature_component",
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
                    deviation_long.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].min(),
            ),
            last_valid_date=(
                "date",
                lambda values: values[
                    deviation_long.loc[
                        values.index,
                        "raw_feature_value",
                    ].notna()
                ].max(),
            ),
        )
        .to_string(index=False)
    )

    for (
        label,
        structural_long,
    ) in (
        (
            "MA6 structural",
            ma6_structural_long,
        ),
        (
            "MA12 structural",
            ma12_structural_long,
        ),
    ):
        print(
            f"\n[generic_smoothing] "
            f"{label} coverage:"
        )
    
        print(
            structural_long.groupby(
                [
                    "geo_id",
                    "feature_component",
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
                        structural_long.loc[
                            values.index,
                            "raw_feature_value",
                        ].notna()
                    ].min(),
                ),
                last_valid_date=(
                    "date",
                    lambda values: values[
                        structural_long.loc[
                            values.index,
                            "raw_feature_value",
                        ].notna()
                    ].max(),
                ),
            )
            .to_string(index=False)
        )


    steady = observations[
        observations["geo_id"].eq(
            "geo_steady"
        )
    ].sort_values("date")

    steady_values = steady[
        "raw_value"
    ].reset_index(drop=True)

    expected_level = (
        steady_values.iloc[
            0:3
        ].mean()
    )

    momentum_steady = (
        momentum_wide[
            momentum_wide["geo_id"].eq(
                "geo_steady"
            )
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )

    deviation_steady = (
        deviation_wide[
            deviation_wide["geo_id"].eq(
                "geo_steady"
            )
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )

    ma6_structural_steady = (
        ma6_structural_wide[
            ma6_structural_wide[
                "geo_id"
            ].eq(
                "geo_steady"
            )
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )
    
    ma12_structural_steady = (
        ma12_structural_wide[
            ma12_structural_wide[
                "geo_id"
            ].eq(
                "geo_steady"
            )
        ]
        .sort_values("date")
        .reset_index(drop=True)
    )

    _assert_close(
        momentum_steady.loc[
            2,
            "smoothed_level_value",
        ],
        expected_level,
        label="First momentum level",
    )

    _assert_close(
        deviation_steady.loc[
            2,
            "smoothed_level_value",
        ],
        expected_level,
        label="First deviation level",
    )

    expected_deviation_short = (
        steady_values.iloc[2]
        / steady_values.iloc[
            0:3
        ].mean()
        - 1.0
    )

    _assert_close(
        deviation_steady.loc[
            2,
            "smoothed_short_value",
        ],
        expected_deviation_short,
        label=(
            "First deviation short feature"
        ),
    )

    if not pd.isna(
        momentum_steady.loc[
            4,
            "smoothed_short_value",
        ]
    ):
        raise AssertionError(
            "Momentum short feature appeared "
            "before full MA and lag history"
        )

    current_short_ma = (
        steady_values.iloc[
            3:6
        ].mean()
    )

    lagged_short_ma = (
        steady_values.iloc[
            0:3
        ].mean()
    )

    expected_momentum_short = (
        current_short_ma
        / lagged_short_ma
        - 1.0
    )

    _assert_close(
        momentum_steady.loc[
            5,
            "smoothed_short_value",
        ],
        expected_momentum_short,
        label=(
            "First momentum short feature"
        ),
    )

    if not pd.isna(
        momentum_steady.loc[
            13,
            "smoothed_long_value",
        ]
    ):
        raise AssertionError(
            "Long feature appeared before "
            "full MA and lag history"
        )

    current_long_ma = (
        steady_values.iloc[
            12:15
        ].mean()
    )

    lagged_long_ma = (
        steady_values.iloc[
            0:3
        ].mean()
    )

    expected_long = (
        current_long_ma
        / lagged_long_ma
        - 1.0
    )

    _assert_close(
        momentum_steady.loc[
            14,
            "smoothed_long_value",
        ],
        expected_long,
        label="First momentum long feature",
    )

    _assert_close(
        deviation_steady.loc[
            14,
            "smoothed_long_value",
        ],
        expected_long,
        label="First deviation long feature",
    )

    expected_features = {
        "active_inventory_level",
        "active_inventory_short",
        "active_inventory_long",
    }

    if set(
        momentum_long["feature_key"]
    ) != expected_features:
        raise AssertionError(
            "Momentum feature-key contract mismatch"
        )

    if set(
        deviation_long["feature_key"]
    ) != expected_features:
        raise AssertionError(
            "Deviation feature-key contract mismatch"
        )

    for (
        label,
        structural_long,
    ) in (
        (
            "MA6 structural",
            ma6_structural_long,
        ),
        (
            "MA12 structural",
            ma12_structural_long,
        ),
    ):
        if set(
            structural_long[
                "feature_key"
            ]
        ) != expected_features:
            raise AssertionError(
                f"{label} feature-key "
                "contract mismatch"
            )

    expected_components = {
        "level",
        "short",
        "long",
    }

    if set(
        momentum_long[
            "feature_component"
        ]
    ) != expected_components:
        raise AssertionError(
            "Momentum feature components mismatch"
        )

    if set(
        deviation_long[
            "feature_component"
        ]
    ) != expected_components:
        raise AssertionError(
            "Deviation feature components mismatch"
        )

    for (
        label,
        structural_long,
    ) in (
        (
            "MA6 structural",
            ma6_structural_long,
        ),
        (
            "MA12 structural",
            ma12_structural_long,
        ),
    ):
        if set(
            structural_long[
                "feature_component"
            ]
        ) != expected_components:
            raise AssertionError(
                f"{label} feature "
                "components mismatch"
            )

    for (
        label,
        feature_frame,
    ) in (
        (
            "Momentum",
            momentum_long,
        ),
        (
            "Deviation",
            deviation_long,
        ),
        (
            "MA6 structural",
            ma6_structural_long,
        ),
        (
            "MA12 structural",
            ma12_structural_long,
        ),
    ):
        finite_count = (
            feature_frame[
                "raw_feature_value"
            ]
            .replace(
                [
                    np.inf,
                    -np.inf,
                ],
                np.nan,
            )
            .notna()
            .sum()
        )

        nonmissing_count = feature_frame[
            "raw_feature_value"
        ].notna().sum()

        if finite_count != nonmissing_count:
            raise AssertionError(
                f"{label} output contains infinity"
            )

    if not pd.isna(
        ma6_structural_steady.loc[
            16,
            "smoothed_long_value",
        ]
    ):
        raise AssertionError(
            "MA6 structural long feature "
            "appeared before full MA6 and "
            "12-month lag history"
        )
    
    expected_ma6_current_long = (
        steady_values.iloc[
            12:18
        ].mean()
    )
    
    expected_ma6_lagged_long = (
        steady_values.iloc[
            0:6
        ].mean()
    )
    
    expected_ma6_long = (
        expected_ma6_current_long
        / expected_ma6_lagged_long
        - 1.0
    )
    
    _assert_close(
        ma6_structural_steady.loc[
            17,
            "smoothed_long_value",
        ],
        expected_ma6_long,
        label="First MA6 structural long",
    )
    
    if not pd.isna(
        ma12_structural_steady.loc[
            22,
            "smoothed_long_value",
        ]
    ):
        raise AssertionError(
            "MA12 structural long feature "
            "appeared before full MA12 and "
            "12-month lag history"
        )
    
    expected_ma12_current_long = (
        steady_values.iloc[
            12:24
        ].mean()
    )
    
    expected_ma12_lagged_long = (
        steady_values.iloc[
            0:12
        ].mean()
    )
    
    expected_ma12_long = (
        expected_ma12_current_long
        / expected_ma12_lagged_long
        - 1.0
    )
    
    _assert_close(
        ma12_structural_steady.loc[
            23,
            "smoothed_long_value",
        ],
        expected_ma12_long,
        label="First MA12 structural long",
    )

    expected_long_rows = (
        len(observations) * 3
    )

    for (
        label,
        feature_frame,
    ) in (
        (
            "Momentum",
            momentum_long,
        ),
        (
            "Deviation",
            deviation_long,
        ),
        (
            "MA6 structural",
            ma6_structural_long,
        ),
        (
            "MA12 structural",
            ma12_structural_long,
        ),
    ):
        if (
            len(feature_frame)
            != expected_long_rows
        ):
            raise AssertionError(
                f"{label} long-form "
                "row count mismatch"
            )

        if not feature_frame[
            "source_observation_date"
        ].notna().all():
            raise AssertionError(
                f"{label} lineage column was lost"
            )

    momentum_short = (
        momentum_long[
            momentum_long[
                "feature_component"
            ].eq("short")
        ][
            [
                "geo_id",
                "date",
                "raw_feature_value",
            ]
        ]
        .rename(
            columns={
                "raw_feature_value": (
                    "momentum_short"
                )
            }
        )
    )

    deviation_short = (
        deviation_long[
            deviation_long[
                "feature_component"
            ].eq("short")
        ][
            [
                "geo_id",
                "date",
                "raw_feature_value",
            ]
        ]
        .rename(
            columns={
                "raw_feature_value": (
                    "deviation_short"
                )
            }
        )
    )

    short_comparison = (
        momentum_short.merge(
            deviation_short,
            on=[
                "geo_id",
                "date",
            ],
            how="inner",
            validate="one_to_one",
        )
    )

    comparable = short_comparison.dropna(
        subset=[
            "momentum_short",
            "deviation_short",
        ]
    )

    if comparable.empty:
        raise AssertionError(
            "No comparable short-feature rows "
            "were generated"
        )

    if np.allclose(
        comparable["momentum_short"],
        comparable["deviation_short"],
        rtol=0.0,
        atol=1e-12,
    ):
        raise AssertionError(
            "Momentum and deviation short "
            "features are unexpectedly identical"
        )

    if not pd.isna(
        ma6_structural_steady.loc[
            4,
            "smoothed_level_value",
        ]
    ):
        raise AssertionError(
            "MA6 level appeared before "
            "six observations were available"
        )
    
    if not pd.isna(
        ma6_structural_steady.loc[
            4,
            "smoothed_short_value",
        ]
    ):
        raise AssertionError(
            "MA6 structural short feature "
            "appeared before the MA6 denominator"
        )

    if not pd.isna(
        ma12_structural_steady.loc[
            10,
            "smoothed_level_value",
        ]
    ):
        raise AssertionError(
            "MA12 level appeared before "
            "twelve observations were available"
        )
    
    if not pd.isna(
        ma12_structural_steady.loc[
            10,
            "smoothed_short_value",
        ]
    ):
        raise AssertionError(
            "MA12 structural short feature "
            "appeared before the MA12 denominator"
        )
    
    expected_ma12_level = (
        steady_values.iloc[
            0:12
        ].mean()
    )
    
    expected_ma12_short_numerator = (
        steady_values.iloc[
            9:12
        ].mean()
    )
    
    expected_ma12_short = (
        expected_ma12_short_numerator
        / expected_ma12_level
        - 1.0
    )
    
    _assert_close(
        ma12_structural_steady.loc[
            11,
            "smoothed_level_value",
        ],
        expected_ma12_level,
        label="First MA12 structural level",
    )
    
    _assert_close(
        ma12_structural_steady.loc[
            11,
            "smoothed_short_value",
        ],
        expected_ma12_short,
        label="First MA12 structural short",
    )
    
    _assert_close(
        ma12_structural_steady.loc[
            11,
            "short_ma_value",
        ],
        expected_ma12_short_numerator,
        label="First MA12 structural short MA",
    )
    
    _assert_close(
        ma12_structural_steady.loc[
            11,
            "short_reference_value",
        ],
        expected_ma12_level,
        label=(
            "First MA12 structural "
            "short reference"
        ),
    )
    
    expected_ma6_level = (
        steady_values.iloc[
            0:6
        ].mean()
    )
    
    expected_ma6_short_numerator = (
        steady_values.iloc[
            3:6
        ].mean()
    )
    
    expected_ma6_short = (
        expected_ma6_short_numerator
        / expected_ma6_level
        - 1.0
    )
    
    _assert_close(
        ma6_structural_steady.loc[
            5,
            "smoothed_level_value",
        ],
        expected_ma6_level,
        label="First MA6 structural level",
    )
    
    _assert_close(
        ma6_structural_steady.loc[
            5,
            "smoothed_short_value",
        ],
        expected_ma6_short,
        label="First MA6 structural short",
    )
    
    _assert_close(
        ma6_structural_steady.loc[
            5,
            "short_ma_value",
        ],
        expected_ma6_short_numerator,
        label="First MA6 structural short MA",
    )
    
    _assert_close(
        ma6_structural_steady.loc[
            5,
            "short_reference_value",
        ],
        expected_ma6_level,
        label=(
            "First MA6 structural "
            "short reference"
        ),
    )

    print(
        "\n[generic_smoothing] latest comparison:"
    )

    print(
        short_comparison.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .tail(6)
        .to_string(index=False)
    )

    ma6_short = (
        ma6_structural_long[
            ma6_structural_long[
                "feature_component"
            ].eq("short")
        ][
            [
                "geo_id",
                "date",
                "raw_feature_value",
            ]
        ]
        .rename(
            columns={
                "raw_feature_value": (
                    "ma6_structural_short"
                ),
            }
        )
    )

    ma12_short = (
        ma12_structural_long[
            ma12_structural_long[
                "feature_component"
            ].eq("short")
        ][
            [
                "geo_id",
                "date",
                "raw_feature_value",
            ]
        ]
        .rename(
            columns={
                "raw_feature_value": (
                    "ma12_structural_short"
                ),
            }
        )
    )

    structural_comparison = (
        ma6_short.merge(
            ma12_short,
            on=[
                "geo_id",
                "date",
            ],
            how="inner",
            validate="one_to_one",
        )
    )

    structural_comparable = (
        structural_comparison.dropna(
            subset=[
                "ma6_structural_short",
                "ma12_structural_short",
            ]
        )
    )

    if structural_comparable.empty:
        raise AssertionError(
            "No comparable MA6 and MA12 "
            "structural short-feature rows"
        )

    if np.allclose(
        structural_comparable[
            "ma6_structural_short"
        ],
        structural_comparable[
            "ma12_structural_short"
        ],
        rtol=0.0,
        atol=1e-12,
    ):
        raise AssertionError(
            "MA6 and MA12 structural short "
            "features are unexpectedly identical"
        )

    print(
        "\n[generic_smoothing] "
        "latest structural comparison:"
    )

    print(
        structural_comparison.sort_values(
            [
                "geo_id",
                "date",
            ]
        )
        .groupby(
            "geo_id",
            as_index=False,
        )
        .tail(6)
        .to_string(index=False)
    )

    print(
        "\n[generic_smoothing] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
