from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# scripts/smoke_tests/40_49/46_labor_demand_comparison.py

import numpy as np
import pandas as pd

from regime.artifacts import RegimeArtifactStore

from regime.experiments.labor_demand_comparison import (
    BASELINE_RUN_ID,
    FEATURE_KEY_MAP,
    FOCUS_GEOS,
    LABOR_METRICS,
    LABOR_POLICIES,
    TARGET_FEATURE_KEYS,
    build_labor_demand_comparison,
    build_labor_policy_features,
)


def _safe_ratio_minus_one(values: pd.Series, periods: int) -> pd.Series:
    numeric = pd.to_numeric(values, errors="coerce")
    output = numeric / numeric.shift(periods) - 1.0
    return output.replace([np.inf, -np.inf], np.nan)


def _assert_feature_values_close(
    actual: pd.DataFrame,
    expected: pd.DataFrame,
    *,
    label: str,
) -> None:
    merged = actual.merge(
        expected,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        how="inner",
        suffixes=("_actual", "_expected"),
    )

    if merged.empty:
        raise AssertionError(f"{label} has no common valid coverage")

    if not np.allclose(
        merged["raw_feature_value_actual"],
        merged["raw_feature_value_expected"],
        rtol=0.0,
        atol=1e-12,
        equal_nan=False,
    ):
        bad = merged[
            ~np.isclose(
                merged["raw_feature_value_actual"],
                merged["raw_feature_value_expected"],
                rtol=0.0,
                atol=1e-12,
            )
        ]
        raise AssertionError(
            f"{label} feature math mismatch:\n"
            + bad.head(20).to_string(index=False)
        )


def _build_frozen_incumbent_expectation(
    source_metrics: pd.DataFrame,
) -> pd.DataFrame:
    labor = source_metrics[
        source_metrics["canonical_metric_key"].isin(LABOR_METRICS)
        & source_metrics["geo_id"].isin(FOCUS_GEOS)
    ][
        ["geo_id", "date", "canonical_metric_key", "value"]
    ].copy()

    labor["date"] = pd.to_datetime(labor["date"])
    labor["value"] = pd.to_numeric(labor["value"], errors="coerce")
    labor = labor.sort_values(["geo_id", "canonical_metric_key", "date"])

    pieces = []
    grouped = labor.groupby(["geo_id", "canonical_metric_key"], sort=False)
    for component, periods in (("short", 1), ("long", 12)):
        frame = labor[["geo_id", "date", "canonical_metric_key"]].copy()
        frame["feature_key"] = [
            FEATURE_KEY_MAP[metric][component]
            for metric in frame["canonical_metric_key"]
        ]
        frame["raw_feature_value"] = grouped["value"].transform(
            lambda values, lag=periods: _safe_ratio_minus_one(values, lag)
        )
        pieces.append(frame)

    expected = pd.concat(pieces, ignore_index=True).dropna(
        subset=["raw_feature_value"]
    )
    return expected.reset_index(drop=True)


def _assert_labor_feature_contracts(result: dict[str, pd.DataFrame]) -> None:
    store = RegimeArtifactStore()
    source_metrics = store.read_dataframe(BASELINE_RUN_ID, "source_metrics")
    baseline_features = store.read_dataframe(BASELINE_RUN_ID, "features")

    incumbent_actual = baseline_features[
        baseline_features["geo_id"].isin(FOCUS_GEOS)
        & baseline_features["canonical_metric_key"].isin(LABOR_METRICS)
        & baseline_features["feature_key"].isin(TARGET_FEATURE_KEYS)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
        ]
    ].copy()
    incumbent_actual["date"] = pd.to_datetime(incumbent_actual["date"])

    incumbent_expected = _build_frozen_incumbent_expectation(source_metrics)
    _assert_feature_values_close(
        incumbent_actual[
            incumbent_actual["feature_key"].str.endswith(("_short", "_long"))
        ],
        incumbent_expected,
        label="frozen incumbent raw lag1/lag12",
    )

    ma6_expected = build_labor_policy_features(
        source_metrics,
        policy_id="labor_ma6_momentum_lag3",
        window=6,
    )
    ma6_expected = ma6_expected[
        ma6_expected["geo_id"].isin(FOCUS_GEOS)
        & ma6_expected["feature_key"].isin(TARGET_FEATURE_KEYS)
    ]
    ma6_actual = result["labor_ma6_momentum_lag3__features"]
    ma6_actual = ma6_actual[
        ma6_actual["geo_id"].isin(FOCUS_GEOS)
        & ma6_actual["canonical_metric_key"].isin(LABOR_METRICS)
        & ma6_actual["feature_key"].isin(TARGET_FEATURE_KEYS)
    ][
        [
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
            "raw_feature_value",
        ]
    ].copy()
    ma6_actual["date"] = pd.to_datetime(ma6_actual["date"])
    _assert_feature_values_close(
        ma6_actual,
        ma6_expected[
            [
                "geo_id",
                "date",
                "canonical_metric_key",
                "feature_key",
                "raw_feature_value",
            ]
        ],
        label="MA6 lag3/lag12 challenger",
    )

    common = incumbent_actual.merge(
        ma6_actual,
        on=[
            "geo_id",
            "date",
            "canonical_metric_key",
            "feature_key",
        ],
        how="inner",
        suffixes=("_incumbent", "_ma6"),
    )
    common = common[common["feature_key"].str.endswith(("_short", "_long"))]
    if common.empty:
        raise AssertionError("Incumbent and MA6 have no common labor coverage")
    if np.allclose(
        common["raw_feature_value_incumbent"],
        common["raw_feature_value_ma6"],
        rtol=0.0,
        atol=1e-12,
    ):
        raise AssertionError("Frozen incumbent and MA6 challenger are identical")



def main() -> int:
    print(
        "[labor_demand_comparison] "
        "building comparison..."
    )

    result = build_labor_demand_comparison()

    _assert_labor_feature_contracts(result)

    isolation = result[
        "isolation_audit"
    ]

    failed = isolation[
        ~isolation[
            "exact_match"
        ]
    ]

    if not failed.empty:
        raise AssertionError(
            "Labor challenger isolation failed:\n"
            + failed[
                [
                    "artifact_name",
                    "scope",
                    "baseline_rows",
                    "challenger_rows",
                    "error_message",
                ]
            ].to_string(index=False)
        )

    feature_summary = result[
        "feature_stability_summary"
    ]

    metric_summary = result[
        "metric_stability_summary"
    ]

    dimension_summary = result[
        "dimension_stability_summary"
    ]

    axis_summary = result[
        "axis_stability_summary"
    ]

    cancellation_summary = result[
        "cancellation_summary"
    ]

    expected_roles = {
        "baseline",
        *LABOR_POLICIES.keys(),
    }

    for frame_name, frame in (
        ("feature summary", feature_summary),
        ("metric summary", metric_summary),
        ("dimension summary", dimension_summary),
        ("axis summary", axis_summary),
        ("cancellation summary", cancellation_summary),
    ):
        if frame.empty:
            raise AssertionError(
                f"{frame_name} is empty"
            )

        if set(
            frame[
                "run_role"
            ].unique()
        ) != expected_roles:
            raise AssertionError(
                f"{frame_name} has unexpected roles"
            )

    if set(
        metric_summary[
            "canonical_metric_key"
        ].unique()
    ) != set(LABOR_METRICS):
        raise AssertionError(
            "Metric summary does not contain "
            "all three labor metrics"
        )

    numeric_checks = [
        (
            feature_summary,
            "mean_absolute_change_1m",
        ),
        (
            metric_summary,
            "mean_absolute_change_1m",
        ),
        (
            dimension_summary,
            "mean_absolute_change_1m",
        ),
        (
            axis_summary,
            "mean_absolute_change_1m",
        ),
        (
            cancellation_summary,
            "mean_cancellation_rate",
        ),
    ]

    for frame, column in numeric_checks:
        values = frame[
            column
        ].dropna()

        if not np.isfinite(
            values
        ).all():
            raise AssertionError(
                f"{column} contains non-finite values"
            )

    rates = cancellation_summary[
        "mean_cancellation_rate"
    ]

    if (
        rates.lt(0).any()
        or rates.gt(1).any()
    ):
        raise AssertionError(
            "Cancellation rate fell outside [0, 1]"
        )

    print(
        "\n[labor_demand_comparison] "
        "isolation audit:"
    )

    print(
        isolation.to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "short-feature stability:"
    )

    print(
        feature_summary[
            feature_summary[
                "feature_component"
            ].eq("short")
        ]
        .sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "run_role",
            ]
        )
        .to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "metric stability:"
    )

    print(
        metric_summary.sort_values(
            [
                "geo_id",
                "canonical_metric_key",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "core Demand dimension:"
    )

    print(
        dimension_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "Demand axis:"
    )

    print(
        axis_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] "
        "core Demand cancellation:"
    )

    print(
        cancellation_summary.sort_values(
            [
                "geo_id",
                "run_role",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[labor_demand_comparison] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
