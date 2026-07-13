from __future__ import annotations
# scripts/smoke_tests/30_39/33_metric_normalization_stability.py

import numpy as np

from regime.experiments.metric_normalization_stability import (
    BASELINE_POLICY_ID,
    CHALLENGER_IDS,
    DEFAULT_RUN_ID,
    FOCUS_GEOS,
    build_metric_normalization_stability_audit,
)


def main() -> int:
    audit = (
        build_metric_normalization_stability_audit(
            run_id=DEFAULT_RUN_ID,
            geo_ids=FOCUS_GEOS,
        )
    )

    feature_history = audit[
        "normalized_feature_history"
    ]

    metric_history = audit[
        "metric_score_history"
    ]

    feature_summary = audit[
        "feature_stability_summary"
    ]

    metric_summary = audit[
        "metric_stability_summary"
    ]

    feature_seasonality = audit[
        "feature_seasonality"
    ]

    metric_seasonality = audit[
        "metric_seasonality"
    ]

    feature_correlations = audit[
        "feature_baseline_correlations"
    ]

    metric_correlations = audit[
        "metric_baseline_correlations"
    ]

    comparison = audit[
        "metric_comparison_vs_baseline"
    ]

    print(
        "[normalization_stability] run:",
        DEFAULT_RUN_ID,
    )

    print(
        "[normalization_stability] "
        "normalized feature rows:",
        len(feature_history),
    )

    print(
        "[normalization_stability] "
        "metric score rows:",
        len(metric_history),
    )

    print(
        "\n[normalization_stability] "
        "feature stability:"
    )

    print(
        feature_summary.sort_values(
            [
                "geo_id",
                "feature_component",
                "policy_id",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[normalization_stability] "
        "metric stability:"
    )

    print(
        metric_summary.sort_values(
            [
                "geo_id",
                "policy_id",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[normalization_stability] "
        "metric challenger vs baseline:"
    )

    print(
        comparison.to_string(
            index=False
        )
    )

    print(
        "\n[normalization_stability] "
        "feature correlation with baseline:"
    )

    print(
        feature_correlations.sort_values(
            [
                "geo_id",
                "feature_component",
                "policy_id",
            ]
        ).to_string(index=False)
    )

    print(
        "\n[normalization_stability] "
        "metric correlation with baseline:"
    )

    print(
        metric_correlations.sort_values(
            [
                "geo_id",
                "policy_id",
            ]
        ).to_string(index=False)
    )

    feature_extremes = (
        feature_seasonality.sort_values(
            [
                "policy_id",
                "geo_id",
                "feature_component",
                (
                    "mean_absolute_feature_"
                    "score_change"
                ),
            ],
            ascending=[
                True,
                True,
                True,
                False,
            ],
        )
        .groupby(
            [
                "policy_id",
                "geo_id",
                "feature_component",
            ],
            as_index=False,
        )
        .head(3)
    )

    print(
        "\n[normalization_stability] "
        "top feature-score seasonal months:"
    )

    print(
        feature_extremes.to_string(
            index=False
        )
    )

    metric_extremes = (
        metric_seasonality.sort_values(
            [
                "policy_id",
                "geo_id",
                (
                    "mean_absolute_metric_"
                    "score_change"
                ),
            ],
            ascending=[
                True,
                True,
                False,
            ],
        )
        .groupby(
            [
                "policy_id",
                "geo_id",
            ],
            as_index=False,
        )
        .head(4)
    )

    print(
        "\n[normalization_stability] "
        "top metric-score seasonal months:"
    )

    print(
        metric_extremes.to_string(
            index=False
        )
    )

    latest = (
        metric_history.sort_values(
            [
                "policy_id",
                "geo_id",
                "date",
            ]
        )
        .groupby(
            [
                "policy_id",
                "geo_id",
            ],
            as_index=False,
        )
        .tail(12)
    )

    print(
        "\n[normalization_stability] "
        "latest metric scores:"
    )

    print(
        latest[
            [
                "policy_id",
                "geo_id",
                "date",
                "metric_score",
                "metric_score_change_1m",
                "feature_count",
                "feature_weight_sum",
                "metric_sign_flip_flag",
            ]
        ].to_string(index=False)
    )

    required_outputs = [
        "normalized_feature_history",
        "metric_score_history",
        "feature_stability_summary",
        "metric_stability_summary",
        "feature_seasonality",
        "metric_seasonality",
        "feature_baseline_correlations",
        "metric_baseline_correlations",
        "metric_comparison_vs_baseline",
    ]

    for key in required_outputs:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty output: {key}"
            )

    expected_policies = {
        BASELINE_POLICY_ID,
        *CHALLENGER_IDS,
    }

    if set(
        feature_history["policy_id"]
    ) != expected_policies:
        raise AssertionError(
            "Normalized-feature policy set mismatch"
        )

    if set(
        metric_history["policy_id"]
    ) != expected_policies:
        raise AssertionError(
            "Metric-score policy set mismatch"
        )

    expected_geos = set(
        FOCUS_GEOS
    )

    if set(
        metric_history["geo_id"]
    ) != expected_geos:
        raise AssertionError(
            "Metric-score focus geography mismatch"
        )

    expected_components = {
        "level",
        "short",
        "long",
    }

    if set(
        feature_history[
            "feature_component"
        ]
    ) != expected_components:
        raise AssertionError(
            "Normalized feature components mismatch"
        )

    numeric_columns = [
        "percentile",
        "feature_score",
    ]

    for column in numeric_columns:
        values = feature_history[
            column
        ].dropna()

        if not np.isfinite(
            values
        ).all():
            raise AssertionError(
                f"{column} contains non-finite values"
            )

    if not (
        feature_history[
            "percentile"
        ]
        .dropna()
        .between(
            0.0,
            1.0,
            inclusive="both",
        )
        .all()
    ):
        raise AssertionError(
            "Percentiles must remain between "
            "zero and one"
        )

    if not (
        feature_history[
            "feature_score"
        ]
        .dropna()
        .between(
            -1.0,
            1.0,
            inclusive="both",
        )
        .all()
    ):
        raise AssertionError(
            "Feature scores must remain between "
            "-1 and one"
        )

    if not (
        metric_history[
            "metric_score"
        ]
        .dropna()
        .between(
            -1.0,
            1.0,
            inclusive="both",
        )
        .all()
    ):
        raise AssertionError(
            "Metric scores must remain between "
            "-1 and one"
        )

    if (
        comparison[
            "minimum_feature_count"
        ] < 1
    ).any():
        raise AssertionError(
            "A challenger metric score has "
            "zero available features"
        )

    correlation_columns = [
        "metric_score_correlation",
    ]

    for column in correlation_columns:
        if not (
            metric_correlations[
                column
            ]
            .dropna()
            .between(
                -1.0,
                1.0,
                inclusive="both",
            )
            .all()
        ):
            raise AssertionError(
                f"{column} must remain between "
                "-1 and one"
            )

    duplicate_feature_rows = (
        feature_history.duplicated(
            subset=[
                "policy_id",
                "geo_id",
                "date",
                "feature_component",
            ],
            keep=False,
        )
    )

    if duplicate_feature_rows.any():
        raise AssertionError(
            "Normalized feature history contains "
            "duplicate policy/geo/date/component rows"
        )

    duplicate_metric_rows = (
        metric_history.duplicated(
            subset=[
                "policy_id",
                "geo_id",
                "date",
            ],
            keep=False,
        )
    )

    if duplicate_metric_rows.any():
        raise AssertionError(
            "Metric history contains duplicate "
            "policy/geo/date rows"
        )

    baseline_counts = metric_summary[
        metric_summary[
            "policy_id"
        ].eq(BASELINE_POLICY_ID)
    ][
        [
            "geo_id",
            "rows",
        ]
    ].rename(
        columns={
            "rows": "baseline_rows",
        }
    )

    challenger_counts = metric_summary[
        metric_summary[
            "policy_id"
        ].isin(CHALLENGER_IDS)
    ].merge(
        baseline_counts,
        on="geo_id",
        how="left",
        validate="many_to_one",
    )

    if (
        challenger_counts[
            "rows"
        ]
        > challenger_counts[
            "baseline_rows"
        ]
    ).any():
        raise AssertionError(
            "A challenger unexpectedly has more "
            "metric-score history than baseline"
        )

    print(
        "\n[normalization_stability] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
