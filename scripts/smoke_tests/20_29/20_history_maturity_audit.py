from __future__ import annotations
# scripts/smoke_tests/20_29/20_history_maturity_audit.py

from regime.diagnostics.history_maturity import (
    DEFAULT_VALIDATION_GEOS,
    build_history_maturity_audit,
)


RUN_ID = "macro_regime_v1"

CHECKPOINT_YEARS = [
    2009,
    2012,
    2013,
    2019,
    2020,
    2021,
    2022,
    2023,
    2026,
]


def main() -> int:
    audit = build_history_maturity_audit(
        run_id=RUN_ID,
    )

    source_geo = audit["annual_source_history_summary"]
    source_axis = audit[
        "annual_axis_source_history_summary"
    ]
    source_metric = audit[
        "annual_metric_source_history_summary"
    ]
    evaluation_axis = audit[
        "annual_evaluation_axis_snapshot"
    ]
    evaluation_metric = audit[
        "annual_evaluation_metric_snapshot"
    ]
    latest = audit["latest_source_feature_summary"]

    print("[history_maturity_v2] run:", RUN_ID)

    print(
        "\n[history_maturity_v2] source-history geography "
        "checkpoints:"
    )
    print(
        source_geo[
            source_geo["source_year"].isin(CHECKPOINT_YEARS)
        ]
        .sort_values(["geo_id", "source_year"])
        .to_string(index=False)
    )

    print(
        "\n[history_maturity_v2] source-history axis "
        "checkpoints:"
    )
    print(
        source_axis[
            source_axis["source_year"].isin(CHECKPOINT_YEARS)
        ]
        .sort_values(
            ["geo_id", "source_year", "axis"]
        )
        .to_string(index=False)
    )

    print(
        "\n[history_maturity_v2] evaluation-calendar axis "
        "checkpoints:"
    )
    print(
        evaluation_axis[
            evaluation_axis["evaluation_year"].isin(
                CHECKPOINT_YEARS
            )
        ][
            [
                "geo_id",
                "evaluation_year",
                "evaluation_date",
                "axis",
                "metric_count",
                "avg_metric_age_days",
                "weighted_avg_metric_age_days",
                "max_metric_age_days",
                "avg_minimum_ratio",
                "avg_lookback_ratio",
                "weighted_avg_lookback_ratio",
                "minimum_met_metric_share",
                "full_window_metric_share",
            ]
        ]
        .sort_values(
            [
                "geo_id",
                "evaluation_year",
                "axis",
            ]
        )
        .to_string(index=False)
    )

    for geo_id in DEFAULT_VALIDATION_GEOS:
        print(
            "\n[history_maturity_v2] Demand evaluation "
            f"metrics: {geo_id}"
        )

        sample = evaluation_metric[
            evaluation_metric["geo_id"].eq(geo_id)
            & evaluation_metric["axis"].eq("demand")
            & evaluation_metric["evaluation_year"].isin(
                CHECKPOINT_YEARS
            )
        ]

        print(
            sample[
                [
                    "geo_id",
                    "source_geo_id",
                    "evaluation_year",
                    "evaluation_date",
                    "dimension",
                    "canonical_metric_key",
                    "metric_date",
                    "metric_age_days",
                    "scored_feature_count_at_metric_date",
                    "source_feature_count",
                    "avg_minimum_ratio",
                    "avg_lookback_ratio",
                    "minimum_met_share",
                    "full_window_share",
                ]
            ]
            .sort_values(
                [
                    "evaluation_year",
                    "dimension",
                    "canonical_metric_key",
                ]
            )
            .to_string(index=False)
        )

    print(
        "\n[history_maturity_v2] latest source-history "
        "features below full window:"
    )

    immature = latest[~latest["full_window"]]

    if immature.empty:
        print("  NONE")
    else:
        print(
            immature[
                [
                    "geo_id",
                    "dimension",
                    "canonical_metric_key",
                    "feature_key",
                    "frequency",
                    "observation_count",
                    "min_periods",
                    "lookback_periods",
                    "minimum_ratio",
                    "lookback_ratio",
                    "maturity_status",
                    "score_available",
                    "first_feature_date",
                    "first_score_date",
                    "date",
                ]
            ]
            .sort_values(
                [
                    "geo_id",
                    "lookback_ratio",
                    "canonical_metric_key",
                    "feature_key",
                ]
            )
            .to_string(index=False)
        )

    required = [
        "annual_source_history_summary",
        "annual_axis_source_history_summary",
        "annual_metric_source_history_summary",
        "evaluation_metric_maturity",
        "evaluation_axis_maturity",
        "annual_evaluation_axis_snapshot",
        "annual_evaluation_metric_snapshot",
    ]

    for key in required:
        if audit[key].empty:
            raise AssertionError(
                f"Expected non-empty audit output: {key}"
            )

    print("\n[history_maturity_v2] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
