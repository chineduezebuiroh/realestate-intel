from __future__ import annotations
# scripts/smoke_tests/20_history_maturity_audit.py

from regime.diagnostics.history_maturity import (
    DEFAULT_VALIDATION_GEOS,
    NATIONAL_GEO_ID,
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

    feature_summary = audit["feature_summary"]
    annual_geo = audit["annual_geo_summary"]
    annual_axis = audit["annual_axis_summary"]
    annual_metric = audit["annual_metric_summary"]
    latest = audit["latest_feature_summary"]

    print("[history_maturity] run:", RUN_ID)
    print("[history_maturity] feature summaries:", len(feature_summary))
    print(
        "[history_maturity] source geographies:",
        feature_summary["geo_id"].nunique(),
    )

    print("\n[history_maturity] annual geography checkpoints:")
    print(
        annual_geo[
            annual_geo["year"].isin(CHECKPOINT_YEARS)
        ]
        .sort_values(["geo_id", "year"])
        .to_string(index=False)
    )

    print("\n[history_maturity] annual axis checkpoints:")
    print(
        annual_axis[
            annual_axis["year"].isin(CHECKPOINT_YEARS)
        ]
        .sort_values(["geo_id", "year", "axis"])
        .to_string(index=False)
    )

    for geo_id in DEFAULT_VALIDATION_GEOS:
        print(
            f"\n[history_maturity] Demand metric checkpoints: {geo_id}"
        )

        sample = annual_metric[
            annual_metric["geo_id"].eq(geo_id)
            & annual_metric["axis"].eq("demand")
            & annual_metric["year"].isin(CHECKPOINT_YEARS)
        ].copy()

        if sample.empty:
            print("  NONE")
        else:
            print(
                sample[
                    [
                        "geo_id",
                        "year",
                        "dimension",
                        "canonical_metric_key",
                        "feature_count",
                        "scored_feature_share",
                        "minimum_met_share",
                        "full_window_share",
                        "avg_minimum_ratio",
                        "avg_lookback_ratio",
                        "min_observation_count",
                        "max_observation_count",
                    ]
                ]
                .sort_values(
                    [
                        "year",
                        "dimension",
                        "canonical_metric_key",
                    ]
                )
                .to_string(index=False)
            )

    print(
        f"\n[history_maturity] National capital-market checkpoints: "
        f"{NATIONAL_GEO_ID}"
    )

    national = annual_metric[
        annual_metric["geo_id"].eq(NATIONAL_GEO_ID)
        & annual_metric["year"].isin(CHECKPOINT_YEARS)
    ].copy()

    if national.empty:
        print("  NONE")
    else:
        print(
            national[
                [
                    "geo_id",
                    "year",
                    "axis",
                    "dimension",
                    "canonical_metric_key",
                    "feature_count",
                    "scored_feature_share",
                    "minimum_met_share",
                    "full_window_share",
                    "avg_lookback_ratio",
                    "min_observation_count",
                    "max_observation_count",
                ]
            ]
            .sort_values(
                [
                    "year",
                    "axis",
                    "canonical_metric_key",
                ]
            )
            .to_string(index=False)
        )

    print("\n[history_maturity] latest immature features:")
    immature = latest[
        ~latest["full_window"]
    ].copy()

    if immature.empty:
        print("  NONE")
    else:
        print(
            immature[
                [
                    "geo_id",
                    "axis",
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
                    "axis",
                    "lookback_ratio",
                    "canonical_metric_key",
                    "feature_key",
                ]
            )
            .to_string(index=False)
        )

    if feature_summary.empty:
        raise AssertionError(
            "Feature maturity summary is empty"
        )

    if annual_axis.empty:
        raise AssertionError(
            "Annual axis maturity summary is empty"
        )

    print("\n[history_maturity] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
