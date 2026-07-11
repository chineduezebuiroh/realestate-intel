from __future__ import annotations
# scripts/smoke_tests/23_derived_input_freshness_policy.py

from regime.artifacts import RegimeArtifactStore
from regime.freshness import (
    evaluate_derived_input_freshness,
    load_derived_input_freshness_policy,
)


SOURCE_RUN_ID = "macro_regime_v1_lineage"

CHECK_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]


def main() -> int:
    store = RegimeArtifactStore()

    policy = load_derived_input_freshness_policy()

    print("[freshness_policy] active policies:")
    print(policy.to_string(index=False))

    lineage = store.read_dataframe(
        SOURCE_RUN_ID,
        "derived_metric_lineage",
    )

    outputs = evaluate_derived_input_freshness(
        lineage
    )

    component = outputs["component_status"]
    derived = outputs["derived_status"]

    print(
        "\n[freshness_policy] component status summary:"
    )
    print(
        component.groupby(
            [
                "derived_metric_key",
                "component_metric_key",
                "freshness_status",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            geos=("geo_id", "nunique"),
            maximum_age_days=(
                "component_age_days",
                "max",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "derived_metric_key",
                "component_metric_key",
                "freshness_severity",
            ]
            if "freshness_severity"
            in component.columns
            else [
                "derived_metric_key",
                "component_metric_key",
                "freshness_status",
            ]
        )
        .to_string(index=False)
    )

    print(
        "\n[freshness_policy] derived status summary:"
    )
    print(
        derived.groupby(
            [
                "derived_metric_key",
                "derived_freshness_status",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            geos=("geo_id", "nunique"),
            maximum_age_days=(
                "governing_component_age_days",
                "max",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "derived_metric_key",
                "derived_freshness_severity",
            ]
            if "derived_freshness_severity"
            in derived.columns
            else [
                "derived_metric_key",
                "derived_freshness_status",
            ]
        )
        .to_string(index=False)
    )

    latest = (
        derived[
            derived["geo_id"].isin(CHECK_GEOS)
        ]
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "date",
            ]
        )
        .groupby(
            [
                "geo_id",
                "derived_metric_key",
            ],
            as_index=False,
        )
        .tail(1)
    )

    print(
        "\n[freshness_policy] latest DC and Alameda:"
    )
    print(
        latest[
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "governing_component_metric_key",
                "governing_component_source_date",
                "governing_component_age_days",
                "governing_warning_days",
                "governing_hard_days",
                "derived_freshness_status",
                "stale_input_flag",
                "exceeded_horizon_flag",
                "suppress_output_flag",
                "confidence_adjustment_required",
            ]
        ]
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
            ]
        )
        .to_string(index=False)
    )

    if component.empty or derived.empty:
        raise AssertionError(
            "Freshness policy outputs are empty"
        )

    if derived["suppress_output_flag"].any():
        raise AssertionError(
            "Production policy must not suppress outputs"
        )

    hard_rows = derived[
        derived["exceeded_horizon_flag"]
    ]

    if hard_rows.empty:
        raise AssertionError(
            "Expected historical hard-horizon breaches"
        )

    if not hard_rows[
        "confidence_adjustment_required"
    ].all():
        raise AssertionError(
            "Hard-horizon breaches must require "
            "confidence adjustment"
        )

    latest_dc_alameda = latest[
        latest["derived_metric_key"].isin(
            {
                "price_to_income",
                "payment_burden",
                "permit_intensity",
            }
        )
    ]

    if latest_dc_alameda.empty:
        raise AssertionError(
            "Expected latest DC and Alameda statuses"
        )

    print("\n[freshness_policy] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
