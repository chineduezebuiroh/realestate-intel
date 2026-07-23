"""Smoke Test 58: Phase D4 dimension influence and weight diagnosis."""

from __future__ import annotations

import json
import shutil
from itertools import combinations
from pathlib import Path

import numpy as np
import pandas as pd

from regime.experiments.dimension_weight_diagnostic import (
    DEFAULT_D2_DIR,
    DEFAULT_OUTPUT_DIR,
    DIMENSIONS,
    run_dimension_weight_diagnostic,
)


def main() -> None:
    output_dir = DEFAULT_OUTPUT_DIR

    if output_dir.exists():
        shutil.rmtree(output_dir)

    results = run_dimension_weight_diagnostic(
        d2_dir=DEFAULT_D2_DIR,
        output_dir=output_dir,
    )

    scorecard = results["scorecard"]
    pair_relationships = results[
        "pair_relationships"
    ]
    sensitivity = results["sensitivity"]
    rankings = results["rankings"]

    expected_dimensions = set(DIMENSIONS)
    geographies = sorted(
        scorecard["geo_id"].unique()
    )

    assert geographies
    assert len(geographies) == 2

    # ------------------------------------------------------------------
    # Scorecard contract
    # ------------------------------------------------------------------

    required_scorecard_columns = {
        "geo_id",
        "dimension",
        "configured_weight",
        "observations",
        "mean_absolute_score_change",
        "score_change_std",
        "mean_absolute_contribution_change",
        "contribution_change_std",
        "mean_share_of_absolute_change",
        "dominant_month_fraction",
        "aligned_activity_total",
        "opposing_activity_total",
        "gross_activity_total",
        "net_activity_total",
        "mean_net_to_gross_ratio",
        "mean_cancellation_rate_when_active",
        "active_month_fraction",
        "opposing_activity_fraction",
    }

    assert required_scorecard_columns.issubset(
        scorecard.columns
    )

    assert len(scorecard) == (
        len(geographies) * len(DIMENSIONS)
    )

    for geo_id, group in scorecard.groupby(
        "geo_id"
    ):
        assert set(group["dimension"]) == (
            expected_dimensions
        )

        weight_by_dimension = (
            group.set_index("dimension")[
                "configured_weight"
            ]
        )

        assert weight_by_dimension.notna().all()
        assert weight_by_dimension.gt(0).all()

        assert np.isclose(
            weight_by_dimension.sum(),
            1.0,
            atol=1e-12,
        )

        assert group[
            "mean_share_of_absolute_change"
        ].between(
            0.0,
            1.0,
            inclusive="both",
        ).all()

        assert group[
            "dominant_month_fraction"
        ].between(
            0.0,
            1.0,
            inclusive="both",
        ).all()

        assert np.isclose(
            group[
                "dominant_month_fraction"
            ].sum(),
            1.0,
            atol=1e-12,
        )

    # ------------------------------------------------------------------
    # Pair relationship contract
    # ------------------------------------------------------------------

    required_pair_columns = {
        "geo_id",
        "dimension_left",
        "dimension_right",
        "observations",
        "both_active_observations",
        "contribution_correlation",
        "absolute_activity_correlation",
        "score_change_correlation",
        "same_sign_fraction_when_both_active",
        "opposite_sign_fraction_when_both_active",
        "mean_joint_gross_activity",
        "total_joint_gross_activity",
        "mean_pair_cancellation",
        "total_pair_cancellation",
        "pair_cancellation_rate",
    }

    assert required_pair_columns.issubset(
        pair_relationships.columns
    )

    expected_pair_count = len(
        list(combinations(DIMENSIONS, 2))
    )

    assert len(pair_relationships) == (
        len(geographies) * expected_pair_count
    )

    for geo_id, group in pair_relationships.groupby(
        "geo_id"
    ):
        assert len(group) == expected_pair_count

        observed_pairs = {
            tuple(sorted(pair))
            for pair in zip(
                group["dimension_left"],
                group["dimension_right"],
            )
        }

        expected_pairs = {
            tuple(sorted(pair))
            for pair in combinations(
                DIMENSIONS,
                2,
            )
        }

        assert observed_pairs == expected_pairs

        active_rows = group[
            "both_active_observations"
        ].gt(0)

        if active_rows.any():
            same_plus_opposite = (
                group.loc[
                    active_rows,
                    "same_sign_fraction_when_both_active",
                ]
                + group.loc[
                    active_rows,
                    "opposite_sign_fraction_when_both_active",
                ]
            )

            assert np.allclose(
                same_plus_opposite,
                1.0,
                atol=1e-12,
            )

        finite_cancellation = group[
            "pair_cancellation_rate"
        ].dropna()

        assert finite_cancellation.between(
            0.0 - 1e-12,
            1.0 + 1e-12,
            inclusive="both",
        ).all()

    # ------------------------------------------------------------------
    # Sensitivity contract
    # ------------------------------------------------------------------

    required_sensitivity_columns = {
        "geo_id",
        "removed_dimension",
        "observations",
        "baseline_mean_absolute_axis_change",
        "counterfactual_mean_absolute_axis_change",
        "change_in_mean_absolute_axis_change",
        "baseline_axis_change_std",
        "counterfactual_axis_change_std",
        "change_in_axis_change_std",
        "baseline_mean_gross_activity",
        "counterfactual_mean_gross_activity",
        "change_in_mean_gross_activity",
        "baseline_mean_cancellation_rate",
        "counterfactual_mean_cancellation_rate",
        "change_in_mean_cancellation_rate",
        "baseline_mean_effective_component_count",
        "counterfactual_mean_effective_component_count",
        "change_in_mean_effective_component_count",
        "dominant_dimension_changed_fraction",
        "mean_absolute_axis_difference",
        "max_absolute_axis_difference",
        "axis_difference_std",
    }

    assert required_sensitivity_columns.issubset(
        sensitivity.columns
    )

    assert len(sensitivity) == (
        len(geographies) * len(DIMENSIONS)
    )

    for geo_id, group in sensitivity.groupby(
        "geo_id"
    ):
        assert set(
            group["removed_dimension"]
        ) == expected_dimensions

        assert group[
            "dominant_dimension_changed_fraction"
        ].between(
            0.0,
            1.0,
            inclusive="both",
        ).all()

        assert group[
            "mean_absolute_axis_difference"
        ].ge(
            -1e-12
        ).all()

        assert group[
            "max_absolute_axis_difference"
        ].ge(
            group["mean_absolute_axis_difference"]
            - 1e-12
        ).all()

    # ------------------------------------------------------------------
    # Ranking contract
    # ------------------------------------------------------------------

    required_ranking_columns = {
        "geo_id",
        "investigation_rank",
        "dimension",
        "priority_band",
        "diagnostic_priority_score",
        "configured_weight",
        "mean_share_of_absolute_change",
        "dominant_month_fraction",
        "mean_absolute_axis_difference",
        "dominant_dimension_changed_fraction",
        "total_pair_cancellation_involvement",
        "activity_priority_component",
        "dominance_priority_component",
        "sensitivity_priority_component",
        "cancellation_priority_component",
        "priority_reason",
    }

    assert required_ranking_columns.issubset(
        rankings.columns
    )

    assert len(rankings) == (
        len(geographies) * len(DIMENSIONS)
    )

    for geo_id, group in rankings.groupby(
        "geo_id"
    ):
        assert set(group["dimension"]) == (
            expected_dimensions
        )

        assert set(
            group["investigation_rank"]
        ) == set(
            range(
                1,
                len(DIMENSIONS) + 1,
            )
        )

        assert group[
            "diagnostic_priority_score"
        ].between(
            0.0 - 1e-12,
            1.0 + 1e-12,
            inclusive="both",
        ).all()

    # ------------------------------------------------------------------
    # File and manifest contract
    # ------------------------------------------------------------------

    expected_files = {
        "dimension_weight_scorecard.csv",
        "dimension_pair_relationships.csv",
        "dimension_sensitivity_analysis.csv",
        "dimension_rankings.csv",
        "dimension_diagnostic_manifest.json",
    }

    observed_files = {
        path.name
        for path in output_dir.iterdir()
        if path.is_file()
    }

    assert expected_files.issubset(observed_files)

    manifest_path = (
        output_dir
        / "dimension_diagnostic_manifest.json"
    )

    with manifest_path.open(
        "r",
        encoding="utf-8",
    ) as handle:
        manifest = json.load(handle)

    assert manifest["phase"] == "D4"

    assert (
        manifest["weight_policy"][
            "configured_weights_preserved"
        ]
        is True
    )

    assert (
        manifest["weight_policy"][
            "alternative_weights_evaluated"
        ]
        is False
    )

    assert (
        manifest["weight_policy"][
            "leave_one_out_renormalization"
        ]
        is False
    )

    assert set(
        manifest["dimensions"]
    ) == expected_dimensions

    top_ranked = (
        rankings.loc[
            rankings["investigation_rank"].eq(1),
            [
                "geo_id",
                "dimension",
                "diagnostic_priority_score",
                "priority_reason",
            ],
        ]
        .sort_values("geo_id")
    )

    highest_cancellation_pairs = (
        pair_relationships.sort_values(
            [
                "geo_id",
                "pair_cancellation_rate",
            ],
            ascending=[True, False],
        )
        .groupby("geo_id", as_index=False)
        .head(1)
    )

    print()
    print("=" * 100)
    print(
        "SMOKE TEST 58 — "
        "DIMENSION INFLUENCE & WEIGHT DIAGNOSIS: PASS"
    )
    print("=" * 100)
    print(
        f"scorecard_rows={len(scorecard):,}"
    )
    print(
        "pair_relationship_rows="
        f"{len(pair_relationships):,}"
    )
    print(
        "sensitivity_rows="
        f"{len(sensitivity):,}"
    )
    print(
        f"ranking_rows={len(rankings):,}"
    )
    print(
        f"geographies={len(geographies):,}"
    )

    print("\nTop-ranked dimension by geography:")
    print(
        top_ranked.to_string(index=False)
    )

    print(
        "\nHighest-cancellation pair by geography:"
    )
    print(
        highest_cancellation_pairs[
            [
                "geo_id",
                "dimension_left",
                "dimension_right",
                "pair_cancellation_rate",
                "opposite_sign_fraction_when_both_active",
            ]
        ].to_string(index=False)
    )


if __name__ == "__main__":
    main()
