"""Smoke Test 82: authoritative artifact Phase A evidence integration."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.review.calibration import run_phase_a_foundation_evidence
from regime.review.calibration.inventory_campaign import (
    FEATURE_COMPONENTS,
    PHASE_A_CANDIDATES,
)


RUN_ID = "macro_regime_v1_bps120_sources"
REQUIRED_TABLES = {
    "inventory_phase_a_campaign", "inventory_phase_a_candidates",
    "inventory_phase_a_feature_weights", "inventory_candidate_feature_coverage",
    "inventory_candidate_lineage_summary", "inventory_candidate_target_replacement",
    "inventory_candidate_non_target_parity", "inventory_candidate_feature_statistics",
    "inventory_candidate_feature_correlations", "inventory_candidate_calendar_month_behavior",
    "inventory_candidate_baseline_feature_comparison",
}


def _run():
    return run_phase_a_foundation_evidence(
        campaign_id="inventory_phase_a_foundation_v1",
        campaign_version="1.0",
        artifact_root=DEFAULT_ARTIFACT_ROOT,
    )


def main() -> int:
    required = ("features.parquet", "source_metrics.parquet", "manifest.json")
    run_dir = Path(DEFAULT_ARTIFACT_ROOT) / RUN_ID
    missing = [str(run_dir / name) for name in required if not (run_dir / name).is_file()]
    if missing:
        print(
            "SMOKE TEST 82 — SKIP: authoritative production artifacts unavailable; "
            f"missing={missing}; synthetic data was not substituted"
        )
        return 0

    artifact_root = Path(DEFAULT_ARTIFACT_ROOT)
    directories_before = {path.name for path in artifact_root.iterdir() if path.is_dir()}
    
    started = perf_counter()
    
    result = _run()

    print(
        f"First authoritative Phase A run completed in "
        f"{perf_counter() - started:,.1f}s",
        flush=True,
    )

    started = perf_counter()

    repeated = _run()

    print(
        f"Repeated authoritative Phase A run completed in "
        f"{perf_counter() - started:,.1f}s",
        flush=True,
    )
    
    directories_after = {path.name for path in artifact_root.iterdir() if path.is_dir()}
    candidates = tuple(PHASE_A_CANDIDATES.values())
    assert result.campaign.candidate_policy_ids == candidates
    assert tuple(result.challengers) == candidates
    assert tuple(result.evidence_results) == (
        "campaign_definition", "coverage_and_lineage",
        "structural_window_behavior", "baseline_comparison",
    )
    assert result.review_bundle.table_count == 11
    names = [table.name for table in result.review_bundle.tables]
    assert len(names) == len(set(names)) and set(names) == REQUIRED_TABLES
    table_by_name = {table.name: table.dataframe for table in result.review_bundle.tables}
    repeated_by_name = {table.name: table.dataframe for table in repeated.review_bundle.tables}
    assert names == [table.name for table in repeated.review_bundle.tables]
    for name in names:
        pd.testing.assert_frame_equal(table_by_name[name], repeated_by_name[name])

    candidate_tables = [name for name in names if "campaign" not in name and "feature_weights" not in name]
    for name in candidate_tables:
        frame = table_by_name[name]
        assert set(frame["candidate_policy_id"]) == set(candidates), name
    coverage = table_by_name["inventory_candidate_feature_coverage"]
    assert coverage.groupby("candidate_policy_id", sort=False)["feature_component"].apply(tuple).tolist() == [FEATURE_COMPONENTS] * 4
    parity = table_by_name["inventory_candidate_non_target_parity"]
    assert parity["parity_pass"].all()
    replacement = table_by_name["inventory_candidate_target_replacement"]
    assert replacement.groupby("candidate_policy_id")["changed_rows"].sum().gt(0).all()

    assert (
        replacement["baseline_rows"]
        == replacement["overlap_rows"] + replacement["baseline_only_rows"]
    ).all()

    assert (
        replacement["challenger_rows"]
        == replacement["overlap_rows"] + replacement["challenger_only_rows"]
    ).all()

    assert replacement["overlap_rows"].gt(0).all()
    assert replacement["baseline_only_rows"].ge(0).all()

    assert replacement["challenger_only_rows"].eq(0).all()
    assert not hasattr(result, "decision") and not hasattr(result, "decision_summary")
    assert directories_before == directories_after
    print("SMOKE TEST 82 — INVENTORY PHASE A AUTHORITATIVE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
