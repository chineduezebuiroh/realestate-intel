"""Smoke Test 84: score one authoritative Phase A materialization twice."""

from __future__ import annotations

from pathlib import Path
from time import perf_counter

import numpy as np
import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.review.calibration import run_phase_a_foundation_evidence, score_inventory_candidates
from regime.review.calibration.inventory_campaign import (
    AUTHORITATIVE_PRODUCER_CODE_IDENTITY,
    evidence_directory,
    invalidate_authoritative_evidence_readiness,
)


def main() -> int:
    run_id = "macro_regime_v1_bps120_sources"
    campaign_id, campaign_version = "inventory_phase_a_authoritative_v1", "1.0"
    canonical = evidence_directory(DEFAULT_ARTIFACT_ROOT, campaign_id, campaign_version)
    invalidate_authoritative_evidence_readiness(
        artifact_root=DEFAULT_ARTIFACT_ROOT, campaign_id=campaign_id, campaign_version=campaign_version,
    )
    print(f"[inventory-scoring] canonical evidence: {canonical}", flush=True)
    print(f"[inventory-scoring] producer identity: {AUTHORITATIVE_PRODUCER_CODE_IDENTITY}", flush=True)
    run_dir = Path(DEFAULT_ARTIFACT_ROOT) / run_id
    missing = [name for name in ("features.parquet", "source_metrics.parquet", "manifest.json")
               if not (run_dir / name).is_file()]
    if missing:
        print(f"SMOKE TEST 84 — SKIP: authoritative production artifacts unavailable; missing={missing}")
        return 0
    started = perf_counter()
    print("[inventory-scoring] building authoritative Phase A evidence once...", flush=True)
    evidence = run_phase_a_foundation_evidence(
        campaign_id=campaign_id, campaign_version=campaign_version,
        artifact_root=DEFAULT_ARTIFACT_ROOT,
        persist_system_evidence=True,
    )
    print(f"[inventory-scoring] evidence ready in {perf_counter() - started:,.1f}s; scoring twice...", flush=True)
    first = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence)
    second = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence)
    for name, frame in first.tables.items():
        pd.testing.assert_frame_equal(frame, second.tables[name])
    ranking = first.inventory_candidate_ranking
    detail = first.inventory_candidate_metric_scores
    recommendation = first.inventory_campaign_recommendation
    assert len(ranking) == 4 and ranking["candidate_policy_id"].nunique() == 4
    assert ranking["eligible"].any()
    assert ranking["recommendation_state"].eq("recommended").sum() == 1
    weighted = first.inventory_candidate_weighted_scores
    assert len(detail) == 60
    assert not detail.duplicated(["candidate_policy_id", "metric_key", "feature_component"]).any()
    assert len(weighted) == 20
    assert weighted.groupby("candidate_policy_id").size().eq(5).all()
    assert not weighted.duplicated(["candidate_policy_id", "metric_key"]).any()
    assert not ranking["candidate_policy_id"].duplicated().any()
    assert len(recommendation) == 1
    eligible = weighted["eligible"]
    assert np.isfinite(weighted.loc[eligible, "normalized_score"]).all()
    assert weighted.loc[eligible, "normalized_score"].between(0, 1).all()
    assert weighted.loc[eligible, "weighted_score"].notna().all()
    assert weighted.loc[~eligible, "weighted_score"].isna().all()
    assert weighted.loc[~eligible, "normalized_score"].isna().all()
    eligible_ranks = ranking.loc[ranking["eligible"], "rank"]
    assert eligible_ranks.notna().all() and sorted(eligible_ranks.tolist()) == list(range(1, len(eligible_ranks) + 1))
    for row in ranking[ranking["eligible"]].itertuples(index=False):
        actual = weighted.loc[weighted["candidate_policy_id"].eq(row.candidate_policy_id), "weighted_score"].sum()
        assert np.isclose(actual, row.total_score)
    failed = first.inventory_candidate_eligibility.query("not gate_pass")
    if not failed.empty:
        print("[inventory-scoring] failed eligibility gates:", flush=True)
        print(failed[["candidate_policy_id", "gate_key", "failure_reason"]].to_string(index=False), flush=True)
    summary = ranking.merge(
        weighted.pivot(index="candidate_policy_id", columns="metric_key", values="normalized_score").reset_index(),
        on="candidate_policy_id", how="left", validate="one_to_one",
    )
    print(summary[["candidate_policy_id", "eligible", "total_score", "rank", "recommendation_state",
                   "warmup_coverage_retention", "seasonality_suppression", "volatility_reduction",
                   "sign_flip_reduction", "trend_shape_preservation"]].to_string(index=False))
    print(f"SMOKE TEST 84 — INVENTORY CANDIDATE SCORING AUTHORITATIVE: PASS ({perf_counter() - started:,.1f}s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
