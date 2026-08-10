"""Smoke Test 86: authoritative inventory review bundle generation."""

from pathlib import Path
from time import perf_counter

from regime.artifacts import DEFAULT_ARTIFACT_ROOT
from regime.review.calibration import score_inventory_candidates
from regime.review.calibration.inventory_campaign import validate_current_authoritative_evidence
from regime.review.calibration.inventory_review_bundle import build_inventory_review_bundle


def main() -> int:
    started = perf_counter(); run_id = "macro_regime_v1_bps120_sources"
    run_dir = Path(DEFAULT_ARTIFACT_ROOT) / run_id
    missing = [name for name in ("features.parquet", "source_metrics.parquet", "manifest.json") if not (run_dir / name).is_file()]
    if missing:
        print(f"SMOKE TEST 86 — SKIP: authoritative production artifacts unavailable; missing={missing}")
        return 0
    evidence = validate_current_authoritative_evidence(
        campaign_id="inventory_phase_a_authoritative_v1", campaign_version="1.0",
        artifact_root=DEFAULT_ARTIFACT_ROOT,
        source_run_id=run_id,
    )
    scoring = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence)
    bundle = build_inventory_review_bundle(campaign=evidence.campaign, phase_a_evidence=evidence,
                                           scoring_result=scoring, system_evidence=evidence.system_evidence,
                                           output_root=Path("artifacts/review"), overwrite=True,
                                           source_lineage={"authoritative_artifact_directory": str(run_dir)})
    recommendation = scoring.inventory_campaign_recommendation.iloc[0]
    assert bundle.manifest["flags"]["promotion_performed"] is False
    assert bundle.manifest["recommended_candidate_policy_id"] == recommendation["recommended_candidate_policy_id"]
    print(f"bundle directory: {bundle.bundle_directory}\nZIP path: {bundle.zip_path}\nfile count: {len(bundle.generated_files)}\nZIP size: {bundle.zip_path.stat().st_size}\nrecommendation: {recommendation['recommended_candidate_policy_id']}\neligible count: {recommendation['eligible_candidate_count']}\ntotal runtime: {perf_counter() - started:,.1f}s")
    print("SMOKE TEST 86 — INVENTORY REVIEW BUNDLE AUTHORITATIVE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
