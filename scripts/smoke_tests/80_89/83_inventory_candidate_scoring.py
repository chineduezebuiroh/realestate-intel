"""Smoke Test 83: fast synthetic inventory candidate scoring contracts."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import numpy as np
import pandas as pd

from regime.review.calibration import (
    PhaseAEvidence, build_inventory_calibration_campaign,
    load_inventory_scoring_policy, score_inventory_candidates,
)
from regime.review.calibration.inventory_campaign import FEATURE_COMPONENTS, PHASE_A_CANDIDATES
from regime.review.models import ReviewBundle
from regime.review.results import ReviewResult
from regime.review.calibration.inventory_candidate_scoring import (
    InventoryScoringPolicy, _ranking_comparison,
)


CANDIDATES = tuple(PHASE_A_CANDIDATES.values())


def _evidence(*, parity_failure: str | None = None, nonfinite: str | None = None,
              equal_metrics: bool = False, all_ineligible: bool = False) -> PhaseAEvidence:
    campaign = build_inventory_calibration_campaign(
        campaign_id="synthetic_scoring", campaign_version="1.0",
        baseline_run_id="macro_regime_v1_bps120_sources",
        incumbent_run_id="macro_regime_v1_bps120_sources",
    )
    coverage, replacement, parity, statistics, calendar, comparison = [], [], [], [], [], []
    # MA6 wins the default fixture: its stability gains outweigh modest warmup.
    coverages = [0.99, 0.95, 0.90, 0.85]
    volatility = [10.0, 4.0, 5.0, 6.0]
    flips = [0.40, 0.10, 0.15, 0.20]
    seasonal = [8.0, 2.0, 3.0, 4.0]
    correlations = [0.99, 0.96, 0.90, 0.84]
    if equal_metrics:
        coverages = volatility = flips = seasonal = correlations = [0.5] * 4
    for index, candidate in enumerate(CANDIDATES):
        for component in FEATURE_COMPONENTS:
            raw = np.nan if nonfinite == candidate and component == "level" else volatility[index]
            coverage.append({"candidate_policy_id": candidate, "feature_component": component,
                             "rows": 100, "valid_rows": int(coverages[index] * 100),
                             "warmup_rows": 100 - int(coverages[index] * 100),
                             "non_finite_rows": 0, "duplicate_key_rows": 0})
            replacement.append({"candidate_policy_id": candidate, "feature_component": component,
                                "baseline_rows": 100, "challenger_rows": 100,
                                "overlap_rows": 100, "baseline_only_rows": 0,
                                "challenger_only_rows": 0})
            statistics.append({"candidate_policy_id": candidate, "feature_component": component,
                               "standard_deviation": raw, "sign_flip_rate": flips[index]})
            comparison.append({"candidate_policy_id": candidate, "feature_component": component,
                               "correlation": correlations[index]})
            for month in range(1, 13):
                calendar.append({"candidate_policy_id": candidate, "feature_component": component,
                                 "calendar_month": month,
                                 "mean_absolute_monthly_change": seasonal[index]})
        passes = not all_ineligible and candidate != parity_failure
        parity.append({"candidate_policy_id": candidate, "parity_pass": passes})
    results = {
        "coverage": ReviewResult(tables={
            "inventory_candidate_feature_coverage": pd.DataFrame(coverage),
            "inventory_candidate_target_replacement": pd.DataFrame(replacement),
            "inventory_candidate_non_target_parity": pd.DataFrame(parity),
        }),
        "behavior": ReviewResult(tables={
            "inventory_candidate_feature_statistics": pd.DataFrame(statistics),
            "inventory_candidate_calendar_month_behavior": pd.DataFrame(calendar),
        }),
        "comparison": ReviewResult(tables={
            "inventory_candidate_baseline_feature_comparison": pd.DataFrame(comparison),
        }),
    }
    return PhaseAEvidence(campaign, {}, results, ReviewBundle(campaign.campaign_id))


def _table(evidence: PhaseAEvidence, name: str) -> pd.DataFrame:
    return next(result.tables[name] for result in evidence.evidence_results.values()
                if name in result.tables)


def _assert_calendar_failure(change) -> None:
    evidence = _evidence()
    calendar = _table(evidence, "inventory_candidate_calendar_month_behavior")
    change(calendar)
    result = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence)
    gate = result.inventory_candidate_eligibility.query(
        "candidate_policy_id == @CANDIDATES[0] and gate_key == 'calendar_month_evidence_reconciles'"
    )
    assert len(gate) == 1 and not gate["gate_pass"].item()


def main() -> int:
    policy = load_inventory_scoring_policy()
    evidence = _evidence(parity_failure=CANDIDATES[3], nonfinite=CANDIDATES[2])
    first = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence, scoring_policy=policy)
    second = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence, scoring_policy=policy)
    for name, frame in first.tables.items():
        pd.testing.assert_frame_equal(frame, second.tables[name])
    ranking = first.inventory_candidate_ranking
    assert ranking["candidate_policy_id"].tolist() == list(CANDIDATES)
    assert len(ranking) == 4 and ranking["eligible"].tolist() == [True, True, False, False]
    assert ranking.loc[1, "recommendation_state"] == "recommended"
    # MA12 warmup is represented as a metric even though its separate parity failure is decisive.
    warmup = first.inventory_candidate_metric_scores.query("metric_key == 'warmup_coverage_retention'")
    assert len(warmup) == 12 and warmup["raw_value"].between(0, 1).all()
    assert first.inventory_candidate_eligibility.query(
        "candidate_policy_id == @CANDIDATES[3] and gate_key == 'required_components_present'"
    )["gate_pass"].item()
    assert not first.inventory_candidate_eligibility.query(
        "candidate_policy_id == @CANDIDATES[3] and gate_key == 'non_target_parity'"
    )["gate_pass"].item()
    assert not first.inventory_candidate_eligibility.query(
        "candidate_policy_id == @CANDIDATES[2] and gate_key == 'required_scoring_inputs_finite'"
    )["gate_pass"].item()
    weighted = first.inventory_candidate_weighted_scores
    ma3_warm = weighted.query("candidate_policy_id == @CANDIDATES[0] and metric_key == 'warmup_coverage_retention'")["normalized_score"].item()
    ma6_vol = weighted.query("candidate_policy_id == @CANDIDATES[1] and metric_key == 'volatility_reduction'")["normalized_score"].item()
    assert ma3_warm == 1.0 and ma6_vol == 1.0  # higher- and lower-is-better
    for candidate in CANDIDATES[:2]:
        rows = weighted[weighted["candidate_policy_id"].eq(candidate)]
        total = ranking.loc[ranking["candidate_policy_id"].eq(candidate), "total_score"].item()
        assert np.isclose(rows["weighted_score"].sum(), total)

    tied_evidence = _evidence(equal_metrics=True)
    tied = score_inventory_candidates(campaign=tied_evidence.campaign, phase_a_evidence=tied_evidence)
    assert tied.inventory_candidate_weighted_scores["normalized_score"].eq(0.5).all()
    assert tied.inventory_candidate_ranking.loc[0, "recommendation_state"] == "recommended"
    assert tied.inventory_candidate_ranking["rank"].tolist() == [1, 2, 3, 4]
    totals = {candidate: 0.5 for candidate in CANDIDATES}
    normalized = {(candidate, metric): 0.5 for candidate in CANDIDATES
                  for metric in ("trend_shape_preservation", "warmup_coverage_retention")}
    assert _ranking_comparison(CANDIDATES[0], CANDIDATES[0], totals=totals,
                               normalized_scores=normalized, candidates=CANDIDATES)[0] == 0
    assert tied.inventory_candidate_ranking.loc[0, "tie_break_reason"] == "canonical_shorter_window_tiebreak"

    none_evidence = _evidence(all_ineligible=True)
    none = score_inventory_candidates(campaign=none_evidence.campaign, phase_a_evidence=none_evidence)
    assert none.inventory_campaign_recommendation["recommendation_status"].item() == "no_recommendation"
    assert none.inventory_candidate_ranking["recommendation_state"].eq("ineligible").all()

    broken = deepcopy(policy.metrics)
    broken.loc[broken["metric_key"].eq("volatility_reduction"), "source_column"] = "not_present"
    try:
        score_inventory_candidates(campaign=tied_evidence.campaign, phase_a_evidence=tied_evidence,
                                   scoring_policy=InventoryScoringPolicy(broken))
    except ValueError as exc:
        assert "source" in str(exc).lower()
    else:
        raise AssertionError("Missing configured evidence must fail closed")

    for field, value in (("campaign_id", "different"), ("campaign_version", "2.0"),
                         ("baseline_run_id", "different"), ("incumbent_run_id", "different")):
        mismatched = replace(tied_evidence.campaign, **{field: value})
        try:
            score_inventory_candidates(campaign=mismatched, phase_a_evidence=tied_evidence)
        except ValueError as exc:
            assert "identity mismatch" in str(exc)
        else:
            raise AssertionError(f"Campaign mismatch must fail closed: {field}")

    _assert_calendar_failure(lambda frame: frame.drop(
        frame[(frame["candidate_policy_id"].eq(CANDIDATES[0])) &
              (frame["feature_component"].eq("level")) & frame["calendar_month"].eq(12)].index,
        inplace=True))
    def duplicate_month(frame: pd.DataFrame) -> None:
        index = frame[(frame["candidate_policy_id"].eq(CANDIDATES[0])) &
                      (frame["feature_component"].eq("level")) & frame["calendar_month"].eq(12)].index[0]
        frame.loc[index, "calendar_month"] = 11
    _assert_calendar_failure(duplicate_month)
    def outside_month(frame: pd.DataFrame) -> None:
        index = frame[(frame["candidate_policy_id"].eq(CANDIDATES[0])) &
                      (frame["feature_component"].eq("level"))].index[0]
        frame.loc[index, "calendar_month"] = 13
    _assert_calendar_failure(outside_month)
    def infinite_seasonality(frame: pd.DataFrame) -> None:
        index = frame[(frame["candidate_policy_id"].eq(CANDIDATES[0])) &
                      (frame["feature_component"].eq("level"))].index[0]
        frame.loc[index, "mean_absolute_monthly_change"] = np.inf
    _assert_calendar_failure(infinite_seasonality)

    for malformed in (np.nan, "False", "yes", 1):
        parity_evidence = _evidence()
        parity = _table(parity_evidence, "inventory_candidate_non_target_parity")
        parity["parity_pass"] = parity["parity_pass"].astype(object)
        parity.loc[parity["candidate_policy_id"].eq(CANDIDATES[0]), "parity_pass"] = malformed
        parsed = score_inventory_candidates(campaign=parity_evidence.campaign,
                                            phase_a_evidence=parity_evidence)
        gate = parsed.inventory_candidate_eligibility.query(
            "candidate_policy_id == @CANDIDATES[0] and gate_key == 'non_target_parity'")
        assert not gate["gate_pass"].item()
    boolean_evidence = _evidence()
    assert score_inventory_candidates(
        campaign=boolean_evidence.campaign, phase_a_evidence=boolean_evidence
    ).inventory_candidate_ranking["eligible"].all()

    # Scoring existing evidence must have no materialization/normalization/orchestration dependency.
    import regime._02_feature_normalizer as normalizer
    import regime.experiments.in_memory_challenger as challenger
    import regime.review.calibration as calibration
    import regime.review.calibration.inventory_campaign as inventory_campaign
    original = (normalizer.normalize_features, challenger.normalize_features,
                inventory_campaign.build_in_memory_smoothing_challenger,
                inventory_campaign.run_phase_a_foundation_evidence,
                calibration.run_phase_a_foundation_evidence)
    def forbidden(*args, **kwargs):
        raise AssertionError("scoring called a forbidden materialization dependency")
    try:
        normalizer.normalize_features = forbidden
        challenger.normalize_features = forbidden
        inventory_campaign.build_in_memory_smoothing_challenger = forbidden
        inventory_campaign.run_phase_a_foundation_evidence = forbidden
        calibration.run_phase_a_foundation_evidence = forbidden
        score_inventory_candidates(campaign=tied_evidence.campaign, phase_a_evidence=tied_evidence)
    finally:
        (normalizer.normalize_features, challenger.normalize_features,
         inventory_campaign.build_in_memory_smoothing_challenger,
         inventory_campaign.run_phase_a_foundation_evidence,
         calibration.run_phase_a_foundation_evidence) = original

    assert first.inventory_candidate_eligibility.columns.tolist() == [
        "candidate_policy_id", "gate_key", "gate_value", "gate_threshold", "gate_pass", "failure_reason"]
    assert first.inventory_candidate_metric_scores.columns.tolist() == [
        "candidate_policy_id", "metric_key", "feature_component", "raw_value",
        "raw_numerator", "raw_denominator", "warmup_rows", "source_table", "source_column", "aggregation",
        "direction", "weight", "eligible"]
    assert first.inventory_candidate_weighted_scores.columns.tolist() == [
        "candidate_policy_id", "metric_key", "raw_value", "normalized_score", "weight", "weighted_score", "eligible"]
    assert len(first.inventory_candidate_eligibility) == len(CANDIDATES) * first.inventory_candidate_eligibility["gate_key"].nunique()
    assert len(first.inventory_candidate_metric_scores) == 60
    assert len(first.inventory_candidate_weighted_scores) == 20
    assert len(first.inventory_candidate_ranking) == 4
    assert len(first.inventory_campaign_recommendation) == 1
    assert not first.inventory_candidate_metric_scores.duplicated(
        ["candidate_policy_id", "metric_key", "feature_component"]).any()
    assert not first.inventory_candidate_weighted_scores.duplicated(
        ["candidate_policy_id", "metric_key"]).any()
    print("SMOKE TEST 83 — INVENTORY CANDIDATE SCORING: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
