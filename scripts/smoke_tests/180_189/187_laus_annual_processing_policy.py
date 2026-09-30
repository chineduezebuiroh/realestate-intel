"""Smoke 187: LAUS-C1 annual detector, scope selection, and satisfaction CAS."""
from __future__ import annotations

import copy
import json
from pathlib import Path

from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.laus_annual_processing import (
    AnnualState, add_satisfaction, classify_governed_processing_classes, evaluate,
    satisfaction_record,
)
from sources.bls_laus.artifact import _classify_observation, load_registry, revision_bounds


def expect(error, operation):
    try:
        operation()
    except error:
        return
    raise AssertionError(f"expected {error.__name__}")


def publication(year, class_statuses):
    base = {"evidence_type": "official_bls_publication",
            "authoritative_url": f"https://www.bls.gov/lau/notices/{year}.htm",
            "annual_reference_year": year}
    return {"schema_version": "laus_annual_processing_evidence_v1",
            "annual_reference_year": year,
            "release_event": {**base, "expected": True,
                              "provider_release_id": f"laus-notice-{year}-annual-start"},
            "processing_classes": [
                {**base, "processing_class": name, "status": status,
                 "provider_release_id": f"laus-notice-{year}-{name}-{status}"}
                for name, status in class_statuses.items()]}


def main():
    policy = json.loads(Path("config/laus_annual_processing_policy_v1.json").read_text())
    groups = classify_governed_processing_classes(load_registry(), policy)
    assert {key: len(value) for key, value in groups.items()} == {
        "model_based_state": 20, "substate_nonmodeled": 800}

    none = evaluate(policy=policy, evidence=None, numeric_changes=True)
    assert (none.state, none.acquisition_mode) == (AnnualState.NOT_EXPECTED, "ordinary_overlap")

    begun = evaluate(policy=policy, evidence=publication(2026, {}))
    assert (begun.state, begun.acquisition_mode) == (AnnualState.WATCHING, "ordinary_overlap")
    state_only = evaluate(policy=policy, evidence=publication(2026, {
        "model_based_state": "complete", "substate_nonmodeled": "underway"}))
    assert state_only.state == AnnualState.WATCHING

    ready_evidence = publication(2026, {"model_based_state": "complete",
                                                 "substate_nonmodeled": "complete"})
    ready = evaluate(policy=policy, evidence=ready_evidence, numeric_changes=False)
    assert (ready.state, ready.acquisition_mode) == (AnnualState.READY_FOR_ANNUAL_DEEP, "annual_deep")
    assert revision_bounds(ready.acquisition_mode, 2026) == (1976, 2026)
    # Failure produces no record, so the exact next evaluation remains eligible.
    assert evaluate(policy=policy, evidence=ready_evidence, satisfactions=[]).state == AnnualState.READY_FOR_ANNUAL_DEEP

    result = {"status": "succeeded", "validation_status": "passed",
              "publication_state": "published_verified", "source_id": "laus",
              "acquisition_mode": "annual_deep", "candidate_artifact_id": "src__laus__candidate",
              "artifact_content_hash": "a" * 64, "package_sha256": "b" * 64,
              "provider_release_id": "laus-annual-current:" + "c" * 64}
    record = satisfaction_record(decision=ready, result=result, cycle_id="monthly_cycle__fixture")
    stored, changed = add_satisfaction(None, record)
    assert changed and add_satisfaction(stored, record) == (stored, False)
    conflict = copy.deepcopy(record); conflict["satisfied_package_sha256"] = "d" * 64
    expect(IdentityCollisionError, lambda: add_satisfaction(stored, conflict))
    satisfied = evaluate(policy=policy, evidence=ready_evidence, satisfactions=[stored])
    assert (satisfied.state, satisfied.acquisition_mode) == (AnnualState.ANNUAL_DEEP_SATISFIED, "ordinary_overlap")

    next_year = evaluate(policy=policy, evidence=publication(2027, {
        "model_based_state": "complete", "substate_nonmodeled": "underway"}), satisfactions=[stored])
    assert next_year.state == AnnualState.WATCHING

    # Numeric changes alone do not authorize deep; unchanged values do not block official completion.
    assert evaluate(policy=policy, evidence=None, numeric_changes=True).state == AnnualState.NOT_EXPECTED
    assert evaluate(policy=policy, evidence=ready_evidence, numeric_changes=False).state == AnnualState.READY_FOR_ANNUAL_DEEP
    status, value, rendered, codes = _classify_observation(
        {"value": "-", "footnotes": [{"code": "X"}, {"code": "R"}]})
    assert (status, value, rendered, codes) == ("provider_unavailable", None, None, ("R", "X"))
    assert evaluate(policy=policy, evidence=None, observation_footnote_codes=codes).state == AnnualState.NOT_EXPECTED

    contradictory = publication(2026, {"model_based_state": "complete"})
    contradictory["processing_classes"][0]["annual_reference_year"] = 2025
    expect(ValueError, lambda: evaluate(policy=policy, evidence=contradictory))
    untrusted = publication(2026, {"model_based_state": "complete", "substate_nonmodeled": "complete"})
    untrusted["processing_classes"][0]["authoritative_url"] = "https://example.com/revision"
    expect(ValueError, lambda: evaluate(policy=policy, evidence=untrusted))
    print("Smoke 187 passed: LAUS annual processing is class-complete, idempotent, and fail closed.")


if __name__ == "__main__":
    main()
