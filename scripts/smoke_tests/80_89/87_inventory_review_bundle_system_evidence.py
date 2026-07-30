"""Smoke Test 87: system-evidence contract fails closed and stays deterministic."""

from __future__ import annotations

import copy
import runpy

from regime.review.calibration.system_evidence import SYSTEM_SECTIONS, validate_system_evidence


def _expect_error(call) -> None:
    try:
        call()
    except ValueError:
        return
    raise AssertionError("Expected ValueError")


def main() -> int:
    fixture = runpy.run_path("scripts/smoke_tests/80_89/85_inventory_review_bundle.py")
    evidence = fixture["_evidence"]() if "_evidence" in fixture else None
    # Test 85 imports the scoring fixture's factory into its module namespace.
    if evidence is None:
        scoring_fixture = runpy.run_path("scripts/smoke_tests/80_89/83_inventory_candidate_scoring.py")
        evidence = scoring_fixture["_evidence"]()
    system = fixture["_system_evidence"](evidence)
    validate_system_evidence(system)
    assert tuple(system.tables) == SYSTEM_SECTIONS
    assert list(system.tables) == list(copy.deepcopy(system).tables)

    missing = copy.deepcopy(system)
    missing.tables.pop("regime_chronology")
    _expect_error(lambda: validate_system_evidence(missing))

    mismatch = copy.deepcopy(system)
    mismatch.tables["axis_chronology"].loc[:, "campaign_id"] = "conflicting_campaign"
    _expect_error(lambda: validate_system_evidence(mismatch))

    candidates = set(evidence.campaign.candidate_policy_ids)
    for frame in system.tables.values():
        assert set(frame["series_id"]) == {"baseline", *candidates}
    print("SMOKE TEST 87 — INVENTORY SYSTEM EVIDENCE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
