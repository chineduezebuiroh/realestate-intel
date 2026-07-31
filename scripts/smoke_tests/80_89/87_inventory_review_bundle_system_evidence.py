"""Smoke Test 87: system-evidence contract fails closed and stays deterministic."""

from __future__ import annotations

import copy
import runpy

import pandas as pd

from regime.review.calibration.system_evidence import (
    NORMALIZED_METRIC_SECTION, SYSTEM_SECTIONS, adapt_aligned_metric_scores,
    adapt_axis_scores, validate_system_evidence,
)


def _expect_error(call) -> None:
    try:
        call()
    except ValueError:
        return
    raise AssertionError("Expected ValueError")


def main() -> int:
    aligned = pd.DataFrame({
        "geo_id": ["b", "a"], "evaluation_date": ["2020-02-01", "2020-01-01"],
        "metric_date": ["2020-01-15", "2019-12-15"], "metric_age_days": [17, 17],
        "canonical_metric_key": ["active_inventory"] * 2, "metric_score": [.2, .1],
    })
    adapted = adapt_aligned_metric_scores(aligned)
    assert adapted["geo_id"].tolist() == ["a", "b"]
    assert adapted["date"].equals(adapted["evaluation_date"])
    assert list(adapted["metric_date"].dt.strftime("%Y-%m-%d")) == ["2019-12-15", "2020-01-15"]
    canonical = aligned.rename(columns={"evaluation_date": "date"})
    assert "date" in adapt_aligned_metric_scores(canonical)
    both = aligned.assign(date=pd.to_datetime(aligned["evaluation_date"]))
    assert adapt_aligned_metric_scores(both)["date"].equals(adapt_aligned_metric_scores(both)["evaluation_date"])
    conflicting = both.copy(); conflicting.loc[0, "date"] = "2021-01-01"
    _expect_error(lambda: adapt_aligned_metric_scores(conflicting))
    _expect_error(lambda: adapt_aligned_metric_scores(aligned.drop(columns="evaluation_date")))
    invalid = aligned.copy(); invalid.loc[0, "evaluation_date"] = "not-a-date"
    _expect_error(lambda: adapt_aligned_metric_scores(invalid))
    axis = pd.DataFrame({"geo_id": ["a"], "date": ["2020-01-01"], "axis": ["supply"], "axis_score": [.2]})
    assert adapt_axis_scores(axis)["date"].notna().all()

    fixture = runpy.run_path("scripts/smoke_tests/80_89/85_inventory_review_bundle.py")
    evidence = fixture["_evidence"]() if "_evidence" in fixture else None
    # Test 85 imports the scoring fixture's factory into its module namespace.
    if evidence is None:
        scoring_fixture = runpy.run_path("scripts/smoke_tests/80_89/83_inventory_candidate_scoring.py")
        evidence = scoring_fixture["_evidence"]()
    system = fixture["_system_evidence"](evidence)
    validate_system_evidence(system)
    assert tuple(system.tables) == (*SYSTEM_SECTIONS, NORMALIZED_METRIC_SECTION)
    assert list(system.tables) == list(copy.deepcopy(system).tables)

    missing = copy.deepcopy(system)
    missing.tables.pop("regime_chronology")
    _expect_error(lambda: validate_system_evidence(missing))

    mismatch = copy.deepcopy(system)
    mismatch.tables["axis_chronology"].loc[:, "campaign_id"] = "conflicting_campaign"
    _expect_error(lambda: validate_system_evidence(mismatch))

    one_dimensional = copy.deepcopy(system)
    one_dimensional.tables["coordinate_trajectories"] = one_dimensional.tables["coordinate_trajectories"].drop(columns="y_demand")
    _expect_error(lambda: validate_system_evidence(one_dimensional))

    continuous_regime = copy.deepcopy(system)
    continuous_regime.tables["regime_chronology"] = continuous_regime.tables["regime_chronology"].drop(columns="major_regime")
    _expect_error(lambda: validate_system_evidence(continuous_regime))

    candidates = set(evidence.campaign.candidate_policy_ids)
    for frame in system.tables.values():
        assert set(frame["series_id"]) == {"baseline", *candidates}
    print("SMOKE TEST 87 — INVENTORY SYSTEM EVIDENCE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
