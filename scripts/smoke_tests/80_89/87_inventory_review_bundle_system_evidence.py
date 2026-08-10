"""Smoke Test 87: system-evidence contract fails closed and stays deterministic."""

from __future__ import annotations

import copy
import runpy

import pandas as pd

from regime.review.calibration.system_evidence import (
    NORMALIZED_METRIC_SECTION, SYSTEM_SECTIONS, adapt_aligned_metric_scores,
    adapt_axis_scores, validate_system_evidence, _governed_axis_scope,
    _validate_transition_metric_uniqueness, PREFERRED_REVIEW_GEOGRAPHIES,
    select_representative_geographies,
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
    assert _governed_axis_scope(evidence.campaign) == ("supply", "demand")
    custom = copy.deepcopy(evidence.campaign)
    object.__setattr__(custom, "supporting_coordinate_axes", ("supply", "other_supporting"))
    assert _governed_axis_scope(custom) == ("supply", "other_supporting")
    transition_metrics = pd.DataFrame({"geo_id": ["g"], "date": [pd.Timestamp("2020-01-01")],
                                       "series_id": ["baseline"], "metric_score": [.1]})
    _validate_transition_metric_uniqueness(transition_metrics)
    _expect_error(lambda: _validate_transition_metric_uniqueness(pd.concat(
        [transition_metrics, transition_metrics], ignore_index=True)))
    validate_system_evidence(system)

    preferred_rows = []
    for number, (geo_id, state, _reason) in enumerate(PREFERRED_REVIEW_GEOGRAPHIES):
        preferred_rows.append({"geo_id": geo_id, "state": state, "valid_review_evidence": True,
                               "invalid_evidence_reason": "", "share_supply_dates_with_all_three": .8,
                               "fully_populated_supply_observation_count": 20,
                               "permit_activity_observation_count": 30 + number,
                               "permit_intensity_observation_count": 30,
                               "active_inventory_observation_count": 40})
    diagnostic = pd.DataFrame(preferred_rows)
    selection = select_representative_geographies(diagnostic.sample(frac=1, random_state=7))
    permuted = select_representative_geographies(diagnostic.sample(frac=1, random_state=11))
    assert selection.to_dict("records") == permuted.to_dict("records")
    assert selection["final_selected_geo_id"].tolist() == [row[0] for row in PREFERRED_REVIEW_GEOGRAPHIES]
    assert "prince_george_s_county_md__county" in set(selection["preferred_geo_id"])
    assert {"district_of_columbia_dc__county", "essex_county_nj__county",
            "san_francisco_county_ca__county", "los_angeles_county_ca__county"}.issubset(
                set(selection["final_selected_geo_id"]))
    fallback_input = diagnostic[diagnostic["geo_id"].ne("essex_county_nj__county")].copy()
    fallback_input.loc[len(fallback_input)] = {
        "geo_id": "fallback_nj__county", "state": "NJ", "valid_review_evidence": True,
        "invalid_evidence_reason": "", "share_supply_dates_with_all_three": .7,
        "fully_populated_supply_observation_count": 15, "permit_activity_observation_count": 20,
        "permit_intensity_observation_count": 20, "active_inventory_observation_count": 20,
    }
    fallback = select_representative_geographies(fallback_input)
    essex = fallback[fallback["preferred_geo_id"].eq("essex_county_nj__county")].iloc[0]
    assert essex["final_selected_geo_id"] == "fallback_nj__county"
    assert essex["selection_role"] == "fallback" and "same-state" in essex["fallback_reason"]

    # Supply and Demand are distinct governed axis rows and may coexist at
    # the same campaign/series/geography/date without being duplicates.
    multi_axis = copy.deepcopy(system)
    axis_frame = multi_axis.tables["axis_chronology"]
    shared_keys = ["campaign_id", "campaign_version", "series_id", "geo_id", "date"]
    shared_group = (
        axis_frame.groupby(shared_keys, dropna=False, sort=False)["axis"]
        .nunique()
    )
    assert shared_group.ge(2).any()
    validate_system_evidence(multi_axis)

    # A repeated row within the same axis identity remains a true duplicate
    # and must fail closed.
    duplicate_axis = copy.deepcopy(system)
    duplicate_axis.tables["axis_chronology"] = pd.concat(
        [
            duplicate_axis.tables["axis_chronology"],
            duplicate_axis.tables["axis_chronology"].iloc[[0]].copy(deep=True),
        ],
        ignore_index=True,
    )
    _expect_error(lambda: validate_system_evidence(duplicate_axis))

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
