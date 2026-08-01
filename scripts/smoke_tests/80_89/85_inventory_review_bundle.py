"""Smoke Test 85: fast deterministic inventory human-review bundle."""

from __future__ import annotations

import copy
import hashlib
import runpy
import zipfile
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

import numpy as np
import pandas as pd

import regime.review.calibration.inventory_campaign as upstream
import regime.review.calibration.inventory_candidate_scoring as scoring_module
from regime.review.calibration import score_inventory_candidates
from regime.review.calibration.inventory_review_bundle import build_inventory_review_bundle
from regime.review.results import ReviewResult
from regime.review.calibration.system_evidence import CalibrationSystemEvidence, SYSTEM_SECTIONS
from regime.review.calibration.system_evidence import NORMALIZED_METRIC_SECTION
from regime.review.calibration.engine_decomposition import DECOMPOSITION_SECTIONS, EngineDecompositionEvidence


def _expect_error(call, error=ValueError):
    try:
        call()
    except error:
        return
    raise AssertionError(f"Expected {error.__name__}")


def _system_evidence(evidence):
    candidates = evidence.campaign.candidate_policy_ids
    rows = []
    for series_number, series_id in enumerate(("baseline", *candidates)):
        for number, date in enumerate(pd.date_range("2020-01-01", periods=8, freq="MS")):
            rows.append({"campaign_id": evidence.campaign.campaign_id,
                         "campaign_version": evidence.campaign.campaign_version,
                         "series_id": series_id, "geo_id": "fixture__county",
                         "date": date, "dimension_score": number / 10 + series_number / 100,
                         "axis_score": number / 10 + series_number / 100,
                         "x_supply": number / 10, "y_demand": .2,
                         "supply_pressure_score": number / 10,
                         "demand_strength_score": .2, "major_regime": "recovery",
                         "minor_regime": "early_recovery", "window_id": "largest_divergence",
                         "quadrant": "high_supply_high_demand",
                         "window_center_date": pd.Timestamp("2020-04-01"),
                         "metric_score": number / 10 + series_number / 100,
                         "dimension_cancellation_ratio": series_number / 10})
    base = pd.DataFrame(rows)
    return CalibrationSystemEvidence(
        campaign_id=evidence.campaign.campaign_id, campaign_version=evidence.campaign.campaign_version,
        candidate_policy_ids=candidates, incumbent_policy_id=evidence.campaign.incumbent_policy_id,
        baseline_policy_id=evidence.campaign.baseline_policy_id, target_metric=evidence.campaign.target_metric,
        target_dimension=evidence.campaign.target_dimension, target_axis=evidence.campaign.target_axis,
        tables={name: base.copy() for name in (*SYSTEM_SECTIONS, NORMALIZED_METRIC_SECTION)},
        representative_geography_rule="all fixture counties ordered by geo_id",
        transition_window_rule="largest candidate/incumbent divergence; stable date tie-break")

def _decomposition_evidence(evidence):
    identity_rows = [{"campaign_id": evidence.campaign.campaign_id,
                      "campaign_version": evidence.campaign.campaign_version,
                      "series_id": series_id, "geo_id": "fixture__county"}
                     for series_id in ("baseline", *evidence.campaign.candidate_policy_ids)]
    date = pd.Timestamp("2020-01-01")
    recon = {"reconciliation_status": "reconciled", "reconciliation_pass": True,
             "reason_code": "none", "reason": "fixture reconciled", "parent_score": .1,
             "summed_contributions": .1, "absolute_residual": 0., "relative_residual": 0.,
             "tolerance": 1e-10}
    availability = {"configured_weight": 1., "available": True,
                    "availability_reason_code": "available", "availability_reason": "fixture available",
                    "available_child_count": 1, "available_weight_sum": 1., "effective_weight": 1.,
                    "weighted_contribution": .1}
    f2m, m2d, d2a, coverage, summary, coordinates, regimes = [], [], [], [], [], [], []
    for identity in identity_rows:
        f2m.append({**identity, "date": date, "canonical_metric_key": "active_inventory",
                    "feature_key": "redfin_inventory_level", "feature_type": "level",
                    "feature_score": .1, **{**availability, "configured_weight": .25, "available_weight_sum": .25}, **recon})
        f2m.append({**identity, "date": date, "canonical_metric_key": "permit_activity",
                    "feature_key": "bps_total_units_short", "feature_type": "short_term_change",
                    "feature_score": .1, **{**availability, "configured_weight": .35, "available_weight_sum": .35}, **recon})
        m2d.append({**identity, "date": date, "dimension": "supply",
                    "canonical_metric_key": "active_inventory", "metric_score": .1,
                    **{**availability, "configured_weight": .3334, "available_weight_sum": .3334}, **recon})
        m2d.append({**identity, "date": date, "dimension": "demand",
                    "canonical_metric_key": "employment", "metric_score": .1,
                    **{**availability, "configured_weight": .1667, "available_weight_sum": .1667}, **recon})
        d2a.append({**identity, "date": date, "axis": "supply", "dimension": "supply",
                    "dimension_score": .1, **{**availability, "configured_weight": .85, "available_weight_sum": .85}, **recon})
        d2a.append({**identity, "date": date, "axis": "demand", "dimension": "demand",
                    "dimension_score": .1, **{**availability, "configured_weight": .65, "available_weight_sum": .65}, **recon})
        for layer, parent in (("feature_to_metric", "active_inventory"),
                              ("metric_to_dimension", "supply"), ("dimension_to_axis", "supply")):
            coverage.append({**identity, "layer": layer, "parent_key": parent,
                             "expected_child_count": 1, "evaluation_universe_start": date,
                             "evaluation_universe_end": date, "first_source_observation_date": date,
                             "first_valid_child_date": date,
                             "first_valid_parent_date": date, "first_fully_reconciled_date": date,
                             "source_present_child_unavailable_rows": 0,
                             "child_available_parent_absent_rows": 0,
                             "partially_available_parent_date_count": 0,
                             "one_child_only_parent_date_count": 1,
                             "zero_available_child_dates": 0,
                             "zero_available_weight_parent_rows": 0, "warmup_rows": 0,
                             "fully_reconciled_row_count": 1})
            summary.append({**identity, "date": date, "layer": layer, "parent_key": parent, **recon})
        coordinates.append({**identity, "date": date, "supply_axis_score": .1, "x_supply": .1,
                            "demand_axis_score": .2, "y_demand": .2, "supply_residual": 0.,
                            "demand_residual": 0., **recon})
        regimes.append({**identity, "date": date, "major_regime_expected": "expansion",
                        "major_regime_actual": "expansion", "minor_regime_expected": "mid_expansion",
                        "minor_regime_actual": "mid_expansion", "quadrant_expected": 1,
                        "quadrant_actual": 1, **recon})
    tables = {"feature_to_metric": pd.DataFrame(f2m), "metric_to_dimension": pd.DataFrame(m2d),
              "dimension_to_axis": pd.DataFrame(d2a), "chronology_coverage": pd.DataFrame(coverage),
              "reconciliation_summary": pd.DataFrame(summary),
              "coordinate_reconciliation": pd.DataFrame(coordinates),
              "regime_reconciliation": pd.DataFrame(regimes)}
    return EngineDecompositionEvidence(evidence.campaign.campaign_id, evidence.campaign.campaign_version,
                                       evidence.campaign.candidate_policy_ids, tables)

def main() -> int:
    fixture = runpy.run_path("scripts/smoke_tests/80_89/83_inventory_candidate_scoring.py")
    evidence = fixture["_evidence"]()
    candidates = evidence.campaign.candidate_policy_ids
    dates = pd.date_range("2020-01-01", periods=24, freq="MS")
    series = []
    for series_number, series_id in enumerate(("baseline", *candidates)):
        for component_number, component in enumerate(upstream.FEATURE_COMPONENTS):
            for number, date in enumerate(dates):
                series.append({
                    "series_id": series_id,
                    "candidate_policy_id": pd.NA if series_id == "baseline" else series_id,
                    "is_baseline": series_id == "baseline", "geo_id": "fixture__county",
                    "date": date, "feature_component": component,
                    "feature_key": upstream.FEATURE_KEY_BY_COMPONENT[component],
                    "raw_feature_value": float(100 + component_number * 10 + np.sin(number / 2) * (5 - series_number / 2) + number),
                })
    windows = [{"geo_id": "fixture__county", "feature_component": component,
                "window_id": "largest_absolute_baseline_change", "selection_rule": "fixture immutable rule",
                "center_date": dates[10], "window_start": dates[7], "window_end": dates[13]}
               for component in upstream.FEATURE_COMPONENTS]
    evidence.evidence_results["series"] = ReviewResult(tables={
        "inventory_candidate_feature_series": pd.DataFrame(series),
        "inventory_transition_review_windows": pd.DataFrame(windows),
    })
    evidence.evidence_results["geography_scope"] = ReviewResult(tables={
        "inventory_campaign_geography_scope": pd.DataFrame([{
            "campaign_id": evidence.campaign.campaign_id,
            "campaign_version": evidence.campaign.campaign_version,
            "geo_id": "fixture__county", "geo_level": "county", "included": True,
            "inclusion_reason": "all_authoritative_counties", "exclusion_reason": None,
            "metadata_source": upstream.GEO_METADATA_SOURCE,
        }])
    })
    result = score_inventory_candidates(campaign=evidence.campaign, phase_a_evidence=evidence)
    system = _system_evidence(evidence)
    evidence = replace(evidence, decomposition_evidence=_decomposition_evidence(evidence))

    forbidden = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("forbidden upstream call"))
    originals = (upstream.materialize_phase_a_challengers, upstream.run_phase_a_foundation_evidence,
                 upstream.build_in_memory_smoothing_challenger, scoring_module.score_inventory_candidates)
    upstream.materialize_phase_a_challengers = forbidden
    upstream.run_phase_a_foundation_evidence = forbidden
    upstream.build_in_memory_smoothing_challenger = forbidden
    scoring_module.score_inventory_candidates = forbidden
    try:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = build_inventory_review_bundle(campaign=evidence.campaign, phase_a_evidence=evidence,
                                                  scoring_result=result, system_evidence=system, output_root=root / "one")
            second = build_inventory_review_bundle(campaign=evidence.campaign, phase_a_evidence=evidence,
                                                   scoring_result=result, system_evidence=system, output_root=root / "two")
            relative_first = [path.relative_to(first.bundle_directory).as_posix() for path in first.generated_files]
            relative_second = [path.relative_to(second.bundle_directory).as_posix() for path in second.generated_files]
            assert relative_first == relative_second
            assert (first.bundle_directory / "README.md").is_file()
            assert (first.bundle_directory / "review_summary.html").is_file()
            assert (first.bundle_directory / "manifest.json").is_file() and first.zip_path.is_file()
            assert all((first.bundle_directory / name).is_dir() for name in ("tables", "figures", "metadata"))
            assert (first.bundle_directory / "technical_evidence").is_dir()
            assert all((first.bundle_directory / "system_evidence" / name).is_dir() for name in SYSTEM_SECTIONS)
            assert first.zip_path.read_bytes() == second.zip_path.read_bytes()
            assert first.manifest["flags"]["promotion_performed"] is False
            assert first.manifest["recommendation_status"] == "recommended_for_human_review"
            page = (first.bundle_directory / "review_summary.html").read_text(encoding="utf-8")
            headings = ["1. Executive Summary", "2. Technical Evidence", "2.1 Raw Metric Chronology",
                        "2.2 Normalized Metric-Score Chronology", "2.3 Candidate Calibration-Score Decomposition",
                        "3. Engine Decomposition", "3.1 Active Inventory — Feature-to-Metric",
                        "3.2 Building Permits (BPS / permit_activity) — Feature-to-Metric", "3.3 Supply — Metric-to-Dimension",
                        "3.4 Supply Axis — Dimension-to-Axis", "3.5 Coverage and Start-Date Explanation",
                        "3.6 Reconciliation Summary", "3.7 Additional Supporting Decompositions",
                        "4.1 Supply Dimension Chronology", "4.2 Supply Axis Chronology",
                        "4.3 Supply–Demand Coordinate Trajectory", "4.4 Regime Chronology",
                        "4.5 Transition Windows", "4.6 Cancellation Diagnostics",
                        "5. Supporting Tables and Artifact Links", "6. Human Decision Status"]
            positions = [page.index(heading) for heading in headings]
            assert positions == sorted(positions)
            assert "x_supply" in page and "y_demand" in page and "major_regime" in page
            for context in ("Engine Decomposition</a>", system.transition_window_rule,
                            "Center event:", "full-history anchors", "start/end markers",
                            "minor_regime", "quadrant", "gross_dimension_contribution_change_1m",
                            "Interpretation of 0:", "Interpretation of 1:",
                            "metadata/system_evidence_selection.json", "metadata/source_lineage.json"):
                assert context in page
            assert "dimension_cancellation_ratio" not in page or "Supply-Axis Contribution Cancellation" in page
            assert "2020-04-01" in page or "Center event" in page
            assert page.count("<img ") >= len(SYSTEM_SECTIONS) + 3
            expected_parent_figures = (
                "fixture__county__active_inventory__feature_to_metric.png",
                "fixture__county__permit_activity__feature_to_metric.png",
                "fixture__county__supply__metric_to_dimension.png",
                "fixture__county__demand__metric_to_dimension.png",
                "fixture__county__supply__dimension_to_axis.png",
                "fixture__county__demand__dimension_to_axis.png",
            )
            for figure in expected_parent_figures:
                assert figure in page
                assert any(path.name == figure for path in first.generated_files)
            for geo_id in system.tables[SYSTEM_SECTIONS[0]]["geo_id"].unique():
                assert str(geo_id) in page
            required_tables = set(result.tables) | set(upstream_name for upstream_name in (
                "inventory_candidate_feature_coverage", "inventory_candidate_calendar_month_behavior",
                "inventory_candidate_feature_statistics", "inventory_candidate_baseline_feature_comparison",
                "inventory_candidate_target_replacement", "inventory_candidate_non_target_parity",
                "inventory_campaign_geography_scope"))
            assert required_tables == {path.stem for path in (first.bundle_directory / "tables").glob("*.csv")}
            paths = [item["relative_path"] for item in first.manifest["files"]]
            assert paths == sorted(paths) and len(paths) == len(set(paths))
            assert first.manifest["regime_scope"] == "macro"
            assert first.manifest["included_geo_levels"] == ["county"]
            assert first.manifest["zip_future_status"] == "reserved_for_future_local_regime"
            assert first.manifest["city_status"] == "out_of_scope_no_current_regime_role"
            for item in first.manifest["files"]:
                path = first.bundle_directory / item["relative_path"]
                assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]
            with zipfile.ZipFile(first.zip_path) as archive:
                names = set(archive.namelist())
                assert all(f"{first.bundle_directory.name}/{path.relative_to(first.bundle_directory).as_posix()}" in names for path in first.generated_files)
            _expect_error(lambda: build_inventory_review_bundle(campaign=evidence.campaign, phase_a_evidence=evidence,
                                                                scoring_result=result, system_evidence=system, output_root=root / "one"), FileExistsError)
            build_inventory_review_bundle(campaign=evidence.campaign, phase_a_evidence=evidence,
                                          scoring_result=result, system_evidence=system, output_root=root / "one", overwrite=True)
            malformed = copy.deepcopy(evidence)
            malformed.evidence_results["series"].tables.pop("inventory_candidate_feature_series")
            _expect_error(lambda: build_inventory_review_bundle(campaign=malformed.campaign,
                                                                phase_a_evidence=malformed, scoring_result=result, system_evidence=system,
                                                                output_root=root / "bad"))
            bad_result = copy.deepcopy(result)
            bad_result.inventory_campaign_recommendation.loc[0, "recommended_candidate_policy_id"] = candidates[-1]
            _expect_error(lambda: build_inventory_review_bundle(campaign=evidence.campaign,
                                                                phase_a_evidence=evidence, scoring_result=bad_result, system_evidence=system,
                                                                output_root=root / "bad2"))
    finally:
        (upstream.materialize_phase_a_challengers, upstream.run_phase_a_foundation_evidence,
         upstream.build_in_memory_smoothing_challenger, scoring_module.score_inventory_candidates) = originals
    print("SMOKE TEST 85 — INVENTORY REVIEW BUNDLE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
