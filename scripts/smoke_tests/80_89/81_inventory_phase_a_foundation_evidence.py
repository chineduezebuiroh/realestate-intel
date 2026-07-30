"""Smoke Test 81: comprehensive deterministic Phase A evidence contracts."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Callable

import numpy as np
import pandas as pd

import regime.review.calibration.inventory_campaign as inventory


CANDIDATES = tuple(inventory.PHASE_A_CANDIDATES.values())
COMPONENTS = inventory.FEATURE_COMPONENTS


def _expect_error(function: Callable[[], object], error: type[Exception] = ValueError) -> None:
    try:
        function()
    except error:
        return
    raise AssertionError(f"Expected {error.__name__}")


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.date_range("2020-01-01", periods=6, freq="MS")
    rows: list[dict[str, object]] = []
    for geo_number, geo_id in enumerate(("a__county", "b__county"), 1):
        for component_number, component in enumerate(COMPONENTS, 1):
            key = inventory.FEATURE_KEY_BY_COMPONENT[component]
            for date_number, date in enumerate(dates):
                rows.append({
                    "geo_id": geo_id, "date": date,
                    "canonical_metric_key": "active_inventory", "feature_key": key,
                    "raw_feature_value": (
                        np.nan if date_number == 0 else float(geo_number * 10 + component_number + date_number)
                    ),
                })
        for date_number, date in enumerate(dates):
            rows.append({
                "geo_id": geo_id, "date": date,
                "canonical_metric_key": "home_sales", "feature_key": "other_feature",
                "raw_feature_value": float(geo_number + date_number),
            })
    source = pd.DataFrame([
        {"geo_id": geo_id, "date": date, "canonical_metric_key": "active_inventory",
         "value": float(100 + number), "metric_origin": "fixture"}
        for geo_id in ("a__county", "b__county")
        for number, date in enumerate(dates)
    ])
    return pd.DataFrame(rows), source


def _challengers(baseline: pd.DataFrame) -> dict[str, SimpleNamespace]:
    result: dict[str, SimpleNamespace] = {}
    for candidate_number, candidate_id in enumerate(CANDIDATES, 1):
        features = baseline.copy(deep=True)
        target = features["feature_key"].isin(inventory.INVENTORY_FEATURE_KEYS)
        features.loc[target & features["raw_feature_value"].notna(), "raw_feature_value"] += candidate_number
        lineage = features[target].copy()
        lineage["experiment_id"] = candidate_id
        lineage["feature_component"] = lineage["feature_key"].map(inventory.COMPONENT_BY_FEATURE_KEY)
        lineage["source_metric_origin"] = "fixture"
        lineage["source_value"] = 1.0
        lineage["challenger_feature_value"] = lineage["raw_feature_value"]
        result[candidate_id] = SimpleNamespace(features=features, smoothing_lineage=lineage)
    return result


def _sections(campaign: object, baseline: pd.DataFrame, challengers: dict[str, object]):
    return {
        "campaign_definition": inventory._campaign_definition_evidence(campaign),
        "coverage_and_lineage": inventory._coverage_lineage_evidence(challengers, baseline),
        "structural_window_behavior": inventory._structural_behavior_evidence(challengers),
        "baseline_comparison": inventory._baseline_comparison_evidence(baseline, challengers),
    }


def _loader_contracts(features: pd.DataFrame, source: pd.DataFrame) -> None:
    with TemporaryDirectory() as temporary:
        root = Path(temporary)
        run_dir = root / inventory.AUTHORITATIVE_RUN_ID
        _expect_error(lambda: inventory._load_authoritative_baseline(root), FileNotFoundError)
        run_dir.mkdir()

        def write(manifest_id: str, feature_frame=features, source_frame=source) -> None:
            feature_frame.to_parquet(run_dir / "features.parquet", index=False)
            source_frame.to_parquet(run_dir / "source_metrics.parquet", index=False)
            (run_dir / "manifest.json").write_text(json.dumps({"run_id": manifest_id}))

        write("wrong")
        _expect_error(lambda: inventory._load_authoritative_baseline(root))
        invalid = features.copy()
        invalid["date"] = invalid["date"].astype(str)
        invalid.loc[0, "date"] = "not-a-date"
        write(inventory.AUTHORITATIVE_RUN_ID, invalid, source)
        _expect_error(lambda: inventory._load_authoritative_baseline(root))
        write(inventory.AUTHORITATIVE_RUN_ID, pd.concat([features, features.iloc[[0]]]), source)
        _expect_error(lambda: inventory._load_authoritative_baseline(root))
        write(inventory.AUTHORITATIVE_RUN_ID, features, pd.concat([source, source.iloc[[0]]]))
        _expect_error(lambda: inventory._load_authoritative_baseline(root))
        write(inventory.AUTHORITATIVE_RUN_ID)
        loaded = inventory._load_authoritative_baseline(root)
        assert loaded.run_id == inventory.AUTHORITATIVE_RUN_ID
        assert loaded.features.equals(loaded.features.sort_values(inventory.FEATURE_KEYS_COLUMNS).reset_index(drop=True))


def _materialization_contracts(campaign, baseline, source, valid) -> None:
    original = inventory.build_in_memory_smoothing_challenger
    baseline_snapshot = baseline.copy(deep=True)
    source_snapshot = source.copy(deep=True)
    try:
        inventory.build_in_memory_smoothing_challenger = lambda **kwargs: valid[kwargs["experiment_id"]]
        materialized = inventory.materialize_phase_a_challengers(campaign, baseline, source)
        assert tuple(materialized) == CANDIDATES
        pd.testing.assert_frame_equal(baseline, baseline_snapshot)
        pd.testing.assert_frame_equal(source, source_snapshot)

        def fails(changes: Callable[[SimpleNamespace], None]) -> None:
            broken = _challengers(baseline)
            changes(broken[CANDIDATES[0]])
            inventory.build_in_memory_smoothing_challenger = lambda **kwargs: broken[kwargs["experiment_id"]]
            _expect_error(lambda: inventory.materialize_phase_a_challengers(campaign, baseline, source))

        fails(lambda item: setattr(item, "smoothing_lineage", item.smoothing_lineage.iloc[0:0]))
        def wrong_lineage(item: SimpleNamespace) -> None:
            item.smoothing_lineage = item.smoothing_lineage.copy()
            item.smoothing_lineage["experiment_id"] = "wrong"

        fails(wrong_lineage)
        fails(lambda item: setattr(item, "features", item.features[item.features["feature_key"].ne(inventory.FEATURE_KEYS[0])]))
        fails(lambda item: setattr(item, "features", pd.concat([item.features, item.features.iloc[[0]]], ignore_index=True)))
        def break_parity(item: SimpleNamespace) -> None:
            item.features = item.features.copy()
            mask = item.features["feature_key"].eq("other_feature")
            item.features.loc[mask, "raw_feature_value"] += 1

        fails(break_parity)
    finally:
        inventory.build_in_memory_smoothing_challenger = original


def _sign_flip_contract() -> None:
    dates = pd.date_range("2020-01-01", periods=5, freq="MS")
    values = {
        "a": [1.0, 2.0, 3.0, 3.0, 4.0],
        "b": [5.0, 4.0, np.nan, 3.0, 2.0],
    }
    frame = pd.DataFrame([
        {"feature_key": inventory.FEATURE_KEYS[0], "geo_id": geo, "date": date,
         "raw_feature_value": value, "canonical_metric_key": "active_inventory"}
        for geo, series in values.items() for date, value in zip(dates, series)
    ])
    assert inventory._sign_flip_rate(frame) == 0.0
    zero_only = frame.assign(raw_feature_value=1.0)
    assert inventory._sign_flip_rate(zero_only) == 0.0
    infinite_gap = frame[frame["geo_id"].eq("a")].iloc[:5].copy()
    infinite_gap["raw_feature_value"] = [1.0, 2.0, np.inf, 1.0, 0.0]
    assert inventory._sign_flip_rate(infinite_gap) == 0.0


def main() -> int:
    baseline, source = _frames()
    baseline_snapshot = baseline.copy(deep=True)
    source_snapshot = source.copy(deep=True)
    campaign = inventory.build_inventory_calibration_campaign(
        campaign_id="fixture_phase_a", campaign_version="1.0",
        baseline_run_id=inventory.AUTHORITATIVE_RUN_ID,
        incumbent_run_id=inventory.AUTHORITATIVE_RUN_ID,
    )
    assert campaign.candidate_policy_ids == CANDIDATES
    assert CANDIDATES != tuple(sorted(CANDIDATES))
    _expect_error(lambda: inventory.build_inventory_calibration_campaign(
        campaign_id="bad", campaign_version="1", baseline_run_id=inventory.AUTHORITATIVE_RUN_ID,
        incumbent_run_id=inventory.AUTHORITATIVE_RUN_ID, candidate_policy_ids=CANDIDATES[:-1]))
    _expect_error(lambda: inventory.build_inventory_calibration_campaign(
        campaign_id="bad", campaign_version="1", baseline_run_id=inventory.AUTHORITATIVE_RUN_ID,
        incumbent_run_id=inventory.AUTHORITATIVE_RUN_ID, candidate_policy_ids=CANDIDATES + ("unexpected",)))
    _expect_error(lambda: inventory.build_inventory_calibration_campaign(
        campaign_id="bad", campaign_version="1", baseline_run_id=inventory.AUTHORITATIVE_RUN_ID,
        incumbent_run_id=inventory.AUTHORITATIVE_RUN_ID, candidate_policy_ids=(CANDIDATES[0],) * 4))
    _expect_error(lambda: inventory.build_inventory_calibration_campaign(
        campaign_id="bad", campaign_version="1", baseline_run_id=inventory.AUTHORITATIVE_RUN_ID,
        incumbent_run_id=inventory.AUTHORITATIVE_RUN_ID, candidate_policy_ids=tuple(sorted(CANDIDATES))))

    challengers = _challengers(baseline)
    _materialization_contracts(campaign, baseline, source, challengers)
    sections = _sections(campaign, baseline, challengers)
    repeated = _sections(campaign, baseline, challengers)
    assert tuple(sections) == ("campaign_definition", "coverage_and_lineage", "structural_window_behavior", "baseline_comparison")
    for section_name in sections:
        assert sections[section_name].metadata == repeated[section_name].metadata
        assert not sections[section_name].plots
        assert tuple(sections[section_name].tables) == tuple(repeated[section_name].tables)
        for table_name in sections[section_name].tables:
            pd.testing.assert_frame_equal(sections[section_name].tables[table_name], repeated[section_name].tables[table_name])

    expected_tables = {
        "inventory_phase_a_campaign", "inventory_phase_a_candidates",
        "inventory_phase_a_feature_weights", "inventory_candidate_feature_coverage",
        "inventory_candidate_lineage_summary", "inventory_candidate_target_replacement",
        "inventory_candidate_non_target_parity", "inventory_candidate_feature_statistics",
        "inventory_candidate_feature_correlations", "inventory_candidate_calendar_month_behavior",
        "inventory_candidate_baseline_feature_comparison",
        "inventory_candidate_feature_series", "inventory_transition_review_windows",
    }
    bundle = inventory.assemble_review_results(campaign.campaign_id, sections)
    assert bundle.table_count == 13
    assert {table.name for table in bundle.tables} == expected_tables

    definition = sections["campaign_definition"].tables
    assert len(definition["inventory_phase_a_campaign"]) == 1
    assert definition["inventory_phase_a_campaign"].columns.tolist() == [
        "campaign_id", "campaign_version", "campaign_phase", "baseline_run_id",
        "incumbent_run_id", "baseline_policy_id", "incumbent_policy_id", "target_metric",
        "target_dimension", "target_axis", "candidate_count", "feature_weights_held_constant",
    ]
    candidates = definition["inventory_phase_a_candidates"]
    assert candidates.columns.tolist() == [
        "candidate_policy_id", "experiment_name", "transform_strategy", "level_window",
        "short_window", "short_lag_periods", "long_window", "long_lag_periods",
        "target_metric", "parent_run", "policy_role", "is_baseline", "recompute_dependents",
    ]
    assert candidates["candidate_policy_id"].tolist() == list(CANDIDATES)
    weights = definition["inventory_phase_a_feature_weights"]
    assert weights.columns.tolist() == [
        "feature_key", "feature_component", "feature_weight", "weight_source", "held_constant",
    ]
    assert weights["feature_component"].tolist() == list(COMPONENTS)
    assert len(weights) == 3 and weights["held_constant"].all()

    coverage_section = sections["coverage_and_lineage"].tables
    coverage = coverage_section["inventory_candidate_feature_coverage"]
    lineage = coverage_section["inventory_candidate_lineage_summary"]
    assert coverage.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "rows", "valid_rows",
        "geography_count", "first_date", "first_valid_date", "last_valid_date",
        "warmup_rows", "non_finite_rows", "duplicate_key_rows",
    ]
    assert lineage.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "lineage_rows",
        "source_geography_count", "first_source_date", "last_source_date",
        "first_challenger_date", "last_challenger_date", "source_metric_origin_count",
    ]
    assert len(coverage) == len(lineage) == 12
    assert coverage["warmup_rows"].gt(0).all()
    assert coverage["non_finite_rows"].eq(0).all()
    assert coverage["duplicate_key_rows"].eq(0).all()
    assert coverage.groupby("candidate_policy_id", sort=False)["feature_component"].apply(tuple).tolist() == [COMPONENTS] * 4
    replacement = coverage_section["inventory_candidate_target_replacement"]
    assert replacement.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "baseline_rows",
        "challenger_rows", "overlap_rows", "changed_rows", "unchanged_rows",
        "baseline_only_rows", "challenger_only_rows",
    ]
    assert len(replacement) == 12 and replacement["changed_rows"].gt(0).all()
    parity = coverage_section["inventory_candidate_non_target_parity"]
    assert parity.columns.tolist() == [
        "candidate_policy_id", "baseline_non_target_rows", "challenger_non_target_rows",
        "matching_key_rows", "value_mismatch_rows", "baseline_only_rows",
        "challenger_only_rows", "parity_pass",
    ]
    assert len(parity) == 4 and parity["parity_pass"].all()

    behavior = sections["structural_window_behavior"].tables
    statistics = behavior["inventory_candidate_feature_statistics"]
    correlations = behavior["inventory_candidate_feature_correlations"]
    assert statistics.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "rows", "valid_rows",
        "mean", "standard_deviation", "mean_absolute_monthly_change",
        "median_absolute_monthly_change", "p90_absolute_monthly_change",
        "maximum_absolute_monthly_change", "sign_flip_rate",
    ]
    assert correlations.columns.tolist() == [
        "candidate_policy_id", "left_feature_component", "right_feature_component",
        "left_feature_key", "right_feature_key", "overlap_rows", "correlation",
    ]
    assert len(statistics) == 12 and len(correlations) == 12
    assert correlations["left_feature_component"].ne(correlations["right_feature_component"]).all()
    assert correlations["overlap_rows"].gt(0).all()
    calendar = behavior["inventory_candidate_calendar_month_behavior"]
    assert calendar.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "calendar_month", "rows",
        "valid_rows", "mean", "standard_deviation", "mean_absolute_value",
        "mean_absolute_monthly_change",
    ]
    assert calendar["calendar_month"].between(1, 12).all()
    _sign_flip_contract()

    comparison = sections["baseline_comparison"].tables["inventory_candidate_baseline_feature_comparison"]
    assert comparison.columns.tolist() == [
        "candidate_policy_id", "feature_component", "feature_key", "baseline_rows",
        "challenger_rows", "overlap_rows", "baseline_only_rows", "challenger_only_rows",
        "valid_comparison_rows", "correlation", "mean_absolute_difference",
        "median_absolute_difference", "p90_absolute_difference", "maximum_absolute_difference",
        "baseline_standard_deviation", "challenger_standard_deviation",
        "standard_deviation_difference", "baseline_sign_flip_rate",
        "challenger_sign_flip_rate", "sign_flip_rate_difference",
    ]
    assert len(comparison) == 12
    assert comparison["baseline_only_rows"].eq(0).all()
    assert comparison["challenger_only_rows"].eq(0).all()
    assert comparison["valid_comparison_rows"].gt(0).all()
    assert not any("rank" in column or "winner" in column for column in comparison.columns)

    missing_feature = _challengers(baseline)
    missing_feature[CANDIDATES[0]].features = missing_feature[CANDIDATES[0]].features[
        missing_feature[CANDIDATES[0]].features["feature_key"].ne(inventory.FEATURE_KEYS[0])]
    _expect_error(lambda: inventory._coverage_lineage_evidence(missing_feature, baseline))
    empty_baseline = baseline[baseline["feature_key"].eq("other_feature")]
    _expect_error(lambda: inventory._baseline_comparison_evidence(empty_baseline, challengers))
    wrong_order = dict(reversed(list(challengers.items())))
    _expect_error(lambda: inventory._structural_behavior_evidence(wrong_order))

    reconciled = _challengers(baseline)
    first = reconciled[CANDIDATES[0]]
    target_index = first.features[first.features["feature_key"].eq(inventory.FEATURE_KEYS[0])].index[0]
    first.features = first.features.drop(index=target_index).reset_index(drop=True)
    
    new_row = (
        first.features[
            first.features["feature_key"].eq(inventory.FEATURE_KEYS[0])
        ]
        .iloc[0]
        .to_dict()
    )
    new_row["date"] = pd.Timestamp("2030-01-01")
    first.features.loc[len(first.features)] = new_row
    first.features = first.features.reset_index(drop=True)

    reconciliation = inventory._baseline_comparison_evidence(baseline, reconciled).tables[
        "inventory_candidate_baseline_feature_comparison"
    ].iloc[0]
    assert reconciliation["baseline_only_rows"] == 1
    assert reconciliation["challenger_only_rows"] == 1

    non_finite = _challengers(baseline)
    finite_frame = non_finite[CANDIDATES[0]].features
    finite_index = finite_frame[
        finite_frame["feature_key"].eq(inventory.FEATURE_KEYS[0])
        & finite_frame["raw_feature_value"].notna()
    ].index[0]
    finite_frame.loc[finite_index, "raw_feature_value"] = np.inf
    non_finite_coverage = inventory._coverage_lineage_evidence(non_finite, baseline).tables[
        "inventory_candidate_feature_coverage"
    ].iloc[0]
    assert non_finite_coverage["non_finite_rows"] == 1

    finite_behavior = inventory._structural_behavior_evidence(non_finite).tables
    finite_statistics = finite_behavior["inventory_candidate_feature_statistics"].iloc[0]
    for column in (
        "mean", "mean_absolute_monthly_change", "maximum_absolute_monthly_change",
        "sign_flip_rate",
    ):
        assert np.isfinite(finite_statistics[column]), column
    finite_calendar = finite_behavior["inventory_candidate_calendar_month_behavior"]
    finite_calendar = finite_calendar[
        finite_calendar["candidate_policy_id"].eq(CANDIDATES[0])
        & finite_calendar["feature_component"].eq(COMPONENTS[0])
    ]
    assert np.isfinite(finite_calendar["mean"].dropna()).all()
    assert np.isfinite(finite_calendar["mean_absolute_value"].dropna()).all()
    assert np.isfinite(finite_calendar["mean_absolute_monthly_change"].dropna()).all()
    sufficient_calendar = finite_calendar[finite_calendar["valid_rows"].ge(2)]
    assert np.isfinite(sufficient_calendar["standard_deviation"]).all()
    finite_correlation = finite_behavior["inventory_candidate_feature_correlations"]
    finite_correlation = finite_correlation[
        finite_correlation["candidate_policy_id"].eq(CANDIDATES[0])
        & finite_correlation["left_feature_component"].eq("level")
        & finite_correlation["right_feature_component"].eq("short")
    ].iloc[0]
    original_correlation = behavior["inventory_candidate_feature_correlations"].iloc[0]
    assert finite_correlation["overlap_rows"] == original_correlation["overlap_rows"] - 1
    assert np.isfinite(finite_correlation["correlation"])

    finite_comparison = inventory._baseline_comparison_evidence(baseline, non_finite).tables[
        "inventory_candidate_baseline_feature_comparison"
    ].iloc[0]
    original_comparison = comparison.iloc[0]
    assert finite_comparison["overlap_rows"] == original_comparison["overlap_rows"]
    assert finite_comparison["valid_comparison_rows"] == original_comparison["valid_comparison_rows"] - 1
    for column in (
        "mean_absolute_difference", "median_absolute_difference", "p90_absolute_difference",
        "maximum_absolute_difference", "baseline_standard_deviation",
        "challenger_standard_deviation", "standard_deviation_difference",
    ):
        assert np.isfinite(finite_comparison[column]), column

    pd.testing.assert_frame_equal(baseline, baseline_snapshot)
    pd.testing.assert_frame_equal(source, source_snapshot)
    _loader_contracts(baseline, source)
    print("SMOKE TEST 81 — INVENTORY PHASE A FOUNDATION EVIDENCE: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
