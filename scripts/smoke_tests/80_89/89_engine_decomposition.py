"""Smoke 89: independent arithmetic and fail-closed decomposition contracts."""
from __future__ import annotations
import copy
import hashlib
import math
import runpy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd

from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime.review.calibration.engine_decomposition import (
    build_dimension_to_axis, build_feature_to_metric, build_metric_to_dimension,
    validate_engine_decomposition, _coordinate_reconciliation, _regime_reconciliation,
    _coverage_universe, _feature_registry,
)
from regime.review.calibration.inventory_review_bundle import _lines, _monthly_gap, _timeline_domain, _timeline_x
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes

def error(call, text):
    try: call()
    except ValueError as exc:
        assert text in str(exc), str(exc); return
    raise AssertionError(f"Expected {text}")

def main() -> int:
    date = pd.Timestamp("2020-01-01")
    normalized = pd.DataFrame([
        {"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "feature_key":"redfin_inventory_level", "feature_score":-0.5},
        {"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "feature_key":"redfin_inventory_long", "feature_score":0.75},
    ])
    # Production parent is supplied, but expected arithmetic below is independent:
    # denominator=.25+.40=.65; contributions=-.5*(.25/.65), .75*(.40/.65).
    expected_parent = -.5 * (.25/.65) + .75 * (.40/.65)
    metrics = pd.DataFrame([{"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "metric_score":expected_parent}])
    rows, rec = build_feature_to_metric(normalized, metrics)
    level = rows[rows.feature_key.eq("redfin_inventory_level")].iloc[0]
    short = rows[rows.feature_key.eq("redfin_inventory_short")].iloc[0]
    long = rows[rows.feature_key.eq("redfin_inventory_long")].iloc[0]
    assert level.configured_weight == .25 and level.available_weight_sum == .65
    assert math.isclose(level.effective_weight, .25/.65) and math.isclose(level.weighted_contribution, -.5*(.25/.65))
    assert not short.available and short.availability_reason_code == "feature_score_missing" and pd.isna(short.effective_weight)
    assert math.isclose(long.weighted_contribution, .75*(.40/.65))
    assert math.isclose(rec.iloc[0].parent_score, expected_parent) and rec.iloc[0].absolute_residual < 1e-12
    assert rec.iloc[0].reconciliation_status == "reconciled"
    wrong = normalized.copy(); wrong.loc[0, "canonical_metric_key"] = "permit_activity"
    error(lambda: build_feature_to_metric(wrong, metrics), "ownership mismatch")
    error(lambda: build_feature_to_metric(pd.concat([normalized, normalized.iloc[[0]]]), metrics), "duplicate child")
    conflicting = pd.concat([metrics, metrics.assign(metric_score=.9)])
    error(lambda: build_feature_to_metric(normalized, conflicting), "conflicting parent")

    aligned = pd.DataFrame([
        {"geo_id":"g", "evaluation_date":date, "canonical_metric_key":"active_inventory", "metric_score":.3, "metric_age_days":0},
        {"geo_id":"g", "evaluation_date":date, "canonical_metric_key":"permit_activity", "metric_score":-.2, "metric_age_days":0},
    ])
    expected_dimension = (.3*.3334 + -.2*.3333) / (.3334+.3333)
    dimensions = pd.DataFrame([{"geo_id":"g", "date":date, "dimension":"supply", "dimension_score":expected_dimension}])
    metric_rows, metric_rec = build_metric_to_dimension(aligned, dimensions)
    inventory = metric_rows[metric_rows.canonical_metric_key.eq("active_inventory")].iloc[0]
    intensity = metric_rows[metric_rows.canonical_metric_key.eq("permit_intensity")].iloc[0]
    assert inventory.configured_weight == .3334 and math.isclose(inventory.available_weight_sum, .6667)
    assert math.isclose(inventory.effective_weight, .3334/.6667)
    assert math.isclose(inventory.weighted_contribution, .3*(.3334/.6667))
    assert not intensity.available and metric_rec.iloc[0].absolute_residual < 1e-12

    dim = pd.DataFrame([{"geo_id":"g", "date":date, "dimension":"capital_markets", "dimension_score":-.5, "max_metric_age_days":0}])
    axes = pd.DataFrame([{"geo_id":"g", "date":date, "axis":"supply", "axis_score":-.5}])
    axis_rows, axis_rec = build_dimension_to_axis(dim, axes)
    capital = axis_rows[axis_rows.dimension.eq("capital_markets")].iloc[0]
    supply = axis_rows[axis_rows.dimension.eq("supply")].iloc[0]
    assert capital.configured_weight == .15 and capital.available_weight_sum == .15
    assert capital.effective_weight == 1 and capital.weighted_contribution == -.5
    assert not supply.available and axis_rec.iloc[0].absolute_residual == 0

    bounded = pd.DataFrame([
        {"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "feature_key":"redfin_inventory_level", "feature_score":1.0},
        {"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "feature_key":"redfin_inventory_short", "feature_score":-1.0},
        {"geo_id":"g", "date":date, "canonical_metric_key":"active_inventory", "feature_key":"redfin_inventory_long", "feature_score":1.0},
    ])
    bounded_parent = score_metrics(bounded)
    assert bounded_parent.metric_score.between(-1, 1).all()
    bounded_rows, _ = build_feature_to_metric(bounded, bounded_parent)
    assert math.isclose(bounded_rows.effective_weight.sum(), 1.0)
    drift_parent = bounded_parent.copy(); drift_parent.loc[0, "metric_score"] += 5e-11
    assert build_feature_to_metric(bounded, drift_parent)[1].iloc[0].reconciliation_status == "reconciled"
    out_of_range = bounded.copy(); out_of_range.loc[0, "feature_score"] = 1.01
    error(lambda: build_feature_to_metric(out_of_range, bounded_parent), "non-finite feature_score")

    source_dates = pd.to_datetime(["2019-01-01", "2019-02-01", "2019-03-01"])
    coverage = _coverage_universe(
        layer="feature_to_metric", parent_key="active_inventory", geo_id="g",
        expected_children=("level", "short"),
        source=pd.DataFrame({"date":[*source_dates, *source_dates], "feature_key":["level"]*3+["short"]*3}),
        source_child="feature_key",
        available=pd.DataFrame({"date":[source_dates[1], source_dates[2]], "feature_key":["level", "level"]}),
        available_child="feature_key",
        parents=pd.DataFrame({"date":[source_dates[2]]}),
        reconciliation=pd.DataFrame({"date":[source_dates[2]], "reconciliation_status":["reconciled"]}),
    )
    assert coverage["evaluation_universe_start"] == source_dates[0]
    assert coverage["first_valid_child_date"] == source_dates[1] and coverage["first_valid_parent_date"] == source_dates[2]
    assert coverage["source_present_child_unavailable_rows"] == 4
    assert coverage["child_available_parent_absent_rows"] == 1
    assert coverage["partially_available_parent_date_count"] == 1
    assert coverage["one_child_only_parent_date_count"] == 1
    assert coverage["zero_available_child_dates"] == 1
    assert coverage["zero_available_weight_parent_rows"] == 0
    assert coverage["warmup_rows"] == 4 and coverage["fully_reconciled_row_count"] == 1

    metric_dates = pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"])
    metric_coverage = _coverage_universe(
        layer="metric_to_dimension", parent_key="supply", geo_id="g",
        expected_children=("active_inventory",),
        source=pd.DataFrame({"date":[metric_dates[0]], "canonical_metric_key":["active_inventory"]}),
        source_child="canonical_metric_key",
        available=pd.DataFrame({"date":[metric_dates[1]], "canonical_metric_key":["active_inventory"]}),
        available_child="canonical_metric_key",
        parents=pd.DataFrame({"date":[metric_dates[2]]}),
        reconciliation=pd.DataFrame({"date":[], "reconciliation_status":[]}),
    )
    assert metric_coverage["first_source_observation_date"] == metric_dates[0]
    assert metric_coverage["first_valid_child_date"] == metric_dates[1]
    assert metric_coverage["first_valid_parent_date"] == metric_dates[2]
    assert metric_coverage["zero_available_child_dates"] == 2
    assert metric_coverage["zero_available_weight_parent_rows"] == 1
    assert metric_coverage["child_available_parent_absent_rows"] == 1

    timeline = [("early", ["2012-01-01", "2012-03-01"], [0., 1.]),
                ("late", ["2021-01-01", "2021-02-01"], [.5, .6])]
    start, end = _timeline_domain(timeline)
    assert _timeline_x("2021-01-01", start, end) > _timeline_x("2012-03-01", start, end)
    assert not _monthly_gap("2012-01-01", "2012-02-01")
    assert _monthly_gap("2012-02-01", "2012-05-01")
    assert _monthly_gap("2012-01-01", "2021-01-01")
    with TemporaryDirectory() as temporary:
        one, two = Path(temporary)/"one.png", Path(temporary)/"two.png"
        _lines(one, "Real chronology", timeline, "score")
        _lines(two, "Real chronology", timeline, "score")
        assert hashlib.sha256(one.read_bytes()).digest() == hashlib.sha256(two.read_bytes()).digest()
        assert one.stat().st_size > 0

    both_axes = pd.DataFrame([
        {"geo_id":"g", "date":date, "axis":"supply", "axis_score":.2, "max_dimension_age_days":0},
        {"geo_id":"g", "date":date, "axis":"demand", "axis_score":.3, "max_dimension_age_days":0},
    ])
    coordinates = build_coordinates(both_axes)
    coordinate_check = _coordinate_reconciliation(both_axes, coordinates)
    assert coordinate_check.iloc[0].x_supply == .2 and coordinate_check.iloc[0].y_demand == .3
    assert coordinate_check.iloc[0].reconciliation_pass
    bad_coordinates = coordinates.copy(); bad_coordinates.loc[0, "x_supply"] = .21
    assert _coordinate_reconciliation(both_axes, bad_coordinates).iloc[0].reconciliation_status == "failed"
    error(lambda: _coordinate_reconciliation(both_axes, coordinates.iloc[0:0]), "key-universe mismatch")
    extra_coordinate = coordinates.copy(deep=True)
    extra_coordinate["date"] = (
        pd.to_datetime(extra_coordinate["date"])
        + pd.offsets.MonthBegin(1)
    )
    extra_coordinates = pd.concat(
        [coordinates.copy(deep=True), extra_coordinate],
        ignore_index=True,
    )
    error(
        lambda: _coordinate_reconciliation(
            both_axes,
            extra_coordinates,
        ),
        "key-universe mismatch",
    )
    regimes = assign_regimes(assign_geometry(coordinates))
    regime_check = _regime_reconciliation(coordinates, regimes)
    assert regime_check.iloc[0].reconciliation_pass
    assert regime_check.iloc[0].major_regime_expected == regime_check.iloc[0].major_regime_actual
    assert regime_check.iloc[0].minor_regime_expected == regime_check.iloc[0].minor_regime_actual
    assert str(regime_check.iloc[0].quadrant_expected) == str(regime_check.iloc[0].quadrant_actual)
    bad_regimes = regimes.copy(); bad_regimes.loc[0, "major_regime"] = "wrong"
    assert _regime_reconciliation(coordinates, bad_regimes).iloc[0].reconciliation_status == "failed"
    error(lambda: _regime_reconciliation(coordinates, regimes.iloc[0:0]), "key-universe mismatch")
    extra_regime = regimes.copy(deep=True)
    extra_regime["date"] = (
        pd.to_datetime(extra_regime["date"])
        + pd.offsets.MonthBegin(1)
    )
    extra_regimes = pd.concat(
        [regimes.copy(deep=True), extra_regime],
        ignore_index=True,
    )
    error(
        lambda: _regime_reconciliation(
            coordinates,
            extra_regimes,
        ),
        "key-universe mismatch",
    )
    bps = _feature_registry().query("feature_key.str.startswith('bps_total_units')", engine="python")
    assert set(bps.canonical_metric_key) == {"permit_activity"}

    fixture = runpy.run_path("scripts/smoke_tests/80_89/85_inventory_review_bundle.py")
    scoring = runpy.run_path("scripts/smoke_tests/80_89/83_inventory_candidate_scoring.py")
    evidence = scoring["_evidence"](); contract = fixture["_decomposition_evidence"](evidence)
    campaign = evidence.campaign
    assert campaign.primary_decomposition_axes == ("supply",)
    assert campaign.supporting_coordinate_axes == ("demand", "supply")
    demand_campaign = replace(campaign, primary_decomposition_axes=("demand",),
                              supporting_coordinate_axes=("supply", "demand"))
    assert demand_campaign.primary_decomposition_axes == ("demand",)
    assert demand_campaign.supporting_coordinate_axes == ("demand", "supply")
    error(lambda: replace(campaign, primary_decomposition_axes=("demand",),
                          supporting_coordinate_axes=("supply",)), "subset")
    error(lambda: replace(campaign, primary_decomposition_axes=("invented",)), "unknown axes")
    error(lambda: replace(campaign, primary_decomposition_axes=("supply", "supply")), "duplicate axes")
    validate_engine_decomposition(contract)
    multi = copy.deepcopy(contract)
    sibling = multi.tables["feature_to_metric"].iloc[[0]].copy()
    sibling["feature_key"] = "redfin_inventory_short"; sibling["feature_type"] = "short_term_change"
    sibling["configured_weight"] = .35; sibling["available"] = False
    sibling[["feature_score", "effective_weight", "weighted_contribution"]] = float("nan")
    sibling["availability_reason_code"] = "feature_score_missing"; sibling["availability_reason"] = "fixture unavailable"
    multi.tables["feature_to_metric"] = pd.concat([multi.tables["feature_to_metric"], sibling], ignore_index=True)
    validate_engine_decomposition(multi)
    mixed_status = copy.deepcopy(multi); mixed_status.tables["feature_to_metric"].loc[mixed_status.tables["feature_to_metric"].index[-1], "reconciliation_status"] = "not_reconcilable"
    error(lambda: validate_engine_decomposition(mixed_status), "inconsistent parent-level reconciliation_status")
    mixed_residual = copy.deepcopy(multi); mixed_residual.tables["feature_to_metric"].loc[mixed_residual.tables["feature_to_metric"].index[-1], "absolute_residual"] = .01
    error(lambda: validate_engine_decomposition(mixed_residual), "inconsistent parent-level absolute_residual")
    mixed_weight = copy.deepcopy(multi); mixed_weight.tables["feature_to_metric"].loc[mixed_weight.tables["feature_to_metric"].index[-1], "available_weight_sum"] = .9
    error(lambda: validate_engine_decomposition(mixed_weight), "inconsistent parent-level available_weight_sum")
    malformed = copy.deepcopy(contract); malformed.tables["feature_to_metric"] = malformed.tables["feature_to_metric"].drop(columns="configured_weight")
    error(lambda: validate_engine_decomposition(malformed), "missing required")
    unknown = copy.deepcopy(contract); unknown.tables["unknown"] = pd.DataFrame({"x":[1]})
    error(lambda: validate_engine_decomposition(unknown), "Unknown decomposition")
    duplicate = copy.deepcopy(contract); duplicate.tables["regime_reconciliation"] = pd.concat([duplicate.tables["regime_reconciliation"], duplicate.tables["regime_reconciliation"].iloc[[0]]])
    error(lambda: validate_engine_decomposition(duplicate), "duplicate keys")
    negative = copy.deepcopy(contract); negative.tables["feature_to_metric"].loc[0, "configured_weight"] = -.1
    error(lambda: validate_engine_decomposition(negative), "invalid configured_weight")
    negative_effective = copy.deepcopy(contract); negative_effective.tables["feature_to_metric"].loc[0, "effective_weight"] = -.1
    error(lambda: validate_engine_decomposition(negative_effective), "invalid effective_weight")
    failed = copy.deepcopy(contract)
    failed.tables["reconciliation_summary"].loc[0, ["reconciliation_status", "reason_code", "reason", "reconciliation_pass", "absolute_residual"]] = ["failed", "residual_exceeds_tolerance", "fixture failure", False, .1]
    error(lambda: validate_engine_decomposition(failed), "failed reconciliation")
    allowed = copy.deepcopy(multi)
    feature = allowed.tables["feature_to_metric"]
    feature["reconciliation_pass"] = feature["reconciliation_pass"].astype("boolean")
    first = feature.iloc[0]
    mask = feature["campaign_id"].eq(first.campaign_id) & feature["campaign_version"].eq(first.campaign_version) & feature["series_id"].eq(first.series_id) & feature["geo_id"].eq(first.geo_id) & feature["date"].eq(first.date) & feature["canonical_metric_key"].eq(first.canonical_metric_key)
    feature.loc[mask, "available"] = False
    feature.loc[mask, ["feature_score", "effective_weight", "weighted_contribution", "summed_contributions", "absolute_residual", "relative_residual"]] = pd.NA
    feature.loc[mask, ["available_child_count", "available_weight_sum"]] = [0, 0.0]
    feature.loc[mask, ["reconciliation_status", "reason_code", "reason"]] = ["not_reconcilable", "no_available_weight", "No production-eligible child weight is available."]
    feature.loc[mask, "reconciliation_pass"] = pd.NA
    validate_engine_decomposition(allowed)
    unapproved = copy.deepcopy(allowed); unapproved.tables["feature_to_metric"].loc[mask, "reason_code"] = "invented"
    error(lambda: validate_engine_decomposition(unapproved), "unapproved")
    sufficient = copy.deepcopy(contract)
    sufficient.tables["feature_to_metric"]["reconciliation_pass"] = sufficient.tables["feature_to_metric"]["reconciliation_pass"].astype("boolean")
    sufficient.tables["feature_to_metric"].loc[0, ["reconciliation_status", "reason_code", "reason", "reconciliation_pass"]] = ["not_reconcilable", "no_available_weight", "wrong", pd.NA]
    error(lambda: validate_engine_decomposition(sufficient), "sufficient inputs")
    print("SMOKE TEST 89 — ENGINE DECOMPOSITION: PASS")
    return 0
if __name__ == "__main__": raise SystemExit(main())
