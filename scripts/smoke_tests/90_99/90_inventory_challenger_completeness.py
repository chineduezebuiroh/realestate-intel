"""Smoke Test 90: real mixed-universe constructor and fail-closed parity."""
from __future__ import annotations

from types import SimpleNamespace

import pandas as pd

import regime.experiments.in_memory_challenger as constructor_module
from regime._03_metric_scorer import score_metrics
from regime._04_asof_aligner import align_metric_scores_asof
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.review.calibration.campaign import CalibrationCampaign
from regime.review.calibration.inventory_campaign import (
    INVENTORY_FEATURE_KEYS,
    _challenger_completeness_evidence,
)

GEO = "fixture__county"
NATION = "united_states__nation"
DATE = pd.Timestamp("2024-01-31")
BPS_KEYS = {"bps_total_units_level", "bps_total_units_short", "bps_total_units_long"}


def _normalized_row(geo: str, metric: str, feature: str, score: float) -> dict[str, object]:
    return {"geo_id": geo, "date": DATE, "canonical_metric_key": metric,
            "feature_key": feature, "source_family": feature.split("_")[0],
            "raw_feature_value": score, "percentile": (score + 1) / 2,
            "feature_score": score, "normalization_method": "expanding_percentile",
            "score_direction": "positive", "lookback_periods": "120", "min_periods": "1"}


def _incumbent_normalized() -> pd.DataFrame:
    rows = []
    for key, score in zip(sorted(INVENTORY_FEATURE_KEYS), (0.1, 0.2, 0.3)):
        rows.append(_normalized_row(GEO, "active_inventory", key, score))
    # Target-family rows outside the county campaign are upstream dependencies,
    # not replacement candidates.
    for key, score in zip(sorted(INVENTORY_FEATURE_KEYS), (-0.1, -0.2, -0.3)):
        rows.append(_normalized_row(NATION, "active_inventory", key, score))
    for key, score in zip(sorted(BPS_KEYS), (0.35, 0.4, 0.45)):
        rows.append(_normalized_row(GEO, "permit_activity", key, score))
    for key, score in zip(
        ("laus_unemployment_rate_level", "laus_unemployment_rate_short", "laus_unemployment_rate_long"),
        (0.5, 0.55, 0.6),
    ):
        rows.append(_normalized_row(GEO, "laus_unemployment_rate", key, score))
    for key, score in zip(
        ("fred_mortgage_30y_level", "fred_mortgage_30y_short", "fred_mortgage_30y_long"),
        (-0.4, -0.3, -0.2),
    ):
        rows.append(_normalized_row(NATION, "mortgage_30y", key, score))
    return pd.DataFrame(rows).sort_values(
        ["geo_id", "date", "canonical_metric_key", "feature_key"], kind="mergesort"
    ).reset_index(drop=True)


def _incumbent_artifacts() -> dict[str, pd.DataFrame]:
    normalized = _incumbent_normalized()
    metrics = score_metrics(normalized)
    aligned = align_metric_scores_asof(metrics)
    dimensions = score_dimensions(aligned)
    axes = score_axes(dimensions)
    # A distinctive persisted supporting-axis value proves it is copied rather
    # than accidentally rebuilt by the Inventory challenger.
    axes.loc[axes["axis"].eq("demand"), "axis_score"] = 0.777
    coordinates = build_coordinates(axes)
    geometry = assign_geometry(coordinates)
    regimes = assign_regimes(geometry)
    return {"normalized_features": normalized, "metric_scores": metrics,
            "aligned_metric_scores": aligned, "dimension_scores": dimensions,
            "axis_scores": axes, "coordinates": coordinates,
            "regime_assignments": regimes}


def _campaign(primary=("supply",), supporting=("supply", "demand")) -> CalibrationCampaign:
    return CalibrationCampaign(campaign_id="mixed_universe_fixture", campaign_version="1.0",
        campaign_phase="phase_a", baseline_run_id="baseline", incumbent_run_id="baseline",
        baseline_policy_id="baseline_current", incumbent_policy_id="baseline_current",
        candidate_policy_ids=("inventory_ma3_structural",), target_metric="active_inventory",
        target_dimension="supply", target_axis="supply", primary_decomposition_axes=primary,
        supporting_coordinate_axes=supporting)


def _clone(challenger, **changes):
    values = {name: frame.copy(deep=True) for name, frame in challenger.as_mapping().items()}
    values.update(changes)
    return SimpleNamespace(**values)


def _expect_parity_failure(campaign, baseline, challenger, text: str) -> None:
    try:
        _challenger_completeness_evidence(campaign, baseline, {"inventory_ma3_structural": challenger})
    except ValueError as exc:
        assert text in str(exc), str(exc)
    else:
        raise AssertionError(f"Expected fail-closed parity error containing {text!r}")


def main() -> int:
    baseline = _incumbent_artifacts()
    target_candidate = baseline["normalized_features"][
        baseline["normalized_features"]["feature_key"].isin(INVENTORY_FEATURE_KEYS)
        & baseline["normalized_features"]["geo_id"].eq(GEO)
    ].copy()
    target_candidate["feature_score"] = (target_candidate["feature_score"] + 0.5).clip(-1, 1)
    target_candidate["percentile"] = (target_candidate["feature_score"] + 1) / 2
    # Partial mode retains enough unrelated rows to produce both axes, but
    # deliberately omits governed BPS. The complete constructor must restore it.
    partial_candidate = pd.concat([
        target_candidate,
        baseline["normalized_features"][baseline["normalized_features"]["canonical_metric_key"].isin(
            {"laus_unemployment_rate", "mortgage_30y"})],
    ], ignore_index=True)
    raw_target = target_candidate[["geo_id", "date", "canonical_metric_key", "feature_key", "raw_feature_value"]]
    lineage = raw_target.assign(experiment_id="inventory_ma3_structural")
    original_apply = constructor_module.apply_smoothing_experiment
    original_normalize = constructor_module.normalize_features
    constructor_module.apply_smoothing_experiment = lambda **_: (raw_target.copy(), lineage.copy())
    constructor_module.normalize_features = lambda _: partial_candidate.copy()
    try:
        campaign = _campaign()
        challenger = constructor_module.build_in_memory_smoothing_challenger(
            baseline_features=raw_target, source_metrics=pd.DataFrame(),
            experiment_id="inventory_ma3_structural", incumbent_artifacts=baseline,
            target_feature_keys=INVENTORY_FEATURE_KEYS,
            primary_axes=campaign.primary_decomposition_axes,
            supporting_axes=campaign.supporting_coordinate_axes,
            campaign_output_geo_ids=(GEO,),
            require_complete_universe=True,
        )
        assert set(challenger.normalized_features.feature_key).issuperset(BPS_KEYS)
        non_target_base = baseline["normalized_features"][~baseline["normalized_features"].feature_key.isin(INVENTORY_FEATURE_KEYS)]
        non_target_new = challenger.normalized_features[~challenger.normalized_features.feature_key.isin(INVENTORY_FEATURE_KEYS)]
        pd.testing.assert_frame_equal(non_target_base.reset_index(drop=True), non_target_new.reset_index(drop=True))
        outside_targets = lambda frame: frame.query("geo_id == @NATION and feature_key in @INVENTORY_FEATURE_KEYS").reset_index(drop=True)
        pd.testing.assert_frame_equal(
            outside_targets(baseline["normalized_features"]),
            outside_targets(challenger.normalized_features),
        )
        assert NATION in set(challenger.normalized_features.geo_id)
        for frame in (challenger.dimension_scores, challenger.axis_scores, challenger.coordinates,
                      challenger.geometry, challenger.regime_assignments):
            assert set(frame.geo_id) == {GEO}
        assert not challenger.metric_scores.query("canonical_metric_key == 'active_inventory'").metric_score.equals(
            baseline["metric_scores"].query("canonical_metric_key == 'active_inventory'").metric_score)
        pd.testing.assert_frame_equal(
            baseline["metric_scores"].query("canonical_metric_key != 'active_inventory'").reset_index(drop=True),
            challenger.metric_scores.query("canonical_metric_key != 'active_inventory'").reset_index(drop=True))
        base_dims = baseline["dimension_scores"].set_index(["geo_id", "date", "dimension"])
        new_dims = challenger.dimension_scores.set_index(["geo_id", "date", "dimension"])
        assert "capital_markets" in new_dims.index.get_level_values("dimension")
        assert "demand" in new_dims.index.get_level_values("dimension")
        assert new_dims.loc[(GEO, DATE, "supply"), "dimension_score"] != base_dims.loc[(GEO, DATE, "supply"), "dimension_score"]
        demand_base = baseline["axis_scores"].query("axis == 'demand'").reset_index(drop=True)
        demand_new = challenger.axis_scores.query("axis == 'demand'").reset_index(drop=True)
        pd.testing.assert_frame_equal(demand_base, demand_new)
        supply = challenger.axis_scores.query("geo_id == @GEO and axis == 'supply'").iloc[0]
        supply_dim = new_dims.loc[(GEO, DATE, "supply"), "dimension_score"]
        capital_dim = new_dims.loc[(GEO, DATE, "capital_markets"), "dimension_score"]
        assert abs(supply.axis_score - (supply_dim * 0.85 + capital_dim * 0.15)) < 1e-12
        coordinate = challenger.coordinates.query("geo_id == @GEO").iloc[0]
        assert coordinate.x_supply == supply.axis_score and coordinate.y_demand == demand_new.iloc[0].axis_score
        expected_regimes = assign_regimes(assign_geometry(challenger.coordinates))
        pd.testing.assert_frame_equal(challenger.regime_assignments, expected_regimes)
        evidence = _challenger_completeness_evidence(
            campaign, baseline, {"inventory_ma3_structural": challenger})
        assert evidence.tables["inventory_challenger_unaffected_parity"].parity_pass.all()

        # Authoritative-style sentinel: incumbent keys are strictly larger,
        # while the normalized challenger is a valid leading-warmup suffix.
        early_november = target_candidate.copy(deep=True)
        early_november["date"] = pd.Series(
            [pd.Timestamp("2023-11-30")] * len(early_november),
            index=early_november.index,
            dtype="datetime64[ns]",
        )

        early_december = target_candidate.copy(deep=True)
        early_december["date"] = pd.Series(
            [pd.Timestamp("2023-12-31")] * len(early_december),
            index=early_december.index,
            dtype="datetime64[ns]",
        )

        early_targets = pd.concat(
            [
                early_november,
                early_december,
            ],
            ignore_index=True,
        )
        warm_baseline = dict(baseline)
        warm_baseline["normalized_features"] = pd.concat([
            early_targets, baseline["normalized_features"]
        ], ignore_index=True).sort_values(
            ["geo_id", "date", "canonical_metric_key", "feature_key"], kind="mergesort"
        ).reset_index(drop=True)
        warm_challenger = constructor_module.build_in_memory_smoothing_challenger(
            baseline_features=raw_target, source_metrics=pd.DataFrame(),
            experiment_id="inventory_ma3_structural", incumbent_artifacts=warm_baseline,
            target_feature_keys=INVENTORY_FEATURE_KEYS, primary_axes=("supply",),
            supporting_axes=("supply", "demand"), campaign_output_geo_ids=(GEO,),
            require_complete_universe=True,
        )
        reconciliation = warm_challenger.target_replacement_reconciliation
        assert reconciliation.leading_warmup_rows.eq(2).all()
        mixed_campaign_targets = warm_challenger.normalized_features.query(
            "geo_id == @GEO and feature_key in @INVENTORY_FEATURE_KEYS"
        )
        assert set(mixed_campaign_targets.date) == {DATE}  # warmup was not backfilled

        def expect_replacement_failure(candidate_frame, expected):
            constructor_module.normalize_features = lambda _: candidate_frame.copy()
            try:
                constructor_module.build_in_memory_smoothing_challenger(
                    baseline_features=raw_target, source_metrics=pd.DataFrame(),
                    experiment_id="inventory_ma3_structural", incumbent_artifacts=baseline,
                    target_feature_keys=INVENTORY_FEATURE_KEYS, primary_axes=("supply",),
                    supporting_axes=("supply", "demand"), campaign_output_geo_ids=(GEO,),
                    require_complete_universe=True,
                )
            except ValueError as exc:
                assert expected in str(exc), str(exc)
            else:
                raise AssertionError(f"Expected replacement-boundary failure: {expected}")

        outside_candidate = pd.concat([
            partial_candidate,
            baseline["normalized_features"].query(
                "geo_id == @NATION and feature_key in @INVENTORY_FEATURE_KEYS"
            ).iloc[[0]],
        ], ignore_index=True)
        expect_replacement_failure(outside_candidate, "out_of_scope_target_rows")
        expect_replacement_failure(
            partial_candidate.drop(partial_candidate.index[0]).reset_index(drop=True),
            "missing_target_series",
        )
        constructor_module.normalize_features = lambda _: partial_candidate.copy()
        dropped_capital = challenger.dimension_scores[
            ~challenger.dimension_scores["dimension"].eq("capital_markets")
        ]
        _expect_parity_failure(
            campaign, baseline, _clone(challenger, dimension_scores=dropped_capital),
            "parity failure",
        )
        rebuilt_demand = challenger.axis_scores.copy()
        rebuilt_demand.loc[rebuilt_demand["axis"].eq("demand"), "axis_score"] = 0.123
        _expect_parity_failure(
            campaign, baseline, _clone(challenger, axis_scores=rebuilt_demand),
            "parity failure",
        )

        # Axis-input equality is exact and null-safe; nullable deltas remain
        # null rather than being interpreted as zero.
        def dimensions_with(frame, dimension, value):
            changed = frame.copy(deep=True)
            changed.loc[changed["dimension"].eq(dimension), "dimension_score"] = value
            return changed

        baseline_capital_null = dict(baseline)
        baseline_capital_null["dimension_scores"] = dimensions_with(
            baseline["dimension_scores"], "capital_markets", float("nan")
        )
        challenger_capital_zero = _clone(
            challenger,
            dimension_scores=dimensions_with(
                challenger.dimension_scores, "capital_markets", 0.0
            ),
        )
        _expect_parity_failure(
            campaign, baseline_capital_null, challenger_capital_zero,
            "Axis-input parity failure",
        )

        baseline_capital_zero = dict(baseline)
        baseline_capital_zero["dimension_scores"] = dimensions_with(
            baseline["dimension_scores"], "capital_markets", 0.0
        )
        challenger_capital_null = _clone(
            challenger,
            dimension_scores=dimensions_with(
                challenger.dimension_scores, "capital_markets", float("nan")
            ),
        )
        _expect_parity_failure(
            campaign, baseline_capital_zero, challenger_capital_null,
            "Axis-input parity failure",
        )

        challenger_both_null = _clone(
            challenger,
            dimension_scores=dimensions_with(
                challenger.dimension_scores, "capital_markets", float("nan")
            ),
        )
        null_evidence = _challenger_completeness_evidence(
            campaign, baseline_capital_null,
            {"inventory_ma3_structural": challenger_both_null},
        )
        null_capital = null_evidence.tables["inventory_challenger_axis_input_parity"].query(
            "dimension == 'capital_markets'"
        )
        assert null_capital["parity_pass"].all()
        assert null_capital["absolute_delta"].isna().all()

        equal_value = float(
            baseline["dimension_scores"].query("dimension == 'capital_markets'")["dimension_score"].iloc[0]
        )
        challenger_equal = _clone(
            challenger,
            dimension_scores=dimensions_with(
                challenger.dimension_scores, "capital_markets", equal_value
            ),
        )
        equal_evidence = _challenger_completeness_evidence(
            campaign, baseline, {"inventory_ma3_structural": challenger_equal}
        )
        assert equal_evidence.tables["inventory_challenger_axis_input_parity"].query(
            "dimension == 'capital_markets'"
        )["parity_pass"].all()
        challenger_different = _clone(
            challenger,
            dimension_scores=dimensions_with(
                challenger.dimension_scores, "capital_markets", equal_value + 0.01
            ),
        )
        _expect_parity_failure(
            campaign, baseline, challenger_different, "Axis-input parity failure"
        )

        def duplicate_dimension(frame, dimension):
            duplicate = frame[frame["dimension"].eq(dimension)].iloc[[0]].copy()
            return pd.concat([frame, duplicate], ignore_index=True)

        duplicate_baseline = dict(baseline)
        duplicate_baseline["dimension_scores"] = duplicate_dimension(
            baseline["dimension_scores"], "capital_markets"
        )
        _expect_parity_failure(
            campaign, duplicate_baseline, challenger,
            "baseline contains duplicate governed keys",
        )
        duplicate_challenger = _clone(
            challenger,
            dimension_scores=duplicate_dimension(
                challenger.dimension_scores, "capital_markets"
            ),
        )
        _expect_parity_failure(
            campaign, baseline, duplicate_challenger,
            "challenger contains duplicate governed keys",
        )
        duplicate_demand = _clone(
            challenger,
            dimension_scores=duplicate_dimension(challenger.dimension_scores, "demand"),
        )
        _expect_parity_failure(
            campaign, baseline, duplicate_demand,
            "challenger contains duplicate governed keys",
        )

        partial = constructor_module.build_in_memory_smoothing_challenger(
            baseline_features=raw_target, source_metrics=pd.DataFrame(),
            experiment_id="inventory_ma3_structural")
        _expect_parity_failure(campaign, baseline, partial, "parity failure")
        try:
            constructor_module.build_in_memory_smoothing_challenger(
                baseline_features=raw_target, source_metrics=pd.DataFrame(),
                experiment_id="inventory_ma3_structural", target_feature_keys=INVENTORY_FEATURE_KEYS,
                primary_axes=("supply",), supporting_axes=("supply", "demand"),
                campaign_output_geo_ids=(GEO,),
                require_complete_universe=True)
        except ValueError as exc:
            assert "requires incumbent_artifacts" in str(exc)
        else:
            raise AssertionError("Authoritative complete mode accepted missing incumbent artifacts")

        # Generic axis scope: Demand primary recomputes Demand and preserves Supply.
        demand_primary = constructor_module.build_in_memory_smoothing_challenger(
            baseline_features=raw_target, source_metrics=pd.DataFrame(), experiment_id="inventory_ma3_structural",
            incumbent_artifacts=baseline, target_feature_keys=INVENTORY_FEATURE_KEYS,
            primary_axes=("demand",), supporting_axes=("supply", "demand"),
            campaign_output_geo_ids=(GEO,), require_complete_universe=True)
        pd.testing.assert_frame_equal(
            demand_primary.axis_scores.query("axis == 'supply'").reset_index(drop=True),
            baseline["axis_scores"].query("axis == 'supply'").reset_index(drop=True))
        assert not demand_primary.axis_scores.query("axis == 'demand'").axis_score.eq(0.777).all()
        assert list(challenger.axis_scores.sort_values(["geo_id", "date", "axis"]).axis) == list(challenger.axis_scores.axis)
        for kwargs, message in (
            ({"primary_axes": ("supply",), "supporting_axes": ("demand",)}, "subset"),
            ({"primary_axes": ("unknown",), "supporting_axes": ("unknown",)}, "unknown"),
            ({"primary_axes": ("supply", "supply"), "supporting_axes": ("supply", "demand")}, "duplicates"),
        ):
            try:
                constructor_module.build_in_memory_smoothing_challenger(
                    baseline_features=raw_target, source_metrics=pd.DataFrame(), experiment_id="inventory_ma3_structural",
                    incumbent_artifacts=baseline, target_feature_keys=INVENTORY_FEATURE_KEYS,
                    campaign_output_geo_ids=(GEO,),
                    require_complete_universe=True, **kwargs)
            except ValueError as exc:
                assert message in str(exc)
            else:
                raise AssertionError(f"Invalid axis scope did not fail: {kwargs}")

        # Exact governed schemas: missing, renamed, or extra fields all fail closed.
        altered = challenger.aligned_metric_scores.drop(columns="metric_age_days")
        _expect_parity_failure(campaign, baseline, _clone(challenger, aligned_metric_scores=altered), "schema_mismatch")
        altered = challenger.metric_scores.assign(unexpected_lineage="x")
        _expect_parity_failure(campaign, baseline, _clone(challenger, metric_scores=altered), "schema_mismatch")
        altered = challenger.dimension_scores.drop(columns="max_metric_age_days")
        _expect_parity_failure(campaign, baseline, _clone(challenger, dimension_scores=altered), "schema_mismatch")
        altered = challenger.axis_scores.drop(columns="dimension_weight_sum")
        _expect_parity_failure(campaign, baseline, _clone(challenger, axis_scores=altered), "schema_mismatch")
        altered = challenger.axis_scores.drop(columns="dimension_count")
        _expect_parity_failure(campaign, baseline, _clone(challenger, axis_scores=altered), "schema_mismatch")
        altered = challenger.metric_scores.rename(columns={"metric_score": "renamed_metric_score"})
        _expect_parity_failure(campaign, baseline, _clone(challenger, metric_scores=altered), "schema_mismatch")

        assert BPS_KEYS.issubset(set(baseline["normalized_features"].feature_key))
        assert set(baseline["normalized_features"].query("feature_key in @BPS_KEYS").canonical_metric_key) == {"permit_activity"}
        assert "capital_markets" in set(baseline["dimension_scores"].dimension)
    finally:
        constructor_module.apply_smoothing_experiment = original_apply
        constructor_module.normalize_features = original_normalize
    print("SMOKE TEST 90 — INVENTORY CHALLENGER COMPLETENESS: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
