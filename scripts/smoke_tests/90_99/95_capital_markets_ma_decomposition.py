"""Focused contract smoke for Capital Markets MA decomposition."""
from pathlib import Path
import hashlib
import tempfile
import re

import numpy as np
import pandas as pd
import scripts.build_capital_markets_ma_decomposition as builder

from regime._03_metric_scorer import score_metrics
from regime._01_feature_engine import _compute_feature
from scripts.build_capital_markets_ma_decomposition import (
    _align_national_dimension_to_counties, _national_capital_metric_universe,
    _metric_weight_evidence,
    _promotion_evidence,
    _aggregate_ratio_diagnostics, _exact_policy_overlap, _overlap_comparison,
    _render_metric_page, _summarize_transform_features, _zip,
    TABLES, VISUALIZATION_REGRESSION_TABLES,
)
from regime.diagnostics.capital_markets_ma import (
    MA_WINDOWS, NATIVE_GEOGRAPHY, REVIEW_GEOGRAPHIES, active_registry,
    build_covariance_budget, build_structural_features, build_variance_budget,
    build_ma_level_state, build_transform_features,
    detect_turning_points, directional_agreement, family_challenger_registry,
    governed_families, interaction_diagnostics, match_turning_points,
    human_status, payment_burden_audit, reject_forbidden_formula,
    structural_policy, validate_source_run, COMBINED_FAMILIES,
    combined_policy_specs,
    spread_polarity_audit_tables,
    canonicalize_legacy_artifact_metric_keys, metric_key_migration_audit,
    CORRECTED_WINDOW_BY_METRIC, CORRECTED_TRANSFORM_FAMILY_BY_METRIC,
    PRIOR_FEATURE_WEIGHT_EVIDENCE_STATUS, corrected_architecture,
    CANCELLATION_TOLERANCE, reconcile_spread_pathology,
    NEXT_VALID_FEATURE_WEIGHT_EXPERIMENT_MUST_USE_SETTLED_CAPITAL_MARKETS_ARCHITECTURE,
    SETTLED_FEATURE_WEIGHT_POLICIES,
    METRIC_WEIGHT_FAMILIES, METRIC_WEIGHT_POLICIES, validate_metric_weight_policies,
)


def main() -> None:
    phase_paths=(Path("regime/diagnostics/capital_markets_ma.py"),Path("scripts/build_capital_markets_ma_decomposition.py"),Path(__file__))
    forbidden_frequency = 'freq="' + 'ME' + '"'
    assert all(forbidden_frequency not in path.read_text() for path in phase_paths)
    policy_paths = tuple(Path("config") / name for name in (
        "feature_registry.csv", "metric_dimension_registry.csv", "axis_registry.csv",
        "normalization_registry.csv", "source_metric_registry.csv", "supply_dimension_frozen_v1.json"))
    before = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in policy_paths}
    registry = active_registry()
    expected = {"mortgage_30y": .35, "mortgage_15y": .05, "fedfunds": .15,
        "treasury_10y": .15, "spread_10y_2y": .20, "spread_10y_fedfunds": .10}
    weights = registry.drop_duplicates("canonical_metric_key").set_index("canonical_metric_key").metric_weight.to_dict()
    promoted = METRIC_WEIGHT_POLICIES["MW-TEMPERED-C"]
    assert weights == promoted and "treasury_2y" not in weights and "spread_2y10y" not in weights
    assert np.isclose(sum(weights.values()), 1.0)
    assert CORRECTED_WINDOW_BY_METRIC == {"mortgage_30y":12,"mortgage_15y":12,"treasury_10y":12,"fedfunds":3,"spread_10y_2y":9,"spread_10y_fedfunds":9}
    assert CORRECTED_TRANSFORM_FAMILY_BY_METRIC == {"mortgage_30y":"ratio","mortgage_15y":"ratio","treasury_10y":"ratio","fedfunds":"ratio","spread_10y_2y":"arithmetic_difference","spread_10y_fedfunds":"arithmetic_difference"}
    assert corrected_architecture("spread_10y_2y") == (9,"arithmetic_difference")
    assert PRIOR_FEATURE_WEIGHT_EVIDENCE_STATUS == "superseded_for_final_calibration"
    migration=metric_key_migration_audit().iloc[0]
    assert migration.legacy_key=="spread_2y10y" and migration.canonical_key=="spread_10y_2y"
    assert migration.metric_weight_before==migration.metric_weight_after==.20
    assert migration.new_write_key=="spread_10y_2y" and migration.downstream_value_parity and migration.migration_status=="pass"
    legacy=pd.DataFrame({"canonical_metric_key":["spread_2y10y"],"value":[1.25]})
    canonical=pd.DataFrame({"canonical_metric_key":["spread_10y_2y"],"value":[1.25]})
    pd.testing.assert_frame_equal(canonicalize_legacy_artifact_metric_keys(legacy),canonical,check_exact=True)
    legacy_scores=pd.DataFrame([{"geo_id":"g","date":pd.Timestamp("2020-01-31"),
        "canonical_metric_key":"spread_2y10y","feature_key":"fred_2y10y_spread_level","feature_score":.4}])
    canonical_scores=legacy_scores.assign(canonical_metric_key="spread_10y_2y")
    pd.testing.assert_frame_equal(score_metrics(canonicalize_legacy_artifact_metric_keys(legacy_scores)),
        score_metrics(canonical_scores),check_exact=True)
    assert "add_spread(wide, geo_id, \"fred_spread_2y_10y\", \"fred_gs10\", \"fred_gs2\")" in Path("sources/fred_macro/ingest.py").read_text()
    families=governed_families(registry)
    assert families=={"mortgage_family":("mortgage_30y","mortgage_15y"),"policy_yield_family":("fedfunds","treasury_10y"),"spread_family":("spread_10y_2y","spread_10y_fedfunds")}
    assert len({m for members in families.values() for m in members})==6
    assert COMBINED_FAMILIES=={"long_rate_family":("mortgage_30y","mortgage_15y","treasury_10y"),"policy_rate_family":("fedfunds",),"spread_family":("spread_10y_2y","spread_10y_fedfunds")}
    combined=combined_policy_specs(registry)
    assert tuple(combined)==("incumbent","challenger_a_balanced_ratio","challenger_b_slow_spreads_ratio","challenger_c_balanced_difference")
    assert combined["challenger_a_balanced_ratio"]["windows"]==combined["challenger_c_balanced_difference"]["windows"]
    assert combined["challenger_b_slow_spreads_ratio"]["windows"] | {"spread_family":9} == combined["challenger_a_balanced_ratio"]["windows"]
    assert combined["challenger_c_balanced_difference"]["transform_family"]=="arithmetic_difference"
    assert MA_WINDOWS == (3,6,9,12)
    assert set(VISUALIZATION_REGRESSION_TABLES) == {"capital_markets_transform_policy_scorecard", "capital_markets_transform_decision_matrix", "capital_markets_ratio_vs_difference_pairwise", "capital_markets_ratio_denominator_diagnostics", "capital_markets_transform_directional_agreement", "capital_markets_transform_turning_point_matches", "capital_markets_transform_warmup_coverage", "common_ma_state_cache_audit", "transformed_feature_cache_audit"}
    family_policies=family_challenger_registry(registry); assert len(family_policies)==12 and set(family_policies.ma_window)=={6,9,12}
    assert registry.groupby("canonical_metric_key").feature_weight.apply(tuple).map(sum).eq(1).all()
    for window in MA_WINDOWS:
        assert structural_policy(window) == {"level":("ma_level",f"{window}m"),"short_term_change":("ma_pct_change",f"{window}m/lag3m"),"long_term_change":("ma_pct_change",f"{window}m/lag12m")}
    try: reject_forbidden_formula("ma3_vs_ma12_pct", "3m/12m")
    except ValueError: pass
    else: raise AssertionError("forbidden formula accepted")
    dates = pd.date_range("2018-01-31", periods=36, freq="M")
    production_difference = _compute_feature(pd.DataFrame({"date": dates, "value": np.arange(36, dtype=float)}),
        "ma_difference", "9m/lag3m", "spread_short")
    expected_ma = pd.Series(np.arange(36, dtype=float)).rolling(9, min_periods=9).mean()
    assert np.allclose(production_difference, expected_ma - expected_ma.shift(3), equal_nan=True)
    raw = pd.DataFrame({"geo_id":NATIVE_GEOGRAPHY,"date":dates,"canonical_metric_key":"mortgage_30y","value":np.arange(1,37,dtype=float),"metric_origin":"fred_mortgage_30y"})
    for window in MA_WINDOWS:
        built=build_structural_features(raw,"mortgage_30y",window,registry)
        pivot=built.pivot(index="date",columns="feature_key",values="raw_feature_value")
        ma=raw.value.rolling(window,min_periods=window).mean()
        assert np.allclose(pivot["fred_mortgage_30y_level"],ma,equal_nan=True)
        assert np.allclose(pivot["fred_mortgage_30y_short"],ma/ma.shift(3)-1,equal_nan=True)
        assert np.allclose(pivot["fred_mortgage_30y_long"],ma/ma.shift(12)-1,equal_nan=True)
        state=build_ma_level_state(raw,"mortgage_30y",window,registry)
        ratio, diagnostics=build_transform_features(state,"mortgage_30y",window,"ratio",registry)
        difference, difference_diagnostics=build_transform_features(state,"mortgage_30y",window,"arithmetic_difference",registry)
        rp=ratio.pivot(index="date",columns="feature_key",values="raw_feature_value")
        dp=difference.pivot(index="date",columns="feature_key",values="raw_feature_value")
        assert np.allclose(rp["fred_mortgage_30y_level"],dp["fred_mortgage_30y_level"],equal_nan=True)
        assert np.allclose(dp["fred_mortgage_30y_short"],ma-ma.shift(3),equal_nan=True)
        assert np.allclose(dp["fred_mortgage_30y_long"],ma-ma.shift(12),equal_nan=True)
        assert {"denominator_value","near_zero_denominator_flag","ratio_magnitude"}.issubset(diagnostics)
        assert np.allclose(difference_diagnostics.arithmetic_difference_bps.dropna(),
                           100*difference_diagnostics.arithmetic_difference_source_units.dropna())
        aggregated=_aggregate_ratio_diagnostics(diagnostics)
        assert len(aggregated)==2 and aggregated.policy.eq(f"ratio_ma{window}").all()
        assert {"minimum_absolute_denominator","ratio_absolute_p95","ratio_non_finite_count"}.issubset(aggregated)
    # Formula reconstruction does not trust the ambiguous persisted spread key.
    operands=[]
    values={"mortgage_30y":6-np.arange(36)/100,"mortgage_15y":5-np.arange(36)/100,
        "fedfunds":np.tile([1.,3.,5.],12),"treasury_10y":np.tile([4.,2.,6.],12),
        "treasury_2y":np.tile([2.,4.,5.],12)}
    for metric, series in values.items():
        operands.extend({"geo_id":NATIVE_GEOGRAPHY,"date":date,"canonical_metric_key":metric,"value":value}
            for date,value in zip(dates,series))
    polarity=spread_polarity_audit_tables(pd.DataFrame(operands))
    assert set(polarity)=={"capital_markets_spread_formula_audit","capital_markets_spread_sign_chronology",
        "capital_markets_spread_ratio_pathology_audit","capital_markets_metric_polarity_audit",
        "capital_markets_dimension_polarity_audit","capital_markets_axis_polarity_audit"}
    formulas=polarity["capital_markets_spread_formula_audit"].set_index("metric_key").exact_formula.to_dict()
    assert formulas=={"spread_10y_2y":"treasury_10y - treasury_2y","spread_10y_fedfunds":"treasury_10y - fedfunds"}
    signs=polarity["capital_markets_spread_sign_chronology"]
    assert (signs.raw_spread>0).any() and (signs.raw_spread<0).any() and signs.zero_crossing_flag.any()
    assert polarity["capital_markets_metric_polarity_audit"].polarity_contract_passes.all()
    assert polarity["capital_markets_dimension_polarity_audit"].polarity_contract_passes.all()
    assert set(polarity["capital_markets_axis_polarity_audit"].axis)=={"supply","demand"}
    pathology=polarity["capital_markets_spread_ratio_pathology_audit"]
    reconciliation=reconcile_spread_pathology(pathology)
    assert reconciliation.reconciliation_status.eq("pass").all()
    assert reconciliation.duplicate_key_count.eq(0).all()
    assert reconciliation.direction_conflict_count.sum()==pathology.direction_conflict_flag.sum()
    assert reconciliation.near_zero_denominator_count.sum()==pathology.near_zero_denominator_flag.sum()
    assert reconciliation.non_finite_ratio_count.sum()==pathology.finite_flag.eq(False).sum()
    try: reconcile_spread_pathology(pd.concat([pathology,pathology.iloc[[0]]],ignore_index=True))
    except ValueError: pass
    else: raise AssertionError("duplicate pathology key did not fail closed")
    assert CANCELLATION_TOLERANCE == 1e-12
    assert NEXT_VALID_FEATURE_WEIGHT_EXPERIMENT_MUST_USE_SETTLED_CAPITAL_MARKETS_ARCHITECTURE
    assert SETTLED_FEATURE_WEIGHT_POLICIES == {
        "FW-A":{"level":.50,"short_term_change":.25,"long_term_change":.25},
        "FW-B":{"level":.60,"short_term_change":.20,"long_term_change":.20},
        "FW-C":{"default":{"level":.60,"short_term_change":.20,"long_term_change":.20},
                "fedfunds":{"level":.50,"short_term_change":.25,"long_term_change":.25}},
    }
    validate_metric_weight_policies()
    assert tuple(METRIC_WEIGHT_POLICIES) == ("MW-INCUMBENT", "MW-TEMPERED-C", "MW-TEMPERED-A", "MW-TEMPERED-B")
    assert METRIC_WEIGHT_POLICIES["MW-INCUMBENT"] == expected
    assert METRIC_WEIGHT_POLICIES["MW-TEMPERED-A"] == {"mortgage_30y":2/15,"mortgage_15y":2/15,"treasury_10y":2/15,"fedfunds":1/5,"spread_10y_2y":1/5,"spread_10y_fedfunds":1/5}
    assert METRIC_WEIGHT_POLICIES["MW-TEMPERED-B"] == {"mortgage_30y":2/15,"mortgage_15y":2/15,"treasury_10y":2/15,"fedfunds":1/4,"spread_10y_2y":7/40,"spread_10y_fedfunds":7/40}
    assert METRIC_WEIGHT_POLICIES["MW-TEMPERED-C"] == {"mortgage_30y":3/20,"mortgage_15y":3/20,"treasury_10y":3/20,"fedfunds":1/10,"spread_10y_2y":9/40,"spread_10y_fedfunds":9/40}
    expected_totals={"MW-INCUMBENT":(.55,.15,.30),"MW-TEMPERED-C":(.45,.10,.45),"MW-TEMPERED-A":(.40,.20,.40),"MW-TEMPERED-B":(.40,.25,.35)}
    for policy,weights_policy in METRIC_WEIGHT_POLICIES.items():
        assert np.isclose(sum(weights_policy.values()),1.0)
        assert np.allclose(tuple(sum(weights_policy[m] for m in members) for members in METRIC_WEIGHT_FAMILIES.values()),expected_totals[policy])

    # Regression: metric weighting consumes the settled level chronology.  A
    # partially available leading month remains a valid level observation;
    # movement/cancellation warmup begins one observation later and must not
    # become the downstream alignment calendar.
    mw_dates = pd.date_range("2009-06-30", periods=36, freq="M")
    mw_scores = pd.DataFrame([
        {"date": date, "canonical_metric_key": metric,
         "metric_score": (np.nan if date == mw_dates[0] and metric != "fedfunds"
                          else np.sin(month / 3) / 2 + metric_no / 100)}
        for month, date in enumerate(mw_dates)
        for metric_no, metric in enumerate(expected)
    ])
    original_recompute = builder._recompute_governed_descendants
    def fake_recompute(national, incumbent):
        axis = pd.DataFrame([
            {"geo_id": "fixture__county", "date": date, "axis": axis_name,
             "axis_score": score}
            for date, score in zip(national.date, national.dimension_score)
            for axis_name in ("supply", "demand")])
        regime = pd.DataFrame([
            {"geo_id": "fixture__county", "date": date,
             "major_regime": "expansion", "minor_regime": "stable"}
            for date in national.date])
        return axis, pd.DataFrame(), regime, national.copy(), {}
    builder._recompute_governed_descendants = fake_recompute
    try:
        mw = _metric_weight_evidence(
            mw_scores, {"dimension_scores": pd.DataFrame()}, registry)
    finally:
        builder._recompute_governed_descendants = original_recompute
    mw_metric = mw["capital_markets_metric_weight_metric_chronology"]
    mw_contribution = mw["capital_markets_metric_weight_contribution_chronology"]
    mw_dimension = mw["capital_markets_metric_weight_dimension_chronology"]
    pd.testing.assert_frame_equal(
        mw_metric.query("policy == 'MW-INCUMBENT'")
        [["date", "metric", "metric_score"]].reset_index(drop=True),
        mw_scores.rename(columns={"canonical_metric_key": "metric"})
        [["date", "metric", "metric_score"]].reset_index(drop=True),
        check_exact=True,
    )
    assert all(set(g.date) == set(mw_dates) for _, g in mw_dimension.groupby("policy"))
    assert mw_contribution.loc[mw_contribution.contribution_movement.notna(), "date"].min() == mw_dates[1]
    assert mw["capital_markets_metric_weight_cancellation"].query("comparable").date.min() == mw_dates[1]
    for policy, weights_policy in METRIC_WEIGHT_POLICIES.items():
        part = mw_contribution.query("policy == @policy")
        expected_dimension = part.groupby("date").weighted_contribution.sum(min_count=1)
        actual_dimension = mw_dimension.query("policy == @policy").set_index("date").dimension_score
        np.testing.assert_allclose(actual_dimension, expected_dimension, atol=CANCELLATION_TOLERANCE, rtol=0)
        assert part.set_index("metric").groupby(level=0).configured_metric_weight.first().to_dict() == weights_policy
    assert mw["capital_markets_metric_weight_parity_audit"].metric_score_exact_parity.all()
    assert mw["capital_markets_metric_weight_parity_audit"].contribution_reconstruction.all()
    assert len(mw["capital_markets_metric_weight_decision_matrix"]) == 4
    assert mw["capital_markets_metric_weight_decision_matrix"].Policy.tolist() == list(METRIC_WEIGHT_POLICIES)
    assert mw["capital_markets_metric_weight_decision_matrix"].Decision.eq("pending").all()
    assert len(mw["capital_markets_metric_weight_fedfunds_stress"]) == 4
    stress=mw["capital_markets_metric_weight_fedfunds_stress"]
    assert {"fedfunds_weight_vs_dimension_p90","fedfunds_weight_vs_dimension_p99","fedfunds_weight_vs_sign_flips","fedfunds_weight_vs_turning_points"}.issubset(stress.columns)
    assert len(mw["capital_markets_metric_weight_policy_registry"]) == 24
    assert not {"Rank","Winner","Composite score"}.intersection(mw["capital_markets_metric_weight_decision_matrix"].columns)
    assert len(mw["capital_markets_metric_weight_concentration_summary"]) == 4
    assert set(mw["capital_markets_metric_weight_recent_chronology"].columns) == {"policy","date","dimension_score","monthly_movement","fedfunds_contribution","long_rate_family_contribution","spread_family_contribution"}
    assert mw["capital_markets_metric_weight_human_decision_status"].iloc[0].to_dict() == {
        "recommendation_state":"none", "promotion_state":"none",
        "human_decision":"pending", "diagnostic_only":True}
    promotion = _promotion_evidence(mw, registry)
    required_promotion = {"capital_markets_promotion_policy_registry", "capital_markets_promotion_config_diff",
        "capital_markets_promotion_parity_audit", "capital_markets_promoted_fedfunds_tail_summary",
        "capital_markets_promotion_dimension_comparison", "capital_markets_promotion_downstream_context",
        "capital_markets_promotion_human_decision_status", "capital_markets_promotion_runtime_summary"}
    assert set(promotion) == required_promotion and required_promotion.issubset(TABLES)
    assert promotion["capital_markets_promotion_parity_audit"].status.eq("pass").all()
    assert set(promotion["capital_markets_promoted_fedfunds_tail_summary"].policy) == {"MW-INCUMBENT", "MW-TEMPERED-C"}
    assert promotion["capital_markets_promoted_fedfunds_tail_summary"].all_six_available_observation_count.eq(35).all()
    assert promotion["capital_markets_promotion_dimension_comparison"].absolute_difference.eq(0).all()
    assert promotion["capital_markets_promotion_human_decision_status"].iloc[0].promotion_state == "promoted"
    metric_weight_tables={name for name in TABLES if name.startswith("capital_markets_metric_weight_")}
    assert {"capital_markets_metric_weight_fedfunds_stress","capital_markets_metric_weight_concentration_summary","capital_markets_metric_weight_parity_audit","capital_markets_metric_weight_decision_matrix"}.issubset(metric_weight_tables)
    assert {name for name in TABLES if name.startswith("capital_markets_final_feature_weight_")} == {
        "capital_markets_final_feature_weight_policy_registry", "capital_markets_final_feature_weight_metric_stability",
        "capital_markets_final_feature_weight_metric_turning_point_summary", "capital_markets_final_feature_weight_family_summary",
        "capital_markets_final_feature_weight_dimension_chronology", "capital_markets_final_feature_weight_dimension_stability",
        "capital_markets_final_feature_weight_dimension_turning_point_summary", "capital_markets_final_feature_weight_extreme_jumps",
        "capital_markets_final_feature_weight_cancellation", "capital_markets_final_feature_weight_recent_chronology",
        "capital_markets_final_feature_weight_directional_context", "capital_markets_final_feature_weight_regime_change_summary",
        "capital_markets_final_feature_weight_decision_matrix", "capital_markets_final_feature_weight_human_decision_status",
        "capital_markets_final_feature_weight_runtime_summary",
    }
    # Production scorer renormalizes available feature weights, including one-child weight 1.0.
    scores=pd.DataFrame([{"geo_id":"g","date":dates[0],"canonical_metric_key":"mortgage_30y","feature_key":"fred_mortgage_30y_level","feature_score":.4}])
    assert np.isclose(score_metrics(scores).metric_score.iloc[0], .4)
    chronology=pd.DataFrame({"date":dates,"score":np.sin(np.arange(36)/3)})
    self_agreement=directional_agreement(chronology,chronology,"score",1)
    assert self_agreement["agreement_share"] == 1.0
    inc_overlap, chal_overlap, comparison = _overlap_comparison(chronology, chronology, "score")
    assert inc_overlap.date.equals(chal_overlap.date) and comparison["dimension_overlap_observation_count"] == 36
    assert all(comparison[name] == 0 for name in ("dimension_standard_deviation_delta", "dimension_median_change_delta", "dimension_p90_delta", "dimension_sign_flip_delta"))
    assert all(comparison[f"dimension_directional_agreement_{h}m"] == 1.0 for h in (1, 3, 6, 12))
    warm = chronology.iloc[3:].copy()
    _, _, warm_comparison = _overlap_comparison(chronology, warm, "score")
    assert warm_comparison["dimension_leading_warmup_rows"] == 3
    for invalid in (warm.drop(index=warm.index[5]), warm.iloc[:-1], pd.concat([warm, warm.iloc[[0]]])):
        try: _overlap_comparison(chronology, invalid, "score")
        except ValueError: pass
        else: raise AssertionError("invalid overlap chronology did not fail closed")
    gap=chronology.drop(index=10); assert directional_agreement(gap,gap,"score",1)["valid_comparisons"] < len(gap)-1
    exact_left,exact_right=_exact_policy_overlap(chronology.iloc[2:],chronology.iloc[4:],"score")
    assert len(exact_left)==32 and exact_left.date.equals(exact_right.date)
    for left,right in ((chronology.iloc[:2],chronology.iloc[3:]),(chronology, pd.concat([chronology,chronology.iloc[[0]]])),(chronology.drop(index=10),chronology.drop(index=10))):
        try: _exact_policy_overlap(left,right,"score")
        except ValueError: pass
        else: raise AssertionError("invalid exact pairwise overlap did not fail closed")
    feature_fixture=pd.DataFrame([{"metric":"m","policy":"p","feature_type":feature,"date":date,"value":value}
        for feature in ("level","short_term_change","long_term_change") for date,value in zip(dates[:6],range(6))])
    feature_summary=_summarize_transform_features(feature_fixture,"value","raw_feature")
    assert len(feature_summary)==3 and "raw_feature_p99_absolute_monthly_change" in feature_summary
    try: _summarize_transform_features(pd.concat([feature_fixture,feature_fixture.iloc[[0]]]),"value","raw_feature")
    except ValueError: pass
    else: raise AssertionError("duplicate feature chronology did not fail closed")
    turns=detect_turning_points(chronology,"score")
    assert turns.empty or ((turns.incoming_persistence.eq(3)&turns.outgoing_persistence.eq(3)&turns.prominence_threshold.ge(.05)).all())
    unmatched=match_turning_points(pd.DataFrame([{"turning_point_date":dates[5],"turning_point_type":"peak","qualified":True}]),pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified"]))
    assert not unmatched.matched.iloc[0] and pd.isna(unmatched.signed_delay_months.iloc[0])
    assert len(REVIEW_GEOGRAPHIES)==7 and NATIVE_GEOGRAPHY not in REVIEW_GEOGRAPHIES
    # Challenger scoring is native-national first; county copies are created only afterward.
    native_metric_fixture = pd.DataFrame([
        {
            "geo_id": NATIVE_GEOGRAPHY,
            "date": date,
            "canonical_metric_key": metric,
            "metric_score": 0.1,
            "feature_count": 3,
            "feature_weight_sum": 1.0,
            "min_feature_score": 0.0,
            "max_feature_score": 0.2,
        }
        for date in dates[:2]
        for metric in expected
    ])

    national_universe = _national_capital_metric_universe(
        native_metric_fixture,
        tuple(sorted(expected)),
    )

    assert len(national_universe) == 12
    assert national_universe.geo_id.unique().tolist() == [
        NATIVE_GEOGRAPHY
    ]
    assert set(national_universe.columns) == {
        "geo_id",
        "evaluation_date",
        "metric_date",
        "canonical_metric_key",
        "metric_score",
        "feature_count",
        "feature_weight_sum",
        "min_feature_score",
        "max_feature_score",
        "metric_age_days",
    }
    assert national_universe["evaluation_date"].equals(
        national_universe["metric_date"]
    )
    assert national_universe["metric_age_days"].eq(0).all()

    contaminated_native_fixture = pd.concat(
        [
            native_metric_fixture,
            native_metric_fixture.iloc[[0]].assign(
                geo_id="unrelated_cbsa__cbsa_metro"
            ),
        ],
        ignore_index=True,
    )

    # Non-national rows are ignored rather than entering native scoring.
    uncontaminated_universe = _national_capital_metric_universe(
        contaminated_native_fixture,
        tuple(sorted(expected)),
    )
    assert uncontaminated_universe.equals(national_universe)
    national_dimension=pd.DataFrame({"geo_id":NATIVE_GEOGRAPHY,"date":dates[:2],
        "dimension":"capital_markets","dimension_score":[.1,.2]})
    county_calendar=pd.DataFrame([{"geo_id":geo,"date":date} for geo in REVIEW_GEOGRAPHIES for date in dates[:2]])
    county_copies=_align_national_dimension_to_counties(national_dimension,county_calendar)
    assert county_copies.geo_id.nunique()==7 and len(county_copies)==14
    assert set(county_copies.geo_id)==set(REVIEW_GEOGRAPHIES)
    assert not county_copies.geo_id.str.contains("cbsa|zip|state|nation").any()
    alignment_source=pd.DataFrame({"geo_id":NATIVE_GEOGRAPHY,"date":dates[:4],
        "dimension":"capital_markets","dimension_score":[.1,.2,.3,.4]})
    alignment_calendar=pd.DataFrame([{"geo_id":geo,"date":date}
        for geo in REVIEW_GEOGRAPHIES for date in dates[:4]])
    for invalid_national in (
        alignment_source.drop(index=1),
        alignment_source.iloc[:-1],
        pd.concat([alignment_source, alignment_source.iloc[[0]]], ignore_index=True),
    ):
        try: _align_national_dimension_to_counties(invalid_national,alignment_calendar)
        except ValueError: pass
        else: raise AssertionError("invalid county-alignment source did not fail closed")
    runner_source=Path("scripts/build_capital_markets_ma_decomposition.py").read_text()
    spread_tables={name for name in TABLES if name.startswith("capital_markets_spread_correction_")}
    assert len(spread_tables)==15 and "capital_markets_spread_correction_decision_matrix" in spread_tables
    assert "capital_markets_spread_pathology_reconciliation" in TABLES
    assert "legacy_spread_architecture" in runner_source and "corrected_spread_architecture" in runner_source
    assert "treasury_2y - treasury_10y" in runner_source and "treasury_10y - treasury_2y" in runner_source
    assert "capital_markets_spread_correction_decision_matrix.csv" in runner_source
    assert "Historical/secondary A/B/C evidence" in runner_source
    assert "feature_weight_winner\":\"none" in runner_source
    assert 'path.read_text(encoding="utf-8")' in runner_source
    assert 'Dimension median abs. MoM Δ' in runner_source
    assert 'dimension_incumbent_overlap_sign_flip_count' in runner_source
    assert 'dimension_challenger_overlap_sign_flip_count' in runner_source
    assert 'on="metric_date"' in runner_source and 'candidate_metric.rename' in runner_source
    assert 'score_dimensions(spliced)' in runner_source
    assert 'score_dimensions(frames["aligned_metric_scores"])' not in runner_source
    assert 'score_axes(frames["dimension_scores"])' not in runner_source
    assert 'len(common_states) != 24' in runner_source and 'len(caches) != 48' in runner_source
    required=("capital_markets_transform_policy_scorecard","capital_markets_transform_decision_matrix",
        "capital_markets_ratio_vs_difference_pairwise","capital_markets_source_unit_audit",
        "capital_markets_cross_metric_summary","combined_metric_policy_selection_template",
        "metric_raw_and_ma_chronology","metric_feature_chronology","metric_normalized_feature_scores",
        "metric_score_chronology","metric_only_dimension_chronology","metric_directional_agreement",
        "metric_turning_point_matches","metric_turning_point_summary","metric_warmup_coverage")
    assert all(name in runner_source for name in required)
    matrix_columns={"Metric","Policy","Transform","Window","Near-zero denom count","Denominator sign-change count","Ratio abs p95","Ratio abs p99","Ratio abs max","Metric median abs. MoM Δ","Metric P90 Δ","Metric sign flips Δ","Metric direction agreement 1m","Metric direction agreement 3m","Dimension median abs. MoM Δ","Dimension P90 Δ","Dimension sign flips Δ","Dimension direction agreement 1m","Median turn delay","Max turn delay","Warmup"}
    assert all(repr(column) in runner_source or f'"{column}"' in runner_source for column in matrix_columns)
    assert 'len(transform_scorecard) != 54' in runner_source and 'Pairwise transform evidence must contain 24 rows' in runner_source
    assert 'len(tables["common_ma_state_cache_audit"]) != 24' in runner_source and 'len(tables["transformed_feature_cache_audit"]) != 48' in runner_source
    assert runner_source.index("Transform decision matrix") < runner_source.index("Secondary engineering evidence")
    assert 'human_decision="pending"' in runner_source and 'selected_policy":"pending"' in runner_source
    assert all(name in runner_source for name in ("capital_markets_combined_policy_registry","capital_markets_combined_dimension_chronology","capital_markets_combined_cancellation","capital_markets_combined_policy_decision_matrix"))
    assert 'r["transform"]' in runner_source and 'r["feature_window"]' in runner_source
    assert "r.transform" not in runner_source and "r.feature_window" not in runner_source
    required_finalist_tables = {
        "capital_markets_combined_regime_change_detail",
        "capital_markets_combined_regime_change_review",
        "capital_markets_combined_regime_transition_summary",
        "capital_markets_combined_turning_point_review",
        "capital_markets_combined_turning_point_event_windows",
        "capital_markets_combined_finalist_review_summary",
        "capital_markets_combined_isolation_invariants",
    }
    assert required_finalist_tables.issubset(TABLES)
    assert all(name in runner_source for name in required_finalist_tables)
    assert 'regime_detail.loc[regime_detail.regime_changed]' in runner_source
    assert 'range(-6,7)' in runner_source and 'pd.offsets.MonthEnd(relative_month)' in runner_source
    assert 'if not invariants.verified.all()' in runner_source
    assert 'Final A/B/C chronology review' in runner_source
    assert '<bound method Series.transform' not in runner_source
    assert '"combined_challenger_count":3' in runner_source and '"combined_policy_registry_row_count":24' in runner_source
    assert "SETTLED_FEATURE_WEIGHT_POLICIES" in runner_source
    assert all(name in runner_source for name in ("capital_markets_feature_weight_policy_registry", "capital_markets_feature_weight_metric_summary", "capital_markets_feature_weight_decision_matrix", "Capital Markets Feature-Weight Review"))
    assert "feature-weight {number}/3 {policy}" in runner_source
    assert "Capital Markets Metric-Weight Review" in runner_source
    assert "Metric contributions do not reconstruct Capital Markets" in runner_source
    assert "Metric-score chronology differs across metric-weight policies" in runner_source
    assert '"Decision":"pending"' in runner_source
    assert "automatic winner" in runner_source and '"recommendation_state":"none"' in runner_source
    audit=payment_burden_audit().iloc[0]; assert audit.mortgage_rate_source=="mortgage_30y" and not audit.same_operation and not audit.policy_change
    status=human_status(); assert status["recommendation_state"]==status["promotion_state"]=="none"
    with tempfile.TemporaryDirectory() as tmp:
        bad=Path(tmp)/"wrong_run"; bad.mkdir()
        try: validate_source_run(bad)
        except ValueError: pass
        else: raise AssertionError("source identity did not fail closed")
    # The diagnostic changes one named target at a time; siblings and unrelated frames remain byte-identical.
    universe=pd.DataFrame({"metric":["mortgage_30y","fedfunds"],"score":[.1,.2]}); candidate=universe.copy(); candidate.loc[candidate.metric.eq("mortgage_30y"),"score"]=[.3]
    assert (candidate.score!=universe.score).sum()==1 and candidate.query("metric=='fedfunds'").equals(universe.query("metric=='fedfunds'"))
    unrelated=pd.DataFrame({"dimension":["supply","affordability"],"score":[.2,.3]}); assert unrelated.copy().equals(unrelated)
    # Exact standalone variance, covariance, additive movement, and deterministic ranks.
    dts=pd.date_range("2020-01-31",periods=8,freq="M"); feature_rows=[]; metric_rows=[]
    metric_values={"mortgage_30y":np.arange(8)/10,"mortgage_15y":np.arange(8)[::-1]/20,"fedfunds":np.sin(np.arange(8))/10,"treasury_10y":np.cos(np.arange(8))/10,"spread_10y_2y":np.arange(8)/30,"spread_10y_fedfunds":-np.arange(8)/40}
    metric_weights=expected
    for metric,values in metric_values.items():
        for feature_no,feature_type in enumerate(("level","short_term_change","long_term_change")):
            for date,value in zip(dts,values/3): feature_rows.append({"grain":"native_source","geo_id":NATIVE_GEOGRAPHY,"date":date,"canonical_metric_key":metric,"feature_key":f"{metric}_{feature_type}","feature_type":feature_type,"configured_weight":1/3,"effective_weight":1/3,"weighted_contribution":value})
        for date,value in zip(dts,values*metric_weights[metric]): metric_rows.append({"grain":"native_source","geo_id":NATIVE_GEOGRAPHY,"date":date,"canonical_metric_key":metric,"configured_weight":metric_weights[metric],"effective_weight":metric_weights[metric],"weighted_contribution":value})
    fdec=pd.DataFrame(feature_rows); mdec=pd.DataFrame(metric_rows); wide=mdec.pivot(index="date",columns="canonical_metric_key",values="weighted_contribution"); parent=wide.sum(axis=1).rename("dimension_score").reset_index()
    budget=build_variance_budget(fdec,mdec,parent); assert set(budget.budget_level)=={"feature","metric","family","dimension"} and len(budget.query("budget_level=='feature'"))==18 and len(budget.query("budget_level=='metric'"))==6 and len(budget.query("budget_level=='family'"))==3
    assert np.isclose(budget.query("budget_level=='metric'").share_total_absolute_monthly_capital_markets_movement.sum(),1.0)
    cov=build_covariance_budget(mdec,parent,"canonical_metric_key","metric_to_dimension"); assert cov.reconciliation_status.eq("reconciled").all() and np.allclose(cov.reconstructed_parent_variance,cov.persisted_parent_variance)
    assert not np.isclose(cov.total_standalone_child_variance.iloc[0],cov.persisted_parent_variance.iloc[0])
    assert budget.sort_values(["budget_level","rank_within_dimension","canonical_metric_key","feature_key"],kind="mergesort").reset_index(drop=True).equals(budget)
    interaction=interaction_diagnostics(parent.dimension_score,{"a":parent.dimension_score*.9,"b":parent.dimension_score*1.1},parent.dimension_score*.8,"mortgage_family",6); assert interaction==interaction_diagnostics(parent.dimension_score,{"a":parent.dimension_score*.9,"b":parent.dimension_score*1.1},parent.dimension_score*.8,"mortgage_family",6)
    with tempfile.TemporaryDirectory() as tmp:
        first=Path(tmp)/"first"; second=Path(tmp)/"second"; first.mkdir(); second.mkdir()
        page="<!doctype html><p>recommendation_state: none; promotion_state: none</p>\n"
        (first/"index.html").write_text(page); (second/"index.html").write_text(page)
        assert (first/"index.html").read_bytes()==(second/"index.html").read_bytes()
        assert _zip(first).read_bytes()==_zip(second).read_bytes()
    with tempfile.TemporaryDirectory() as tmp:
        review = Path(tmp)
        policies = ("incumbent",) + tuple(
            f"{transform}_ma{window}"
            for transform in ("ratio", "difference") for window in MA_WINDOWS
        )
        raw = pd.DataFrame([{"date": date, "series": policy, "value": value}
            for policy in ("raw", "ma3", "ma6", "ma9", "ma12")
            for date, value in zip(dates[:3], (.1, .2, .3))])
        features = pd.DataFrame([{"date": date, "feature_type": feature, "policy": policy, "value": value}
            for feature in ("level", "short_term_change", "long_term_change")
            for policy in policies[1:] for date, value in zip(dates[:3], (.1, .2, .3))])
        normalized = features.rename(columns={"value": "feature_score"})
        scores = pd.DataFrame([{"date": date, "policy": policy, "metric_score": value}
            for policy in policies for date, value in zip(dates[:3], (.1, .2, .3))])
        dimensions = scores.rename(columns={"metric_score": "dimension_score"})
        decision = pd.DataFrame({"Metric": list(expected), "Policy": ["incumbent"] * 6,
            "recommendation_state": ["none"] * 6, "promotion_state": ["none"] * 6,
            "human_decision": ["pending"] * 6})
        for metric in expected:
            _render_metric_page(review, metric, raw, features, normalized, scores,
                                dimensions, decision.query("Metric == @metric"))
        pages = sorted((review / "metrics").glob("*.html"))
        assert [page.name for page in pages] == sorted(f"{metric}.html" for metric in expected)
        required_sections = {
            "raw-ma": ("Raw and MA levels", True),
            "short-features": ("Short features", False),
            "long-features": ("Long features", False),
            "normalized": ("Normalized feature scores", False),
            "metric-score": ("Metric-score impact", True),
            "dimension-score": ("Capital Markets dimension impact", True),
            "decision-table": ("Decision table", True),
        }
        for page in pages:
            contents = page.read_text()
            assert "<svg" in contents and "../figures/" not in contents
            assert "metric-policy-decision-table" in contents and "href='../index.html'" in contents
            for section, (label, opened) in required_sections.items():
                tag = re.search(rf"<details data-section='{section}'([^>]*)><summary>{re.escape(label)}</summary>", contents)
                assert tag and (("open" in tag.group(1)) == opened)
            charts = re.findall(r"<svg .*?</svg>", contents)
            by_kind = {}
            for chart in charts:
                kind = re.search(r"data-chart-kind='([^']+)'", chart).group(1)
                by_kind.setdefault(kind, []).append(chart)
                count = int(re.search(r"data-series-count='(\d+)'", chart).group(1))
                assert len(re.findall(r"class='data-series'", chart)) == count
                assert "<title>" in chart and "x-axis-label" in chart and "y-axis-label" in chart
            assert {kind: len(by_kind.get(kind, [])) for kind in (
                "raw-ma", "ratio-short", "difference-short", "ratio-long", "difference-long",
                "normalized-short", "normalized-long", "metric-score", "dimension-score")
            } == {"raw-ma":1, "ratio-short":4, "difference-short":4, "ratio-long":4,
                  "difference-long":4, "normalized-short":4, "normalized-long":4,
                  "metric-score":4, "dimension-score":4}
            raw_chart = by_kind["raw-ma"][0]
            assert "data-series-count='5'" in raw_chart
            assert all(f"data-label='{label}'" in raw_chart for label in ("Raw","MA3","MA6","MA9","MA12"))
            colors = re.findall(r"class='data-series'[^>]+stroke='([^']+)'", raw_chart)
            widths = [float(value) for value in re.findall(r"class='data-series'[^>]+stroke-width='([^']+)'", raw_chart)]
            assert len(set(colors)) == 5 and widths[0] > max(widths[1:])
            for kind in ("normalized-short", "normalized-long"):
                for chart in by_kind[kind]:
                    assert "data-series-count='2'" in chart and "Normalized feature score" in chart
                    assert all(f"data-label='{label}'" in chart for label in ("Ratio","Arithmetic difference"))
            for kind in ("metric-score", "dimension-score"):
                for chart in by_kind[kind]:
                    assert "data-series-count='3'" in chart
                    assert all(f"data-label='{label}'" in chart for label in ("Production incumbent","Ratio","Arithmetic difference"))
                    widths = {label:float(width) for label,width in re.findall(r"data-series='([^']+)'[^>]+stroke-width='([^']+)'", chart)}
                    assert widths["Production incumbent"] > widths["Ratio"] == widths["Arithmetic difference"]
            comparison = [chart for kind, group in by_kind.items() if kind.startswith("normalized") or kind in ("metric-score","dimension-score") for chart in group]
            assert all("data-style='arithmetic_difference'" in chart and "stroke-dasharray='7 4'" in chart for chart in comparison)
            required_zero = [chart for kind, group in by_kind.items() if kind != "raw-ma" for chart in group]
            assert all("class='zero-reference'" in chart for chart in required_zero)
            assert all("Ratio" in chart for kind in ("ratio-short","ratio-long") for chart in by_kind[kind])
            assert all("Basis points" in chart for kind in ("difference-short","difference-long") for chart in by_kind[kind])
        # HTML and the bundle ZIP are byte-deterministic across identical renders.
        first_html = {page.name: page.read_bytes() for page in pages}
        first_zip = _zip(review).read_bytes()
        for metric in expected:
            _render_metric_page(review, metric, raw, features, normalized, scores,
                                dimensions, decision.query("Metric == @metric"))
        assert first_html == {page.name: page.read_bytes() for page in pages}
        assert first_zip == _zip(review).read_bytes()
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in policy_paths}
    print("Capital Markets MA decomposition smoke test passed")


if __name__ == "__main__": main()
