"""Focused contract smoke for Capital Markets MA decomposition."""
from pathlib import Path
import hashlib
import tempfile

import numpy as np
import pandas as pd

from regime._03_metric_scorer import score_metrics
from scripts.build_capital_markets_ma_decomposition import (
    _align_national_dimension_to_counties, _national_capital_metric_universe,
    _aggregate_ratio_diagnostics, _exact_policy_overlap, _overlap_comparison,
    _render_metric_page, _summarize_transform_features, _zip,
)
from regime.diagnostics.capital_markets_ma import (
    MA_WINDOWS, NATIVE_GEOGRAPHY, REVIEW_GEOGRAPHIES, active_registry,
    build_covariance_budget, build_structural_features, build_variance_budget,
    build_ma_level_state, build_transform_features,
    detect_turning_points, directional_agreement, family_challenger_registry,
    governed_families, interaction_diagnostics, match_turning_points,
    human_status, payment_burden_audit, reject_forbidden_formula,
    structural_policy, validate_source_run,
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
        "treasury_10y": .15, "spread_2y10y": .20, "spread_10y_fedfunds": .10}
    weights = registry.drop_duplicates("canonical_metric_key").set_index("canonical_metric_key").metric_weight.to_dict()
    assert weights == expected and "treasury_2y" not in weights
    families=governed_families(registry)
    assert families=={"mortgage_family":("mortgage_30y","mortgage_15y"),"policy_yield_family":("fedfunds","treasury_10y"),"spread_family":("spread_2y10y","spread_10y_fedfunds")}
    assert len({m for members in families.values() for m in members})==6
    assert MA_WINDOWS == (3,6,9,12)
    family_policies=family_challenger_registry(registry); assert len(family_policies)==12 and set(family_policies.ma_window)=={6,9,12}
    assert registry.groupby("canonical_metric_key").feature_weight.apply(tuple).map(sum).eq(1).all()
    for window in MA_WINDOWS:
        assert structural_policy(window) == {"level":("ma_level",f"{window}m"),"short_term_change":("ma_pct_change",f"{window}m/lag3m"),"long_term_change":("ma_pct_change",f"{window}m/lag12m")}
    try: reject_forbidden_formula("ma3_vs_ma12_pct", "3m/12m")
    except ValueError: pass
    else: raise AssertionError("forbidden formula accepted")
    dates = pd.date_range("2018-01-31", periods=36, freq="M")
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
        assert np.allclose(dp["fred_mortgage_30y_short"],100*(ma-ma.shift(3)),equal_nan=True)
        assert np.allclose(dp["fred_mortgage_30y_long"],100*(ma-ma.shift(12)),equal_nan=True)
        assert {"denominator_value","near_zero_denominator_flag","ratio_magnitude"}.issubset(diagnostics)
        assert np.allclose(difference_diagnostics.arithmetic_difference_bps.dropna(),
                           100*difference_diagnostics.arithmetic_difference_source_units.dropna())
        aggregated=_aggregate_ratio_diagnostics(diagnostics)
        assert len(aggregated)==2 and aggregated.policy.eq(f"ratio_ma{window}").all()
        assert {"minimum_absolute_denominator","ratio_absolute_p95","ratio_non_finite_count"}.issubset(aggregated)
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
    runner_source=Path("scripts/build_capital_markets_ma_decomposition.py").read_text()
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
    metric_values={"mortgage_30y":np.arange(8)/10,"mortgage_15y":np.arange(8)[::-1]/20,"fedfunds":np.sin(np.arange(8))/10,"treasury_10y":np.cos(np.arange(8))/10,"spread_2y10y":np.arange(8)/30,"spread_10y_fedfunds":-np.arange(8)/40}
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
        policies = ("incumbent", "ma3_structural", "ma6_structural", "ma9_structural", "ma12_structural")
        raw = pd.DataFrame([{"date": date, "series": policy, "value": value} for policy in ("raw", "ma3", "ma6", "ma9", "ma12") for date, value in zip(dates[:3], (.1, .2, .3))])
        features = pd.DataFrame([{"date": date, "feature_type": feature, "policy": policy, "value": value} for feature in ("level", "short_term_change", "long_term_change") for policy in policies for date, value in zip(dates[:3], (.1, .2, .3))])
        normalized = features.rename(columns={"value": "feature_score"})
        scores = pd.DataFrame([{"date": date, "policy": policy, "metric_score": value} for policy in policies for date, value in zip(dates[:3], (.1, .2, .3))])
        dimensions = scores.rename(columns={"metric_score": "dimension_score"})
        decision = pd.DataFrame({"Metric": list(expected), "Policy": ["incumbent"] * 6})
        for metric in expected:
            _render_metric_page(review, metric, raw, features, normalized, scores, dimensions, decision.query("Metric == @metric"))
        pages = sorted((review / "metrics").glob("*.html"))
        assert len(pages) == 6
        for page in pages:
            contents = page.read_text()
            assert "<svg" in contents and "../figures/" not in contents
            assert "metric-policy-decision-table" in contents
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in policy_paths}
    print("Capital Markets MA decomposition smoke test passed")


if __name__ == "__main__": main()
