"""Focused contract smoke for Capital Markets MA decomposition."""
from pathlib import Path
import hashlib
import tempfile

import numpy as np
import pandas as pd

from regime._03_metric_scorer import score_metrics
from scripts.build_capital_markets_ma_decomposition import _zip
from regime.diagnostics.capital_markets_ma import (
    MA_WINDOWS, NATIVE_GEOGRAPHY, REVIEW_GEOGRAPHIES, active_registry,
    build_covariance_budget, build_structural_features, build_variance_budget,
    detect_turning_points, directional_agreement, family_challenger_registry,
    governed_families, interaction_diagnostics, match_turning_points,
    human_status, payment_burden_audit, reject_forbidden_formula,
    structural_policy, validate_source_run,
)


def main() -> None:
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
    family_policies=family_challenger_registry(registry); assert len(family_policies)==12 and set(family_policies.ma_window)=={6,9,12}
    assert registry.groupby("canonical_metric_key").feature_weight.apply(tuple).map(sum).eq(1).all()
    for window in MA_WINDOWS:
        assert structural_policy(window) == {"level":("ma_level",f"{window}m"),"short_term_change":("ma_pct_change",f"{window}m/lag3m"),"long_term_change":("ma_pct_change",f"{window}m/lag12m")}
    try: reject_forbidden_formula("ma3_vs_ma12_pct", "3m/12m")
    except ValueError: pass
    else: raise AssertionError("forbidden formula accepted")
    dates = pd.date_range("2018-01-31", periods=36, freq="ME")
    raw = pd.DataFrame({"geo_id":NATIVE_GEOGRAPHY,"date":dates,"canonical_metric_key":"mortgage_30y","value":np.arange(1,37,dtype=float),"metric_origin":"fred_mortgage_30y"})
    for window in MA_WINDOWS:
        built=build_structural_features(raw,"mortgage_30y",window,registry)
        pivot=built.pivot(index="date",columns="feature_key",values="raw_feature_value")
        ma=raw.value.rolling(window,min_periods=window).mean()
        assert np.allclose(pivot["fred_mortgage_30y_level"],ma,equal_nan=True)
        assert np.allclose(pivot["fred_mortgage_30y_short"],ma/ma.shift(3)-1,equal_nan=True)
        assert np.allclose(pivot["fred_mortgage_30y_long"],ma/ma.shift(12)-1,equal_nan=True)
    # Production scorer renormalizes available feature weights, including one-child weight 1.0.
    scores=pd.DataFrame([{"geo_id":"g","date":dates[0],"canonical_metric_key":"mortgage_30y","feature_key":"fred_mortgage_30y_level","feature_score":.4}])
    assert np.isclose(score_metrics(scores).metric_score.iloc[0], .4)
    chronology=pd.DataFrame({"date":dates,"score":np.sin(np.arange(36)/3)})
    self_agreement=directional_agreement(chronology,chronology,"score",1)
    assert self_agreement["agreement_share"] == 1.0
    gap=chronology.drop(index=10); assert directional_agreement(gap,gap,"score",1)["valid_comparisons"] < len(gap)-1
    turns=detect_turning_points(chronology,"score")
    assert turns.empty or ((turns.incoming_persistence.eq(3)&turns.outgoing_persistence.eq(3)&turns.prominence_threshold.ge(.05)).all())
    unmatched=match_turning_points(pd.DataFrame([{"turning_point_date":dates[5],"turning_point_type":"peak","qualified":True}]),pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified"]))
    assert not unmatched.matched.iloc[0] and pd.isna(unmatched.signed_delay_months.iloc[0])
    assert len(REVIEW_GEOGRAPHIES)==7 and NATIVE_GEOGRAPHY not in REVIEW_GEOGRAPHIES
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
    dts=pd.date_range("2020-01-31",periods=8,freq="ME"); feature_rows=[]; metric_rows=[]
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
    assert before == {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in policy_paths}
    print("Capital Markets MA decomposition smoke test passed")


if __name__ == "__main__": main()
