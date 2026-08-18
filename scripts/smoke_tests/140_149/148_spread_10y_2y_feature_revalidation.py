"""Smoke 148: targeted corrected-polarity spread feature-policy revalidation."""
from __future__ import annotations
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.spread_10y_2y_feature_revalidation import (
    POLICIES, LONG_LADDER, FIXED_POLICIES, EXPORTS, METRIC, build, load_run,
    write_review,
)
from regime.diagnostics.capital_markets_feature_anatomy import resolve_contract, EXPECTED_WEIGHTS, EXPECTED_AXIS_WEIGHTS, NATIVE_GEO

def fixture():
    contract,_=resolve_contract(Path(".")); spec=contract.query("metric==@METRIC")
    dates=pd.date_range("2018-01-31",periods=84,freq="ME"); x=np.sin(np.arange(84)/6)+np.arange(84)/100
    features={"level":pd.Series(x).rolling(9,min_periods=9).mean(),}
    features["short"]=features["level"]-features["level"].shift(3); features["long"]=features["level"]-features["level"].shift(12)
    norm=[]
    for r in spec.itertuples():
        norm.extend({"geo_id":NATIVE_GEO,"date":d,"feature_key":r.feature_key,"feature_score":features[r.feature_type].iloc[i]} for i,d in enumerate(dates))
    weighted=pd.concat([.35*features["level"],.10*features["short"],.55*features["long"]],axis=1)
    available=pd.concat([features["level"].notna()*.35,features["short"].notna()*.10,features["long"].notna()*.55],axis=1).sum(axis=1)
    p7=weighted.sum(axis=1,min_count=1)/available.replace(0,np.nan)
    metric=pd.DataFrame({"geo_id":NATIVE_GEO,"date":dates,"metric":METRIC,"metric_score":p7})
    source=pd.DataFrame({"geo_id":NATIVE_GEO,"date":dates,"metric":METRIC,"value":x})
    source=pd.concat([source,pd.DataFrame({"geo_id":NATIVE_GEO,"date":dates,"metric":"spread_10y_fedfunds","value":x*.8+.1})],ignore_index=True)
    geos=[NATIVE_GEO,"district_of_columbia__state",*[f"county_{i}" for i in range(7)]]
    aligned=[]; dims=[]; axes=[]
    for geo in geos:
        aligned.extend({"geo_id":geo,"date":d,"metric_date":d,"metric":METRIC,"metric_score":p7.iloc[i]} for i,d in enumerate(dates))
        dims.extend({"geo_id":geo,"date":d,"dimension":"capital_markets","dimension_score":0.1+x[i]/20} for i,d in enumerate(dates))
        for axis in ("demand","supply"):
            axes.extend({"geo_id":geo,"date":d,"axis":axis,"axis_score":0.2+x[i]/30} for i,d in enumerate(dates))
    return {"normalized_features":pd.DataFrame(norm),"metric_scores":pd.concat([metric,pd.DataFrame({"geo_id":NATIVE_GEO,"date":dates,"metric":"spread_10y_fedfunds","metric_score":p7*.75})]),"source_metrics":source,"aligned_metric_scores":pd.DataFrame(aligned),"dimension_scores":pd.DataFrame(dims),"axis_scores":pd.DataFrame(axes)}

def main():
    assert METRIC=="spread_10y_2y" and list(POLICIES)==[f"P{i}" for i in range(10)] and "P10" not in POLICIES
    assert POLICIES=={
        "P0":(.60,.20,.20), "P1":(.60,.15,.25), "P2":(.60,.10,.30),
        "P3":(.55,.15,.30), "P4":(.55,.10,.35), "P5":(.50,.10,.40),
        "P6":(.60,.05,.35), "P7":(.35,.10,.55), "P8":(.45,.10,.45),
        "P9":(.40,.10,.50),
    }
    assert LONG_LADDER==("P4","P5","P8","P9","P7")
    assert FIXED_POLICIES=={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_fedfunds":"P9"}
    assert EXPECTED_WEIGHTS=={"mortgage_30y":.15,"mortgage_15y":.15,"treasury_10y":.15,"fedfunds":.10,"spread_10y_2y":.225,"spread_10y_fedfunds":.225}
    assert EXPECTED_AXIS_WEIGHTS=={"demand":.10,"supply":.15}
    tables=build(fixture(),Path(".")); assert set(EXPORTS)<=set(tables)
    reg=tables["scenario_registry"]; assert len(reg)==10 and reg.query("policy=='P7'").iloc[0].policy_status=="revalidation_required"
    assert reg.query("policy=='P0'").iloc[0].policy_semantics=="historical_60_20_20_reference"
    assert reg.query("policy=='P7'").iloc[0].policy_semantics=="corrected_persisted_run_arithmetic_baseline"
    assert reg.feature_construction.eq("MA9; MA9-lag3(MA9); MA9-lag12(MA9)").all() and reg.normalization_direction.eq("positive").all()
    c=tables["feature_contributions"]; assert c.groupby(["date","feature_type"]).normalized_feature_score.nunique(dropna=False).le(1).all()
    assert tables["responsiveness"].groupby("period").materiality_threshold.nunique(dropna=False).eq(1).all()
    assert not tables["corrected_raw_cycle_comparison"].empty and set(tables["cross_spread_context"].context_semantics)=={"secondary_descriptive_only_not_optimization"}
    gov=tables["governance_status"].iloc[0]; assert gov.family_metric_weight_calibration=="invalidated_pending_rerun" and not gov.automated_winner and not gov.production_policy_changed
    assert gov.persisted_reconstruction_anchor=="P7_corrected_persisted_run" and gov.historical_feature_policy_reference=="P0_60_20_20"
    assert all(tables[k].policy.nunique()==10 for k in ("capital_markets_dimension","demand_axis","supply_axis"))
    metric=tables["metric_chronology"].query("policy=='P7'").candidate_metric_score
    persisted=fixture()["metric_scores"].query("metric==@METRIC").metric_score
    assert np.allclose(metric,persisted,equal_nan=True,atol=1e-12,rtol=0)
    p0=tables["metric_chronology"].query("policy=='P0'").candidate_metric_score
    assert not np.allclose(p0,persisted,equal_nan=True,atol=1e-12,rtol=0)
    for key in ("capital_markets_dimension","demand_axis","supply_axis"):
        p7=tables[key].query("policy=='P7'")
        assert np.allclose(p7.candidate_score,p7.persisted_baseline_score,equal_nan=True,atol=1e-12,rtol=0)
    with tempfile.TemporaryDirectory() as tmp:
        out=Path(tmp); write_review(tables,out); assert all((out/f"spread_10y_2y_revalidation_{x}.csv").exists() for x in EXPORTS)
        svgs=list(out.glob("*.svg")); assert len(svgs)>=10 and all("<path" in p.read_text() for p in svgs)
        try: load_run(out/"absent")
        except FileNotFoundError: pass
        else: raise AssertionError("absent authoritative run did not fail closed")
    print("Smoke 148 passed: exact scope/grid/construction, invariant features, corrected evidence, propagation, governance, SVGs, fail-closed")

if __name__=="__main__": main()
