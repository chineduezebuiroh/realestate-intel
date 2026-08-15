"""Smoke 129: Price Phase-2 isolation, arithmetic, scope, plots, and governance."""
from __future__ import annotations
import hashlib, tempfile, warnings
from pathlib import Path
import numpy as np
import pandas as pd
from regime.diagnostics.price_feature_weight_calibration import POLICIES,ADJACENT,EXPORTS,build,load_run,write_review,_turn_evidence
from regime.diagnostics.price_feature_anatomy import REVIEW_GEOS,TARGET_METRICS,_plot

def fixture():
 # Keep this fixture independent of production construction: its normalized
 # features are already persisted inputs, as required by the diagnostic.
 dates=pd.date_range("2019-01-31",periods=84,freq="ME"); source=[]; features=[]; normalized=[]; metrics=[]; dims=[]; axes=[]
 keys={"median_sale_price":"redfin_median_sale_price","median_ppsf":"redfin_median_ppsf"}; fkeys={m:{f:f"redfin_{m}_{f}" for f in ("level","short","long")} for m in TARGET_METRICS}
 for j,geo in enumerate(REVIEW_GEOS):
  for i,date in enumerate(dates):
   scores=[]
   for mi,m in enumerate(TARGET_METRICS):
    raw=(350000 if m=="median_sale_price" else 300)*(1+.003*i+.04*np.sin(i/5+mi))+j
    # One absent source month proves a sparse-row positional lag cannot pass.
    if not (j==0 and mi==0 and i==24):
     source.append({"geo_id":geo,"date":date,"metric_key":keys[m],"value":raw})
    vals={"level":np.tanh((i-30)/30),"short":.7*np.sin(i/2+j/9),"long":.8*np.sin(i/10+mi/3)}
    # Exercise governed missing-feature renormalization.
    if i==0: vals["long"]=np.nan
    for ft,v in vals.items():
     row={"geo_id":geo,"date":date,"canonical_metric_key":m,"feature_key":fkeys[m][ft],"raw_feature_value":raw if ft=="level" else v}
     features.append(row); normalized.append({**row,"feature_score":v})
    available={k:v for k,v in vals.items() if pd.notna(v)}; denom=sum({"level":.5,"short":.25,"long":.25}[k] for k in available); score=sum(v*{"level":.5,"short":.25,"long":.25}[k]/denom for k,v in available.items()); scores.append(score)
    metrics.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":m,"metric_score":score})
   dims.append({"geo_id":geo,"date":date,"dimension":"price","dimension_score":sum(scores)/2})
   axes.append({"geo_id":geo,"date":date,"axis":"demand","axis_score":.65*np.sin(i/11)+.175*sum(scores)/2})
 return {"source_metrics":pd.DataFrame(source),"features":pd.DataFrame(features),"normalized_features":pd.DataFrame(normalized),"aligned_metric_scores":pd.DataFrame(metrics),"dimension_scores":pd.DataFrame(dims),"axis_scores":pd.DataFrame(axes)}

def main():
 protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv"),Path("config/normalization_registry.csv")]; before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 assert list(POLICIES)==[f"P{i}" for i in range(7)]; assert POLICIES=={"P0":(.5,.25,.25),"P1":(.45,.2,.35),"P2":(.4,.2,.4),"P3":(.4,.15,.45),"P4":(.35,.15,.5),"P5":(.3,.15,.55),"P6":(.35,.2,.45)}
 assert all(sum(x)==1 for x in POLICIES.values()); assert ("P3","P6") in ADJACENT and ("P4","P6") in ADJACENT
 tables=build(fixture(),Path(".")); assert set(EXPORTS).issubset(tables); reg=tables["scenario_registry"]; assert set(reg.scenario_id)=={f"MA12__P{i}" for i in range(7)} and set(reg.ma_window)=={"MA12_FIXED"}
 raw=tables["raw_cycle_chronology"]; missing=raw[(raw.geo_id.eq(REVIEW_GEOS[0]))&(raw.metric.eq("median_sale_price"))]
 absent=missing.set_index("date").loc[pd.Timestamp("2021-01-31")]; assert pd.isna(absent.raw_value) and pd.isna(absent.raw_12m_change)
 lagged=missing.set_index("date").loc[pd.Timestamp("2022-01-31")]; assert pd.isna(lagged.lag12_raw_value) and pd.isna(lagged.raw_12m_change)
 assert missing.raw_12m_change.notna().any() and raw.raw_cycle_zscore.notna().any()
 rc=tables["raw_change_comparison"]; lr=tables["long_reference_comparison"]
 assert set(rc.reference_type)=={"raw_cycle_reference"} and set(lr.reference_type)=={"long_feature_reference"}
 assert rc.correlation.notna().any() and rc.direction_agreement.notna().any()
 boundary=rc.query("policy in ['P4','P5'] and period=='full_history'").sort_values(["metric","geo_id","policy"])
 assert set(boundary.policy)=={"P4","P5"} and boundary.correlation.notna().all()
 dates=pd.date_range("2020-01-31",periods=36,freq="ME"); wave=np.sin(np.arange(36)*np.pi/6)
 turns=_turn_evidence(pd.Series(wave),pd.Series(wave),dates)
 assert turns["reference_turn_count"]>0 and turns["matched_turn_count"]==turns["reference_turn_count"] and turns["median_turning_point_latency_months"]==0
 c=tables["feature_contributions"]; assert set(c.metric)==set(TARGET_METRICS); assert c.groupby(["geo_id","date","metric","feature_type"])[["raw_feature_value","normalized_feature_score"]].nunique(dropna=False).max().max()==1
 replay=c.groupby(["policy","geo_id","date","metric"]).weighted_contribution.sum(min_count=1); actual=c.drop_duplicates(["policy","geo_id","date","metric"]).set_index(["policy","geo_id","date","metric"]).metric_score.reindex(replay.index); assert np.allclose(replay,actual,equal_nan=True)
 missing=c[(c.date.eq(pd.Timestamp("2019-01-31")))&c.feature_type.eq("level")]; assert np.allclose(missing.effective_feature_weight,missing.configured_feature_weight/missing.available_weight_sum)
 assert np.allclose(tables["_dimension_chronology"].price_dimension_score,tables["_dimension_chronology"][list(TARGET_METRICS)].mean(axis=1))
 gov=tables["governance_status"].iloc[0]; assert not gov.automated_winner and not gov.production_policy_changed and gov.ma_window=="MA12_FIXED"
 with tempfile.TemporaryDirectory() as tmp:
  out=Path(tmp); write_review(tables,out); assert all((out/f"price_phase2_{x}.csv").is_file() for x in EXPORTS); svgs=list(out.glob("*.svg")); assert svgs and all("<path" in x.read_text() for x in svgs); assert all("<circle" in x.read_text() for x in out.glob("*turning_point_overlay.svg"))
  with warnings.catch_warnings():
   warnings.simplefilter("error",FutureWarning); _plot(out/"empty_dates.svg",[("empty",pd.DataFrame({"date":[pd.NaT],"value":[np.nan]})),("valid",pd.DataFrame({"date":[pd.Timestamp("2020-01-31")],"value":[0.]}))],"warning regression")
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 try: load_run(Path("/definitely/absent/authoritative-run"))
 except FileNotFoundError: pass
 else: raise AssertionError("authoritative input absence did not fail closed")
 print("Smoke 129 passed: seven policies, MA12 isolation, renormalization, propagation, comparisons, SVGs, governance, fail closed, warning cleanup")
if __name__=="__main__": main()
