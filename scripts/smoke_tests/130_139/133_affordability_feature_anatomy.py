"""Smoke 133: Affordability Phase-1 artifact, scope, arithmetic, chart, and governance contracts."""
from __future__ import annotations
import hashlib
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.affordability_feature_anatomy import REVIEW_GEOS,OUTPUTS,build,load_run,write_review

TARGET_METRICS=("price_to_income","payment_burden")

def fixture():
 dates=pd.date_range("2019-01-31",periods=84,freq="ME"); source=[]; features=[]; normalized=[]; metrics=[]; dims=[]
 keys={"price_to_income":"derived_price_to_income","payment_burden":"derived_payment_burden"}
 fkeys={m:{"level":f"{m}_level","short":f"{m}_short","long":f"{m}_long"} for m in TARGET_METRICS}
 weights={"level":.35,"short":.20,"long":.45}
 for j,geo in enumerate(REVIEW_GEOS):
  for mi,m in enumerate(TARGET_METRICS):
   for i,date in enumerate(dates):
    raw=(6 if m=="price_to_income" else .30)*(1+.004*i)+.2*np.sin(i/8+mi)+j*.01
    if not (geo==REVIEW_GEOS[1] and m=="price_to_income" and i==30): source.append({"geo_id":geo,"date":date,"metric_key":keys[m],"value":raw})
    scores={"level":np.tanh((i-30)/30),"short":.7*np.sin(i/2+j/9),"long":.8*np.sin(i/10+mi/3)}
    vals={"level":raw*.98,"short":.02*np.sin(i/2),"long":.12*np.sin(i/10)}
    score=sum(scores[k]*weights[k] for k in weights)
    for ft in weights:
     features.append({"geo_id":geo,"date":date,"canonical_metric_key":m,"feature_key":fkeys[m][ft],"raw_feature_value":vals[ft]})
     normalized.append({**features[-1],"feature_score":scores[ft]})
    metrics.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":m,"metric_score":score})
  # append dimension after both metrics at date
  for i,date in enumerate(dates):
   vals=[x["metric_score"] for x in metrics if x["geo_id"]==geo and x["evaluation_date"]==date]
   dims.append({"geo_id":geo,"date":date,"dimension":"affordability","dimension_score":sum(vals)/2})
 native=pd.DataFrame(metrics).rename(columns={"evaluation_date":"date"})
 aligned=pd.DataFrame(metrics); aligned["metric_date"]=aligned["evaluation_date"]
 return {"source_metrics":pd.DataFrame(source),"features":pd.DataFrame(features),"normalized_features":pd.DataFrame(normalized),"metric_scores":native,"aligned_metric_scores":aligned,"dimension_scores":pd.DataFrame(dims)}

def main():
 protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv"),Path("config/normalization_registry.csv")]
 before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 assert TARGET_METRICS==("price_to_income","payment_burden")
 tables=build(fixture(),Path(".")); assert set(OUTPUTS).issubset(tables)
 assert set(tables["production_contract"].metric)==set(TARGET_METRICS)
 forbidden=" ".join(tables["production_contract"].astype(str).stack()).lower()
 assert not any(x in forbidden for x in ("days_on_market","median_sale_price","inventory","permit_intensity"))
 gap=tables["raw_chronology"][(tables["raw_chronology"].geo_id.eq(REVIEW_GEOS[1])) & (tables["raw_chronology"].metric.eq("price_to_income")) & (tables["raw_chronology"].date.eq(pd.Timestamp("2021-07-31")))]
 assert len(gap)==1 and gap.raw_value.isna().all()
 replay=tables["feature_contributions"].groupby(["geo_id","date","metric"]).weighted_feature_contribution.sum()
 actual=tables["feature_contributions"].drop_duplicates(["geo_id","date","metric"]).set_index(["geo_id","date","metric"]).production_metric_score
 actual=actual.reindex(replay.index); assert np.allclose(replay,actual,atol=1e-12)
 assert tables["governance_status"].iloc[0].to_dict()=={"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"affordability_feature_anatomy_review_pending","automated_winner":False,"production_policy_changed":False}
 with tempfile.TemporaryDirectory() as tmp:
  out=Path(tmp); write_review(tables,out)
  assert all((out/f"affordability_phase1_{n}.csv").is_file() for n in OUTPUTS)
  svgs=list(out.glob("*.svg")); assert len(svgs)==14
  assert all(("<path" in p.read_text() or "<polyline" in p.read_text()) for p in svgs)
  assert (out/"affordability_phase1_review_index.html").is_file()
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 try: load_run(Path("/definitely/absent/authoritative-run"))
 except FileNotFoundError: pass
 else: raise AssertionError("absent authoritative run did not fail closed")
 print("Smoke 133 passed: exact Affordability scope, calendar gaps, equal-footing exports, reconstruction, plotted SVGs, governance, fail closed")
if __name__=="__main__": main()
