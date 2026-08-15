"""Smoke 128: Price Phase-1 artifact, scope, arithmetic, chart, and governance contracts."""
from __future__ import annotations
import hashlib
import tempfile
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.price_feature_anatomy import TARGET_METRICS,REVIEW_GEOS,OUTPUTS,build,load_run,write_review

def fixture():
 dates=pd.date_range("2019-01-31",periods=84,freq="ME"); source=[]; features=[]; normalized=[]; metrics=[]; dims=[]
 keys={"median_sale_price":"redfin_median_sale_price","median_ppsf":"redfin_median_ppsf"}
 fkeys={m:{"level":f"redfin_{m}_level","short":f"redfin_{m}_short","long":f"redfin_{m}_long"} for m in TARGET_METRICS}
 weights={"level":.35,"short":.20,"long":.45}
 for j,geo in enumerate(REVIEW_GEOS):
  for mi,m in enumerate(TARGET_METRICS):
   for i,date in enumerate(dates):
    raw=(350000 if m=="median_sale_price" else 300)*(1+.004*i)+8000*np.sin(i/8+mi)+j*100
    if not (geo==REVIEW_GEOS[1] and m=="median_sale_price" and i==30): source.append({"geo_id":geo,"date":date,"metric_key":keys[m],"value":raw})
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
   dims.append({"geo_id":geo,"date":date,"dimension":"price","dimension_score":sum(vals)/2})
 return {"source_metrics":pd.DataFrame(source),"features":pd.DataFrame(features),"normalized_features":pd.DataFrame(normalized),"aligned_metric_scores":pd.DataFrame(metrics),"dimension_scores":pd.DataFrame(dims)}

def main():
 protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/axis_registry.csv"),Path("config/normalization_registry.csv")]
 before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 assert TARGET_METRICS==("median_sale_price","median_ppsf")
 tables=build(fixture(),Path(".")); assert set(OUTPUTS).issubset(tables)
 assert set(tables["production_contract"].metric)==set(TARGET_METRICS)
 forbidden=" ".join(tables["production_contract"].astype(str).stack()).lower()
 assert not any(x in forbidden for x in ("days_on_market","affordability","inventory","permit_intensity"))
 gap=tables["raw_chronology"][(tables["raw_chronology"].geo_id.eq(REVIEW_GEOS[1])) & (tables["raw_chronology"].metric.eq("median_sale_price")) & (tables["raw_chronology"].date.eq(pd.Timestamp("2021-07-31")))]
 assert len(gap)==1 and gap.raw_value.isna().all()
 replay=tables["feature_contributions"].groupby(["geo_id","date","metric"]).weighted_feature_contribution.sum()
 actual=tables["feature_contributions"].drop_duplicates(["geo_id","date","metric"]).set_index(["geo_id","date","metric"]).production_metric_score
 actual=actual.reindex(replay.index); assert np.allclose(replay,actual,atol=1e-12)
 assert tables["governance_status"].iloc[0].to_dict()=={"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"price_feature_anatomy_review_pending","automated_winner":False,"production_policy_changed":False}
 with tempfile.TemporaryDirectory() as tmp:
  out=Path(tmp); write_review(tables,out)
  assert all((out/f"price_phase1_{n}.csv").is_file() for n in OUTPUTS)
  svgs=list(out.glob("*.svg")); assert len(svgs)==14
  assert all(("<path" in p.read_text() or "<polyline" in p.read_text()) for p in svgs)
  assert (out/"price_phase1_review_index.html").is_file()
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 try: load_run(Path("/definitely/absent/authoritative-run"))
 except FileNotFoundError: pass
 else: raise AssertionError("absent authoritative run did not fail closed")
 print("Smoke 128 passed: exact Price scope, calendar gaps, equal-footing exports, reconstruction, plotted SVGs, governance, fail closed")
if __name__=="__main__": main()
