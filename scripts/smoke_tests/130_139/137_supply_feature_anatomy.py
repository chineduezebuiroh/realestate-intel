"""Smoke 137: Supply Phase-1 scope, reconstruction, family evidence, and governance."""
from __future__ import annotations
import hashlib,tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime.diagnostics.supply_feature_anatomy import EXPECTED_METRICS,EXPECTED_WEIGHTS,OUTPUTS,REVIEW_GEOS,build,load_run,write_review
from scripts.build_supply_feature_anatomy_diagnostic import DEFAULT_RUN

def fixture(reverse=False):
 dates=pd.date_range("2019-01-31",periods=48,freq="ME"); source=[]; features=[]; normalized=[]; metrics=[]; dims=[]
 keys={"active_inventory":"redfin_inventory","permit_activity":"bps_total_units","permit_intensity":"derived_permit_intensity"}
 fkeys={"active_inventory":{"level":"redfin_inventory_level","short":"redfin_inventory_short","long":"redfin_inventory_long"},"permit_activity":{"level":"bps_total_units_level","short":"bps_total_units_short","long":"bps_total_units_long"},"permit_intensity":{"level":"permit_intensity_level","short":"permit_intensity_short","long":"permit_intensity_long"}}
 fw={"active_inventory":{"level":.5,"short":.25,"long":.25},"permit_activity":{"level":.8,"short":.1,"long":.1},"permit_intensity":{"level":.5,"short":.25,"long":.25}}
 for j,geo in enumerate(REVIEW_GEOS):
  bydate={}
  for mi,m in enumerate(EXPECTED_METRICS):
   for i,date in enumerate(dates):
    raw=(100+mi*20)*(1+.002*i)+10*np.sin(i/6+mi*.2)+j
    if not (j==1 and m=="active_inventory" and i==30): source.append({"geo_id":geo,"date":date,"metric_key":keys[m],"value":raw})
    scores={"level":np.tanh((i-30)/25),"short":.7*np.sin(i/3+mi*.2),"long":.8*np.sin(i/9+mi*.3)}
    score=sum(scores[k]*fw[m][k] for k in scores); bydate.setdefault(date,{})[m]=score
    for ft in scores:
     row={"geo_id":geo,"date":date,"feature_key":fkeys[m][ft],"raw_feature_value":raw*(1 if ft=="level" else .01*np.sin(i/(3 if ft=="short" else 9)))}
     features.append(row); normalized.append({**row,"feature_score":scores[ft]})
    metrics.append({"geo_id":geo,"evaluation_date":date,"canonical_metric_key":m,"metric_score":score})
  for date,values in bydate.items(): dims.append({"geo_id":geo,"date":date,"dimension":"supply","dimension_score":sum(values[m]*EXPECTED_WEIGHTS[m] for m in EXPECTED_METRICS)})
 frames={"source_metrics":pd.DataFrame(source),"features":pd.DataFrame(features),"normalized_features":pd.DataFrame(normalized),"aligned_metric_scores":pd.DataFrame(metrics),"dimension_scores":pd.DataFrame(dims)}
 if reverse:
  frames={k:v.iloc[::-1].reset_index(drop=True) for k,v in frames.items()}
 return frames

def main():
 protected=[Path("config/feature_registry.csv"),Path("config/metric_dimension_registry.csv"),Path("config/normalization_registry.csv")]; before={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 assert DEFAULT_RUN==Path("artifacts/regime/runs/affordability_ma12_p4_production_20260816")
 tables=build(fixture(),Path(".")); assert set(OUTPUTS).issubset(tables); assert set(tables["production_contract"].metric)==set(EXPECTED_METRICS)
 assert tables["production_contract"].groupby("metric").metric_weight.first().to_dict()==EXPECTED_WEIGHTS
 registry=pd.read_csv("config/feature_registry.csv"); contract=tables["production_contract"]
 expected=registry.set_index("feature_key").feature_weight.astype(float); actual=contract.set_index("feature_key").configured_feature_weight.astype(float)
 assert np.allclose(actual,expected.reindex(actual.index))
 permit=contract[contract.metric.eq("permit_activity")]; assert permit.feature_policy_provenance.str.contains("BPS-FINAL-80").all() and permit.prior_explicit_calibration_or_promotion.all()
 assert len(tables["cross_metric_relationship"].query("period=='full_history'"))==21
 assert len(tables["permit_family_overlap"].query("period=='full_history'"))==7
 assert len(tables["dimension_contribution_structure"].query("period=='full_history'"))==7
 raw=tables["raw_chronology"]; gap=raw[(raw.geo_id==REVIEW_GEOS[1]) & (raw.metric=="active_inventory") & (raw.date==pd.Timestamp("2021-07-31"))]; assert len(gap)==1 and gap.raw_value.isna().all()
 replay=tables["feature_contributions"].groupby(["geo_id","date","metric"]).weighted_feature_contribution.sum(); actual=tables["feature_contributions"].drop_duplicates(["geo_id","date","metric"]).set_index(["geo_id","date","metric"]).production_metric_score.reindex(replay.index); assert np.allclose(replay,actual)
 reverse=build(fixture(True),Path(".")); pd.testing.assert_frame_equal(tables["cross_metric_relationship"].reset_index(drop=True),reverse["cross_metric_relationship"].reset_index(drop=True))
 gov=tables["governance_status"].iloc[0]; assert gov.recommendation_state=="none" and not gov.production_policy_changed and not gov.metric_weight_policy_changed and not gov.capital_markets_changed
 with tempfile.TemporaryDirectory() as tmp:
  out=Path(tmp); write_review(tables,out); assert all((out/f"supply_phase1_{name}.csv").is_file() for name in OUTPUTS); svgs=list(out.glob("*.svg")); assert len(svgs)==24 and all("<path" in p.read_text() for p in svgs)
  assert "different incumbent feature policies" in (out/"supply_phase1_review_index.html").read_text()
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 try: load_run(Path("/absent/governed-production-run"))
 except FileNotFoundError: pass
 else: raise AssertionError("authoritative input did not fail closed")
 print("Smoke 137 passed: frozen Supply scope/weights, registry features, reconstruction, gaps, pairwise/permit evidence, SVGs, ordering, immutability, fail closed")
if __name__=="__main__": main()
