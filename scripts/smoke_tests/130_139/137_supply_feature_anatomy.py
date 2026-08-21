"""Smoke 137: Supply Phase-1 scope, reconstruction, family evidence, and governance."""
from __future__ import annotations
import hashlib,re,tempfile
from pathlib import Path
import numpy as np
import pandas as pd
from regime.diagnostics.supply_feature_anatomy import EXPECTED_METRICS,EXPECTED_WEIGHTS,OUTPUTS,REVIEW_GEOS,build,load_run,write_review
from scripts.build_supply_feature_anatomy_diagnostic import DEFAULT_RUN

def fixture(reverse=False):
 dates=pd.date_range("2022-08-31",periods=48,freq="ME"); source=[]; features=[]; normalized=[]; native=[]; aligned=[]; dims=[]
 fkeys={"active_inventory":{"level":"redfin_inventory_level","short":"redfin_inventory_short","long":"redfin_inventory_long"},"permit_activity":{"level":"bps_total_units_level","short":"bps_total_units_short","long":"bps_total_units_long"},"permit_intensity":{"level":"permit_intensity_level","short":"permit_intensity_short","long":"permit_intensity_long"}}
 fw={"active_inventory":{"level":.4,"short":.15,"long":.45},"permit_activity":{"level":.75,"short":.1,"long":.15},"permit_intensity":{"level":.4,"short":.15,"long":.45}}
 for j,geo in enumerate(REVIEW_GEOS):
  bydate={}
  for mi,m in enumerate(EXPECTED_METRICS):
   for i,date in enumerate(dates):
    native_date=date.to_period("M").to_timestamp() if m.startswith("permit_") else date
    raw=(100+mi*20)*(1+.002*i)+10*np.sin(i/6+mi*.2)+j
    if not (j==1 and m=="active_inventory" and i==30): source.append({"geo_id":geo,"date":native_date,"canonical_metric_key":m,"value":raw})
    scores={"level":np.tanh((i-30)/25),"short":.7*np.sin(i/3+mi*.2),"long":.8*np.sin(i/9+mi*.3)}
    score=sum(scores[k]*fw[m][k] for k in scores); bydate.setdefault(date,{})[m]=score
    # Inventory deliberately has no native feature/metric rows in the last two
    # evaluation months; the alignment layer legitimately carries May forward.
    has_native=not (m=="active_inventory" and i>=46)
    if has_native:
     for ft in scores:
      row={"geo_id":geo,"date":native_date,"feature_key":fkeys[m][ft],"raw_feature_value":raw*(1 if ft=="level" else .01*np.sin(i/(3 if ft=="short" else 9)))}
      features.append(row); normalized.append({**row,"feature_score":scores[ft]})
     native.append({"geo_id":geo,"date":native_date,"canonical_metric_key":m,"metric_score":score})
    aligned_score=score if has_native else bydate[dates[45]][m]
    aligned_date=native_date if has_native else dates[45]
    aligned.append({"geo_id":geo,"evaluation_date":date,"metric_date":aligned_date,"canonical_metric_key":m,"metric_score":aligned_score})
    bydate[date][m]=aligned_score
  for date,values in bydate.items(): dims.append({"geo_id":geo,"date":date,"dimension":"supply","dimension_score":sum(values[m]*EXPECTED_WEIGHTS[m] for m in EXPECTED_METRICS)})
 # One unavailable aligned metric exercises production weight renormalization;
 # native Feature -> Metric evidence remains present and is not fabricated.
 aligned_frame=pd.DataFrame(aligned)
 aligned_frame=aligned_frame[~((aligned_frame.geo_id==REVIEW_GEOS[1])&(aligned_frame.canonical_metric_key=="permit_intensity")&(aligned_frame.evaluation_date==dates[5]))]
 aw=aligned_frame.pivot(index=["geo_id","evaluation_date"],columns="canonical_metric_key",values="metric_score"); valid=aw.notna(); weights=pd.Series(EXPECTED_WEIGHTS)
 dim=aw.mul(weights).sum(axis=1,min_count=1).div(valid.mul(weights).sum(axis=1)).rename("dimension_score").reset_index().rename(columns={"evaluation_date":"date"}); dim["dimension"]="supply"
 frames={"source_metrics":pd.DataFrame(source),"features":pd.DataFrame(features),"normalized_features":pd.DataFrame(normalized),"metric_scores":pd.DataFrame(native),"aligned_metric_scores":aligned_frame,"dimension_scores":dim}
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
 permit=contract[contract.metric.eq("permit_activity")]; assert permit.feature_policy_provenance.str.contains("MA12/A2").all() and permit.prior_explicit_calibration_or_promotion.all()
 assert len(tables["cross_metric_relationship"].query("period=='full_history'"))==21
 assert len(tables["permit_family_overlap"].query("period=='full_history'"))==7
 assert len(tables["dimension_contribution_structure"].query("period=='full_history'"))==7
 raw=tables["raw_chronology"]; assert set(raw.metric.unique())==set(EXPECTED_METRICS)
 assert raw.query("metric=='active_inventory'").native_date.dt.is_month_end.all() and raw.query("metric=='permit_activity'").native_date.dt.is_month_start.all()
 pair=tables["raw_cross_metric_alignment"].query("left_metric=='active_inventory' and right_metric=='permit_activity'")
 assert (pair.left_native_date!=pair.right_native_date).all() and pair.calendar_month.notna().all()
 assert tables["cross_metric_relationship"].raw_chronology_correlation.notna().all()
 replay=tables["feature_contributions"].groupby(["geo_id","date","metric"]).weighted_feature_contribution.sum(); actual=tables["feature_contributions"].drop_duplicates(["geo_id","date","metric"]).set_index(["geo_id","date","metric"]).production_metric_score.reindex(replay.index); assert np.allclose(replay,actual)
 permit_native=tables["feature_contributions"].query("metric=='permit_activity'"); assert permit_native.date.dt.is_month_start.all()
 permit_aligned=tables["_aligned_metrics"].query("metric=='permit_activity'"); assert permit_aligned.date.dt.is_month_end.all() and permit_aligned.metric_date.dt.is_month_start.all()
 inventory_aligned=tables["_aligned_metrics"].query("metric=='active_inventory'").sort_values("date"); tail=inventory_aligned.groupby("geo_id").tail(2)
 assert set(tail.date.dt.strftime("%Y-%m-%d"))=={"2026-06-30","2026-07-31"}
 assert (tail.date>tail.metric_date).all() and not tables["feature_contributions"].merge(tail[["geo_id","date","metric"]],on=["geo_id","date","metric"]).shape[0]
 aligned_contrib=tables["aligned_metric_contributions"]; assert aligned_contrib.evaluation_date.dt.is_month_end.all()
 replay2=aligned_contrib.groupby(["geo_id","evaluation_date"]).weighted_metric_contribution.sum(); dim2=aligned_contrib.drop_duplicates(["geo_id","evaluation_date"]).set_index(["geo_id","evaluation_date"]).supply_dimension_score.reindex(replay2.index); assert np.allclose(replay2,dim2,atol=1e-12)
 unavailable=aligned_contrib[(aligned_contrib.geo_id==REVIEW_GEOS[1])&(aligned_contrib.evaluation_date==pd.Timestamp("2023-01-31"))]
 assert len(unavailable)==3 and (~unavailable.metric_available).sum()==1 and np.isclose(unavailable.effective_metric_weight.sum(),1) and np.isclose(unavailable.available_configured_weight_sum.unique(),.95)
 assert tables["dimension_contribution_structure"].mean_cancellation_ratio.between(0,1).all() and tables["dimension_contribution_structure"].net_to_gross_contribution.between(0,1).all()
 pc=tables["permit_family_overlap"].query("geo_id==@REVIEW_GEOS[0]").set_index("period").contribution_correlation
 assert pc.round(8).nunique()>1
 reverse=build(fixture(True),Path(".")); pd.testing.assert_frame_equal(tables["cross_metric_relationship"].reset_index(drop=True),reverse["cross_metric_relationship"].reset_index(drop=True))
 gov=tables["governance_status"].iloc[0]; assert gov.recommendation_state=="none" and not gov.production_policy_changed and not gov.metric_weight_policy_changed and not gov.capital_markets_changed
 with tempfile.TemporaryDirectory() as tmp:
  out=Path(tmp); write_review(tables,out); assert all((out/f"supply_phase1_{name}.csv").is_file() for name in OUTPUTS); svgs=list(out.glob("*.svg")); assert len(svgs)==24 and all("<path" in p.read_text() and not re.search(r"(?:NaN|Inf)",p.read_text(),re.I) for p in svgs)
  for metric_name in EXPECTED_METRICS:
   for scope in ("dc","seven_county_standardized"):
    svg=(out/f"supply_phase1_{metric_name}_{scope}_raw_features.svg").read_text(); paths=re.findall(r'<path d="([^"]*)"',svg); assert len(paths)>=4 and all(re.search(r"[ML][0-9]",d) for d in paths[1:])
  index=(out/"supply_phase1_review_index.html").read_text(); assert "different incumbent feature policies" in index and "calendar-month identity" in index and "aligned evaluation dates" in index and "native feature dates" in index
 assert before=={p:hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}
 missing=fixture(); missing["source_metrics"]=missing["source_metrics"].query("canonical_metric_key != 'permit_intensity'")
 try: build(missing,Path("."))
 except ValueError as exc: assert "governed raw metrics missing" in str(exc)
 else: raise AssertionError("missing governed raw metric did not fail closed")
 duplicate=fixture(); duplicate["source_metrics"]=pd.concat([duplicate["source_metrics"],duplicate["source_metrics"].iloc[[0]]],ignore_index=True)
 try: build(duplicate,Path("."))
 except ValueError as exc: assert "duplicate raw calendar-month chronology" in str(exc)
 else: raise AssertionError("duplicate canonical raw row did not fail closed")
 try: load_run(Path("/absent/governed-production-run"))
 except FileNotFoundError: pass
 else: raise AssertionError("authoritative input did not fail closed")
 print("Smoke 137 passed: frozen Supply scope/weights, registry features, reconstruction, gaps, pairwise/permit evidence, SVGs, ordering, immutability, fail closed")
if __name__=="__main__": main()
