#!/usr/bin/env python3
"""Fail-closed validator for the final frozen Capital Markets F4 production run."""
from __future__ import annotations
import argparse, json
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
from regime._00_config_loader import load_regime_config
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime._07_coordinate_engine import build_coordinates
from regime._08_geometry_engine import assign_geometry
from regime._09_regime_assignment import assign_regimes
from regime.artifacts import RegimeArtifactStore
from regime.canonical_metrics import validate_spread_10y_2y_parity

RUN="capital_markets_f4_production_20260818"
EXPERIMENT="capital_markets_f4_production"
BASELINE="capital_markets_feature_policy_corrected_production_20260818"
CM={"mortgage_30y","mortgage_15y","treasury_10y","fedfunds","spread_10y_2y","spread_10y_fedfunds"}
POLICIES={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P6","spread_10y_fedfunds":"P9"}
WEIGHTS={"mortgage_30y":.11666666666666667,"mortgage_15y":.11666666666666667,"treasury_10y":.11666666666666667,"fedfunds":.10,"spread_10y_2y":.275,"spread_10y_fedfunds":.275}
FAMILIES={"long_term_rates":.35,"fedfunds":.10,"spreads":.55}

def same(a,b,keys):
 a=a.sort_values(keys).reset_index(drop=True); b=b.sort_values(keys).reset_index(drop=True)
 pd.testing.assert_frame_equal(a,b,check_dtype=False,check_exact=False,rtol=0,atol=1e-12)
 return len(a)
def equal(a,b,keys,col):
 j=a.merge(b,on=keys,suffixes=("_persisted","_rebuilt"),validate="one_to_one")
 assert len(j)==len(a)==len(b) and np.allclose(j[f"{col}_persisted"],j[f"{col}_rebuilt"],rtol=0,atol=1e-12,equal_nan=True)
 return len(j)
def fact(con,metric):
 return con.execute("SELECT geo_id,date,value FROM fact_timeseries WHERE source_id='fred_macro' AND metric_id=? AND value IS NOT NULL",[metric]).fetchdf()
def agreement(a,b):
 q=pd.concat([a,b],axis=1).dropna(); material=(q.iloc[:,0].abs()>1e-12)|(q.iloc[:,1].abs()>1e-12)
 return float((np.sign(q.loc[material].iloc[:,0])==np.sign(q.loc[material].iloc[:,1])).mean()) if material.any() else None

def main():
 p=argparse.ArgumentParser(); p.add_argument("--artifact-root",type=Path,default=Path("artifacts/regime/runs")); p.add_argument("--serving-db",type=Path,default=Path("data/market_serving.duckdb")); a=p.parse_args()
 for run in (RUN,BASELINE):
  if not (a.artifact_root/run).is_dir(): raise FileNotFoundError(f"required immutable run is absent: {run}; no substitution permitted")
 if not a.serving_db.is_file(): raise FileNotFoundError(f"authoritative serving database is absent: {a.serving_db}; no substitution permitted")
 store=RegimeArtifactStore(a.artifact_root); manifest=store.read_manifest(RUN)
 assert manifest["run_id"]==RUN and manifest["experiment_id"]==EXPERIMENT and manifest["status"]=="complete"
 md=manifest["metadata"]
 expected={"promotion_contract":"capital_markets_family_weight_f4_2026_08_18","capital_markets_family_policy":"F4","corrected_spread_polarity":True,"normalization_changed":False,"human_decision":"capital_markets_f4_family_weight_approved","automated_winner":False,"family_metric_weight_calibration":"closed","capital_markets_calibration":"closed","capital_markets_fully_frozen":True}
 assert md.get("capital_markets_feature_policies")==POLICIES
 assert md.get("capital_markets_metric_weights")==WEIGHTS
 assert md.get("capital_markets_family_weights")==FAMILIES
 assert all(md.get(k)==v for k,v in expected.items())
 verify=store.verify_run(RUN); assert len(verify)==len(manifest["artifacts"]) and verify.exists.all() and verify.hash_matches.all()
 source=store.read_dataframe(RUN,"source_metrics"); baseline_source=store.read_dataframe(BASELINE,"source_metrics")
 canonical=source[source.canonical_metric_key.eq("spread_10y_2y")][["geo_id","date","value"]]
 con=duckdb.connect(str(a.serving_db),read_only=True)
 ten,two,physical=fact(con,"fred_gs10"),fact(con,"fred_gs2"),fact(con,"fred_spread_2y_10y"); con.close()
 parity=validate_spread_10y_2y_parity(canonical,ten,two,physical)
 cfg=load_regime_config(validate=True)
 norm_cfg=pd.read_csv("config/normalization_registry.csv"); spread_features=cfg.features[cfg.features.metric_key.eq("fred_2y10y_spread")]
 assert set(norm_cfg[norm_cfg.policy_key.isin(spread_features.feature_key)].score_direction)=={"positive"}
 assert set(spread_features["transform"]) == {"ma_level", "ma_difference"} and set(
        spread_features["feature_window"]
    ) == {"9m", "9m/lag3m", "9m/lag12m"}
 assert spread_features.set_index("feature_type").feature_weight.astype(float).to_dict()=={"level":.60,"short_term_change":.05,"long_term_change":.35}
 weights=cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(CM)].set_index("canonical_metric_key").metric_weight.astype(float).to_dict()
 assert weights==WEIGHTS
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 unchanged_source=same(source,baseline_source,["geo_id","date","canonical_metric_key"])
 features=store.read_dataframe(RUN,"features"); base_features=store.read_dataframe(BASELINE,"features")
 unchanged_features=same(features,base_features,["geo_id","date","canonical_metric_key","feature_key"])
 normalized=store.read_dataframe(RUN,"normalized_features"); base_normalized=store.read_dataframe(BASELINE,"normalized_features")
 unchanged_normalized=same(normalized,base_normalized,["geo_id","date","canonical_metric_key","feature_key"])
 metrics=store.read_dataframe(RUN,"metric_scores"); base_metrics=store.read_dataframe(BASELINE,"metric_scores")
 metric_rows=equal(metrics,score_metrics(normalized),["geo_id","date","canonical_metric_key"],"metric_score")
 unchanged_metrics=same(metrics,base_metrics,["geo_id","date","canonical_metric_key"])
 unchanged_non_cm=same(metrics[~metrics.canonical_metric_key.isin(CM)],base_metrics[~base_metrics.canonical_metric_key.isin(CM)],["geo_id","date","canonical_metric_key"])
 aligned=store.read_dataframe(RUN,"aligned_metric_scores"); base_aligned=store.read_dataframe(BASELINE,"aligned_metric_scores")
 unchanged_aligned=same(aligned,base_aligned,["geo_id","date","canonical_metric_key"])
 dims=store.read_dataframe(RUN,"dimension_scores"); base_dims=store.read_dataframe(BASELINE,"dimension_scores")
 unchanged_non_cm_dims=same(dims[~dims.dimension.eq("capital_markets")],base_dims[~base_dims.dimension.eq("capital_markets")],["geo_id","date","dimension"])
 assert not dims[dims.dimension.eq("capital_markets")].equals(base_dims[base_dims.dimension.eq("capital_markets")])
 dim_rows=equal(dims,score_dimensions(aligned),["geo_id","date","dimension"],"dimension_score")
 axes=store.read_dataframe(RUN,"axis_scores"); axis_rows=equal(axes,score_axes(dims),["geo_id","date","axis"],"axis_score")
 coordinates=store.read_dataframe(RUN,"coordinates"); rebuilt_coordinates=build_coordinates(axes)
 coordinate_rows=same(coordinates,rebuilt_coordinates,["geo_id","date"])
 geometry=store.read_dataframe(RUN,"geometry"); rebuilt_geometry=assign_geometry(rebuilt_coordinates)
 geometry_rows=same(geometry,rebuilt_geometry,["geo_id","date"])
 regimes=store.read_dataframe(RUN,"regime_assignments"); regime_rows=same(regimes,assign_regimes(rebuilt_geometry),["geo_id","date"])
 raw=source[source.canonical_metric_key.isin({"spread_10y_2y","spread_10y_fedfunds"})].pivot(index=["geo_id","date"],columns="canonical_metric_key",values="value")
 scored=metrics[metrics.canonical_metric_key.isin({"spread_10y_2y","spread_10y_fedfunds"})].pivot(index=["geo_id","date"],columns="canonical_metric_key",values="metric_score")
 behavior={"raw_correlation":raw.corr().loc["spread_10y_2y","spread_10y_fedfunds"],"raw_sign_agreement":agreement(raw.spread_10y_2y,raw.spread_10y_fedfunds),"metric_score_correlation":scored.corr().loc["spread_10y_2y","spread_10y_fedfunds"],"metric_score_sign_agreement":agreement(scored.spread_10y_2y,scored.spread_10y_fedfunds)}
 print(json.dumps({"manifest_status":"complete","artifact_hashes":"verified",**parity,**behavior,"unchanged_source_rows":unchanged_source,"unchanged_feature_rows":unchanged_features,"unchanged_normalized_rows":unchanged_normalized,"unchanged_metric_rows":unchanged_metrics,"unchanged_aligned_metric_rows":unchanged_aligned,"unchanged_non_capital_markets_dimension_rows":unchanged_non_cm_dims,"non_capital_markets_metric_rows_unchanged":unchanged_non_cm,"metric_rows_rebuilt":metric_rows,"dimension_rows_rebuilt":dim_rows,"demand_supply_rows_rebuilt":axis_rows,"coordinate_rows_rebuilt":coordinate_rows,"geometry_rows_rebuilt":geometry_rows,"regime_rows_rebuilt":regime_rows,"family_metric_weight_calibration":"closed","capital_markets_calibration":"closed","capital_markets_fully_frozen":True},sort_keys=True))
if __name__=="__main__": main()
