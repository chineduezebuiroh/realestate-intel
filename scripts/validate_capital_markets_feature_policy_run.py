#!/usr/bin/env python3
"""Fail-closed validation of the immutable Capital Markets feature-policy run."""
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
from regime._00_config_loader import load_regime_config
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime.artifacts import RegimeArtifactStore

RUN="capital_markets_feature_policy_production_20260818"; EXPERIMENT="capital_markets_feature_policy_production"
BASELINE="supply_s8_production_20260817"
POLICIES={"mortgage_30y":"P4","mortgage_15y":"P2","treasury_10y":"P1","fedfunds":"P5","spread_10y_2y":"P7","spread_10y_fedfunds":"P9"}
WEIGHTS={"mortgage_30y":.15,"mortgage_15y":.15,"treasury_10y":.15,"fedfunds":.10,"spread_10y_2y":.225,"spread_10y_fedfunds":.225}
KEYS={"fred_mortgage_30y","fred_mortgage_15y","fred_10y","fred_fedfunds","fred_2y10y_spread","fred_10y_fedfunds_spread"}

def equal(actual, rebuilt, keys, col):
 j=actual.merge(rebuilt,on=keys,suffixes=("_persisted","_rebuilt"),validate="one_to_one"); assert len(j)==len(actual)==len(rebuilt)
 assert np.allclose(j[f"{col}_persisted"],j[f"{col}_rebuilt"],equal_nan=True,atol=1e-12); return len(j)
def same(a,b,keys):
 a=a.sort_values(keys).reset_index(drop=True); b=b.sort_values(keys).reset_index(drop=True)
 pd.testing.assert_frame_equal(a,b,check_dtype=False,check_exact=False,rtol=0,atol=1e-12); return len(a)
def main():
 p=argparse.ArgumentParser(); p.add_argument("--artifact-root",type=Path,default=Path("artifacts/regime/runs")); args=p.parse_args()
 for run in (RUN,BASELINE):
  if not (args.artifact_root/run).is_dir(): raise FileNotFoundError(f"required immutable run is absent: {run}")
 store=RegimeArtifactStore(args.artifact_root); manifest=store.read_manifest(RUN)
 assert manifest["run_id"]==RUN and manifest["experiment_id"]==EXPERIMENT and manifest["status"]=="complete"
 md=manifest["metadata"]; assert md["promotion_contract"]=="capital_markets_native_feature_policy_2026_08_18"
 assert {m:md[f"{m}_feature_policy"] for m in POLICIES}==POLICIES and md["capital_markets_metric_weights"]==WEIGHTS
 assert md["human_decision"]=="capital_markets_native_feature_policy_approved" and md["automated_winner"] is False and md["family_metric_weight_calibration"]=="pending"
 verify=store.verify_run(RUN); assert len(verify)==len(manifest["artifacts"]) and verify.exists.all() and verify.hash_matches.all()
 cfg=load_regime_config(validate=True); governed=cfg.metric_dimensions[cfg.metric_dimensions.canonical_metric_key.isin(WEIGHTS)]
 assert governed.set_index("canonical_metric_key").metric_weight.astype(float).to_dict()==WEIGHTS
 assert cfg.axes.query("dimension=='capital_markets'").set_index("axis").dimension_weight.astype(float).to_dict()=={"demand":.10,"supply":.15}
 norm=store.read_dataframe(RUN,"normalized_features"); base_norm=store.read_dataframe(BASELINE,"normalized_features"); norm_rows=same(norm,base_norm,["geo_id","date","feature_key"])
 metrics=store.read_dataframe(RUN,"metric_scores"); rebuilt_metrics=score_metrics(norm); metric_rows=equal(metrics,rebuilt_metrics,["geo_id","date","canonical_metric_key"],"metric_score")
 base_metrics=store.read_dataframe(BASELINE,"metric_scores"); non_cm=lambda x:x[~x.canonical_metric_key.isin(WEIGHTS)]
 unchanged_metrics=same(non_cm(metrics),non_cm(base_metrics),["geo_id","date","canonical_metric_key"])
 aligned=store.read_dataframe(RUN,"aligned_metric_scores"); dims=store.read_dataframe(RUN,"dimension_scores"); dim_rows=equal(dims,score_dimensions(aligned),["geo_id","date","dimension"],"dimension_score")
 base_dims=store.read_dataframe(BASELINE,"dimension_scores"); unchanged_dims=same(dims[dims.dimension.ne("capital_markets")],base_dims[base_dims.dimension.ne("capital_markets")],["geo_id","date","dimension"])
 axes=store.read_dataframe(RUN,"axis_scores"); axis_rows=equal(axes,score_axes(dims),["geo_id","date","axis"],"axis_score")
 for axis in ("demand","supply"): assert not axes[axes.axis.eq(axis)].empty
 for name in ("coordinates","geometry","regime_assignments"): assert not store.read_dataframe(RUN,name).empty
 print(json.dumps({"manifest_status":"complete","artifact_count":len(verify),"artifact_hashes":"verified","native_feature_to_metric_rows":metric_rows,"metric_to_dimension_rows":dim_rows,"dimension_to_axis_rows":axis_rows,"normalization_rows_unchanged":norm_rows,"non_capital_markets_metric_rows_unchanged":unchanged_metrics,"non_capital_markets_dimension_rows_unchanged":unchanged_dims,"coordinates_geometry_regimes":"built","family_metric_weight_calibration":"pending"},sort_keys=True))
if __name__=="__main__": main()
