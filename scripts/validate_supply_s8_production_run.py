#!/usr/bin/env python3
"""Fail-closed validation for the immutable Supply S8 production run."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from regime._00_config_loader import load_regime_config
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
from regime.artifacts import RegimeArtifactStore

RUN = "supply_s8_production_20260817"
EXPERIMENT = "supply_s8_production"
PRIOR = "supply_feature_policy_production_20260817"
SUPPLY = {"active_inventory": .65, "permit_activity": .30, "permit_intensity": .05}
FEATURES = {
 "redfin_inventory": {"level": ("ma_level","12m",.40), "short_term_change": ("ma_pct_change","12m/lag3m",.15), "long_term_change": ("ma_pct_change","12m/lag12m",.45)},
 "bps_total_units": {"level": ("ma_level","12m",.75), "short_term_change": ("ma_pct_change","12m/lag6m",.10), "long_term_change": ("ma_pct_change","12m/lag12m",.15)},
 "derived_permit_intensity": {"level": ("ma_level","12m",.40), "short_term_change": ("ma_pct_change","12m/lag3m",.15), "long_term_change": ("ma_pct_change","12m/lag12m",.45)},
}

def _equal_scores(actual, rebuilt, keys, column):
    joined=actual.merge(rebuilt,on=keys,suffixes=("_persisted","_rebuilt"),validate="one_to_one")
    assert len(joined)==len(actual)==len(rebuilt)
    assert np.allclose(joined[f"{column}_persisted"],joined[f"{column}_rebuilt"],equal_nan=True,atol=1e-12)
    return len(joined)

def _same(left, right, keys):
    left=left.sort_values(keys).reset_index(drop=True); right=right.sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(left,right,check_dtype=False,check_exact=False,rtol=0,atol=1e-12)
    return len(left)

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--artifact-root",type=Path,default=Path("artifacts/regime/runs")); args=parser.parse_args()
    store=RegimeArtifactStore(args.artifact_root)
    for run in (RUN,PRIOR):
      if not (args.artifact_root/run).is_dir(): raise FileNotFoundError(f"required immutable run is absent: {run}")
    manifest=store.read_manifest(RUN)
    assert manifest["status"]=="complete" and manifest["run_id"]==RUN and manifest["experiment_id"]==EXPERIMENT
    metadata=manifest["metadata"]
    assert metadata["promotion_contract"]=="supply_metric_weight_s8_2026_08_17"
    assert metadata["supply_metric_weights"]==SUPPLY and metadata["human_decision"]=="supply_s8_metric_weight_approved"
    assert metadata["automated_winner"] is False and metadata["supply_calibration"]=="closed" and metadata["capital_markets"]=="unchanged"
    verification=store.verify_run(RUN); assert verification.exists.all() and verification.hash_matches.all()

    config=load_regime_config(validate=True)
    supply=config.metric_dimensions[config.metric_dimensions.canonical_metric_key.isin(SUPPLY)]
    assert len(supply)==3 and supply.set_index("canonical_metric_key").metric_weight.astype(float).to_dict()==SUPPLY
    for metric,expected in FEATURES.items():
      family=config.features[config.features.metric_key.eq(metric)]
      assert len(family)==3
      for feature_type,wanted in expected.items():
       row=family[family.feature_type.eq(feature_type)].iloc[0]
       assert (row["transform"],row["feature_window"],float(row.feature_weight))==wanted

    normalized=store.read_dataframe(RUN,"normalized_features"); prior_normalized=store.read_dataframe(PRIOR,"normalized_features")
    normalization_rows=_same(normalized,prior_normalized,["geo_id","date","feature_key"])
    metrics=store.read_dataframe(RUN,"metric_scores"); prior_metrics=store.read_dataframe(PRIOR,"metric_scores")
    metric_rows=_equal_scores(metrics,score_metrics(normalized),["geo_id","date","canonical_metric_key"],"metric_score")
    unchanged_metric_rows=_same(metrics,prior_metrics,["geo_id","date","canonical_metric_key"])
    aligned=store.read_dataframe(RUN,"aligned_metric_scores"); dimensions=store.read_dataframe(RUN,"dimension_scores")
    dimension_rows=_equal_scores(dimensions,score_dimensions(aligned),["geo_id","date","dimension"],"dimension_score")
    prior_dimensions=store.read_dataframe(PRIOR,"dimension_scores")
    unchanged_dimensions=_same(dimensions[dimensions.dimension.ne("supply")],prior_dimensions[prior_dimensions.dimension.ne("supply")],["geo_id","date","dimension"])
    axes=store.read_dataframe(RUN,"axis_scores"); axis_rows=_equal_scores(axes,score_axes(dimensions),["geo_id","date","axis"],"axis_score")
    prior_axes=store.read_dataframe(PRIOR,"axis_scores")
    demand_rows=_same(axes[axes.axis.eq("demand")],prior_axes[prior_axes.axis.eq("demand")],["geo_id","date","axis"])
    assert not axes[axes.axis.eq("supply")].empty
    for name in ("coordinates","geometry","regime_assignments"): assert not store.read_dataframe(RUN,name).empty
    print(json.dumps({"manifest_status":"complete","artifact_count":len(verification),"artifact_hashes":"verified",
      "metric_rows_reconstructed":metric_rows,"dimension_rows_reconstructed":dimension_rows,"axis_rows_reconstructed":axis_rows,
      "normalization_rows_unchanged":normalization_rows,"metric_rows_unchanged":unchanged_metric_rows,
      "non_supply_dimension_rows_unchanged":unchanged_dimensions,"demand_rows_unchanged":demand_rows,
      "supply_axis":"ok","coordinates_geometry_regimes":"ok"},sort_keys=True))

if __name__=="__main__": main()
