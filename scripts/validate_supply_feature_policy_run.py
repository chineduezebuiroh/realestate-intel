"""Validate the immutable Supply native-feature-policy production candidate."""
from __future__ import annotations
import argparse, json
from pathlib import Path
import numpy as np
import pandas as pd
from regime.artifacts import RegimeArtifactStore
from regime._03_metric_scorer import score_metrics
from regime._05_dimension_scorer import score_dimensions
from regime._06_axis_engine import score_axes
RUN='supply_feature_policy_production_20260817'; EXPERIMENT='supply_feature_policy_production'

def equal_scores(actual, rebuilt, keys, column):
 x=actual.merge(rebuilt,on=keys,suffixes=('_persisted','_rebuilt'),validate='one_to_one')
 assert len(x)==len(actual)==len(rebuilt)
 assert np.allclose(x[f'{column}_persisted'],x[f'{column}_rebuilt'],equal_nan=True)
 return len(x)

def main():
 p=argparse.ArgumentParser(); p.add_argument('--artifact-root',type=Path,default=Path('artifacts/regime/runs')); a=p.parse_args()
 store=RegimeArtifactStore(a.artifact_root); m=store.read_manifest(RUN)
 assert m['status']=='complete' and m['run_id']==RUN and m['experiment_id']==EXPERIMENT
 assert m['metadata']['promotion_contract']=='supply_native_feature_policy_2026_08_17'
 v=store.verify_run(RUN); assert len(v)==29 and v.exists.all() and v.hash_matches.all()
 normalized=store.read_dataframe(RUN,'normalized_features'); metrics=store.read_dataframe(RUN,'metric_scores')
 mc=equal_scores(metrics,score_metrics(normalized),['geo_id','date','canonical_metric_key'],'metric_score')
 aligned=store.read_dataframe(RUN,'aligned_metric_scores'); dimensions=store.read_dataframe(RUN,'dimension_scores')
 dc=equal_scores(dimensions,score_dimensions(aligned),['geo_id','date','dimension'],'dimension_score')
 axes=store.read_dataframe(RUN,'axis_scores'); ac=equal_scores(axes,score_axes(dimensions),['geo_id','date','axis'],'axis_score')
 assert {'demand','supply'}<=set(axes.axis)
 for name in ('coordinates','geometry','regime_assignments'): assert not store.read_dataframe(RUN,name).empty
 print(json.dumps({'manifest_status':'complete','artifact_count':len(v),'metric_rows_reconstructed':mc,'dimension_rows_reconstructed':dc,'axis_rows_reconstructed':ac,'coordinates_geometry_regimes':'ok'}))
if __name__=='__main__': main()
