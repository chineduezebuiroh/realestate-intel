"""Smoke 180: late CES bootstrap recovery consumes evidence and never acquires."""
import json
from argparse import Namespace
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
import jobs.monthly_refresh.ces_bootstrap as bootstrap
from core.source_artifacts.hashing import write_canonical_json
from sources.bls_ces.artifact import build_request_plan, load_series_spec
plan=build_request_plan(load_series_spec(),start_year=1960,end_year=2024,
 acquisition_mode='deep_reconciliation',config_hashes={'x':'0'*64})
rows=[{'geo_id':i['geo_id'],'metric_id':i['metric_id'],'date':pd.Timestamp('2024-01-31').date(),
 'property_type_id':'all','value':100.,'source_id':'ces','property_type':'all'} for i in plan['series']]
frame=pd.DataFrame(rows)
diagnostics={'missing_mandatory_series':[],'mandatory_series':[i['series_id'] for i in plan['series'] if i['mandatory_for_target']],
 'target_month':'2024-01','missing_request_memberships':[],'unit':'thousands_of_jobs','scale_transform':'none'}
equivalence={c.lower()+'_count':0 for c in bootstrap.CATEGORIES}; equivalence.update({
 'exact_match_count':59,'identity_mismatch_count':0,'unexplained_numeric_mismatch_count':0,
 'unexplained_legacy_prior_only_count':0,'unit_scale':{'unit_scale_mismatch':False}})
detail=pd.DataFrame({'comparison_category':['EXACT_MATCH']*59})
with TemporaryDirectory() as td:
 root=Path(td)
 write_canonical_json(root/'preflight.json',{'series_count':59,'mandatory_series_count':50})
 write_canonical_json(root/'request_plan.json',plan)
 write_canonical_json(root/'acquisition.json',{'source_request_identity':plan['source_request_identity'],'row_count':59,'target_month':'2024-01'})
 frame.to_parquet(root/'canonical.parquet',index=False); write_canonical_json(root/'completeness.json',diagnostics)
 write_canonical_json(root/'equivalence.json',equivalence); detail.to_parquet(root/'equivalence_detail.parquet',index=False)
 write_canonical_json(root/'secondary_equivalence.json',{})
 original=bootstrap.acquire
 bootstrap.acquire=lambda *a,**k: (_ for _ in ()).throw(AssertionError('provider called'))
 try:
  first=bootstrap.recover(Namespace(output_root=root,retrieved_at='2026-01-01T00:00:00Z'))
 finally: bootstrap.acquire=original
 assert first['status']=='recovery_passed' and (root/'artifact'/'manifest.json').is_file()
 manifest=json.loads((root/'artifact'/'manifest.json').read_text())
 assert first['artifact_id']==manifest['artifact_id']
print('Smoke 180 CES bootstrap recovery passed')
