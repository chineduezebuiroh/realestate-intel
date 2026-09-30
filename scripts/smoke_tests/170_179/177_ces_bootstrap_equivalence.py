"""Smoke 177: CES bootstrap equivalence classification and fail-closed gates."""
from copy import deepcopy
import pandas as pd
from jobs.monthly_refresh.ces_bootstrap import acceptance_gates, equivalence_audit
from sources.bls_ces.artifact import build_request_plan

rows=[]
for i in range(50): rows.append({'geo_id':f'g{i}','series_id':f'SMS{i:017d}','metric_base':'ces_total_nonfarm','seasonal':'S'})
rows += [{'geo_id':'g0','series_id':'SMS90000000000000001','metric_base':'ces_total_private','seasonal':'S'},
         {'geo_id':'g1','series_id':'SMS90000000000000002','metric_base':'ces_construction','seasonal':'S'}]
plan=build_request_plan(rows,start_year=2022,end_year=2024,acquisition_mode='ordinary_overlap',config_hashes={'x':'0'*64})
facts=[]
for item in plan['series']:
 facts.append({'geo_id':item['geo_id'],'metric_id':item['metric_id'],'date':pd.Timestamp('2024-01-31').date(),
  'property_type_id':'all','value':100.0,'source_id':'ces','property_type':'all'})
provider=pd.DataFrame(facts)
legacy=provider[['geo_id','metric_id','date','property_type_id','value','source_id']].copy(); legacy['identity_configured']=True
# Exact, explicit revision, provider newer, provider historical, and prior-only.
revision_key=('g1','ces_total_nonfarm_sa',pd.Timestamp('2024-01-31').date(),'all')
provider.loc[(provider.geo_id=='g1')&(provider.metric_id=='ces_total_nonfarm_sa'),'value']=101.0
provider=pd.concat([provider,pd.DataFrame([{'geo_id':'g0','metric_id':'ces_total_nonfarm_sa','date':pd.Timestamp('2024-02-29').date(),'property_type_id':'all','value':102.,'source_id':'ces','property_type':'all'},
 {'geo_id':'g0','metric_id':'ces_total_nonfarm_sa','date':pd.Timestamp('2023-12-31').date(),'property_type_id':'all','value':99.,'source_id':'ces','property_type':'all'}])],ignore_index=True)
legacy=pd.concat([legacy,pd.DataFrame([{'geo_id':'g0','metric_id':'ces_total_nonfarm_sa','date':pd.Timestamp('2022-12-31').date(),'property_type_id':'all','value':98.,'source_id':'ces','identity_configured':True}])],ignore_index=True)
detail,summary=equivalence_audit(provider,legacy,plan,revision_policy='explicit',revision_keys={revision_key})
assert summary['exact_match_count']==51 and summary['provider_revision_count']==1
assert summary['provider_newer_count']==1 and summary['provider_historical_only_count']==1
assert summary['legacy_prior_only_count']==1 and summary['earliest_revision_date']=='2024-01-31'
assert summary['unit_scale']['status']=='provider_scale_supported'
# A non-approved numeric divergence is visible and fails acceptance.
_, unexplained=equivalence_audit(provider,legacy,plan,revision_policy='explicit',revision_keys=set())
assert unexplained['unexplained_numeric_mismatch_count']==1
# Systematic x1000 mismatch is never relabeled as revision.
scaled=legacy.copy(); scaled['value']=scaled['value']*1000
_,scale=equivalence_audit(provider[provider.date.eq(pd.Timestamp('2024-01-31').date())],scaled[scaled.date.eq(pd.Timestamp('2024-01-31').date())],plan)
assert scale['unit_scale']['unit_scale_mismatch'] and scale['unit_scale_mismatch_count']>0
# Unknown configured identity is classified explicitly.
bad=legacy.copy(); bad.loc[0,'geo_id']='unknown'; bad.loc[0,'identity_configured']=False
_,identity=equivalence_audit(provider,bad,plan)
assert identity['identity_mismatch_count']>=1
diagnostics={'missing_mandatory_series':[],'mandatory_series':[f'SMS{i:017d}' for i in range(50)],
 'target_month':'2024-01','missing_request_memberships':[],'unit':'thousands_of_jobs','scale_transform':'none'}
gate=acceptance_gates(provider[provider.date.eq(pd.Timestamp('2024-01-31').date())],diagnostics,
 equivalence_audit(provider[provider.date.eq(pd.Timestamp('2024-01-31').date())],legacy[legacy.date.eq(pd.Timestamp('2024-01-31').date())],plan)[1])
assert gate['status']=='passed'
bad_gate=acceptance_gates(provider[provider.date.eq(pd.Timestamp('2024-01-31').date())],diagnostics,unexplained)
assert bad_gate['status']=='failed' and 'no_unexplained_numeric_mismatch' in bad_gate['failed_checks']
print('Smoke 177 CES bootstrap equivalence passed')

# Acceptance evidence stays JSON-native at its actual canonical writer boundary.
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from argparse import Namespace
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.ces_bootstrap import recover
assert all(type(value) is bool for value in gate['checks'].values())
json.dumps(gate)
with TemporaryDirectory() as td:
 evidence=Path(td)
 write_canonical_json(evidence/'acceptance-boundary.json',gate)
 assert json.loads((evidence/'acceptance-boundary.json').read_text())==gate
