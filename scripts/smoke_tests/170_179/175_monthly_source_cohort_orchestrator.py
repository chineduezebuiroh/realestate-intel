"""Smoke 175: governed Phase 3B cohort control plane and workflow safety."""
import hashlib, json
from pathlib import Path
from tempfile import TemporaryDirectory
import yaml
from jobs.monthly_refresh.cohort import barrier_evidence, resolve_invocation, resume_plan
from jobs.monthly_refresh.production import evaluate_barrier

policy=Path('config/monthly_refresh_policy.json')
drop={'drop_id':'2026-07','drop_content_hash':'a'*64,'target_month':'2026-07','status':'validated','validation_status':'passed','complete_family_count':7,'required_family_count':7,'quarantined':False}
noop=resolve_invocation(mode='normal',policy_path=policy,drop=None,consumed_drop_ids=set())
assert noop=={'status':'no_op','reason':'no_eligible_redfin_catalyst','fan_out':False,'invocation_mode':'normal'}
ready=resolve_invocation(mode='normal',policy_path=policy,drop=drop,consumed_drop_ids=set())
assert ready['fan_out'] and ready['cycle_id'].startswith('monthly_cycle__2026-07__')
for mode in ('resume','replay'):
    try: resolve_invocation(mode=mode,policy_path=policy,drop=drop,consumed_drop_ids=set())
    except ValueError as exc: assert 'cycle identity' in str(exc)
    else: raise AssertionError('explicit identity was not required')
replay=resolve_invocation(mode='replay',policy_path=policy,drop=drop,consumed_drop_ids=set(),supplied_cycle_id=ready['cycle_id'])

def result(source,status='succeeded',retry='not_applicable'):
 return {'schema_version':'monthly_source_execution_result_v1','source_id':source,'cycle_id':ready['cycle_id'],'status':status,'candidate_artifact_id':'id-'+source,'artifact_content_hash':'b'*64,'package_sha256':'c'*64,'publication_state':'published_verified' if status=='succeeded' else 'not_published','validation_status':'passed' if status=='succeeded' else 'failed','provider_release_id':'release','observation_max':'2026-07-31','prior_artifact_id':'prior','source_change_detected':False,'retryability':retry,'evidence_uri':'artifact://evidence/'+source}
r,f=result('redfin'),result('fred_macro')
ev=barrier_evidence(cycle=replay,results=[r,f],pins=None,github={'run_id':'fixture'})
assert ev['barrier_status']=='ready' and not ev['source_set_created'] and not ev['accepted_pointers_advanced'] and not ev['redfin_consumption_committed']
retry=result('fred_macro','failed','retryable'); assert evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[r,retry]).status=='incomplete_retryable'
terminal=result('redfin','failed','terminal'); assert evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[terminal,f]).status=='failed_terminal'
plan=resume_plan(('redfin','fred_macro'),[r,retry],expected_cycle_id=ready['cycle_id']); assert plan['reuse']==['redfin'] and plan['run']==['fred_macro']
evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[r,f],pinned_candidates=plan['pins'])
for mutation,message in [(('cycle_id','wrong'),'cycle'),(('source_id','other'),'unexpected')]:
 bad=dict(r);bad[mutation[0]]=mutation[1]
 try:evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[bad,f])
 except ValueError as exc: assert message in str(exc)
 else:raise AssertionError('invalid result accepted')
bad=dict(r);bad.pop('package_sha256')
try:evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[bad,f])
except ValueError:pass
else:raise AssertionError('malformed result accepted')
pins={'redfin':{k:r[k] for k in ('candidate_artifact_id','artifact_content_hash','package_sha256','publication_state','provider_release_id')}}; drift=dict(r);drift['package_sha256']='d'*64
try:evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[drift,f],pinned_candidates=pins)
except ValueError as exc:assert 'drift' in str(exc)
else:raise AssertionError('candidate drift accepted')
workflow_text=Path('.github/workflows/monthly-refresh-production.yml').read_text(); workflow=yaml.safe_load(workflow_text)
# PyYAML parses the YAML 1.1 key `on` as boolean True.
triggers=workflow.get(True,workflow.get('on')); assert 'workflow_dispatch' in triggers and 'schedule' not in triggers
assert 'always()' in workflow['jobs']['barrier']['if']; assert set(workflow['jobs']['barrier']['needs'])=={'resolve-cycle','redfin','fred'}
assert workflow['jobs']['redfin']['needs']=='resolve-cycle' and workflow['jobs']['fred']['needs']=='resolve-cycle'
assert 'force:' not in workflow_text.lower() and 'force=true' not in workflow_text.lower()
for path in ('data/market.duckdb','data/market_serving.duckdb','data/market_public.duckdb'):
 if Path(path).exists(): hashlib.sha256(Path(path).read_bytes()).hexdigest()
print('Smoke 175 monthly source cohort orchestrator passed')
