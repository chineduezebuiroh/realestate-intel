"""Smoke 175: governed Phase 3B cohort control plane and workflow safety."""
import hashlib, json
from urllib.error import HTTPError
from pathlib import Path
from tempfile import TemporaryDirectory
import yaml
from jobs.monthly_refresh.cohort import barrier_evidence, durable_redfin_result, resolve_invocation, resume_plan
from jobs.monthly_refresh.fred_macro import TransientFREDAcquisitionError, acquire_with_retry
from jobs.monthly_refresh.fred_result import build_result
from jobs.monthly_refresh.production import evaluate_barrier

policy=Path('config/monthly_refresh_policy.json')
catalog=json.load(open('config/artifact_catalog.json')); readiness=json.load(open('config/monthly_refresh_readiness.json'))
empty={'schema_version':'monthly_refresh_readiness_v1','records':[]}
noop=resolve_invocation(mode='normal',policy_path=policy,readiness=empty,catalog=catalog)
assert noop=={'status':'no_op','reason':'no_eligible_redfin_catalyst','fan_out':False,'invocation_mode':'normal'}
ready=resolve_invocation(mode='normal',policy_path=policy,readiness=readiness,catalog=catalog)
assert ready['fan_out'] and ready['cycle_id'].startswith('monthly_cycle__2026-07__')
for mode in ('resume','replay'):
    try: resolve_invocation(mode=mode,policy_path=policy,readiness=readiness,catalog=catalog)
    except ValueError as exc: assert 'cycle identity' in str(exc)
    else: raise AssertionError('explicit identity was not required')
replay=resolve_invocation(mode='replay',policy_path=policy,readiness=readiness,catalog=catalog,supplied_cycle_id=ready['cycle_id'])

def result(source,status='succeeded',retry='not_applicable'):
 return {'schema_version':'monthly_source_execution_result_v1','source_id':source,'cycle_id':ready['cycle_id'],'status':status,'candidate_artifact_id':'id-'+source,'artifact_content_hash':'b'*64,'package_sha256':'c'*64,'publication_state':'published_verified' if status=='succeeded' else 'not_published','validation_status':'passed' if status=='succeeded' else 'failed','provider_release_id':'release','observation_max':'2026-07-31','prior_artifact_id':'prior','source_change_detected':False,'retryability':retry,'accepted_pointer_changed':False,'evidence_uri':'artifact://evidence/'+source}
r,f,c,l=result('redfin'),result('fred_macro'),result('ces'),result('laus')
f['observation_max']='2026-08-31'
ev=barrier_evidence(cycle=replay,results=[r,f,c,l],pins=None,github={'run_id':'fixture'})
assert ev['barrier_status']=='ready' and not ev['source_set_created'] and not ev['accepted_pointers_advanced'] and not ev['redfin_consumption_committed']
assert ev['reused_source_ids']==[] and ev['retry_source_ids']==[]
assert ev['cycle_id']==ready['cycle_id'] and next(x for x in ev['candidates'] if x['source_id']=='fred_macro')['observation_max']=='2026-08-31'
# Reuse evidence follows the explicit validated resume-pin path, not unchanged
# artifact content returned by a source job.
fresh_unchanged_fred=dict(f); fresh_unchanged_fred['prior_artifact_id']=fresh_unchanged_fred['candidate_artifact_id']
one_reused=barrier_evidence(cycle=replay,results=[fresh_unchanged_fred,c,l],reused_results=[r],pins=None,github={})
assert one_reused['reused_source_ids']==['redfin'] and one_reused['retry_source_ids']==[]
assert fresh_unchanged_fred['prior_artifact_id']==fresh_unchanged_fred['candidate_artifact_id']
assert not fresh_unchanged_fred['source_change_detected']
assert 'fred_macro' not in one_reused['reused_source_ids']
both_reused=barrier_evidence(cycle=replay,results=[],reused_results=[r,f,c,l],pins=None,github={})
assert both_reused['reused_source_ids']==['ces','fred_macro','laus','redfin']
unchanged_redfin=dict(r); unchanged_redfin['prior_artifact_id']=unchanged_redfin['candidate_artifact_id']
normal_unchanged=barrier_evidence(cycle=replay,results=[unchanged_redfin,f,c,l],pins=None,github={})
assert normal_unchanged['reused_source_ids']==[]
retry=result('fred_macro','failed','retryable'); assert evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[r,retry]).status=='incomplete_retryable'
terminal=result('redfin','failed','terminal'); assert evaluate_barrier(expected_cycle_id=ready['cycle_id'],required_source_ids=('redfin','fred_macro'),results=[terminal,f]).status=='failed_terminal'
plan=resume_plan(('redfin','fred_macro','ces'),[r,c,retry],expected_cycle_id=ready['cycle_id']); assert plan['reuse']==['ces','redfin'] and plan['run']==['fred_macro']
resume_redfin=durable_redfin_result(cycle=ready,catalog=catalog)
assert resume_redfin['candidate_artifact_id']==ready['redfin_candidate_pin']['candidate_artifact_id']
assert resume_redfin['artifact_content_hash']==ready['redfin_candidate_pin']['artifact_content_hash']
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

# FRED retries provider/transport failures only: transient success, exhaustion,
# and deterministic failure all remain offline and test exact attempt counts.
attempts=[]
def transient_then_success():
 attempts.append(1)
 if len(attempts)==1: raise HTTPError('https://fred.test',502,'Bad Gateway',{},None)
 return 'current'
assert acquire_with_retry(transient_then_success,backoff_seconds=(0,0),sleep=lambda _:None)=='current'
assert len(attempts)==2
attempts=[]
def always_transient():
 attempts.append(1); raise HTTPError('https://fred.test',503,'Unavailable',{},None)
try: acquire_with_retry(always_transient,backoff_seconds=(0,0),sleep=lambda _:None)
except TransientFREDAcquisitionError: assert len(attempts)==3
else: raise AssertionError('exhausted transient acquisition succeeded')
attempts=[]
def deterministic_failure():
 attempts.append(1); raise ValueError('schema mismatch')
try: acquire_with_retry(deterministic_failure,backoff_seconds=(0,0),sleep=lambda _:None)
except ValueError: assert len(attempts)==1
else: raise AssertionError('deterministic acquisition failure succeeded')

# Actions outcomes are authoritative and missing/malformed evidence always
# becomes a governed failure rather than escaping from the result step.
with TemporaryDirectory() as td:
 root=Path(td); (root/'artifact').mkdir()
 run={'run_status':'refreshed','resulting_artifact_id':'fred-id','resulting_artifact_content_hash':'b'*64,
      'observation_max':'2026-08-31','prior_artifact_id':'prior','source_change_detected':True}
 manifest={'provider_release_id':'ordinary-current:fixture'}
 (root/'run_report.json').write_text(json.dumps(run)); (root/'artifact/manifest.json').write_text(json.dumps(manifest))
 (root/'publication.json').write_text(json.dumps({'package_sha256':'c'*64,'publication_state':'published_immutable_verified','accepted_pointer_changed':False,'durable_resolution_passed':True}))
 success=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='success',publish_outcome='success')
 assert success['status']=='succeeded' and success['retryability']=='not_applicable'
 (root/'run_report.json').write_text(json.dumps({'run_status':'failed','retryability':'retryable'}))
 transient=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='failure',publish_outcome='skipped')
 assert transient['status']=='failed' and transient['retryability']=='retryable'
 (root/'run_report.json').write_text(json.dumps({'run_status':'failed','retryability':'terminal'}))
 deterministic=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='failure',publish_outcome='skipped')
 assert deterministic['retryability']=='terminal'
 (root/'run_report.json').write_text(json.dumps(run))
 (root/'publication_failure.json').write_text(json.dumps({'retryability':'retryable'}))
 publication_transient=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='success',publish_outcome='failure')
 assert publication_transient['status']=='failed' and publication_transient['retryability']=='retryable' and publication_transient['publication_state']=='not_published'
 (root/'publication_failure.json').write_text(json.dumps({'retryability':'terminal'}))
 publication_terminal=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='success',publish_outcome='failure')
 assert publication_terminal['retryability']=='terminal'
 (root/'publication.json').unlink()
 missing_publication=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='success',publish_outcome='success')
 assert missing_publication['status']=='failed' and missing_publication['retryability']=='terminal'
 (root/'run_report.json').unlink()
 missing_run=build_result(root=root,cycle_id=ready['cycle_id'],acquire_outcome='success',publish_outcome='success')
 assert missing_run['status']=='failed' and missing_run['retryability']=='terminal'
 resume_failure=barrier_evidence(cycle=replay,results=[publication_transient],reused_results=[r],pins=None,github={})
 assert resume_failure['barrier_status']=='incomplete_retryable'
 assert resume_failure['reused_source_ids']==['redfin'] and resume_failure['retry_source_ids']==['ces','fred_macro','laus']
workflow_text=Path('.github/workflows/monthly-refresh-production.yml').read_text(); workflow=yaml.safe_load(workflow_text)
# PyYAML parses the YAML 1.1 key `on` as boolean True.
triggers=workflow.get(True,workflow.get('on')); assert 'workflow_dispatch' in triggers and 'push' in triggers and 'schedule' not in triggers and 'pull_request' not in triggers
assert triggers['push']['branches']==['monthly-refresh-orchestration'] and 1 <= len(triggers['push']['paths']) <= 12
assert 'always()' in workflow['jobs']['barrier']['if']; assert set(workflow['jobs']['barrier']['needs'])=={'resolve-cycle','redfin','fred','ces','laus','laus-satisfaction-repair'}
assert workflow['jobs']['redfin']['needs']=='resolve-cycle' and workflow['jobs']['fred']['needs']=='resolve-cycle' and workflow['jobs']['ces']['needs']=='resolve-cycle'
assert "run_redfin == 'true'" in workflow['jobs']['redfin']['if']
assert 'source_target_month' not in workflow['jobs']['fred'].get('with',{})
assert 'pinned_redfin_result' not in triggers['workflow_dispatch']['inputs']
fred_workflow=yaml.safe_load(Path('.github/workflows/fred-monthly-source.yml').read_text())
fred_inputs=fred_workflow.get(True,fred_workflow.get('on'))['workflow_call']['inputs']
assert not fred_inputs['source_target_month']['required'] and fred_inputs['source_target_month']['default']==''
fred_steps=fred_workflow['jobs']['source']['steps']; fred_by_id={step.get('id'):step for step in fred_steps}
assert fred_by_id['acquire']['continue-on-error'] and fred_by_id['publish']['continue-on-error']
assert fred_by_id['publish']['if']=="steps.acquire.outcome == 'success'"
assert fred_by_id['result']['env']['ACQUIRE_OUTCOME']=="${{ steps.acquire.outcome }}"
assert fred_by_id['result']['env']['PUBLISH_OUTCOME']=="${{ steps.publish.outcome }}"
assert 'raw_files' not in Path('.github/workflows/redfin-monthly-source.yml').read_text()
assert 'force:' not in workflow_text.lower() and 'force=true' not in workflow_text.lower()

# Every hosted Phase 3B job must establish the same governed Python runtime used
# by the repository's accepted source workflows before repository code executes.
phase3b_workflows={
 'monthly-refresh-production.yml':('resolve-cycle','barrier'),
 'redfin-monthly-source.yml':('source',),
 'fred-monthly-source.yml':('source',),
}
for filename,job_names in phase3b_workflows.items():
 parsed=yaml.safe_load(Path('.github/workflows',filename).read_text())
 for job_name in job_names:
  job=parsed['jobs'][job_name]; steps=job['steps']
  python_steps=[i for i,step in enumerate(steps) if 'python' in str(step.get('run',''))]
  assert python_steps, f'{filename}:{job_name} does not execute Python'
  first_python=min(python_steps)
  setup=[i for i,step in enumerate(steps) if step.get('uses')=='actions/setup-python@v5']
  installs=[i for i,step in enumerate(steps) if step.get('run')=='pip install -r requirements.txt']
  assert setup and setup[0] < first_python, f'{filename}:{job_name} sets up Python too late'
  assert installs and installs[0] < first_python, f'{filename}:{job_name} installs governed dependencies too late'
  assert job.get('env',{}).get('PYTHONPATH')=='.', f'{filename}:{job_name} lacks repository import path'
for path in ('data/market.duckdb','data/market_serving.duckdb','data/market_public.duckdb'):
 if Path(path).exists(): hashlib.sha256(Path(path).read_bytes()).hexdigest()
print('Smoke 175 monthly source cohort orchestrator passed')
