"""Smoke 190: LAUS C2 selection, satisfaction, modes, and cohort wiring."""
from datetime import date
import copy
from pathlib import Path
from tempfile import TemporaryDirectory
import json, subprocess
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.laus_annual_processing import select_routine,satisfaction_record,add_satisfaction
from jobs.monthly_refresh.laus_result import build_result
from jobs.monthly_refresh.laus_routine import satisfaction_from_cycle_record
from jobs.monthly_refresh.cohort import REQUIRED_SOURCES

def fails(error, fn):
    try: fn()
    except error: return
    raise AssertionError('expected failure')

def main():
    june=select_routine(cycle_date=date(2026,6,30),annual_reference_year=2026)
    july=select_routine(cycle_date=date(2026,7,1),annual_reference_year=2026)
    later=select_routine(cycle_date=date(2026,12,1),annual_reference_year=2026)
    assert june.acquisition_mode=='ordinary_overlap'
    assert july.acquisition_mode==later.acquisition_mode=='annual_deep'
    result={'status':'succeeded','validation_status':'passed','publication_state':'published_verified',
      'source_id':'laus','acquisition_mode':'annual_deep','candidate_artifact_id':'candidate',
      'artifact_content_hash':'a'*64,'package_sha256':'b'*64,'provider_release_id':'release'}
    record=satisfaction_record(decision=july,result=result,cycle_id='cycle')
    assert select_routine(cycle_date=date(2026,8,1),annual_reference_year=2026,satisfactions=[record]).acquisition_mode=='ordinary_overlap'
    assert select_routine(cycle_date=date(2027,8,1),annual_reference_year=2027,satisfactions=[record]).acquisition_mode=='annual_deep'
    assert add_satisfaction(record,record)==(record,False)
    conflict=copy.deepcopy(record); conflict['cycle_id']='other'
    fails(IdentityCollisionError,lambda:add_satisfaction(record,conflict))
    fails(ValueError,lambda:satisfaction_record(decision=june,result=result,cycle_id='cycle'))
    failed=copy.deepcopy(result); failed['status']='failed'
    fails(ValueError,lambda:satisfaction_record(decision=july,result=failed,cycle_id='cycle'))

    with TemporaryDirectory() as td:
      root=Path(td); (root/'artifact').mkdir()
      run={'run_status':'refreshed','acquisition_mode':'annual_deep','resulting_artifact_id':'candidate','resulting_artifact_content_hash':'a'*64,'observation_max':'2026-07-31','prior_artifact_id':'prior','source_change_detected':True}
      (root/'run_report.json').write_text(json.dumps(run)); (root/'artifact/manifest.json').write_text(json.dumps({'provider_release_id':'release'}))
      (root/'publication.json').write_text(json.dumps({'package_sha256':'b'*64,'publication_state':'published_immutable_verified','accepted_pointer_changed':False,'durable_resolution_passed':True}))
      successful=build_result(root=root,cycle_id='cycle',acquire_outcome='success',publish_outcome='success',acquisition_mode='annual_deep')
      assert successful['status']=='succeeded'
      assert build_result(root=root,cycle_id='cycle',acquire_outcome='success',publish_outcome='success',acquisition_mode='ordinary_overlap')['status']=='failed'
      for outcome in ('failure','cancelled','skipped'):
        assert build_result(root=root,cycle_id='cycle',acquire_outcome=outcome,publish_outcome='success',acquisition_mode='annual_deep')['status']=='failed'
        assert build_result(root=root,cycle_id='cycle',acquire_outcome='success',publish_outcome=outcome,acquisition_mode='annual_deep')['status']=='failed'
      evidence={'schema_version':'laus_cycle_execution_evidence_v1','cycle_id':'cycle','target_month':'2026-07','annual_reference_year':2026,'annual_vintage_id':'bls-laus-annual-processing-v1:2026','acquisition_mode':'annual_deep'}
      durable={'schema_version':'monthly_source_cycle_result_v1','cycle_id':'cycle','source_id':'laus','result_contract':'monthly_source_execution_result_v1','policy_schema_version':'monthly_refresh_policy_v2','result':successful,'source_evidence':evidence}
      repaired,due=satisfaction_from_cycle_record(record=durable,target_month='2026-07',invocation_mode='resume')
      assert due and repaired['annual_vintage_id']==evidence['annual_vintage_id']
      assert satisfaction_from_cycle_record(record=durable,target_month='2026-07',invocation_mode='resume',existing=repaired)==(repaired,False)
      ordinary=json.loads(json.dumps(durable)); ordinary['source_evidence']['acquisition_mode']='ordinary_overlap'
      assert satisfaction_from_cycle_record(record=ordinary,target_month='2026-07',invocation_mode='resume')==(None,False)
      failed=json.loads(json.dumps(durable)); failed['result']['status']='failed'
      fails(ValueError,lambda:satisfaction_from_cycle_record(record=failed,target_month='2026-07',invocation_mode='resume'))
      fails(ValueError,lambda:satisfaction_from_cycle_record(record=durable,target_month='2027-07',invocation_mode='resume'))
      fails(ValueError,lambda:satisfaction_from_cycle_record(record=durable,target_month='2026-07',invocation_mode='replay'))
      supplied=root/'satisfactions'; supplied.mkdir(); (supplied/'2026.json').write_text(json.dumps(repaired))
      output=root/'selection.json'
      subprocess.run(['python','-m','jobs.monthly_refresh.laus_routine','select','--target-month','2026-07','--satisfactions',str(supplied),'--output',str(output)],check=True,capture_output=True,text=True)
      assert json.loads(output.read_text())['acquisition_mode']=='ordinary_overlap'
      # Mutating only an in-memory common result cannot establish the durable mode.
      no_evidence=dict(durable); no_evidence.pop('source_evidence')
      fails(ValueError,lambda:satisfaction_from_cycle_record(record=no_evidence,target_month='2026-07',invocation_mode='resume'))
    assert not any('suclauss' in p.read_text() for p in Path('jobs/monthly_refresh').glob('laus_*.py'))
    assert 'laus' in REQUIRED_SOURCES
    master=Path('.github/workflows/monthly-refresh-production.yml').read_text()
    workflow=Path('.github/workflows/laus-monthly-source.yml').read_text()
    assert 'needs: [resolve-cycle, redfin, fred, ces, laus, laus-satisfaction-repair]' in master and 'LAUS_RESULT' in master
    assert "inputs.invocation_mode != 'replay'" in workflow
    assert "executed_mode" in workflow and "laus-satisfaction-repair" in master and 'steps.record.outcome == \'success\'' in workflow
    assert 'accepted_pointer_changed' in Path('jobs/monthly_refresh/laus_monthly.py').read_text()
    print('Smoke 190 passed: LAUS C2 routine selection and integration are governed.')
if __name__=='__main__': main()
