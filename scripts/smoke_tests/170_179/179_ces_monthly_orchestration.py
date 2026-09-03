"""Smoke 179: CES-C reconciliation, result contract, and hosted wiring."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import pandas as pd
import yaml
from core.source_artifacts.reconciliation import preserve_prior
from jobs.monthly_refresh.ces_result import build_result
from jobs.monthly_refresh.production import validate_source_result

key=lambda date,value:{'geo_id':'g','metric_id':'ces_total_nonfarm_sa','date':pd.Timestamp(date).date(),
 'property_type_id':'all','value':value,'source_id':'ces','property_type':'all'}
prior=pd.DataFrame([key('2023-01-31',1.),key('2024-01-31',2.)])
current=pd.DataFrame([key('2024-01-31',3.),key('2024-02-29',4.)])
merged=preserve_prior(prior,current)
assert list(merged.value)==[1.,3.,4.] # prior-only kept, overlap provider wins, newer added
with TemporaryDirectory() as td:
 root=Path(td); (root/'artifact').mkdir();
 run={'run_status':'refreshed','resulting_artifact_id':'src__ces__fixture',
  'resulting_artifact_content_hash':'a'*64,'observation_max':'2024-02-29',
  'prior_artifact_id':'src__ces__prior','source_change_detected':True}
 (root/'run_report.json').write_text(json.dumps(run));
 (root/'artifact'/'manifest.json').write_text(json.dumps({'provider_release_id':'ordinary-current:'+'b'*64}))
 (root/'publication.json').write_text(json.dumps({'package_sha256':'c'*64,
  'publication_state':'published_immutable_verified','accepted_pointer_changed':False,'durable_resolution_passed':True}))
 result=build_result(root=root,cycle_id='cycle',acquire_outcome='success',publish_outcome='success')
 assert result['source_id']=='ces' and result['accepted_pointer_changed'] is False
 validate_source_result(result,expected_cycle_id='cycle')
 (root/'publication_failure.json').write_text(json.dumps({'retryability':'retryable'}))
 failed=build_result(root=root,cycle_id='cycle',acquire_outcome='success',publish_outcome='failure')
 assert failed['retryability']=='retryable'
workflow=yaml.safe_load(Path('.github/workflows/monthly-refresh-production.yml').read_text())
assert workflow['jobs']['ces']['needs']=='resolve-cycle'
assert set(workflow['jobs']['barrier']['needs'])=={'resolve-cycle','redfin','fred','ces','laus','census-bps','census-bps-provisional','laus-satisfaction-repair'}
assert 'schedule' not in workflow.get(True,workflow.get('on'))
source=yaml.safe_load(Path('.github/workflows/ces-monthly-source.yml').read_text())
assert source.get(True,source.get('on'))['workflow_call']['secrets']['BLS_API_KEY']['required']
text=Path('jobs/monthly_refresh/cohort.py').read_text(); assert '("redfin", "fred_macro", "ces", "laus", "census_bps", "census_bps_provisional")' in text
print('Smoke 179 CES monthly orchestration passed')
