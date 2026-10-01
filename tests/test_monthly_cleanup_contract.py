"""Cross-file regression coverage for post-migration monthly-refresh contracts."""
import json
from pathlib import Path

import yaml

from jobs.monthly_refresh.cohort import required_sources
from jobs.monthly_refresh.readiness import validate_readiness


def _json(path):
    return json.loads(Path(path).read_text())


def _workflow():
    return yaml.safe_load(Path('.github/workflows/monthly-refresh-production.yml').read_text())


def _triggers(workflow):
    # PyYAML 1.1 parses the unquoted GitHub Actions `on` key as True.
    return workflow.get(True, workflow.get('on'))


def test_required_policy_inventory_matches_hosted_execution_inventory():
    policy = _json('config/monthly_refresh_policy.json')
    registry = _json('config/monthly_source_execution_registry.json')
    policy_required = {item['source_id'] for item in policy['sources'] if item['required']}
    hosted_required = {item['source_id'] for item in registry['members']
                       if item['required'] and item['hosted_cohort_enabled']}
    assert policy_required == hosted_required == set(required_sources(registry))
    assert len(policy_required) == 12


def test_bps_acquisitions_are_independent_siblings_and_join_after_barrier():
    policy = {item['source_id']: item for item in _json('config/monthly_refresh_policy.json')['sources']}
    registry = {item['source_id']: item for item in
                _json('config/monthly_source_execution_registry.json')['members']}
    workflow = _workflow()['jobs']
    for source, job in (('census_bps', 'census-bps'),
                        ('census_bps_provisional', 'census-bps-provisional')):
        assert policy[source]['required'] is True
        assert policy[source]['dependencies'] == []
        assert registry[source]['required'] is True
        assert registry[source]['dependencies'] == []
        assert workflow[job]['needs'] == 'resolve-cycle'
        assert job in workflow['barrier']['needs']
    assert workflow['resolve-bps-family']['needs'] == ['resolve-cycle', 'barrier']
    family_inputs = workflow['resolve-bps-family']['with']
    assert family_inputs['compiled_artifact_id'] == '${{ needs.barrier.outputs.bps_artifact_id }}'
    assert family_inputs['provisional_artifact_id'] == '${{ needs.barrier.outputs.bps_provisional_artifact_id }}'


def test_dispatch_exposes_only_inputs_consumed_by_cycle_resolver():
    inputs = _triggers(_workflow())['workflow_dispatch']['inputs']
    assert set(inputs) == {'mode', 'cycle_id'}
    resolve = next(step for step in _workflow()['jobs']['resolve-cycle']['steps']
                   if step.get('id') == 'resolve')
    assert resolve['env']['SUPPLIED_CYCLE'] == '${{ inputs.cycle_id }}'
    assert 'inputs.mode' in resolve['env']['MODE']


def test_no_active_runtime_uses_retired_migration_branch():
    active = [Path('jobs/monthly_refresh/redfin.py'),
              Path('.github/workflows/monthly-refresh-production.yml'),
              Path('docs/contracts/monthly_refresh_production_v1.md'),
              Path('docs/contracts/redfin_monthly_source_v1.md')]
    assert all('monthly-refresh-orchestration' not in path.read_text() for path in active)


def test_schedule_is_enabled_without_changing_its_policy_contract():
    schedule = _json('config/monthly_refresh_policy.json')['schedule_policy']
    assert schedule == {
        'cadence': 'weekly_saturday_readiness_check',
        'enabled': True,
        'not_ready_behavior': 'successful_noop',
    }


def test_consumed_legacy_readiness_survives_new_policy_hash(tmp_path):
    readiness_path = Path('config/monthly_refresh_readiness.json')
    before = readiness_path.read_bytes()
    readiness = json.loads(before)
    assert {record['target_month'] for record in readiness['records']} >= {'2026-07', '2026-08'}
    assert all(record['consumed'] for record in readiness['records']
               if record['target_month'] in {'2026-07', '2026-08'})
    validate_readiness(readiness, catalog=_json('config/artifact_catalog.json'),
                       policy_path=Path('config/monthly_refresh_policy.json'))
    assert readiness_path.read_bytes() == before
