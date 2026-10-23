"""Proposal generation never establishes governance, even with self-consistent IDs."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
import sys

import pytest

from jobs.monthly_refresh import acs_scope_bootstrap as bootstrap
from jobs.monthly_refresh.acs_scope import ACSGovernance, SCOPE_VERSION, identify

ROOT = Path('.').resolve()
PROPOSALS = ROOT / 'docs/proposals/acs_historical_scope_bootstrap'


@pytest.fixture
def documents():
    return bootstrap.generate(ROOT)


def fixture_governance(documents, source, *, approved=False):
    registry = deepcopy(documents[bootstrap.REGISTRY_FILE]['registry_template'])
    registry['namespace'] = 'test_only'
    scope = deepcopy(documents[bootstrap.FILENAMES[source]]['scope_record'])
    scope['namespace'] = 'test_only'
    if approved:
        scope['approval_provenance'] = {'review': 'offline test fixture only'}
    scope = identify(SCOPE_VERSION, scope, 'authority_id')
    registry['scopes'][scope['authority_id']] = scope
    registry['current_scope'][source] = scope['authority_id']
    return registry, scope


def test_deterministic_checked_in_artifacts(documents):
    retained = {p.name: json.loads(p.read_bytes()) for p in PROPOSALS.iterdir()}
    assert retained == documents == bootstrap.generate(ROOT)
    result = bootstrap.validate(retained, ROOT)
    assert result['proposal_validation'] == 'passed'
    assert result['production_authority'] is False and result['activation'] is False
    initial = documents[bootstrap.REGISTRY_FILE]['registry_template']
    assert all(initial[key] == {} for key in ['scopes', 'current_scope', 'gap_evidence', 'states', 'current_state'])


def test_scope_and_coverage_are_separate(documents):
    one = documents[bootstrap.FILENAMES['census_acs1']]
    five = documents[bootstrap.FILENAMES['census_acs5']]
    assert one['scope_record']['historical_range'] == [2005, 2024]
    assert one['partition']['unresolved_vintages'] == [2020]
    assert one['scope_record']['governed_gaps'] == []
    assert 2020 not in one['scope_record']['expected_available_vintages']
    assert five['scope_record']['historical_range'] == [2009, 2024]
    assert 2023 in five['scope_record']['expected_available_vintages']
    assert 2023 in five['acquisition_coverage']['missing_acquisition_vintages']
    for value in [one, five]:
        assert value['request_scope']['requested_geographies'] == 223
        assert len(value['request_scope']['excluded_geographies']) == 10
        assert len(value['request_scope']['metrics']) == 2
        assert value['scope_record']['approval_provenance'] is None
        assert value['physical_state'] == 'not_accepted'


@pytest.mark.parametrize('source', bootstrap.RANGES)
def test_missing_approval_rejected_by_existing_scope_validator(documents, source):
    registry, scope = fixture_governance(documents, source)
    with pytest.raises(ValueError, match='invalid approved'):
        ACSGovernance.from_fixture(registry).scope(source, scope['authority_id'], ROOT)


def test_existing_contract_rejects_unresolved_2020_partition(documents):
    registry, scope = fixture_governance(documents, 'census_acs1', approved=True)
    with pytest.raises(ValueError, match='exactly partition'):
        ACSGovernance.from_fixture(registry).scope('census_acs1', scope['authority_id'], ROOT)


def test_existing_contract_can_represent_acs5_scope_without_acquisition(documents):
    registry, scope = fixture_governance(documents, 'census_acs5', approved=True)
    assert ACSGovernance.from_fixture(registry).scope('census_acs5', scope['authority_id'], ROOT) == scope
    with pytest.raises(ValueError, match='test_only'):
        ACSGovernance.from_fixture(registry).require_production()


@pytest.mark.parametrize('attack', ['approval', 'gap', 'partition', 'range', 'authority', 'active', 'registry', 'metrics', 'exclusions', 'coverage'])
def test_forged_or_activated_proposal_rejected(documents, attack):
    one = documents[bootstrap.FILENAMES['census_acs1']]
    record = one['scope_record']
    if attack == 'approval': record['approval_provenance'] = {'review': 'forged'}
    elif attack == 'gap': record['governed_gaps'] = [{'vintage': 2020, 'evidence_id': 'invented'}]
    elif attack == 'partition': record['expected_available_vintages'].append(2020)
    elif attack == 'range': record['historical_range'] = [2024, 2024]
    elif attack == 'authority': record['authority_id'] = 'forged'
    elif attack == 'active': one['active'] = True
    elif attack == 'registry': documents[bootstrap.REGISTRY_FILE]['registry_template']['current_scope']['census_acs1'] = record['authority_id']
    elif attack == 'metrics': one['request_scope']['metrics'] = []
    elif attack == 'exclusions': one['request_scope']['excluded_geographies'] = []
    elif attack == 'coverage': one['physical_state'] = 'accepted'
    if attack != 'authority': one['scope_record'] = identify(SCOPE_VERSION, record, 'authority_id')
    with pytest.raises(ValueError): bootstrap.validate(documents, ROOT)


def test_unauthorized_gap_rejected_by_existing_governance(documents):
    registry, scope = fixture_governance(documents, 'census_acs1', approved=True)
    scope['governed_gaps'] = [{'vintage': 2020, 'evidence_id': 'invented'}]
    scope = identify(SCOPE_VERSION, scope, 'authority_id')
    registry['scopes'] = {scope['authority_id']: scope}
    registry['current_scope']['census_acs1'] = scope['authority_id']
    with pytest.raises(ValueError): ACSGovernance.from_fixture(registry).scope('census_acs1', scope['authority_id'], ROOT)


def test_real_configuration_drift(documents, tmp_path):
    files = set(documents[bootstrap.REGISTRY_FILE]['input_file_sha256']) | set(documents[bootstrap.REGISTRY_FILE]['config_hashes'])
    for name in files:
        dest = tmp_path / name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, dest)
    target = tmp_path / 'config/monthly_refresh_policy.json'
    target.write_bytes(target.read_bytes() + b'\n')
    with pytest.raises(ValueError, match='drift'): bootstrap.validate(documents, tmp_path)


def test_raw_proposals_cannot_become_production_governance(documents):
    with pytest.raises(ValueError, match='test_only'):
        ACSGovernance.from_fixture(documents[bootstrap.REGISTRY_FILE]['registry_template'])
    with pytest.raises(ValueError, match='injectable'):
        ACSGovernance.from_main(documents)


@pytest.mark.parametrize('mode', ['production_path', 'activate'])
def test_cli_rejects_activation(monkeypatch, tmp_path, mode):
    args = ['bootstrap', '--repository-root', str(ROOT)]
    args += ['--output', str(ROOT / 'config/acs_historical_governance.json')] if mode == 'production_path' else ['--activate']
    monkeypatch.setattr(sys, 'argv', args)
    with pytest.raises(SystemExit) as exc: bootstrap.main()
    assert exc.value.code == 2
    assert not (ROOT / 'config/acs_historical_governance.json').exists()
