"""Offline governance counterexamples; fixture reports are never production evidence."""
from copy import deepcopy
import base64
import json
from pathlib import Path

import pytest

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh import acs_2024_authorization as auth
from jobs.monthly_refresh.acs_scope import ACSGovernance, identify
from test_acs_historical_materialization import registry, scope

ROOT = Path('.').resolve()


@pytest.fixture
def authorized(tmp_path, monkeypatch):
    reg = registry()
    reg['namespace'] = 'production'
    scopes = {s: scope(reg, s, start=2024)['authority_id'] for s in auth.QUALIFICATIONS}
    reports, paths = {}, {}
    for source in auth.QUALIFICATIONS:
        pin = auth.qualification.read_json(ROOT / f'config/monthly_source_input_pins/{auth.qualification.CYCLE}/{source}.json')
        evidence = pin['members']['snapshot']['evidence']
        cells = [{'geo_id': geo, 'classification': classification} for field, classification in
                 [('available_membership', 'retained_value'), ('provider_ineligible', 'monthly_no_content_attested')]
                 for geo in evidence[field]]
        report = {'schema_version': auth.qualification.VERSION, 'source_id': source, 'vintage': 2024,
            'artifact_id': auth.qualification.ELIGIBLE[source][0], 'technical_reconciliation': 'passed',
            'compatibility_governance': 'not_approved', 'disposition': 'blocked_pending_governance',
            'original_provider_response_independently_verified': 'not_established',
            'residual_limitations': auth.qualification.LIMITATIONS, 'predicates': [{'passed': True}],
            'evidence_inventory': {'fixture': {'sha256': 'a' * 64}}, 'parent': {'fixture': True},
            'code_provenance': {'manifest_git_sha': 'unknown', 'manifest_code_commit_recorded': False},
            'configuration': {}, 'validator_sha256': 'b' * 64, 'historical_scope': {'status': 'missing_authority'},
            'provenance': {'original_publication': {'cycle_id': auth.qualification.PUBLICATION_CYCLE},
                           'august_acceptance': {'cycle_id': auth.qualification.CYCLE}},
            'reconciliation': {'cells': cells}}
        reports[source] = identify(auth.qualification.VERSION, report, 'qualification_id')
        paths[source] = tmp_path / (source + '.json')
        paths[source].write_text(json.dumps(reports[source]))
    monkeypatch.setattr(auth, 'QUALIFICATIONS', {s: r['qualification_id'] for s, r in reports.items()})
    proposed = auth.proposal(reports, scopes, ROOT)
    record = identify(auth.VERSION, {**proposed, 'status': 'approved', 'approval_provenance': {'review': 'offline test only'}}, 'authorization_id')
    reg['compatibility_authorizations'] = {record['authorization_id']: record}
    reg['current_compatibility_authorization'] = record['authorization_id']
    class API:
        def __init__(self, repository, token, read_only):
            assert repository == auth.REPOSITORY and read_only
        def request(self, method, path, **kwargs):
            assert method == 'GET'
            if path == '/commits/main': return {'sha': 'a' * 40}, {}
            assert path.endswith('?ref=' + 'a' * 40)
            return {'content': base64.b64encode(json.dumps(reg).encode()).decode()}, {}
    monkeypatch.setattr('jobs.monthly_refresh.acs_scope._ProductionAPI', API)
    monkeypatch.setattr(auth.qualification, 'qualify', lambda **kw: deepcopy(reports[kw['source_id']]))
    def invoke(governance=None):
        return auth.verify(governance=governance or ACSGovernance.load_production(auth.REPOSITORY, 'fixture'),
            authorization_id=record['authorization_id'], reports=paths,
            packages={s: tmp_path / (s + '.tar') for s in reports}, catalog=tmp_path / 'catalog.json', root=ROOT)
    return reg, record, proposed, reports, paths, invoke


def test_exact_pair_and_geography_partition(authorized):
    reg, record, proposed, reports, paths, invoke = authorized
    checked = invoke()
    assert len(checked['acs5_only_geographies']) == 67
    assert len(checked['jointly_unobserved_geographies']) == 4
    assert proposed['status'] == 'proposed' and proposed['approval_provenance'] is None
    assert all(r['code_provenance']['manifest_git_sha'] == 'unknown' for r in checked['qualifications'].values())
    assert record['limitations']['historical_acceptance'] is False


@pytest.mark.parametrize('attack', ['missing', 'superseded', 'proposed', 'tampered', 'wrong_key', 'scope', 'configuration', 'classifications', 'limitations', 'consumer'])
def test_authority_rejects(authorized, attack):
    reg, record, proposed, reports, paths, invoke = authorized
    if attack == 'missing': reg['compatibility_authorizations'] = {}
    elif attack == 'superseded': reg['current_compatibility_authorization'] = 'other'
    elif attack == 'wrong_key': record['authorization_id'] = 'other'
    elif attack == 'scope': reg['current_scope'] = {}
    else:
        field, value = {'proposed': ('status', 'proposed'), 'tampered': ('repository', 'other/repo'),
            'configuration': ('config_hashes', {}), 'classifications': ('authorized_classifications', ['provider_verified']),
            'limitations': ('limitations', {}), 'consumer': ('consumer_sha256', 'c' * 64)}[attack]
        record[field] = value
        # Even a self-consistent substituted record cannot bypass the contract.
        if attack != 'tampered':
            updated = identify(auth.VERSION, record, 'authorization_id')
            record.update(updated)
            reg['compatibility_authorizations'] = {record['authorization_id']: record}
            reg['current_compatibility_authorization'] = record['authorization_id']
    with pytest.raises(ValueError): invoke()


@pytest.mark.parametrize('attack', ['absent', 'tampered', 'rehashed', 'failed', 'code', 'evidence', 'lineage', 'partition'])
def test_qualification_rejects(authorized, monkeypatch, attack):
    reg, record, proposed, reports, paths, invoke = authorized
    source = 'census_acs1'
    report = reports[source]
    if attack == 'absent': paths[source].unlink()
    elif attack == 'tampered': paths[source].write_text('{}')
    elif attack == 'rehashed':
        report['evidence_inventory']['extra'] = {'sha256': 'c' * 64}
        paths[source].write_text(json.dumps(identify(auth.qualification.VERSION, report, 'qualification_id')))
    else:
        # Simulate independent requalification discovering changed underlying evidence.
        field, value = {'failed': ('technical_reconciliation', 'failed'), 'code': ('code_provenance', {}),
            'evidence': ('evidence_inventory', {}), 'lineage': ('provenance', {}),
            'partition': ('reconciliation', {'cells': []})}[attack]
        report[field] = value
    with pytest.raises((ValueError, KeyError, OSError)): invoke()


def test_fixture_cannot_authorize(authorized):
    *_, invoke = authorized
    with pytest.raises(ValueError, match='test_only'):
        invoke(ACSGovernance.from_fixture(registry()))
    with pytest.raises(ValueError, match='capability'):
        invoke({'approved': True})


def test_current_main_drift_during_verification(authorized, monkeypatch):
    reg, record, proposed, reports, paths, invoke = authorized
    def qualify(**kw):
        reg['changed_during_read'] = True
        return deepcopy(reports[kw['source_id']])
    monkeypatch.setattr(auth.qualification, 'qualify', qualify)
    with pytest.raises(ValueError, match='superseded'): invoke()


def test_geography_partition_cannot_be_widened(authorized, monkeypatch):
    reg, record, proposed, reports, paths, invoke = authorized
    source = 'census_acs1'
    reports[source]['reconciliation']['cells'] = []
    reports[source] = identify(auth.qualification.VERSION, reports[source], 'qualification_id')
    monkeypatch.setitem(auth.QUALIFICATIONS, source, reports[source]['qualification_id'])
    paths[source].write_text(json.dumps(reports[source]))
    record['qualifications'][source] = auth.report_binding(reports[source], source)
    record.update(identify(auth.VERSION, record, 'authorization_id'))
    reg['compatibility_authorizations'] = {record['authorization_id']: record}
    reg['current_compatibility_authorization'] = record['authorization_id']
    with pytest.raises(ValueError, match='geography partition'): invoke()


def test_recovery_snapshot_cannot_authorize(authorized, monkeypatch):
    *_, invoke = authorized
    from jobs.monthly_refresh import acs_scope
    original = ACSGovernance.load_production(auth.REPOSITORY, 'fixture')
    # This is the same private capability used by load_production(snapshot=...).
    acs_scope._HISTORICAL.add(original)
    with pytest.raises(ValueError, match='recovery-only'): invoke(original)


def test_proposed_inputs_are_deliberately_not_authority():
    record = auth.qualification.read_json(ROOT / 'docs/proposals/acs_2024_compatibility_authorization.json')
    assert record['status'] == 'proposed_incomplete' and record['active'] is False
    assert record['approval_provenance'] is None
    assert all(r['retained_report'] is None for r in record['eligible_parents'].values())
    with pytest.raises(ValueError):
        auth.proposal(record['eligible_parents'], {}, ROOT)
