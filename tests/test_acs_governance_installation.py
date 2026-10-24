"""Installation checks are structural; all approval data here are synthetic fixtures."""
from copy import deepcopy
import base64
import json
from pathlib import Path

import pytest

from jobs.monthly_refresh import acs_governance_installation as install
from jobs.monthly_refresh.acs_scope import ACSGovernance, GAP_VERSION, SCOPE_VERSION, identify, check_identity

ROOT = Path('.').resolve()


def fixture_registry():
    """NEVER install: invented approvals for isolated HTTP/contract tests only."""
    proposal = install.generate(ROOT)
    registry = deepcopy(proposal['initial_registry'])
    records = deepcopy(proposal['records'])
    for name, value in records.items():
        approval = deepcopy(proposal['approval_templates'][name])
        approval.update(provenance_kind='github_approved_review', decision='approved', reviewer_login='synthetic-test-reviewer',
                        reviewed_at='2026-10-23T00:00:00Z', reviewed_pr=999999,
                        reviewed_commit='f' * 40,
                        review_url=f'https://github.com/{install.REPOSITORY}/pull/999999#pullrequestreview-999999',
                        decision_rationale='Synthetic fixture only; no human approval.')
        value['approval_provenance'] = approval
    gap = records['acs1_2020_gap']; gap['status'] = 'approved'
    gap = identify(GAP_VERSION, gap, 'evidence_id')
    registry['gap_evidence'][gap['evidence_id']] = gap
    for name in ['acs1_scope', 'acs5_scope']:
        scope = records[name]
        if name == 'acs1_scope':
            scope['governed_gaps'][0]['evidence_id'] = gap['evidence_id']
        scope = identify(SCOPE_VERSION, scope, 'authority_id')
        registry['scopes'][scope['authority_id']] = scope
        registry['current_scope'][scope['source_id']] = scope['authority_id']
    return registry


def rehash(registry):
    gap = next(iter(registry['gap_evidence'].values()))
    gap = identify(GAP_VERSION, gap, 'evidence_id')
    registry['gap_evidence'] = {gap['evidence_id']: gap}
    scopes = list(registry['scopes'].values()); registry['scopes'] = {}
    for scope in scopes:
        if scope['source_id'] == 'census_acs1':
            scope['governed_gaps'] = [{'vintage': 2020, 'evidence_id': gap['evidence_id']}]
        scope = identify(SCOPE_VERSION, scope, 'authority_id')
        registry['scopes'][scope['authority_id']] = scope
        registry['current_scope'][scope['source_id']] = scope['authority_id']


def test_exact_proposal_and_existing_identities():
    proposal = install.generate(ROOT)
    assert json.loads((ROOT / install.PROPOSAL_PATH).read_bytes()) == proposal
    assert install.validate_proposal(proposal, ROOT)['production_authority'] is False
    for name, record in proposal['records'].items():
        gap = name.endswith('_gap')
        check_identity(GAP_VERSION if gap else SCOPE_VERSION, record, 'evidence_id' if gap else 'authority_id')
        assert record['approval_provenance'] is None
        template = proposal['approval_templates'][name]
        assert template['reviewer_login'] is template['decision'] is template['reviewed_commit'] is None
        assert template['approved_request_scope']['requested_geographies'] == 223
        assert len(template['approved_request_scope']['excluded_geographies']) == 10
    assert all(proposal['initial_registry'][key] == {} for key in ['gap_evidence', 'scopes', 'current_scope', 'states', 'current_state'])
    assert not (ROOT / 'config/acs_historical_governance.json').exists()


def test_fixture_structural_contract_and_identity_cascade():
    registry = fixture_registry()
    result = install.validate_initial_registry(registry, ROOT)
    assert result['production_authority'] is False
    assert result['human_review_authenticity'] == 'requires independent verification'
    original = install.generate(ROOT)['records']
    assert next(iter(registry['gap_evidence'])) != original['acs1_2020_gap']['evidence_id']
    for source, key in result['scope_ids'].items():
        name = 'acs1_scope' if source == 'census_acs1' else 'acs5_scope'
        assert key != original[name]['authority_id']
    # Exercise the actual existing contract in a separately identified fixture namespace.
    fixture = deepcopy(registry); fixture['namespace'] = 'test_only'
    for value in [*fixture['scopes'].values(), *fixture['gap_evidence'].values()]:
        value['namespace'] = 'test_only'
    rehash(fixture)
    governance = ACSGovernance.from_fixture(fixture)
    for source, key in fixture['current_scope'].items():
        scope = governance.scope(source, key, ROOT)
        available = scope['expected_available_vintages']
        gaps = [g['vintage'] for g in scope['governed_gaps']]
        assert not set(available) & set(gaps)
        assert sorted(available + gaps) == list(range(scope['historical_range'][0], 2025))
        assert len(available) == (19 if source == 'census_acs1' else 16)
        assert governance.current_state(source) is None
    with pytest.raises(ValueError, match='test_only'): governance.require_production()


@pytest.mark.parametrize('field', ['decision', 'reviewer_login', 'reviewed_at', 'reviewed_pr', 'reviewed_commit', 'review_url', 'decision_rationale'])
def test_missing_genuine_approval_fields_rejected(field):
    registry = fixture_registry()
    next(iter(registry['gap_evidence'].values()))['approval_provenance'][field] = None
    rehash(registry)
    with pytest.raises(ValueError): install.validate_initial_registry(registry, ROOT)


@pytest.mark.parametrize('attack', ['subject', 'evidence', 'geography', 'limitations', 'config', 'available', 'extra_gap', 'status', 'approval', 'timestamp', 'review_commit', 'review_pr', 'gap_reference', 'scope_key', 'gap_key', 'pointer', 'extra_scope', 'state', 'state_pointer'])
def test_rehashed_forgery_or_invalid_installation_rejected(attack):
    registry = fixture_registry()
    gap = next(iter(registry['gap_evidence'].values()))
    scope = registry['scopes'][registry['current_scope']['census_acs5']]
    provenance = scope['approval_provenance']
    if attack == 'subject': provenance['subject']['proposal_identity'] = 'forged'
    elif attack == 'evidence': provenance['documentary_evidence']['sha256'] = '0' * 64
    elif attack == 'geography': provenance['approved_request_scope']['excluded_geographies'] = []
    elif attack == 'limitations': provenance['limitations'] = []
    elif attack == 'config': scope['config_hashes'] = {}
    elif attack == 'available': scope['expected_available_vintages'].remove(2023)
    elif attack == 'extra_gap': scope['governed_gaps'] = [{'vintage': 2023, 'evidence_id': gap['evidence_id']}]
    elif attack == 'status': gap['status'] = 'proposed'
    elif attack == 'approval': gap['approval_provenance'] = {'review': 'caller invented approval'}
    elif attack == 'timestamp': provenance['reviewed_at'] = '2026-10-23T00:00:00'
    elif attack == 'review_commit': provenance['reviewed_commit'] = 'main'
    elif attack == 'review_pr': provenance['review_url'] = 'https://github.com/other/repo/pull/1#pullrequestreview-1'
    rehash(registry)
    if attack == 'gap_reference':
        scope = registry['scopes'][registry['current_scope']['census_acs1']]
        scope['governed_gaps'][0]['evidence_id'] = 'forged'
        scope = identify(SCOPE_VERSION, scope, 'authority_id')
        registry['scopes'] = {k: v for k, v in registry['scopes'].items() if v['source_id'] != 'census_acs1'}
        registry['scopes'][scope['authority_id']] = scope
        registry['current_scope']['census_acs1'] = scope['authority_id']
    elif attack == 'scope_key': registry['scopes']['forged'] = registry['scopes'].pop(registry['current_scope']['census_acs5'])
    elif attack == 'gap_key': registry['gap_evidence']['forged'] = registry['gap_evidence'].pop(gap['evidence_id'])
    elif attack == 'pointer': registry['current_scope']['census_acs5'] = 'forged'
    elif attack == 'extra_scope': registry['scopes']['forged'] = scope
    elif attack == 'state': registry['states']['forged'] = {}
    elif attack == 'state_pointer': registry['current_state']['census_acs1'] = 'forged'
    with pytest.raises(ValueError): install.validate_initial_registry(registry, ROOT)


@pytest.mark.parametrize('attack', ['approve', 'pointer', 'geography', 'gap'])
def test_proposal_cannot_authorize_itself(attack):
    proposal = install.generate(ROOT)
    if attack == 'approve': proposal['approval_templates']['acs5_scope']['decision'] = 'approved'
    elif attack == 'pointer': proposal['initial_registry']['current_scope']['census_acs1'] = 'forged'
    elif attack == 'geography': proposal['approval_templates']['acs1_scope']['approved_request_scope']['requested_geographies'] = 224
    else: proposal['records']['acs1_2020_gap']['status'] = 'approved'
    with pytest.raises(ValueError): install.validate_proposal(proposal, ROOT)


@pytest.mark.parametrize('mode', ['valid', 'absent', 'proposed', 'drift', 'duplicate', 'wrong_namespace'])
def test_remote_main_boundary(monkeypatch, mode):
    # The dedicated provenance suite exercises actual authorization GETs; this
    # test isolates unchanged remote-main registry/capability behavior.
    monkeypatch.setattr('jobs.monthly_refresh.acs_governance_provenance.verify_authorizations',
                        lambda values, token, root: {'test_only': True})
    registry = fixture_registry()
    if mode == 'proposed':
        registry['gap_evidence'] = {install.generate(ROOT)['records']['acs1_2020_gap']['evidence_id']: install.generate(ROOT)['records']['acs1_2020_gap']}
    if mode == 'wrong_namespace': registry['namespace'] = 'test_only'
    calls = []
    class Response:
        headers = {}
        def __init__(self, value): self.value = value
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def getcode(self): return 200
        def read(self): return json.dumps(self.value).encode()
    class Opener:
        def open(self, request):
            assert request.method == 'GET'
            calls.append(request.full_url)
            if request.full_url.endswith('/commits/main'): return Response({'sha': 'a' * 40})
            assert request.full_url.endswith('?ref=' + 'a' * 40)
            if mode == 'absent': return Response(None)
            raw = json.dumps(registry)
            if mode == 'duplicate': raw = raw[:-1] + ', "states": {}}'
            if mode == 'drift' and len(calls) > 2: raw = json.dumps({**registry, 'extra': 'drift'})
            return Response({'content': base64.b64encode(raw.encode()).decode()})
    monkeypatch.setattr('urllib.request.build_opener', lambda *a, **k: Opener())
    if mode == 'valid':
        result = install.verify_remote(install.REPOSITORY, 'synthetic-token', ROOT)
        assert result['production_authority'] is True
        assert result['validated_main_commit'] == 'a' * 40
        assert result['activation'] is False
        assert len(calls) == 4
    else:
        with pytest.raises(ValueError): install.verify_remote(install.REPOSITORY, 'synthetic-token', ROOT)


def test_production_capability_rejects_local_and_wrong_repository():
    with pytest.raises(ValueError): ACSGovernance.from_fixture(fixture_registry())
    with pytest.raises(ValueError): ACSGovernance.from_main(fixture_registry())
    with pytest.raises(ValueError): install.verify_remote('other/repo', '', ROOT)


def test_mutating_review_output_cannot_change_validation_policy():
    proposal = install.generate(ROOT)
    proposal['limitations'].clear()
    with pytest.raises(ValueError): install.validate_proposal(proposal, ROOT)
    assert install.generate(ROOT)['limitations']
