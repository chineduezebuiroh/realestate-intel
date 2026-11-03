"""Installation artifact checks; retained audits never confer runtime authority."""
from copy import deepcopy
import json
from pathlib import Path
import zipfile

import pytest

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh import acs_2024_authorization as auth
from jobs.monthly_refresh.acs_governance_provenance import body_hash, validate_comment_metadata
from jobs.monthly_refresh.acs_scope import ACSGovernance, check_identity

ROOT = Path(__file__).resolve().parents[1]


def artifacts():
    registry = json.loads((ROOT / 'config/acs_historical_governance.json').read_text())
    with zipfile.ZipFile(ROOT / 'docs/audit/acs302-owner-audits.zip') as archive:
        audits = [json.loads(archive.read(name)) for name in (
            'compatibility-302-owner-audit.json', 'compatibility-302-owner-audit-preinstall.json')]
    return registry, audits


def test_only_compatibility_fields_added_to_audited_registry():
    registry, audits = artifacts()
    baseline = {k: v for k, v in registry.items() if k not in (
        'compatibility_authorizations', 'current_compatibility_authorization')}
    for audit in audits:
        assert sha256_json(baseline) == audit['scope_verification']['governance_snapshot']['registry_sha256']
        assert audit['snapshot_sha256'] == sha256_json({k: v for k, v in audit.items() if k != 'snapshot_sha256'})
    assert baseline['states'] == baseline['current_state'] == {}
    assert audits[0]['approval_provenance'] == audits[1]['approval_provenance']
    pointer = registry['current_compatibility_authorization']
    assert list(registry['compatibility_authorizations']) == [pointer]
    record = registry['compatibility_authorizations'][pointer]
    check_identity(auth.VERSION, record, 'authorization_id')
    assert record['approval_provenance'] == audits[1]['approval_provenance']
    proposal = deepcopy(record)
    proposal.update(status='proposed', approval_provenance=None,
                    authorization_id=record['approval_provenance']['subject']['proposal_identity'])
    auth.validate_proposal(proposal, ROOT)
    assert sha256_json(proposal) == record['approval_provenance']['subject']['record_sha256']
    assert record['scope_authorities'] == baseline['current_scope']


def test_retained_owner_body_and_no_superseding_discussion():
    _, audits = artifacts()
    for audit in audits:
        provenance = audit['approval_provenance']
        validate_comment_metadata(provenance)
        assert body_hash(audit['github_record']['body']) == provenance['owner_comment']['body_sha256']
        assert sha256_json(audit['github_record']) == provenance['owner_comment']['snapshot_sha256']
        body = json.loads(audit['github_record']['body'])
        assert body['personally_reviewed_and_authorized'] is True
        assert body['reviewed_commit'] == provenance['reviewed_commit']
        assert body['decisions']['acs_2024_compatibility']['subject'] == provenance['subject']
        for scan in audit['complete_scans'].values():
            assert [item['id'] for item in scan['comments']] == [6590583804]
            assert scan['reviews'] == scan['inline'] == []
        assert audit['production_authority'] is audit['activation'] is False


def test_audit_and_registry_cannot_be_used_as_production_capability():
    registry, _ = artifacts()
    with pytest.raises(ValueError, match='test_only'):
        ACSGovernance.from_fixture(registry)


@pytest.mark.parametrize('mutation', ['approval', 'vintage', 'scope'])
def test_installed_identity_rejects_commitment_changes(mutation):
    registry, _ = artifacts()
    record = deepcopy(registry['compatibility_authorizations'][registry['current_compatibility_authorization']])
    if mutation == 'approval':
        record['approval_provenance']['owner_comment']['body_sha256'] = '0' * 64
    elif mutation == 'vintage':
        record['limitations']['vintage'] = 2023
    else:
        record['scope_authorities']['census_acs1'] = record['scope_authorities']['census_acs5']
    with pytest.raises(ValueError, match='identity mismatch'):
        check_identity(auth.VERSION, record, 'authorization_id')
