"""Inactive installation proposal and read-only initial-registry verification.

Structural validation does not authenticate a human or authorize installation.
There is deliberately no approve, install, merge or activation operation.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
import re

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh import acs_scope_bootstrap as bootstrap
from jobs.monthly_refresh.acs_2020_gap import GAP_FILE, CANDIDATE_FILE, SOURCE_PATH, SOURCE_SHA256
from jobs.monthly_refresh.acs_scope import ACSGovernance, GAP_VERSION, SCOPE_VERSION, check_identity

VERSION = 'acs_historical_governance_installation_proposal_v1'
PROVENANCE_VERSION = 'acs_historical_governance_approval_provenance_v2'
REPOSITORY = 'chineduezebuiroh/realestate-intel'
PROPOSAL_PATH = 'docs/proposals/acs_historical_governance_installation.json'
LIMITATIONS = [
    'scope_and_standard_acs1_2020_nonpublication_only',
    'acs5_2023_remains_expected_available_acquisition_recovery_required',
    'no_2024_compatibility_exception_authorization',
    'no_physical_state_acceptance_materialization_publication_serving_or_macro_authorization',
    'documentary_capture_is_reference_metadata_and_excerpts_not_full_html_or_endpoint_response',
]


def generate(root: Path) -> dict:
    inputs = bootstrap.generate(root)
    records = {'acs1_2020_gap': inputs[GAP_FILE],
               'acs1_scope': inputs[CANDIDATE_FILE],
               'acs5_scope': inputs[bootstrap.FILENAMES['census_acs5']]['scope_record']}
    commitments = {}
    for name, record in records.items():
        field = 'evidence_id' if name.endswith('_gap') else 'authority_id'
        commitments[name] = {'proposal_identity': record[field], 'record_sha256': sha256_json(record)}
    return deepcopy({'schema_version': VERSION, 'status': 'proposed', 'active': False,
        'records': records,
        'initial_registry': inputs[bootstrap.REGISTRY_FILE]['registry_template'],
        'approval_templates': {name: {
            'schema_version': PROVENANCE_VERSION, 'repository': REPOSITORY,
            'subject': commitments[name], 'provenance_kind': None, 'owner_comment': None,
            'decision': None, 'reviewer_login': None, 'reviewed_at': None,
            'reviewed_pr': None, 'reviewed_commit': None, 'review_url': None,
            'decision_rationale': None,
            'documentary_evidence': {'path': SOURCE_PATH, 'sha256': SOURCE_SHA256},
            'approved_request_scope': inputs[bootstrap.FILENAMES[
                'census_acs5' if name == 'acs5_scope' else 'census_acs1']]['request_scope'],
            'config_hashes': inputs[bootstrap.REGISTRY_FILE]['config_hashes'],
            'limitations': LIMITATIONS,
        } for name in records},
        'limitations': LIMITATIONS,
        'validation_is_authorization': False,
        'installation_requires': 'genuine human decisions followed by a separate reviewed main-branch merge'})


def validate_proposal(value: dict, root: Path) -> dict:
    for name, record in value['records'].items():
        gap = name.endswith('_gap')
        check_identity(GAP_VERSION if gap else SCOPE_VERSION, record, 'evidence_id' if gap else 'authority_id')
    if sha256_json(value) != sha256_json(generate(root)):
        raise ValueError('installation proposal drift or attempted approval/activation')
    return {'proposal_validation': 'passed', 'production_authority': False, 'activation': False}


def _provenance(value: dict, template: dict) -> None:
    """Require precise review commitments; GitHub/human authenticity is external."""
    genuine = {'decision', 'reviewer_login', 'reviewed_at', 'reviewed_pr',
               'reviewed_commit', 'review_url', 'decision_rationale', 'provenance_kind', 'owner_comment'}
    if not isinstance(value, dict) or set(value) != set(template):
        raise ValueError('approval provenance fields required')
    if any(sha256_json(value[k]) != sha256_json(template[k]) for k in set(template) - genuine):
        raise ValueError('approval subject, evidence, scope or limitations mismatch')
    if value['decision'] != 'approved':
        raise ValueError('explicit human approval required')
    if not isinstance(value['reviewer_login'], str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9-]{0,38}', value['reviewer_login']):
        raise ValueError('reviewer login required')
    if type(value['reviewed_pr']) is not int or value['reviewed_pr'] <= 0:
        raise ValueError('exact reviewed PR required')
    if not isinstance(value['reviewed_commit'], str) or not re.fullmatch(r'[0-9a-f]{40}', value['reviewed_commit']):
        raise ValueError('exact reviewed commit required')
    kind = value['provenance_kind']
    if kind == 'github_approved_review':
        if value['owner_comment'] is not None:
            raise ValueError('approved review cannot carry owner-comment metadata')
        anchor = 'pullrequestreview'
    elif kind == 'github_owner_decision_comment':
        from jobs.monthly_refresh.acs_governance_provenance import validate_comment_metadata
        validate_comment_metadata(value)
        anchor = 'issuecomment'
    else:
        raise ValueError('explicit supported provenance kind required')
    if not isinstance(value['review_url'], str) or not re.fullmatch(
            rf'https://github.com/{REPOSITORY}/pull/{value["reviewed_pr"]}#{anchor}-[1-9][0-9]*', value['review_url']):
        raise ValueError('exact GitHub decision reference required')
    try:
        stamp = datetime.fromisoformat(value['reviewed_at'].replace('Z', '+00:00'))
        if stamp.utcoffset() is None:
            raise ValueError('timezone required')
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError('genuine approval timestamp with timezone required') from exc
    if not isinstance(value['decision_rationale'], str) or not value['decision_rationale'].strip():
        raise ValueError('human decision rationale required')


def validate_initial_registry(registry: dict, root: Path) -> dict:
    """Read-only structural verification for the later installation PR.

    Even a forged but well-shaped review can pass this offline check. Only actual
    authorized review and separately reviewed main membership establish trust.
    """
    proposal = generate(root)
    if set(registry) != set(proposal['initial_registry']) or registry['schema_version'] != 'acs_historical_governance_registry_v1' or registry['namespace'] != 'production':
        raise ValueError('initial production registry structure mismatch')
    if registry['states'] != {} or registry['current_state'] != {}:
        raise ValueError('initial physical-state maps must be empty')
    if not all(isinstance(registry[k], dict) for k in ['gap_evidence', 'scopes', 'current_scope']):
        raise ValueError('registry maps required')
    if len(registry['gap_evidence']) != 1 or len(registry['scopes']) != 2 or set(registry['current_scope']) != set(bootstrap.RANGES):
        raise ValueError('exact initial registry membership required')
    gap_id, gap = next(iter(registry['gap_evidence'].items()))
    check_identity(GAP_VERSION, gap, 'evidence_id')
    if gap_id != gap['evidence_id'] or gap.get('status') != 'approved':
        raise ValueError('approved gap and exact registry key required')
    _provenance(gap.get('approval_provenance'), proposal['approval_templates']['acs1_2020_gap'])
    expected = deepcopy(proposal['records']['acs1_2020_gap'])
    expected.update(status='approved', approval_provenance=gap['approval_provenance'], evidence_id=gap_id)
    if sha256_json(gap) != sha256_json(expected):
        raise ValueError('gap documentary/request scope mismatch')
    ids = {}
    for source in bootstrap.RANGES:
        key = registry['current_scope'][source]
        scope = registry['scopes'].get(key, {})
        check_identity(SCOPE_VERSION, scope, 'authority_id')
        name = 'acs1_scope' if source == 'census_acs1' else 'acs5_scope'
        _provenance(scope.get('approval_provenance'), proposal['approval_templates'][name])
        expected = deepcopy(proposal['records'][name])
        expected.update(approval_provenance=scope['approval_provenance'], authority_id=key)
        if source == 'census_acs1':
            expected['governed_gaps'] = [{'vintage': 2020, 'evidence_id': gap_id}]
        if sha256_json(scope) != sha256_json(expected):
            raise ValueError('approved scope partition/configuration/reference mismatch')
        ids[source] = key
    if set(ids.values()) != set(registry['scopes']):
        raise ValueError('scope registry key/current pointer mismatch')
    return {'structural_validation': 'passed', 'scope_ids': ids,
            'human_review_authenticity': 'requires independent verification',
            'production_authority': False, 'activation': False}


def verify_remote(repository: str, token: str, root: Path) -> dict:
    """Verify current main via existing constrained transport, never a local API."""
    if repository != REPOSITORY:
        raise ValueError('installation repository mismatch')
    governance = ACSGovernance.load_production(repository, token)
    governance.require_destination(repository)
    # Verification needs the exact registry, including unreferenced membership.
    result = validate_initial_registry(governance._registry, root)
    for source, key in result['scope_ids'].items():
        governance.scope(source, key, root)
        if governance.current_state(source) is not None:
            raise ValueError('initial physical state must be absent')
    from jobs.monthly_refresh.acs_governance_provenance import verify_authorizations
    provenances = {'acs1_2020_gap': next(iter(governance._registry['gap_evidence'].values()))['approval_provenance']}
    for source, key in result['scope_ids'].items():
        provenances['acs1_scope' if source == 'census_acs1' else 'acs5_scope'] = governance._registry['scopes'][key]['approval_provenance']
    result['authorization_audit'] = verify_authorizations(provenances, token, root)
    result['validated_main_commit'] = governance.current_commit(repository)
    result['production_authority'] = True
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--check-proposal', type=Path)
    mode.add_argument('--check-initial-registry', type=Path)
    args = parser.parse_args()
    path = args.check_proposal or args.check_initial_registry
    # Reject duplicate keys just like the production loader.
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('ambiguous installation metadata')
            result[key] = value
        return result
    value = json.loads(path.read_bytes(), object_pairs_hook=unique)
    check = validate_proposal if args.check_proposal else validate_initial_registry
    print(json.dumps(check(value, args.repository_root.resolve()), sort_keys=True))


if __name__ == '__main__':
    main()
