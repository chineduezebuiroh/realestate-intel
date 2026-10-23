"""Deterministic, inactive ACS scope proposals; no approval or installation API."""
from __future__ import annotations

import argparse
from copy import deepcopy
from collections import Counter
import json
from pathlib import Path

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_2020_gap import generate_gap, validate_gap, GAP_FILE, CANDIDATE_FILE, SOURCE_PATH
from jobs.monthly_refresh.acs_scope import SCOPE_VERSION, check_identity, identify
from jobs.monthly_refresh.acs_2024_compatibility import read_json
from sources.census_acs.artifact import PRODUCTS, VARIABLES, geography_plan, governed_config_hashes

VERSION = 'acs_historical_scope_bootstrap_proposal_v1'
RANGES = {'census_acs1': [2005, 2024], 'census_acs5': [2009, 2024]}
FILENAMES = {s: s + '.scope.proposal.json' for s in RANGES}
REGISTRY_FILE = 'initial-registry.proposal.json'


def generate(root: Path) -> dict[str, dict]:
    """Generate review inputs, never approved scopes or acquisition assertions."""
    root = Path(root)
    hashes = governed_config_hashes(root)
    plan, excluded = geography_plan(root / 'config/geo_manifest.generated.csv')
    if len(plan) != 223 or len(excluded) != 10:
        raise ValueError('bootstrap requires the existing 223/10 geography plan')
    catalog = read_json(root / 'config/artifact_catalog.json')
    inputs = {'config/artifact_catalog.json': sha256_file(root / 'config/artifact_catalog.json'),
              'sources/census_acs/artifact.py': sha256_file(root / 'sources/census_acs/artifact.py')}
    plans = {s: set() for s in RANGES}
    for path in sorted((root / 'config/acs_backfill_reviews').glob('plan-*.json')):
        value = read_json(path)
        if value.get('source_id') in plans:
            plans[value['source_id']].update(value['vintages'])
            inputs[path.relative_to(root).as_posix()] = sha256_file(path)
    # Adopted plans plus exact August results establish intended scope, not approval.
    cycle = 'monthly_cycle__2026-08__a9e022a980d29cd7'
    for source in RANGES:
        path = root / f'config/monthly_source_cycle_results/{cycle}/{source}.json'
        result = read_json(path)['result']
        if result['source_id'] != source or result['status'] != 'succeeded':
            raise ValueError('original August planning evidence mismatch')
        plans[source].add(int(result['provider_release_id'].rsplit(':', 1)[1]))
        if not plans[source] or [min(plans[source]), max(plans[source])] != RANGES[source]:
            raise ValueError('intended historical range differs from adopted plans')
        inputs[path.relative_to(root).as_posix()] = sha256_file(path)
    gap = generate_gap(root)
    inputs[SOURCE_PATH] = sha256_file(root / SOURCE_PATH)
    inputs['jobs/monthly_refresh/acs_2020_gap.py'] = sha256_file(root / 'jobs/monthly_refresh/acs_2020_gap.py')
    output = {GAP_FILE: gap}
    for source, (start, end) in RANGES.items():
        unresolved = [2020] if source == 'census_acs1' else []
        available = [year for year in range(start, end + 1) if year not in unresolved]
        record = identify(SCOPE_VERSION, {'schema_version': SCOPE_VERSION, 'namespace': 'production',
            'source_id': source, 'product': PRODUCTS[source], 'historical_range': [start, end],
            'expected_available_vintages': available, 'governed_gaps': [],
            'config_hashes': hashes, 'approval_provenance': None}, 'authority_id')
        cataloged = sorted({int(r['metadata']['provider_release_id'].rsplit(':', 1)[1])
            for r in catalog['immutable_records'] if r.get('object_type') == 'source'
            and r.get('metadata', {}).get('source_id') == source
            and r.get('metadata', {}).get('provider_release_id', '').startswith(PRODUCTS[source] + ':')
            and start <= int(r['metadata']['provider_release_id'].rsplit(':', 1)[1]) <= end})
        output[FILENAMES[source]] = {'schema_version': VERSION, 'status': 'proposed',
            'active': False, 'scope_record': record,
            'authority_identity_semantics': 'proposal content identity only; not established authority',
            'partition': {'complete': not unresolved, 'unresolved_vintages': unresolved,
                'decisions': [{'vintage': 2020, 'status': 'unresolved',
                    'required': 'independent provider evidence and human-reviewed whole-vintage gap decision'}] if unresolved else []},
            'request_scope': {'metrics': [{'variable': v, 'metric_id': source + '_' + suffix}
                for v, suffix in VARIABLES.items()], 'requested_geographies': len(plan),
                'membership_sha256': sha256_json(plan), 'by_level': dict(sorted(Counter(g['level'] for g in plan).items())),
                'excluded_geographies': excluded},
            'acquisition_coverage': {'basis': 'catalog presence only; not package verification or acceptance',
                'cataloged_vintages': cataloged, 'missing_acquisition_vintages': sorted(set(available) - set(cataloged)),
                'missing_acquisition_is_governed_gap': False},
            'physical_state': 'not_accepted', 'compatibility_exception': 'separate_review_required',
            'approval_blockers': ['explicit_human_approval_and_reviewed_main_installation_required'] +
                (['incomplete_vintage_partition_2020'] if unresolved else [])}
    # governed_gaps denotes approved evidence in the existing production contract.
    # Keep the conditional complete candidate separate from the still unapproved scope.
    one = output[FILENAMES['census_acs1']]
    candidate = deepcopy(one['scope_record'])
    candidate['governed_gaps'] = [{'vintage': 2020, 'evidence_id': gap['evidence_id']}]
    output[CANDIDATE_FILE] = identify(SCOPE_VERSION, candidate, 'authority_id')
    one['scope_record_if_gap_approved'] = {'path': CANDIDATE_FILE,
        'authority_id': output[CANDIDATE_FILE]['authority_id'], 'sha256': sha256_json(output[CANDIDATE_FILE]),
        'condition': 'independent human gap approval and separate historical scope approval required'}
    one['proposed_gap'] = {'path': GAP_FILE, 'evidence_id': gap['evidence_id'], 'sha256': sha256_json(gap)}
    one['proposed_partition'] = {'complete': True, 'available_vintages': candidate['expected_available_vintages'],
        'unavailable_vintages': [2020], 'classification': 'standard_whole_vintage_not_published',
        'approval': 'pending', 'missing_acquisition': False}
    one['partition']['decisions'][0] = {'vintage': 2020, 'status': 'provider_documentation_obtained_approval_pending',
        'required': 'independent human approval of proposed gap evidence and historical scope'}
    one['approval_blockers'] = ['explicit_human_approval_and_reviewed_main_installation_required',
        'proposed_gap_evidence_and_scope_approval_required']
    output[REGISTRY_FILE] = {'schema_version': VERSION, 'status': 'proposed', 'active': False,
        'approval_provenance': None,
        'registry_template': {'schema_version': 'acs_historical_governance_registry_v1',
            'namespace': 'production', 'scopes': {}, 'current_scope': {}, 'gap_evidence': {},
            'states': {}, 'current_state': {}},
        'gap_proposals': {'census_acs1:2020': {'path': GAP_FILE, 'evidence_id': gap['evidence_id'], 'sha256': sha256_json(gap)}},
        'scope_proposals': {s: {'path': FILENAMES[s], 'sha256': sha256_json(output[FILENAMES[s]])} for s in RANGES},
        'input_file_sha256': dict(sorted(inputs.items())), 'config_hashes': hashes,
        'generator_sha256': sha256_file(Path(__file__)),
        'installation': 'forbidden: review-only template, not a production registry'}
    return output


def validate(documents: dict[str, dict], root: Path) -> dict:
    """Validate exact review inputs. Success never confers approval/readiness."""
    validate_gap(documents[GAP_FILE], root)
    check_identity(SCOPE_VERSION, documents[CANDIDATE_FILE], 'authority_id')
    for source in RANGES:
        record = documents[FILENAMES[source]]['scope_record']
        check_identity(SCOPE_VERSION, record, 'authority_id')
        if record.get('approval_provenance') is not None or record.get('governed_gaps'):
            raise ValueError('proposal cannot contain approvals or authorized gaps')
    if documents != generate(root):
        raise ValueError('proposal drift, forged scope, incomplete evidence or attempted activation')
    return {'proposal_validation': 'passed', 'production_authority': False,
            'scope_partition': {'census_acs1': 'conditional_partition_complete_gap_and_scope_unapproved', 'census_acs5': 'complete_unapproved'},
            'physical_state_acceptance': False, 'activation': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--output', type=Path, help='New proposal directory; never the production config directory')
    mode.add_argument('--check', type=Path, help='Validate an existing proposal directory without writes')
    args = parser.parse_args()
    root = args.repository_root.resolve()
    if args.check:
        if {p.name for p in args.check.iterdir()} != {*FILENAMES.values(), GAP_FILE, CANDIDATE_FILE, REGISTRY_FILE}:
            parser.error('exact proposal file set required')
        result = validate({name: read_json(args.check / name) for name in [*FILENAMES.values(), GAP_FILE, CANDIDATE_FILE, REGISTRY_FILE]}, root)
    else:
        destination = args.output.resolve()
        if destination.is_relative_to(root / 'config') or destination == root or destination.exists():
            parser.error('new directory outside production config required')
        documents = generate(root)
        result = validate(documents, root)
        destination.mkdir(parents=True, exist_ok=False)
        for name, value in documents.items():
            (destination / name).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
