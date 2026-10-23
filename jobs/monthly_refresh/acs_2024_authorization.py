"""Bounded compatibility review; no acceptance, publication or activation adapter."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh import acs_2024_compatibility as qualification
from jobs.monthly_refresh.acs_scope import ACSGovernance, check_identity, identify

VERSION = 'acs_2024_compatibility_authorization_v1'
REPOSITORY = 'chineduezebuiroh/realestate-intel'
QUALIFICATIONS = {
    'census_acs1': qualification.VERSION + '__e5d99b1fc7e006fb3462659d4ef10bd5e8c99d14f1d26288ca2252fe6ea23603',
    'census_acs5': qualification.VERSION + '__421911b7acd6443df19cc2887577d85f356ea412a7f034e88fd11c2cf8b34b43',
}
CLASSIFICATIONS = ['retained_value', 'monthly_no_content_attested']
LIMITATIONS = {
    'vintage': 2024, 'cycle_id': qualification.CYCLE,
    'acs5_substitution': 'only_for_recorded_acs1_monthly_no_content',
    'preserve_physical_source_identity': True,
    'jointly_unobserved': 'missing', 'imputation': False,
    'provider_ineligibility_inference': False,
    'provider_response_verification': 'not_established',
    'historical_acceptance': False, 'source_set_publication': False,
    'serving_promotion': False, 'macro_execution': False,
}


def report_binding(report: dict, source: str) -> dict:
    """Bind the complete semantic report as well as its independently useful fields."""
    check_identity(qualification.VERSION, report, 'qualification_id')
    if (report['qualification_id'] != QUALIFICATIONS[source]
            or report.get('schema_version') != qualification.VERSION
            or report.get('source_id') != source or report.get('vintage') != 2024
            or report.get('artifact_id') != qualification.ELIGIBLE[source][0]
            or report.get('technical_reconciliation') != 'passed'
            or report.get('compatibility_governance') != 'not_approved'
            or report.get('disposition') != 'blocked_pending_governance'
            or report.get('original_provider_response_independently_verified') != 'not_established'
            or report.get('residual_limitations') != qualification.LIMITATIONS
            or not report.get('evidence_inventory')
            or not report.get('predicates')
            or any(p.get('passed') is not True for p in report['predicates'])):
        raise ValueError('not an exact successful original qualification')
    code = report['code_provenance']
    if code.get('manifest_git_sha') != 'unknown' or code.get('manifest_code_commit_recorded') is not False:
        raise ValueError('original manifest provenance must remain unknown')
    provenance = report['provenance']
    if (provenance['original_publication']['cycle_id'] != qualification.PUBLICATION_CYCLE
            or provenance['august_acceptance']['cycle_id'] != qualification.CYCLE):
        raise ValueError('original publication/acceptance lineage mismatch')
    lineage = {'sha256': sha256_json(provenance), **{name: {
        'cycle_id': value['cycle_id'], 'sha256': sha256_json(value)}
        for name, value in provenance.items()}}
    return {'qualification_id': report['qualification_id'], 'report_sha256': sha256_json(report),
            'provenance': lineage,
            **{key: deepcopy(report[key]) for key in ('parent', 'evidence_inventory',
                 'code_provenance', 'configuration', 'validator_sha256', 'historical_scope')}}


def report_file_binding(path: Path, source: str) -> dict:
    """Bind unchanged file bytes separately from canonical JSON semantic identity."""
    path = Path(path)
    return {**report_binding(qualification.read_json(path), source),
            'report_file_sha256': sha256_file(path), 'report_size_bytes': path.stat().st_size}


def proposal(reports: dict, scopes: dict, root: Path, *, report_files: dict) -> dict:
    """Construct an inactive proposal. A semantic hash never confers approval."""
    if (set(reports) != set(QUALIFICATIONS) or set(scopes) != set(QUALIFICATIONS)
            or set(report_files) != set(QUALIFICATIONS)):
        raise ValueError('both exact reports and governing scopes required')
    bindings = {s: report_file_binding(report_files[s], s) for s in QUALIFICATIONS}
    if any(qualification.read_json(report_files[s]) != reports[s] for s in QUALIFICATIONS):
        raise ValueError('report files differ from supplied report objects')
    return identify(VERSION, {'schema_version': VERSION, 'namespace': 'production',
        'status': 'proposed', 'approval_provenance': None, 'repository': REPOSITORY,
        'scope_authorities': deepcopy(scopes), 'qualifications': bindings,
        'consumer_sha256': sha256_file(Path(__file__)),
        'config_hashes': qualification.governed_config_hashes(root),
        'authorized_classifications': deepcopy(CLASSIFICATIONS), 'limitations': deepcopy(LIMITATIONS)}, 'authorization_id')


def verify(*, governance: ACSGovernance, authorization_id: str, reports: dict,
           packages: dict, catalog: Path, root: Path) -> dict:
    """Independently requalify both original TARs, then recheck current remote main."""
    if type(governance) is not ACSGovernance:
        raise ValueError('production ACS governance capability required')
    governance.require_destination(REPOSITORY)
    governance.current_commit(REPOSITORY)
    record = governance.compatibility_authorization(authorization_id)
    check_identity(VERSION, record, 'authorization_id')
    if (record.get('schema_version') != VERSION or record.get('namespace') != 'production'
            or record.get('status') != 'approved' or not record.get('approval_provenance')
            or record.get('repository') != REPOSITORY
            or record.get('limitations') != LIMITATIONS
            or record.get('authorized_classifications') != CLASSIFICATIONS
            or record.get('consumer_sha256') != sha256_file(Path(__file__))
            or record.get('config_hashes') != qualification.governed_config_hashes(root)
            or set(record.get('qualifications', {})) != set(QUALIFICATIONS)
            or set(record.get('scope_authorities', {})) != set(QUALIFICATIONS)
            or set(reports) != set(QUALIFICATIONS) or set(packages) != set(QUALIFICATIONS)):
        raise ValueError('compatibility authorization scope, status or provenance mismatch')
    checked = {}
    for source in QUALIFICATIONS:
        scope_id = record['scope_authorities'][source]
        scope = governance.scope(source, scope_id, root)
        if 2024 not in scope['expected_available_vintages']:
            raise ValueError('2024 outside approved historical scope')
        retained = qualification.read_json(reports[source])
        binding = report_file_binding(reports[source], source)
        if binding != record['qualifications'][source]:
            raise ValueError('retained qualification or complete evidence binding mismatch')
        # A retained status/hash cannot replace validating the original evidence.
        fresh = qualification.qualify(root=root, source_id=source,
                                      package=Path(packages[source]), catalog=Path(catalog))
        if fresh != retained:
            raise ValueError('qualification evidence is stale, altered or mismatched')
        checked[source] = fresh
    one = checked['census_acs1']['reconciliation']['cells']
    five = checked['census_acs5']['reconciliation']['cells']
    memberships = [{c['geo_id'] for c in cells if c['classification'] == 'retained_value'} for cells in (one, five)]
    absent = [{c['geo_id'] for c in cells if c['classification'] == 'monthly_no_content_attested'} for cells in (one, five)]
    if (len(memberships[1] - memberships[0]) != 67 or len(absent[0] & absent[1]) != 4
            or memberships[0] - memberships[1] or memberships[1] - memberships[0] != absent[0] - absent[1]):
        raise ValueError('exact original geography partition mismatch')
    governance.current_commit(REPOSITORY)
    return {'authorization_id': authorization_id, 'governance_snapshot': governance.production_snapshot(),
            'qualifications': checked, 'acs5_only_geographies': sorted(memberships[1] - memberships[0]),
            'jointly_unobserved_geographies': sorted(absent[0] & absent[1])}


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Build an inactive exact-report compatibility proposal; never approves or uploads.')
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    for product in ('acs1', 'acs5'):
        parser.add_argument('--' + product + '-report', type=Path, required=True)
        parser.add_argument('--' + product + '-scope', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    reports = {s: qualification.read_json(getattr(args, s[7:] + '_report')) for s in QUALIFICATIONS}
    scopes = {s: getattr(args, s[7:] + '_scope') for s in QUALIFICATIONS}
    report_files = {s: getattr(args, s[7:] + '_report') for s in QUALIFICATIONS}
    record = proposal(reports, scopes, args.repository_root, report_files=report_files)
    import json
    with args.output.open('x') as stream:
        stream.write(json.dumps(record, sort_keys=True, indent=2) + '\n')


if __name__ == '__main__':
    main()
