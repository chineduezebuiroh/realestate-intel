"""Offline qualification of exactly two August ACS parents; never approval or activation."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile

import pandas as pd

from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import extract_publication_package
from core.source_artifacts.validation import ArtifactValidationError
from jobs.monthly_refresh.acs_family_resolution import _parent
from jobs.monthly_refresh.acs_history import frame_identity
from jobs.monthly_refresh.acs_scope import check_identity, SCOPE_VERSION
from jobs.monthly_refresh.source_inputs import validate_pin
from sources.census_acs.artifact import (PRODUCTS, VARIABLES, SENTINELS, CONFIG_PATHS,
                                       geography_plan, governed_config_hashes, request_params)

VERSION = 'acs_2024_monthly_compatibility_v1'
# Immutable repository evidence anchors, not caller-configurable approvals.
BASE = '40b950b4edda9dff30a93754735654aab0da28e1'
AUTHORITY_REFERENCE = '4beea6b8b860cc1f825bd0f6b33728acbc90c9dc'
ORIGINAL_CODE = '47ff8c6bc2ff5a73ca36a6d0e40951faaa59ffa4'
PUBLICATION_CYCLE = 'monthly_cycle__2026-07__7cab1c5df177a1e4'
CYCLE = 'monthly_cycle__2026-08__a9e022a980d29cd7'
FAMILY = 'config/acs_family_resolutions/acs_family_resolution__534c942ae503c8a3eb556304.json'
ELIGIBLE = {
    'census_acs1': ('src__census_acs1__2024-12__r1__a1f5cd392af90fcb', 152, 71, 304),
    'census_acs5': ('src__census_acs5__2024-12__r1__2bebdc938d8c2fb6', 219, 4, 438),
}
CODE = {
    'jobs/monthly_refresh/acs_monthly.py': ['acquire_snapshot', '_safe_url'],
    'sources/census_acs/artifact.py': ['canonicalize', 'geography_plan', 'request_params',
                                      'PRODUCTS', 'VARIABLES', 'SENTINELS', 'DIVISIONS', 'CONTRACT_VERSION'],
}
LIMITATIONS = [
    'Original normalized snapshot bytes are unavailable; its digest and size are commitments only.',
    'Original HTTP responses, exact headers/returned codes and empty 204 bodies are not independently verified.',
    'Monthly no-content classifications are acquisition-process attestations, not historical omission receipts or eligibility claims.',
    'Monthly validation does not establish historical discovery or variable-metadata validation.',
    'Offline configuration/scope comparison cannot establish current-main governance authority.',
    'Qualification does not authorize historical acceptance, publication, fallback activation or execution.',
]
ERRORS = (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError,
          ArtifactValidationError, tarfile.TarError, EOFError, AttributeError)


def read_json(path):
    path = Path(path)
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError('JSON size bound exceeded')
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError('duplicate JSON key')
            out[key] = value
        return out
    return json.loads(path.read_bytes(), object_pairs_hook=unique)


def git_bytes(root, revision, path):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('full original code commit required')
    return subprocess.check_output(['git', '-C', str(root), 'show', f'{revision}:{path}'],
                                   stderr=subprocess.PIPE)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def semantics(raw, names):
    tree = ast.parse(raw)
    selected = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name in names:
            selected[node.name] = ast.dump(node, include_attributes=False)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    selected[target.id] = ast.dump(node.value, include_attributes=False)
    if set(selected) != set(names):
        raise ValueError('required source-code semantics missing')
    return selected


def reconcile(frame, pin, coverage, plan, excluded, source):
    """Inspect actual canonical cells; counts alone never establish completeness."""
    _, n_available, n_absent, n_rows = ELIGIBLE[source]
    evidence = pin['members']['snapshot']['evidence']
    available, absent = evidence['available_membership'], evidence['provider_ineligible']
    requested = [g['geo_id'] for g in plan]
    if (len(plan) != 223 or len(set(requested)) != 223 or len(excluded) != 10
            or len({g['geo_id'] for g in excluded}) != 10
            or set(requested) & {g['geo_id'] for g in excluded}
            or len(available) != n_available or len(absent) != n_absent
            or len(set(available)) != len(available) or len(set(absent)) != len(absent)
            or set(available) & set(absent) or set(available) | set(absent) != set(requested)
            or evidence['excluded_geographies'] != excluded):
        raise ValueError('membership partition or exclusions mismatch')
    expected_coverage = {'available_membership': sorted(available), 'available_geographies': n_available,
        'provider_ineligible': [{'geo_id': geo, 'classification': 'provider_ineligible_no_content'} for geo in absent],
        'excluded_geographies': excluded}
    if coverage != expected_coverage:
        raise ValueError('acquisition coverage diagnostics mismatch')
    metrics = {f'{source}_{suffix}' for suffix in VARIABLES.values()}
    expected = {(geo, metric) for geo in available for metric in metrics}
    actual = list(zip(frame.geo_id, frame.metric_id))
    if (len(frame) != n_rows or len(actual) != len(set(actual)) or set(actual) != expected
            or not frame.source_id.eq(source).all()
            or not frame.property_type_id.eq('all').all() or not frame.property_type.eq('all').all()
            or not pd.to_datetime(frame.date).eq(pd.Timestamp('2024-12-31')).all()
            or not pd.api.types.is_numeric_dtype(frame.value) or pd.api.types.is_bool_dtype(frame.value)
            or not frame.value.map(lambda v: math.isfinite(v) and v not in SENTINELS).all()):
        raise ValueError('canonical observation cells or semantics mismatch')
    # Reconstruct request identity from the configured codes and recorded statuses.
    members = []
    from urllib.parse import urlencode
    for geo in plan:
        status = 'available' if geo['geo_id'] in available else 'provider_ineligible_no_content'
        url = f'https://api.census.gov/data/2024/{PRODUCTS[source]}?' + urlencode(sorted(request_params(geo).items()))
        members.append({**geo, 'request_url': url, 'status': status})
    if sha256_json(members) != evidence['request_identity']:
        raise ValueError('request-plan identity mismatch')
    cells = [{'geo_id': g['geo_id'], 'variable': variable,
              'classification': 'retained_value' if g['geo_id'] in available else 'monthly_no_content_attested'}
             for g in plan for variable in VARIABLES]
    return {'requested_geographies': len(plan), 'available_geographies': n_available,
            'recorded_no_content_geographies': n_absent, 'retained_rows': n_rows,
            'accounted_cells': len(cells), 'cells': cells, 'excluded_geographies': excluded,
            'canonical_rows_sha256': frame_identity(frame), 'exact_keys': True}


def qualify(*, root: Path, source_id: str, package: Path, catalog: Path | None = None,
            governance: Path | None = None) -> dict:
    """Recompute from original TAR and anchored records; accepts no reported statuses."""
    root, package = Path(root), Path(package)
    report = {'schema_version': VERSION, 'validator_sha256': sha256_file(Path(__file__)), 'advisory_only': True, 'source_id': source_id, 'vintage': 2024,
              'technical_reconciliation': 'failed', 'compatibility_governance': 'not_approved',
              'original_provider_response_independently_verified': 'not_established',
              'predicates': [], 'evidence_inventory': {}, 'residual_limitations': LIMITATIONS,
              'governance_requirements': [
                  'Independent human review of this exact qualification_id and original evidence inventory.',
                  'Explicit review of policy differences and acceptance of process-attested monthly no-content evidence.',
                  'Approved current historical scope and separately reviewed main-branch compatibility authority for these exact parents.',
                  'Separate historical acceptance and activation authorization; this tool supplies neither.']}
    def require(condition, name):
        report['stage'] = name
        report['predicates'].append({'predicate': name, 'passed': bool(condition)})
        if not condition:
            raise ValueError(name)
    def local(path, name):
        report['stage'] = name
        value = read_json(path)
        report['evidence_inventory'][name] = {'sha256': sha256_file(Path(path))}
        return value
    def anchored(path, name):
        report['stage'] = name
        raw = git_bytes(root, BASE, path)
        value = local(root / path, name)
        require(value == json.loads(raw), name + '_matches_immutable_repository_anchor')
        report['evidence_inventory'][name]['anchor_sha256'] = digest(raw)
        return value
    try:
        require(source_id in ELIGIBLE, 'exact_eligible_product')
        artifact_id, _, _, _ = ELIGIBLE[source_id]
        report['artifact_id'] = artifact_id
        cat = local(catalog or root / 'config/artifact_catalog.json', 'catalog')
        baseline_cat = json.loads(git_bytes(root, BASE, 'config/artifact_catalog.json'))
        matches = [r for r in cat['immutable_records'] if r['object_id'] == artifact_id]
        originals = [r for r in baseline_cat['immutable_records'] if r['object_id'] == artifact_id]
        require(len(matches) == len(originals) == 1 and matches == originals, 'exact_original_catalog_record')
        record = matches[0]
        result = anchored(f'config/monthly_source_cycle_results/{CYCLE}/{source_id}.json', 'august_result')['result']
        require(result['source_id'] == source_id and result['cycle_id'] == CYCLE
                and result['provider_release_id'] == f'{PRODUCTS[source_id]}:2024'
                and result['candidate_artifact_id'] == artifact_id
                and result['artifact_content_hash'] == record['artifact_content_hash']
                and result['package_sha256'] == record['package_sha256']
                and result['status'] == 'succeeded' and result['validation_status'] == 'passed', 'august_result_binding')
        publication_result = anchored(
            f'config/monthly_source_cycle_results/{PUBLICATION_CYCLE}/{source_id}.json', 'july_publication_result')['result']
        require(publication_result['source_id'] == source_id and publication_result['cycle_id'] == PUBLICATION_CYCLE
                and publication_result['provider_release_id'] == f'{PRODUCTS[source_id]}:2024'
                and publication_result['candidate_artifact_id'] == artifact_id
                and publication_result['artifact_content_hash'] == record['artifact_content_hash']
                and publication_result['package_sha256'] == record['package_sha256']
                and publication_result['status'] == 'succeeded'
                and publication_result['validation_status'] == 'passed', 'july_publication_result_binding')
        family = anchored(FAMILY, 'accepted_family')
        parents = [p for p in family['parents'] if p['source_id'] == source_id]
        require(cat['accepted']['source']['acs'] == family['output_artifact_id'] and len(parents) == 1
                and all(parents[0][k] == record[v] for k, v in [('artifact_id', 'object_id'),
                    ('artifact_content_hash', 'artifact_content_hash'), ('package_sha256', 'package_sha256')])
                and parents[0]['data_sha256'] == record['metadata']['data_sha256'], 'accepted_family_parent_binding')
        report['stage'] = 'original_pin'
        pin = anchored(f'config/monthly_source_input_pins/{CYCLE}/{source_id}.json', 'august_pin')
        validate_pin(pin, cycle_id=CYCLE, source_id=source_id, required_members={'snapshot'})
        require(pin['provider_release_id'] == f'{PRODUCTS[source_id]}:2024'
                and pin['members']['snapshot']['evidence']['product'] == PRODUCTS[source_id]
                and pin['members']['snapshot']['evidence']['vintage'] == 2024, 'original_pin_product_vintage')
        publication_pin = anchored(
            f'config/monthly_source_input_pins/{PUBLICATION_CYCLE}/{source_id}.json', 'july_publication_pin')
        validate_pin(publication_pin, cycle_id=PUBLICATION_CYCLE, source_id=source_id, required_members={'snapshot'})
        report['original_pin'] = publication_pin
        # Only cycle/pin identity and retrieval metadata differ. Every snapshot
        # commitment and every geographic/request fact must remain exactly equal.
        publication_snapshot = publication_pin['members']['snapshot']
        acceptance_snapshot = pin['members']['snapshot']
        require(publication_pin['provider_release_id'] == pin['provider_release_id']
                and {k: v for k, v in publication_snapshot.items() if k != 'retrieved_at'}
                == {k: v for k, v in acceptance_snapshot.items() if k != 'retrieved_at'},
                'exact_august_snapshot_and_geographic_evidence_binding')
        require(package.stat().st_size <= 512 * 1024 * 1024, 'bounded_original_tar')
        actual_hash = sha256_file(package)
        report['evidence_inventory']['original_tar'] = {'sha256': actual_hash, 'size_bytes': package.stat().st_size}
        require(actual_hash == record['package_sha256'], 'original_tar_sha256')
        report['stage'] = 'package_manifest_data_validation'
        with tempfile.TemporaryDirectory(prefix='acs-2024-compatibility-') as temp:
            directory = extract_publication_package(package, Path(temp) / 'parent', expected_sha256=record['package_sha256'])
            frame, manifest, identity = _parent(record, directory, source_id)
            for path in sorted(directory.iterdir()):
                report['evidence_inventory']['package/' + path.name] = {'sha256': sha256_file(path)}
            require(manifest['artifact_status'] == 'complete' and manifest['validation_status'] == 'passed'
                    and read_json(directory / 'validation.json')['status'] == 'passed'
                    and manifest['provider_release_id'] == f'{PRODUCTS[source_id]}:2024'
                    and manifest['target_month'] == '2024-12', 'manifest_validation_and_vintage')
            require(manifest['raw_source_lineage']['provider_pin'] == publication_pin
                    and manifest['raw_source_lineage']['cycle_id'] == PUBLICATION_CYCLE
                    and manifest['raw_source_lineage']['physical_source_id'] == source_id
                    and manifest['source_request_identity'] == publication_snapshot['evidence']['request_identity']
                    and manifest['retrieved_at'] == publication_snapshot['retrieved_at']
                    and manifest['acquisition_time_status'] == 'retrieved_at_recorded'
                    and manifest['source_urls_or_endpoint_identity'] == [publication_snapshot['url']], 'exact_original_pin_binding')
            report['provenance'] = {
                'original_publication': {'cycle_id': PUBLICATION_CYCLE, 'pin': publication_pin,
                                         'cycle_result': publication_result, 'manifest_binding': 'exact'},
                'august_acceptance': {'cycle_id': CYCLE, 'pin': pin, 'cycle_result': result,
                                      'same_immutable_package': True, 'same_snapshot_commitments_and_evidence': True}}
            plan, excluded = geography_plan(root / CONFIG_PATHS[0])
            report['stage'] = 'exact_retained_cell_accounting'
            report['reconciliation'] = reconcile(frame, pin, manifest['raw_source_lineage']['coverage'], plan, excluded, source_id)
            require(True, 'exact_retained_cell_accounting')
            original_commit = manifest['git_sha']
            require(bool(re.fullmatch('[0-9a-f]{40}', original_commit)), 'original_code_commit_recorded')
            subprocess.run(['git', '-C', str(root), 'merge-base', '--is-ancestor', original_commit, BASE],
                           check=True, capture_output=True)
            report['stage'] = 'configuration_and_code_provenance'
            current_configs = governed_config_hashes(root)
            original_configs = {p: digest(git_bytes(root, original_commit, p)) for p in CONFIG_PATHS}
            authority_configs = {p: digest(git_bytes(root, AUTHORITY_REFERENCE, p)) for p in CONFIG_PATHS}
            require(manifest['config_hashes'] == original_configs, 'original_configuration_provenance')
            require(current_configs == authority_configs, 'historical_configuration_reference')
            require(all(original_configs[p] == current_configs[p] for p in CONFIG_PATHS[:2]), 'geography_and_metric_compatibility')
            differences = {p: {'original': original_configs[p], 'historical_reference': current_configs[p]}
                           for p in CONFIG_PATHS[2:] if original_configs[p] != current_configs[p]}
            report['configuration'] = {'original': original_configs, 'historical_reference': current_configs,
                'policy_differences_requiring_human_review': differences,
                'policy_review_status': 'not_approved' if differences else 'no_hash_differences'}
            code = {}
            for path, names in CODE.items():
                report['stage'] = 'code_semantics:' + path
                original = git_bytes(root, original_commit, path)
                reference = git_bytes(root, ORIGINAL_CODE, path)
                authority = git_bytes(root, AUTHORITY_REFERENCE, path)
                require(semantics(original, names) == semantics(reference, names)
                        == semantics(authority, names) == semantics((root / path).read_bytes(), names), 'code_semantics:' + path)
                if path.endswith('acs_monthly.py'):
                    require(semantics(original, ['candidate']) == semantics(reference, ['candidate']),
                            'original_candidate_processing_semantics')
                code[path] = {'original_sha256': digest(original), 'historical_reference_sha256': digest(authority),
                              'semantic_sha256': sha256_json(semantics(original, names))}
            report['code_provenance'] = {'original_commit': original_commit, 'repository_anchor': BASE,
                                       'historical_reference_commit': AUTHORITY_REFERENCE, 'files': code}
            report['parent'] = {**identity, 'data_sha256': manifest['data_sha256'], 'vintage': 2024}
        report['historical_scope'] = {'status': 'missing_authority'}
        if governance is not None:
            registry = local(governance, 'offline_governance')
            require(registry['schema_version'] == 'acs_historical_governance_registry_v1'
                    and registry['namespace'] == 'production', 'historical_registry_schema')
            report['stage'] = 'historical_scope_configuration'
            authority_id = registry['current_scope'][source_id]
            scope = registry['scopes'][authority_id]
            check_identity(SCOPE_VERSION, scope, 'authority_id')
            require(scope['authority_id'] == authority_id and scope['source_id'] == source_id
                    and scope['schema_version'] == SCOPE_VERSION and scope['product'] == PRODUCTS[source_id]
                    and scope['namespace'] == 'production' and bool(scope['approval_provenance'])
                    and scope['config_hashes'] == current_configs and 2024 in scope['expected_available_vintages'], 'historical_scope_configuration')
            report['historical_scope'] = {'status': 'offline_scope_matches_requires_main_review', 'authority_id': authority_id}
        report['technical_reconciliation'] = 'passed'
    except ERRORS as exc:
        # No absolute temporary paths or transient exception text enter identity.
        report['failure'] = {'type': type(exc).__name__, 'predicate': report.get('stage', 'input')}
        if not report['predicates'] or report['predicates'][-1]['passed']:
            report['predicates'].append({'predicate': report.get('stage', 'input'), 'passed': False})
    report.pop('stage', None)
    report['disposition'] = 'failed' if report['technical_reconciliation'] == 'failed' else 'blocked_pending_governance'
    report['qualification_id'] = VERSION + '__' + sha256_json(report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--source-id', choices=sorted(ELIGIBLE), required=True)
    parser.add_argument('--package', type=Path, required=True)
    parser.add_argument('--catalog', type=Path)
    parser.add_argument('--governance', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.repository_root.resolve()
    if args.output.resolve().is_relative_to(root) or args.output.exists():
        parser.error('output must be a new directory outside the repository')
    report = qualify(root=root, source_id=args.source_id, package=args.package, catalog=args.catalog, governance=args.governance)
    args.output.mkdir(parents=True)
    write_canonical_json(args.output / (report['qualification_id'] + '.json'), report)
    return 1 if report['technical_reconciliation'] == 'failed' else 2


if __name__ == '__main__':
    raise SystemExit(main())
