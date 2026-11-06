"""Read-only preparation of the exact initial ACS historical baseline.

No publisher, approval adapter, registry writer or downstream execution exists.
"""
from __future__ import annotations

import argparse
import base64
from copy import deepcopy
from datetime import datetime
import json
import os
from pathlib import Path
import tempfile

from core.source_artifacts.github_release import GitHubAPI
from core.source_artifacts.publication import TransientPublicationError
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package, extract_publication_package
from jobs.monthly_refresh import acs_2024_authorization as compatibility
from jobs.monthly_refresh import acs_history as history
from jobs.monthly_refresh.acs_backfill import read_json, verify_historical_pin
from jobs.monthly_refresh.acs_family_resolution import _parent
from jobs.monthly_refresh.acs_scope import ACSGovernance
from sources.census_acs.artifact import PRODUCTS, canonicalize, geography_plan
from sources.census_acs.historical import validate_evidence

REPOSITORY = compatibility.REPOSITORY
INPUT_VERSION = 'acs_history_bootstrap_inputs_v1'
BUNDLE_VERSION = 'acs_history_bootstrap_review_v1'
YEARS = {'census_acs1': [y for y in range(2005, 2025) if y != 2020],
         'census_acs5': list(range(2009, 2025))}
PARENT_FIELDS = {'source_id', 'vintage', 'artifact_id', 'artifact_content_hash', 'package_sha256', 'data_sha256'}
INPUT_FIELDS = {'schema_version', 'catalog', 'governance', 'scope_authorities', 'parents', 'packages',
                'evidence', 'acs5_2023_pin', 'compatibility_authorization', 'reports', 'artifact_created_at'}
class BootstrapBlocked(ValueError):
    """Missing inputs are unresolved gates, not contradictory evidence."""


GATES = ['human_acceptance_authorization', 'published_candidate_catalog_receipts',
         'fresh_preinstallation_verification', 'atomic_two_state_installation']


def _fresh(spec, governance):
    """Local files are evidence; only the existing production loader grants trust."""
    if type(governance) is not ACSGovernance:
        raise ValueError('existing ACSGovernance required')
    registry = read_json(Path(spec['governance']))
    catalog = read_json(Path(spec['catalog']))
    if governance.namespace == 'test_only' and governance.provenance == 'test_only':
        return {'namespace': 'test_only', 'registry_sha256': sha256_json(registry),
                'catalog_sha256': sha256_json(catalog), 'governance': 'fixture_not_authority'}
    governance.require_destination(REPOSITORY)
    snapshot = governance.production_snapshot()
    if sha256_json(registry) != snapshot['registry_sha256']:
        raise ValueError('local registry differs from current production snapshot')
    tip = governance.current_commit(REPOSITORY)
    api = GitHubAPI(REPOSITORY, os.environ.get('GITHUB_TOKEN', ''), read_only=True)
    content, _ = api.request('GET', f'/contents/config/artifact_catalog.json?ref={tip}')
    # Reuse duplicate-key rejection for downloaded catalog JSON as well.
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'catalog.json'
        path.write_bytes(base64.b64decode(content['content']))
        remote = read_json(path)
    if sha256_json(remote) != sha256_json(catalog):
        raise ValueError('catalog differs from current main')
    if governance.current_commit(REPOSITORY) != tip:
        raise ValueError('main changed during verification')
    return {'namespace': 'production', 'registry_sha256': snapshot['registry_sha256'],
            'catalog_sha256': sha256_json(catalog), 'governance_snapshot': snapshot,
            'validated_main_commit': tip, 'governance': 'live_verified_for_preparation_only'}


def _scopes(spec, governance, root):
    if set(spec['scope_authorities']) != set(YEARS):
        raise ValueError('both exact scope authorities required')
    registry = read_json(Path(spec['governance']))
    if registry['states'] or registry['current_state']:
        raise ValueError('initial bootstrap requires both state maps empty')
    scopes = {s: governance.scope(s, spec['scope_authorities'][s], root) for s in YEARS}
    for source, scope in scopes.items():
        if scope['expected_available_vintages'] != YEARS[source]:
            raise ValueError('scope differs from exact 35-parent baseline')
    gaps = scopes['census_acs1']['governed_gaps']
    if len(gaps) != 1 or gaps[0]['vintage'] != 2020 or scopes['census_acs5']['governed_gaps']:
        raise ValueError('only standard ACS1 2020 may be a whole-vintage gap')
    gap = registry['gap_evidence'][gaps[0]['evidence_id']]
    evidence = gap['provider_evidence']
    if (gap.get('status') != 'approved' or evidence.get('classification') != 'standard_whole_vintage_not_published'
            or evidence.get('requested_provider_release_id') != 'acs/acs1:2020'
            or evidence.get('provider_published_dataset_identifier') is not None):
        raise ValueError('approved standard ACS1 2020 nonpublication evidence required')
    return scopes


def _provider(spec, parent, frame, manifest, root):
    source, year = parent['source_id'], parent['vintage']
    path = Path(spec['evidence'][f'{source}:{year}'])
    value = read_json(path)
    value = value[str(year)] if str(year) in value else value
    geography, excluded = geography_plan(root / 'config/geo_manifest.generated.csv')
    checked = validate_evidence(value, source_id=source, vintage=year, plan=geography, excluded=excluded)
    expected, _ = canonicalize(source, year, value['snapshot'])
    if history.frame_identity(frame) != history.frame_identity(expected):
        raise ValueError('provider evidence differs from acquisition rows')
    lineage = manifest['raw_source_lineage']
    pin = lineage['provider_pin']
    verify_historical_pin(pin, evidence=value, source_id=source, vintage=year, cycle_id=lineage['cycle_id'])
    if source == 'census_acs5' and year == 2023:
        if pin != read_json(Path(spec['acs5_2023_pin'])):
            raise ValueError('ACS5 2023 manifest differs from durable acquisition pin')
        omissions = value['omission_evidence']
        if len(omissions) != 4 or any(x['http_status'] != 204 for x in omissions):
            raise ValueError('four exact ACS5 2023 HTTP 204 receipts required')
        counts = {k: sum(x['classification'] == k for x in checked['accounting'])
                  for k in ('available', 'provider_no_content')}
        if counts != {'available': 438, 'provider_no_content': 8}:
            raise ValueError('ACS5 2023 provider accounting drift')
    return {'provider_evidence_file_sha256': sha256_file(path),
            'snapshot_sha256': checked['snapshot_sha256'], 'pin_sha256': sha256_json(pin)}


def _qualification_catalog(root, records, workspace):
    """Keep original qualification evidence distinct from today's full catalog."""
    data = compatibility.qualification.git_bytes(root, compatibility.qualification.BASE,
                                               'config/artifact_catalog.json')
    path = workspace / 'original-qualification-catalog.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    original = read_json(path)['immutable_records']
    for source in YEARS:
        identity = compatibility.qualification.ELIGIBLE[source][0]
        matches = [r for r in original if r['object_id'] == identity]
        if len(matches) != 1 or records[identity] != matches[0]:
            raise ValueError('current original 2024 catalog record changed')
    return path


def reconcile_inventory(spec, *, governance, root: Path, workspace: Path):
    """Verify every explicit original TAR and independently account for its rows."""
    if set(spec) != INPUT_FIELDS or spec['schema_version'] != INPUT_VERSION:
        raise ValueError('exact bootstrap input schema required')
    baseline = _fresh(spec, governance)
    scopes = _scopes(spec, governance, root)
    records_list = read_json(Path(spec['catalog']))['immutable_records']
    records = {}
    for record in records_list:
        if record['object_id'] in records:
            raise ValueError('duplicate catalog identity')
        records[record['object_id']] = record
    parents = spec['parents']
    expected = {(s, y) for s in YEARS for y in YEARS[s]}
    if (len(parents) != 35 or any(set(p) != PARENT_FIELDS for p in parents)
            or {(p['source_id'], p['vintage']) for p in parents} != expected
            or len({p['artifact_id'] for p in parents}) != 35
            or set(spec['packages']) != {p['artifact_id'] for p in parents}):
        raise ValueError('35 explicit unique parent identities and package mappings required')
    by_source = {s: [] for s in YEARS}
    paths, findings = {}, {}
    for parent in sorted(parents, key=lambda p: (p['source_id'], p['vintage'])):
        source, year, identity = parent['source_id'], parent['vintage'], parent['artifact_id']
        if year == 2024 and identity != compatibility.qualification.ELIGIBLE[source][0]:
            raise ValueError('alternative 2024 parent substitution forbidden')
        record = records[identity]
        if (record['metadata']['provider_release_id'] != f'{PRODUCTS[source]}:{year}'
                or record['metadata']['data_sha256'] != parent['data_sha256']):
            raise ValueError('catalog vintage/data identity mismatch')
        if year != 2024 and sum(r.get('metadata', {}).get('source_id') == source and
                r.get('metadata', {}).get('provider_release_id') == f'{PRODUCTS[source]}:{year}'
                for r in records_list) != 1:
            raise ValueError('ambiguous ordinary acquisition vintage')
        package = Path(spec['packages'][identity])
        if not package.is_file():
            raise FileNotFoundError('original TAR not supplied')
        if sha256_file(package) != parent['package_sha256']:
            raise ValueError('mismatched original TAR')
        path = extract_publication_package(package, workspace / identity, expected_sha256=record['package_sha256'])
        frame, manifest, actual = _parent(record, path, source)
        if (any(parent[k] != v for k, v in actual.items()) or not frame.source_id.eq(source).all()
                or manifest['artifact_status'] != 'complete' or manifest['validation_status'] != 'passed'
                or read_json(path / 'validation.json')['status'] != 'passed'):
            raise ValueError('acquisition identity, physical source or validation mismatch')
        # Full date/key/scope/package/lineage reconstruction is also checked below.
        findings[identity] = {'original_tar_sha256': sha256_file(package)}
        if year != 2024:
            findings[identity].update(_provider(spec, parent, frame, manifest, root))
        paths[identity] = path
        by_source[source].append(parent)
    qualification_catalog = _qualification_catalog(root, records, workspace)
    qualified = compatibility.verify(governance=governance,
        authorization_id=spec['compatibility_authorization'], reports=spec['reports'],
        packages={s: spec['packages'][compatibility.qualification.ELIGIBLE[s][0]] for s in YEARS},
        catalog=qualification_catalog, root=root)
    inventories = {s: history.inventory(authority=scopes[s], parents=by_source[s]) for s in YEARS}
    for value in inventories.values():
        history.reconstruct(value, governance=governance, paths=paths, records=records, repository_root=root)
    if _fresh(spec, governance) != baseline:
        raise ValueError('governance/catalog changed during parent verification')
    return {'baseline': baseline, 'inventories': inventories, 'paths': paths, 'records': records,
            'parent_verification': findings, 'compatibility': {
                'authorization_id': qualified['authorization_id'], 'limitations': compatibility.LIMITATIONS,
                'original_qualification_catalog_sha256': sha256_file(qualification_catalog),
                'acs5_only_geographies': qualified['acs5_only_geographies'],
                'jointly_unobserved_geographies': qualified['jointly_unobserved_geographies']}}


def propose_acceptance(states, *, baseline_registry):
    """Pure inactive delta. Does not grant approval or write a registry."""
    if baseline_registry['states'] or baseline_registry['current_state'] or set(states) != set(YEARS):
        raise ValueError('initial two-state acceptance only; partial or existing state rejected')
    for source, state in states.items():
        value = state['inventory']
        history.check_identity(history.INVENTORY_VERSION, value, 'inventory_id')
        if (value['namespace'] != baseline_registry['namespace']
                or value['authority_id'] != baseline_registry['current_scope'][source]
                or [p['vintage'] for p in value['parents']] != YEARS[source]):
            raise ValueError('acceptance must retain exact current complete inventories')
        if state['inventory']['source_id'] != source:
            raise ValueError('state product mismatch')
        history.check_identity(history.STATE_VERSION, state, 'state_id')
    return {'status': 'proposed', 'active': False, 'approval_provenance': None,
            'expected_registry_sha256': sha256_json(baseline_registry),
            'expected_current_state': {},
            'add_states': {v['state_id']: deepcopy(v) for v in states.values()},
            'set_current_state': {s: v['state_id'] for s, v in states.items()},
            'installation_contract': {'atomic_both_products': True, 'preserve_all_other_fields': True,
                'publication_established': False, 'requires': GATES}}


def _created_at(spec):
    value = spec['artifact_created_at']
    if value is None:
        raise BootstrapBlocked('candidate creation time not supplied')
    if not isinstance(value, str) or datetime.fromisoformat(value.replace('Z', '+00:00')).utcoffset() is None:
        raise ValueError('explicit timezone-aware candidate creation time required; retain it for replay')
    return value


def _bundle(spec, checked, candidates):
    result = {'schema_version': BUNDLE_VERSION, 'namespace': checked['baseline']['namespace'],
        'status': 'proposed', 'active': False, 'approval_provenance': None,
        'input_sha256': sha256_json(spec), 'baseline': checked['baseline'],
        'parents': sorted(spec['parents'], key=lambda p: (p['source_id'], p['vintage'])),
        'parent_verification': checked['parent_verification'], 'compatibility': checked['compatibility'],
        'candidates': candidates, 'proposed_acceptance': propose_acceptance(
            {s: c['state'] for s, c in candidates.items()}, baseline_registry=read_json(Path(spec['governance']))),
        'verification': {'verified_original_parents': 35, 'independent_state_reconstruction': 'passed',
            'evidence_kind': 'production_inputs' if checked['baseline']['namespace'] == 'production' else 'test_fixture'},
        'publication': 'not_established', 'installation_readiness': 'blocked', 'remaining_gates': GATES}
    return {**result, 'bundle_id': BUNDLE_VERSION + '__' + sha256_json(result)}


def prepare_bootstrap(spec, *, governance, root: Path, output: Path):
    """Create deterministic local candidates only. A failed directory is retained."""
    created_at = _created_at(spec)
    if output.resolve().is_relative_to(root.resolve()):
        raise ValueError('candidate output must be outside repository')
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory() as directory:
        checked = reconcile_inventory(spec, governance=governance, root=root, workspace=Path(directory))
        candidates = {}
        for source in YEARS:
            relative = f'candidates/{source}'
            built = history.materialize_current(mode='BOOTSTRAP', governance=governance,
                value=checked['inventories'][source], paths=checked['paths'], records=checked['records'],
                output=output / relative, repository_root=root, artifact_created_at=created_at)
            history.verify_state(output / relative, governance=governance, paths=checked['paths'],
                                 records=checked['records'], repository_root=root)
            package_path = f'packages/{source}.tar'
            package = build_publication_package(output / relative, output / package_path)
            candidates[source] = {'state': built['state'], 'artifact_id': built['manifest']['artifact_id'],
                'artifact_content_hash': built['manifest']['artifact_content_hash'],
                'package_sha256': package['package_sha256'], 'artifact_path': relative, 'package_path': package_path}
        if _fresh(spec, governance) != checked['baseline']:
            raise ValueError('governance/catalog changed during preparation')
        bundle = _bundle(spec, checked, candidates)
        write_canonical_json(output / 'bundle.json', bundle)
        return bundle


def verify_bootstrap(spec, bundle, *, governance, root: Path, directory: Path):
    """Re-extract immutable parents and candidates; never trust a retained pass."""
    if bundle.get('input_sha256') != sha256_json(spec):
        raise ValueError('bootstrap inputs changed')
    with tempfile.TemporaryDirectory() as temp:
        checked = reconcile_inventory(spec, governance=governance, root=root, workspace=Path(temp) / 'parents')
        candidates = {}
        for source in YEARS:
            claimed = bundle['candidates'][source]
            # Paths are contract-fixed; a bundle cannot redirect reads elsewhere.
            relative, package_path = f'candidates/{source}', f'packages/{source}.tar'
            if claimed['artifact_path'] != relative or claimed['package_path'] != package_path:
                raise ValueError('candidate paths changed')
            path = extract_publication_package(directory / package_path, Path(temp) / source,
                                               expected_sha256=claimed['package_sha256'])
            _, _, state = history.verify_state(path, governance=governance, paths=checked['paths'],
                                               records=checked['records'], repository_root=root)
            history.verify_state(directory / relative, governance=governance, paths=checked['paths'],
                                 records=checked['records'], repository_root=root)
            envelope = build_publication_package(directory / relative, Path(temp) / (source + '-check.tar'))
            if envelope['package_sha256'] != claimed['package_sha256']:
                raise ValueError('loose candidate differs from retained package')
            manifest = read_json(path / 'manifest.json')
            if manifest['artifact_created_at'] != _created_at(spec):
                raise ValueError('candidate creation provenance changed')
            candidates[source] = {'state': state, 'artifact_id': manifest['artifact_id'],
                'artifact_content_hash': manifest['artifact_content_hash'],
                'package_sha256': sha256_file(directory / package_path), 'artifact_path': relative, 'package_path': package_path}
        if _fresh(spec, governance) != checked['baseline'] or _bundle(spec, checked, candidates) != bundle:
            raise ValueError('bundle, candidate, authority or acceptance delta changed')
    return {'verification': 'passed', 'verified_original_parents': 35,
            'production_inputs_verified': checked['baseline']['namespace'] == 'production',
            'fresh_governance': checked['baseline']['governance'], 'installation_readiness': 'blocked',
            'publication': 'not_established', 'activation': False, 'remaining_gates': GATES}


def input_template(root: Path, package_root: Path):
    """Explicit review template from catalog; never materializes or confers authority."""
    registry = read_json(root / 'config/acs_historical_governance.json')
    catalog = read_json(root / 'config/artifact_catalog.json')
    parents = []
    for source in YEARS:
        for year in YEARS[source]:
            matches = [r for r in catalog['immutable_records'] if r.get('metadata', {}).get('source_id') == source
                       and r.get('metadata', {}).get('provider_release_id') == f'{PRODUCTS[source]}:{year}']
            if year == 2024:
                matches = [r for r in matches if r['object_id'] == compatibility.qualification.ELIGIBLE[source][0]]
            if len(matches) != 1:
                raise ValueError('template needs an unambiguous exact baseline catalog')
            record = matches[0]
            parents.append({'source_id': source, 'vintage': year, 'artifact_id': record['object_id'],
                'artifact_content_hash': record['artifact_content_hash'], 'package_sha256': record['package_sha256'],
                'data_sha256': record['metadata']['data_sha256']})
    pin_paths = sorted((root / 'config/monthly_source_input_pins').glob('acs_backfill__census_acs5__2023__*/census_acs5.json'))
    if len(pin_paths) != 1:
        raise ValueError('exact ACS5 2023 durable pin required')
    return {'schema_version': INPUT_VERSION, 'catalog': str(root / 'config/artifact_catalog.json'),
        'governance': str(root / 'config/acs_historical_governance.json'), 'scope_authorities': registry['current_scope'],
        'parents': parents, 'packages': {p['artifact_id']: str(package_root / (p['artifact_id'] + '.tar')) for p in parents},
        'evidence': {f'{s}:{y}': str(root / f'config/acs_backfill_reviews/provider-{s[7:]}-{y}.json')
                     for s in YEARS for y in YEARS[s] if y != 2024},
        'acs5_2023_pin': str(pin_paths[0]), 'compatibility_authorization': registry['current_compatibility_authorization'],
        'reports': {s: str(root / 'docs/audit/acs_2024_qualification_reports' / (h + '.json')) for s, h in {
            'census_acs1': 'c11e8cc403683582270be830092408220a21299f9e15dbb0d178016dccdaf8e1',
            'census_acs5': '23f02c9cf19cf93ea7b1fa277865076b9ce97170e55c7c9945c7dab7b9a571de'}.items()},
        'artifact_created_at': None}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--package-root', type=Path)
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--output', type=Path, required=True, help='New directory outside checkout')
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--template', action='store_true')
    mode.add_argument('--prepare', action='store_true')
    mode.add_argument('--verify', type=Path, help='Existing bundle.json; no regeneration')
    args = parser.parse_args()
    root, output = args.repository_root.resolve(), args.output.resolve()
    if output.is_relative_to(root) or output.exists():
        parser.error('new output directory outside checkout required')
    if args.template:
        if args.package_root is None: parser.error('--package-root required')
        output.mkdir(parents=True)
        write_canonical_json(output / 'inputs.json', input_template(root, args.package_root.resolve()))
        return 0
    if args.inputs is None: parser.error('--inputs required')
    spec = {}
    report = {'schema_version': 'acs_history_bootstrap_verification_v1', 'functionality': 'implemented',
        'actual_35_package_verification': 'not_established', 'verified_original_parents': 0,
        'required_original_parents': 35, 'local_tar_files_present': 0,
        'publication': 'not_established', 'installation_readiness': 'blocked', 'activation': False,
        'governance_verification': 'not_established'}
    stage = 'read_inputs'
    try:
        spec = read_json(args.inputs)
        report['local_tar_files_present'] = sum(isinstance(p, str) and Path(p).is_file() for p in spec.get('packages', {}).values())
        stage = 'load_production_governance'
        governance = ACSGovernance.load_production(REPOSITORY, os.environ.get('GITHUB_TOKEN', ''))
        governance.require_destination(REPOSITORY)
        report['governance_snapshot'] = governance.production_snapshot()
        report['governance_verification'] = 'production_loader_verified_at_snapshot'
        stage = 'prepare_or_reverify_real_inputs'
        if args.prepare:
            bundle = prepare_bootstrap(spec, governance=governance, root=root, output=output)
            verification = verify_bootstrap(spec, bundle, governance=governance, root=root, directory=output)
        else:
            output.mkdir(parents=True)
            bundle = read_json(args.verify)
            verification = verify_bootstrap(spec, bundle, governance=governance, root=root, directory=args.verify.resolve().parent)
        report.update(verification, actual_35_package_verification='passed', disposition='passed_preparation_only', bundle_id=bundle['bundle_id'])
        code = 0
    except Exception as exc:
        # No exception values: they can contain input URLs or credential-bearing paths.
        blocked = isinstance(exc, (BootstrapBlocked, FileNotFoundError, OSError, TransientPublicationError))
        report.update(disposition='blocked' if blocked else 'failed', error_type=type(exc).__name__, failed_stage=stage,
                      unresolved_gates=['complete_real_inputs', 'fresh_production_governance', *GATES])
        code = 2 if blocked else 1
    output.mkdir(parents=True, exist_ok=True)
    write_canonical_json(output / 'verification.json', report)
    print(json.dumps(report, sort_keys=True))
    return code


if __name__ == '__main__':
    raise SystemExit(main())
