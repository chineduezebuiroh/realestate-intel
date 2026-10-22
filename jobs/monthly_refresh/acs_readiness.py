"""Offline advisory ACS readiness. Never acquires, accepts, resolves or activates data."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import tempfile
import tarfile

import pandas as pd

from core.source_artifacts.hashing import sha256_file, sha256_json
from core.source_artifacts.package import extract_publication_package
from core.source_artifacts.validation import ArtifactValidationError
from jobs.monthly_refresh.source_inputs import validate_pin, verify_member_bytes
from jobs.monthly_refresh.acs_scope import check_identity
from jobs.monthly_refresh.acs_family_resolution import _parent
from jobs.monthly_refresh.acs_history import frame_identity
from sources.census_acs.artifact import (PRODUCTS, VARIABLES, canonicalize, geography_plan,
                                       governed_config_hashes, request_params)
from sources.census_acs.historical import validate_evidence, validate_snapshot

VERSION = 'acs_historical_readiness_v1'


def read_json(path):
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError('duplicate JSON identity: ' + key)
            out[key] = value
        return out
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def repository_inputs(root: Path) -> dict:
    """An explicit editable input template from adopted acquisition plans/results."""
    ranges, evidence, accepted = {}, {}, {}
    catalog = read_json(root / 'config/artifact_catalog.json')
    family_id = catalog['accepted']['source']['acs']
    resolutions = [read_json(p) for p in sorted((root / 'config/acs_family_resolutions').glob('*.json'))]
    resolutions = [r for r in resolutions if r.get('output_artifact_id') == family_id]
    if len(resolutions) != 1:
        raise ValueError('accepted ACS family must have exactly one resolution record')
    accepted_parents = resolutions[0]['parents']
    for source in PRODUCTS:
        years = set()
        for path in sorted((root / 'config/acs_backfill_reviews').glob('plan-*.json')):
            plan = read_json(path)
            if plan.get('source_id') == source:
                years.update(plan['vintages'])
        results = []
        for path in sorted((root / 'config/monthly_source_cycle_results').glob('monthly_cycle__2026-08__*/' + source + '.json')):
            result = read_json(path)['result']
            if result.get('status') == 'succeeded':
                results.append(result)
        if len(results) != 1 or not years:
            raise ValueError('cannot uniquely derive August identity or planned range: ' + source)
        result = results[0]
        years.add(int(result['provider_release_id'].rsplit(':', 1)[1]))
        ranges[source] = [min(years), max(years)]
        accepted[source] = {'artifact_id': result['candidate_artifact_id'],
            'artifact_content_hash': result['artifact_content_hash'], 'package_sha256': result['package_sha256']}
        family_parents = [p for p in accepted_parents if p['source_id'] == source]
        if len(family_parents) != 1 or any(family_parents[0].get(k) != v for k, v in accepted[source].items()):
            raise ValueError('August result differs from accepted ACS family parent')
        for path in sorted((root / 'config/acs_backfill_reviews').glob('provider-' + source.replace('census_', '') + '-????.json')):
            for year in read_json(path):
                evidence[f'{source}:{year}'] = str(path.resolve())
    return {'schema_version': VERSION, 'ranges': ranges, 'catalog': str((root / 'config/artifact_catalog.json').resolve()),
        'packages': {}, 'evidence': evidence, 'selected_parents': {}, 'accepted_august': accepted,
        'governance': str((root / 'config/acs_historical_governance.json').resolve())
            if (root / 'config/acs_historical_governance.json').is_file() else None, 'pins': [], 'baseline': None, 'comparison': None}


def continuity(baseline: Path, comparison: Path | None, available: set[tuple]) -> list[dict]:
    """Compare supplied observations only; annual ACS keys stay annual.

    Long CSV interface: geo_id,series,date,value. Scores and derived histories
    must be exported observations; no replacement calculation occurs here.
    """
    series = {'income', 'population', 'price_to_income', 'payment_burden', 'permit_intensity',
              'normalized_score', 'affordability', 'supply'}
    def load(path):
        frame = pd.read_csv(path, dtype={'geo_id': str, 'series': str})
        if not {'geo_id', 'series', 'date', 'value'} <= set(frame):
            raise ValueError('continuity CSV requires geo_id,series,date,value')
        frame['date'] = pd.to_datetime(frame.date, errors='raise').dt.strftime('%Y-%m-%d')
        if frame.duplicated(['geo_id', 'series', 'date']).any() or not set(frame.series) <= series:
            raise ValueError('duplicate or unknown continuity observation')
        if not pd.to_numeric(frame.value, errors='raise').map(lambda v: pd.notna(v) and abs(v) != float('inf')).all():
            raise ValueError('invalid continuity value')
        return frame
    old = load(baseline)
    new = load(comparison) if comparison else None
    rows = []
    for (geo, metric), group in old.groupby(['geo_id', 'series'], sort=True):
        dates = set(group.date)
        if new is not None:
            current = set(new.loc[(new.geo_id == geo) & (new.series == metric), 'date'])
            basis = 'supplied_observations'
        elif metric in {'income', 'population'}:
            suffix = 'median_household_income' if metric == 'income' else 'pop_total'
            current = {f'{year}-12-31' for g, year, s in available if g == geo and s == suffix}
            basis = 'verified_annual_sources'
        else:
            current, basis = set(), 'unresolved_requires_export'
        lost = sorted(dates - current) if basis != 'unresolved_requires_export' else None
        rows.append({'geo_id': geo, 'series': metric, 'baseline_dates': sorted(dates),
            'comparison_dates': sorted(current), 'lost_dates': lost, 'basis': basis,
            'baseline_observations': len(dates), 'comparison_observations': len(current),
            'baseline_first': min(dates), 'baseline_last': max(dates),
            'comparison_first': min(current) if current else None, 'comparison_last': max(current) if current else None,
            'disposition': 'blocked' if lost is None else ('fail' if lost else 'pass'),
            'dc_regression': geo.startswith('district_of_columbia')})
    return rows


def evaluate(spec: dict, root: Path) -> dict:
    if spec.get('schema_version') != VERSION or set(spec['ranges']) != set(PRODUCTS):
        raise ValueError('readiness schema/products mismatch')
    plan, excluded = geography_plan(root / 'config/geo_manifest.generated.csv')
    for geo in plan:
        request_params(geo)
    blockers, matrix, proposed = [], [], {}
    def issue(code, kind, disposition='blocked', **context):
        blockers.append({'code': code, 'kind': kind, 'disposition': disposition, **context})
    configured = repository_inputs(root)
    if spec.get('accepted_august') != configured['accepted_august']:
        issue('accepted_august_identity_differs_from_repository', 'engineering', 'fail')
    if spec['ranges'] != configured['ranges']:
        issue('incomplete_configured_historical_range', 'engineering')
    catalog = read_json(spec['catalog'])
    records = catalog['immutable_records']
    by_id = {}
    for rec in records:
        if rec.get('metadata', {}).get('source_id') in PRODUCTS:
            by_id.setdefault(rec['object_id'], []).append(rec)
    if any(len(items) != 1 for items in by_id.values()):
        issue('duplicate_catalog_identity', 'engineering', 'fail')
    governance = read_json(spec['governance']) if spec.get('governance') else None
    if governance is None:
        issue('missing_production_historical_governance', 'governance')
    else:
        try:
            if governance.get('schema_version') != 'acs_historical_governance_registry_v1' or governance.get('namespace') != 'production':
                raise ValueError('production registry schema/namespace mismatch')
            for field, kind, identity in [('scopes', 'acs_historical_scope_authority_v1', 'authority_id'),
                                          ('states', 'acs_physical_historical_state_v2', 'state_id'),
                                          ('gap_evidence', 'acs_governed_gap_evidence_v1', 'evidence_id')]:
                for key, value in governance.get(field, {}).items():
                    check_identity(kind, value, identity)
                    if value[identity] != key:
                        raise ValueError('registry identity key mismatch')
                    if field == 'scopes' and value.get('config_hashes') != governed_config_hashes(root):
                        raise ValueError('historical governance configuration drift')
            for source in PRODUCTS:
                if governance.get('current_scope', {}).get(source) not in governance.get('scopes', {}):
                    issue('missing_product_scope_authority', 'governance', source_id=source)
                if governance.get('current_state', {}).get(source) not in governance.get('states', {}):
                    issue('incomplete_historical_physical_state', 'governance', source_id=source)
        except (ValueError, KeyError, TypeError) as exc:
            issue('invalid_governance_records', 'engineering', 'fail', detail=str(exc))
        # Offline files cannot acquire the live main provenance capability.
        issue('offline_governance_provenance_requires_independent_review', 'governance')
    issue('historical_physical_state_acceptance_not_established', 'governance')
    issue('downstream_activation_not_established', 'governance')
    available, manifests = set(), {}
    with tempfile.TemporaryDirectory(prefix='acs-readiness-') as temp:
        for source, product in PRODUCTS.items():
            start, end = spec['ranges'][source]
            if type(start) is not int or type(end) is not int or not 2005 <= start <= end <= 2100:
                raise ValueError('invalid explicit historical range')
            parents = []
            for year in range(start, end + 1):
                key = f'{source}:{year}'
                candidates = [rec for rec in records if rec.get('metadata', {}).get('source_id') == source
                    and rec.get('metadata', {}).get('provider_release_id') == f'{product}:{year}']
                selected = spec.get('selected_parents', {}).get(key)
                august = configured['accepted_august'].get(source) if year == 2024 else None
                if year == 2024 and not august:
                    issue('missing_exact_august_parent', 'engineering', source_id=source)
                if august:
                    if selected and selected != august['artifact_id']:
                        issue('august_parent_replacement', 'engineering', 'fail', source_id=source)
                    selected = august['artifact_id']
                matches = [r for r in candidates if not selected or r['object_id'] == selected]
                ambiguous = len(matches) > 1 or any(len(by_id[r['object_id']]) != 1 for r in matches)
                if ambiguous:
                    issue('ambiguous_same_vintage_parent', 'engineering', 'fail', source_id=source, vintage=year)
                record = matches[0] if len(matches) == 1 and not ambiguous else None
                accounting, expected, evidence_hash = {}, None, None
                evidence_path = spec.get('evidence', {}).get(key)
                if evidence_path:
                    try:
                        evidence = read_json(evidence_path)
                        evidence = evidence[str(year)] if str(year) in evidence else evidence
                        if evidence.get('schema_version') == 'acs_historical_provider_preflight_v1':
                            checked = validate_evidence(evidence, source_id=source, vintage=year, plan=plan, excluded=excluded)
                        else:
                            checked = validate_snapshot(source_id=source, vintage=year, snapshot=evidence['snapshot'],
                                plan=plan, excluded=excluded, omissions=evidence['omission_evidence'])
                        accounting = {(a['geo_id'], a['variable']): a['classification'] for a in checked['accounting']}
                        expected, _ = canonicalize(source, year, evidence['snapshot'])
                        evidence_hash = sha256_file(Path(evidence_path))
                    except (ValueError, KeyError, TypeError, OSError, ArtifactValidationError, tarfile.TarError, EOFError) as exc:
                        issue('invalid_provider_accounting_or_omissions', 'engineering', 'fail', source_id=source, vintage=year,
                              detail=str(exc))
                verified, package_verified = False, False
                package = spec.get('packages', {}).get(record['object_id']) if record else None
                if record and package:
                    try:
                        if august and any(record.get(k) != august[k] for k in ('artifact_content_hash', 'package_sha256')):
                            raise ValueError('exact August parent hash mismatch')
                        path = extract_publication_package(Path(package), Path(temp) / f'{source}-{year}',
                                                           expected_sha256=record['package_sha256'])
                        frame, manifest, identity = _parent(record, path, source)
                        if (manifest['provider_release_id'] != f'{product}:{year}' or manifest['target_month'] != f'{year}-12'
                                or not pd.to_datetime(frame.date).eq(pd.Timestamp(year, 12, 31)).all()
                                or not frame.property_type_id.eq('all').all() or not frame.property_type.eq('all').all()
                                or not frame.source_id.eq(source).all()
                                or frame.duplicated(['geo_id', 'metric_id', 'date', 'property_type_id']).any()
                                or manifest.get('artifact_status') != 'complete' or manifest.get('validation_status') != 'passed'):
                            raise ValueError('acquisition row/product/vintage identity mismatch')
                        package_verified = True
                        if expected is None:
                            issue('missing_independent_provider_evidence', 'engineering', source_id=source, vintage=year)
                        elif frame_identity(frame) != frame_identity(expected):
                            raise ValueError('provider accounting differs from acquisition rows')
                        else:
                            verified = True
                            parent = {**identity, 'vintage': year, 'data_sha256': manifest['data_sha256']}
                            parents.append(parent)
                            manifests[(source, year)] = manifest
                    except (ValueError, KeyError, TypeError, OSError, ArtifactValidationError, tarfile.TarError, EOFError) as exc:
                        issue('invalid_acquisition_identity', 'engineering', 'fail', source_id=source, vintage=year, detail=str(exc))
                if source == 'census_acs1' and year == 2020:
                    issue('acs1_2020_whole_vintage_gap_requires_governance', 'governance', source_id=source, vintage=year)
                if not package:
                    if record:
                        issue('local_immutable_package_not_supplied', 'engineering', source_id=source, vintage=year)
                    else:
                        issue('missing_acs5_2023_acquisition' if source == 'census_acs5' and year == 2023 else 'missing_acquisition',
                              'acquisition', source_id=source, vintage=year, provider_evidence_present=bool(accounting))
                for geo in plan + excluded:
                    for variable, suffix in VARIABLES.items():
                        observed = accounting.get((geo['geo_id'], variable))
                        status = ('explicitly_excluded' if geo in excluded else
                            'ambiguous_or_conflicting_identity' if ambiguous else
                            'evidenced_unavailable' if observed and observed != 'available' else
                            'available_and_verified' if verified else
                            'missing_acquisition' if record is None else 'unresolved_evidence')
                        if status == 'available_and_verified':
                            available.add((geo['geo_id'], year, suffix))
                        matrix.append({'source_id': source, 'vintage': year, 'geo_id': geo['geo_id'], 'variable': variable,
                            'availability': status, 'provider_classification': observed, 'evidence_sha256': evidence_hash,
                            'artifact_id': record['object_id'] if record else None,
                            'acquisition_status': 'cataloged_immutable' if record else 'not_established',
                            'acquisition_verified': package_verified,
                            'independent_provider_verification': 'passed' if verified else 'not_established',
                            'historical_acceptance': 'not_established',
                            'downstream_activation': 'not_established'})
            proposed[source] = {'advisory_only': True, 'governance_authority': False, 'source_id': source,
                                'parents': parents, 'requested_range': [start, end],
                                'unverified_vintages': sorted(set(range(start, end + 1)) - {p['vintage'] for p in parents})}
    # Logical potential is a report, never a materialized family or acceptance.
    logical = []
    indexed = {(r['source_id'], r['vintage'], r['geo_id'], r['variable']): r['availability'] for r in matrix}
    for geo, year, suffix in sorted(available):
        variable = next(v for v, s in VARIABLES.items() if s == suffix)
        one = indexed.get(('census_acs1', year, geo, variable))
        logical.append({'geo_id': geo, 'vintage': year, 'metric': suffix,
            'proposed_source': 'census_acs1' if one == 'available_and_verified' else 'census_acs5',
            'resolution': 'potential_only', 'acs1_evidence_resolved': one in {'available_and_verified', 'evidenced_unavailable'},
            'historical_acceptance': 'not_established', 'downstream_activation': 'not_established'})
    for item in spec.get('pins', []):
        try:
            pin = read_json(item['pin'])
            validate_pin(pin, cycle_id=pin['cycle_id'], source_id=pin['source_id'], required_members={'snapshot'})
            members = {name: Path(path) for name, path in item['members'].items()}
            verify_member_bytes(pin, members)
            snapshot = read_json(members['snapshot'])
            year = snapshot['vintage']
            key = f"{pin['source_id']}:{year}"
            evidence = read_json(spec['evidence'][key])
            evidence = evidence[str(year)] if str(year) in evidence else evidence
            manifest = manifests.get((pin['source_id'], year))
            if manifest is None or (manifest.get('raw_source_lineage') or {}).get('provider_pin') != pin:
                raise ValueError('source pin not bound to verified acquisition manifest')
            if evidence['snapshot'] != snapshot or pin['provider_release_id'] != f"{PRODUCTS[pin['source_id']]}:{year}":
                raise ValueError('pin snapshot differs from independently checked evidence')
        except (ValueError, KeyError, TypeError, OSError) as exc:
            issue('invalid_source_pin_evidence', 'engineering', 'fail', detail=str(exc))
    comparison = continuity(Path(spec['baseline']), Path(spec['comparison']) if spec.get('comparison') else None, available) if spec.get('baseline') else []
    comparison = [row for row in comparison if row['geo_id'] in {g['geo_id'] for g in plan}]
    for row in comparison:
        if row['disposition'] != 'pass':
            issue('historical_contraction' if row['disposition'] == 'fail' else 'continuity_evidence_missing',
                  'engineering', row['disposition'], geo_id=row['geo_id'], series=row['series'])
    if not comparison:
        issue('previous_macro_baseline_not_supplied', 'engineering')
    counts = dict(sorted(Counter(r['availability'] for r in matrix).items()))
    if any(r['availability'] in {'missing_acquisition', 'unresolved_evidence'} for r in matrix):
        issue('incomplete_historical_logical_inputs', 'engineering')
    if any(not r['acs1_evidence_resolved'] for r in logical):
        issue('acs5_fallback_preference_evidence_unresolved', 'engineering')
    fingerprints = {}
    paths = [spec['catalog'], *spec.get('packages', {}).values(), *spec.get('evidence', {}).values()]
    paths += [spec[k] for k in ('governance', 'baseline', 'comparison') if spec.get(k)]
    for item in spec.get('pins', []):
        paths += [item['pin'], *item['members'].values()]
    for path in sorted(set(paths)):
        fingerprints[path] = sha256_file(Path(path)) if Path(path).is_file() else None
    return {'schema_version': VERSION, 'advisory_only': True, 'input_sha256': sha256_json(spec),
        'input_file_sha256': fingerprints, 'config_hashes': governed_config_hashes(root),
        'scope': {'requested': len(plan), 'by_level': dict(sorted(Counter(g['level'] for g in plan).items())),
                  'excluded': excluded}, 'coverage': matrix, 'availability_counts': counts,
        'proposed_inventories': proposed, 'logical_potential': logical, 'blockers': blockers,
        'continuity': comparison, 'validation': {'disposition': 'fail' if any(b['disposition'] == 'fail' for b in blockers) else 'blocked' if blockers else 'pass',
            'source_availability': 'pass' if all(r['availability'] in {'available_and_verified', 'evidenced_unavailable', 'explicitly_excluded'} for r in matrix) else 'blocked',
            'historical_acceptance': 'blocked', 'downstream_activation': 'blocked'}}


def write_reports(report: dict, output: Path):
    output.mkdir(parents=True, exist_ok=False)
    for name, value in [('coverage', report['coverage']), ('proposed-inventories', report['proposed_inventories']),
                        ('blockers', report['blockers']), ('continuity', report['continuity']), ('validation', report)]:
        (output / (name + '.json')).write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')
    (output / 'summary.md').write_text('# ACS historical readiness\n\nDisposition: ' + report['validation']['disposition'] +
        '\n\nAdvisory only. No historical acceptance or activation authority.\n\nRequested geographies: ' +
        str(report['scope']['requested']) + '\n\n' + '\n'.join(f'- {k}: {v}' for k, v in report['availability_counts'].items()) +
        '\n\nBlockers:\n\n' + '\n'.join('- ' + b['kind'] + ': ' + b['code'] +
            ' ' + ' '.join(str(b[k]) for k in ('source_id', 'vintage', 'geo_id', 'series') if k in b)
            for b in report['blockers']) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--inputs', type=Path)
    parser.add_argument('--template', type=Path)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    root = args.repository_root.resolve()
    destination = args.template or args.output
    if destination is None or destination.resolve().is_relative_to(root):
        parser.error('output/template must be a new location outside the repository')
    if args.template:
        with args.template.open('x') as stream:
            stream.write(json.dumps(repository_inputs(root), indent=2, sort_keys=True) + '\n')
        return 0
    if args.inputs is None:
        parser.error('--inputs required')
    try:
        report = evaluate(read_json(args.inputs), root)
    except (ValueError, KeyError, TypeError, OSError, AttributeError, IndexError, ArtifactValidationError, tarfile.TarError, EOFError) as exc:
        report = {'schema_version': VERSION, 'advisory_only': True, 'coverage': [],
            'proposed_inventories': {}, 'continuity': [], 'availability_counts': {},
            'scope': {'requested': 'unresolved'},
            'blockers': [{'code': 'invalid_or_missing_input', 'kind': 'engineering',
                          'disposition': 'fail', 'error_type': type(exc).__name__}],
            'validation': {'disposition': 'fail', 'source_availability': 'blocked',
                           'historical_acceptance': 'blocked', 'downstream_activation': 'blocked'}}
    write_reports(report, args.output)
    return {'pass': 0, 'fail': 1, 'blocked': 2}[report['validation']['disposition']]


if __name__ == '__main__':
    raise SystemExit(main())
