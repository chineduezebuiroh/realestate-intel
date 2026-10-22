"""Bounded offline discovery of operator inputs; retrieval is a separate command."""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import json
import os
from pathlib import Path
import re
import shlex
import tempfile

from core.source_artifacts.hashing import sha256_file, sha256_json
from core.source_artifacts.catalog import validate_record
from core.source_artifacts.package import extract_publication_package
from jobs.monthly_refresh.acs_family_resolution import _parent
from jobs.monthly_refresh.acs_readiness import read_json, repository_inputs
from jobs.monthly_refresh.source_inputs import validate_pin
from sources.census_acs.artifact import PRODUCTS, geography_plan
from sources.census_acs.historical import validate_evidence, validate_snapshot

MAX_FILES = 10000
MAX_DEPTH = 8
MAX_JSON = 32 * 1024 * 1024
MAX_PACKAGE = 512 * 1024 * 1024
MAX_TOTAL = 2 * 1024 * 1024 * 1024


def local_files(roots: list[Path], repository: Path, output: Path) -> list[Path]:
    """Never walk symlinks or the repository (which contains test evidence)."""
    files = set()
    def scan_error(error):
        raise error
    for root in sorted({p.expanduser().resolve() for p in roots}):
        if not root.is_dir() or root.is_relative_to(repository):
            raise ValueError('local roots must be existing directories outside the repository')
        for directory, dirs, names in os.walk(root, followlinks=False, onerror=scan_error):
            current = Path(directory)
            dirs[:] = sorted(d for d in dirs if d not in {'.git', '.venv', 'node_modules'}
                and not (current / d).is_symlink()
                and not (current / d).resolve().is_relative_to(repository)
                and not (current / d).resolve().is_relative_to(output))
            if len(current.relative_to(root).parts) >= MAX_DEPTH and dirs:
                raise ValueError('local discovery depth limit; select narrower roots')
            for name in sorted(names):
                path = current / name
                if path.is_symlink() or not path.is_file():
                    continue
                files.add(path.resolve())
                if len(files) > MAX_FILES:
                    raise ValueError('local discovery file limit; select narrower roots')
    return sorted(files)


def discover(root: Path, roots: list[Path], output: Path, *, catalog: Path | None = None,
             baseline: Path | None = None, comparison: Path | None = None) -> dict:
    root, output = root.resolve(), output.resolve()
    if output.exists() or output.is_relative_to(root):
        raise ValueError('output must be a new directory outside the repository')
    spec = repository_inputs(root)
    spec.update(packages={}, selected_parents={}, pins=[])
    if catalog:
        spec['catalog'] = str(catalog.expanduser().resolve())
    records = [r for r in read_json(spec['catalog'])['immutable_records']
        if r.get('metadata', {}).get('source_id') in PRODUCTS and r.get('object_type') == 'source']
    files = local_files(roots, root, output)
    notes, package_rows, evidence_rows = [], [], []
    generated = {}
    def emit(code, **context):
        notes.append({'code': code, **context})
    digests, json_files = defaultdict(list), []
    total = 0
    for path in files:
        if path.suffix not in {'.tar', '.json'}:
            continue
        size = path.stat().st_size
        limit = MAX_PACKAGE if path.suffix == '.tar' else MAX_JSON
        total += size
        if size > limit or total > MAX_TOTAL:
            raise ValueError('local discovery byte limit; select narrower roots')
        digests[sha256_file(path)].append(path)
        if path.suffix == '.json':
            json_files.append(path)
    groups, ids = defaultdict(list), defaultdict(list)
    for rec in records:
        try:
            validate_record(rec)
            if not re.fullmatch(r'[A-Za-z0-9_.-]+', rec['object_id']):
                raise ValueError('unsafe artifact ID')
        except Exception as exc:
            emit('invalid_catalog_record', artifact_id=rec.get('object_id'), error_type=type(exc).__name__)
            continue
        ids[rec['object_id']].append(rec)
        groups[(rec['metadata']['source_id'], rec['metadata'].get('provider_release_id'))].append(rec)
    for artifact_id, items in sorted(ids.items()):
        if len(items) > 1:
            emit('duplicate_catalog_identity', artifact_id=artifact_id)
    selected_records = []
    for source, (start, end) in spec['ranges'].items():
        for year in range(start, end + 1):
            key = f'{source}:{year}'
            candidates = groups[(source, f'{PRODUCTS[source]}:{year}')]
            if len(candidates) > 1:
                emit('duplicate_same_vintage_identities', key=key,
                     artifact_ids=sorted(r['object_id'] for r in candidates))
            august = spec['accepted_august'].get(source) if year == 2024 else None
            matches = [r for r in candidates if not august or r['object_id'] == august['artifact_id']]
            if len(matches) != 1 or len(ids[matches[0]['object_id']]) != 1:
                emit('missing_or_ambiguous_catalog_parent', key=key)
                continue
            rec = matches[0]
            if august and any(rec.get(k) != august[k] for k in ('artifact_content_hash', 'package_sha256')):
                emit('august_parent_hash_conflict', key=key)
                continue
            spec['selected_parents'][key] = rec['object_id']
            selected_records.append(rec)
    pin_candidates, verified_pins = [], set()
    with tempfile.TemporaryDirectory(prefix='acs-discovery-') as temporary:
        for index, rec in enumerate(selected_records):
            artifact_id = rec['object_id']
            paths = digests.get(rec['package_sha256'], [])
            row = {'artifact_id': artifact_id, 'package_sha256': rec['package_sha256'],
                'status': 'missing_local_package', 'matching_paths': [str(p) for p in paths],
                'release_id': rec.get('release_id'), 'asset_id': rec.get('asset_id'),
                'release_tag': rec.get('release_tag'), 'asset_filename': rec.get('asset_filename')}
            if paths:
                try:
                    extracted = extract_publication_package(paths[0], Path(temporary) / str(index), expected_sha256=rec['package_sha256'])
                    _, manifest, _ = _parent(rec, extracted, rec['metadata']['source_id'])
                    row['status'] = 'package_verified'
                    spec['packages'][artifact_id] = str(paths[0])
                    pin = (manifest.get('raw_source_lineage') or {}).get('provider_pin')
                    if pin:
                        pin_candidates.append((f'package:{artifact_id}', pin))
                        verified_pins.add(sha256_json(pin))
                except Exception as exc:
                    row.update(status='invalid_package', error_type=type(exc).__name__)
            else:
                # Record near matches, but never trust a filename or rebuild a cache.
                row['unverified_filename_matches'] = [str(p) for p in files if p.name in {rec.get('asset_filename'), artifact_id + '.tar'}]
            package_rows.append(row)
    plan, excluded = geography_plan(root / 'config/geo_manifest.generated.csv')
    evidence_candidates = defaultdict(list)
    snapshots, receipts, extracted_caches = [], [], defaultdict(list)
    known = set(map(Path, spec['evidence'].values()))
    pin_paths = sorted((root / 'config/monthly_source_input_pins').glob('*/*.json'))
    for path in sorted(set(json_files) | known | set(pin_paths)):
        try:
            data = read_json(path)
        except (OSError, ValueError):
            emit('unreadable_json', path=str(path))
            continue
        if isinstance(data, list):
            if data and all(isinstance(r, dict) and r.get('schema_version') == 'acs_historical_omission_evidence_v1' for r in data):
                receipts.append((path, data))
            continue
        if not isinstance(data, dict):
            continue
        if data.get('schema_version') == 'source_artifact_schema_v1' and data.get('artifact_id') in ids:
            extracted_caches[data['artifact_id']].append(str(path.parent))
        if data.get('schema_version') == 'monthly_source_input_pin_v1' and data.get('source_id') in PRODUCTS:
            pin_candidates.append((str(path), data))
        if data.get('contract_version') == 'census_acs_physical_source_v1' and data.get('source_id') in PRODUCTS:
            snapshots.append((path, data))
        candidates = [data] if 'snapshot' in data else [v for v in data.values() if isinstance(v, dict) and 'snapshot' in v]
        for value in candidates:
            snapshot = value.get('snapshot', {})
            source, year = snapshot.get('source_id'), snapshot.get('vintage')
            if source not in PRODUCTS or not isinstance(year, int):
                continue
            key = f'{source}:{year}'
            if value.get('omission_evidence'):
                receipts.append((path, value['omission_evidence']))
            try:
                if value.get('schema_version') == 'acs_historical_provider_preflight_v1':
                    validate_evidence(value, source_id=source, vintage=year, plan=plan, excluded=excluded)
                else:
                    validate_snapshot(source_id=source, vintage=year, snapshot=snapshot, plan=plan,
                                      excluded=excluded, omissions=value['omission_evidence'])
                evidence_candidates[key].append((str(path), value))
            except Exception as exc:
                emit('invalid_provider_evidence', key=key, path=str(path), error_type=type(exc).__name__)
    # Pair retained snapshots only with exact existing receipts. Empty receipts
    # are valid only when the snapshot has no omissions; never generate receipts.
    for path, snapshot in snapshots:
        key = f"{snapshot['source_id']}:{snapshot['vintage']}"
        matches = []
        for receipt_path, values in [(None, [])] + receipts:
            values = [r for r in values if r.get('source_id') == snapshot['source_id'] and r.get('vintage') == snapshot['vintage']]
            try:
                validate_snapshot(source_id=snapshot['source_id'], vintage=snapshot['vintage'], snapshot=snapshot,
                                  plan=plan, excluded=excluded, omissions=values)
                matches.append((receipt_path, values))
            except (ValueError, KeyError, TypeError):
                continue
        if matches:
            value = {'snapshot': snapshot, 'omission_evidence': matches[0][1]}
            evidence_candidates[key].append((str(path), value))
        else:
            emit('retained_snapshot_missing_exact_omission_receipts', key=key, path=str(path))
    spec['evidence'] = {}
    for key, items in sorted(evidence_candidates.items()):
        identities = {sha256_json({'snapshot': v['snapshot'], 'omission_evidence': v['omission_evidence']}) for _, v in items}
        if len(identities) != 1:
            emit('conflicting_provider_evidence', key=key, paths=sorted({p for p, _ in items}))
            continue
        value = sorted(items, key=lambda item: item[0])[0][1]
        relative = f'evidence/{key.replace(":", "-")}.json'
        generated[relative] = value
        spec['evidence'][key] = str(output / relative)
        evidence_rows.append({'key': key, 'status': 'accounting_verified', 'source_paths': sorted({p for p, _ in items})})
    pins = {}
    for origin, pin in sorted(pin_candidates, key=lambda item: item[0]):
        try:
            validate_pin(pin, cycle_id=pin['cycle_id'], source_id=pin['source_id'], required_members={'snapshot'})
            if sha256_json(pin) not in verified_pins:
                emit('pin_not_bound_to_verified_selected_package', pin_id=pin['pin_id'], origin=origin)
                continue
            matches = digests.get(pin['members']['snapshot']['sha256'], [])
            if not matches:
                emit('pin_snapshot_bytes_missing', pin_id=pin['pin_id'], origin=origin)
                continue
            if pin['pin_id'] in pins and pins[pin['pin_id']] != pin:
                raise ValueError('conflicting pin identity')
            pins[pin['pin_id']] = pin
            relative = f'pins/{sha256_json(pin)}.json'
            generated[relative] = pin
        except (ValueError, KeyError, TypeError):
            emit('invalid_pin', origin=origin)
    spec['pins'] = [{'pin': str(output / f'pins/{sha256_json(pin)}.json'),
        'members': {'snapshot': str(digests[pin['members']['snapshot']['sha256']][0])}} for _, pin in sorted(pins.items())]
    for field, explicit, filename in [('baseline', baseline, 'macro-baseline.csv'), ('comparison', comparison, 'macro-comparison.csv')]:
        matches = [explicit.expanduser().resolve()] if explicit else [p for p in files if p.name == filename]
        if len(matches) == 1 and matches[0].is_file():
            with matches[0].open(newline='') as stream:
                headers = csv.DictReader(stream).fieldnames or []
            if {'geo_id', 'series', 'date', 'value'} <= set(headers):
                spec[field] = str(matches[0])
            else:
                spec[field] = None
                emit('export_schema_unresolved', field=field, path=str(matches[0]))
        else:
            spec[field] = None
            emit('unresolved_' + field, candidates=[str(p) for p in matches])
    for source, (start, end) in spec['ranges'].items():
        for year in range(start, end + 1):
            if f'{source}:{year}' not in spec['evidence']:
                emit('provider_evidence_unresolved', key=f'{source}:{year}')
    for row in package_rows:
        if row['status'] != 'package_verified':
            row['extracted_caches_not_original_packages'] = sorted(extracted_caches[row['artifact_id']])
    commands = ['#!/bin/sh', 'set -eu', '# Explicit opt-in: downloads existing GitHub assets only. Never provider data.',
                '# Requires gh auth login; original files are never overwritten.', 'set -C']
    for row, rec in zip(package_rows, selected_records):
        if row['status'] == 'package_verified':
            continue
        repo, asset = rec.get('remote_repository', ''), rec.get('asset_id')
        if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+', repo) or type(asset) is not int or asset <= 0:
            emit('retrieval_identity_missing', artifact_id=rec['object_id'])
            continue
        target = output / 'retrieved-packages' / (rec['object_id'] + '.tar')
        commands += [f'mkdir -p {shlex.quote(str(target.parent))}',
            f'gh api --method GET -H "Accept: application/octet-stream" {shlex.quote(f"repos/{repo}/releases/assets/{asset}")} > {shlex.quote(str(target))}',
            f'printf \'%s  %s\\n\' {shlex.quote(rec["package_sha256"])} {shlex.quote(str(target))} | shasum -a 256 -c -']
    report = {'schema_version': 'acs_readiness_discovery_v1', 'offline': True, 'advisory_only': True,
        'catalog': spec['catalog'], 'catalog_sha256': sha256_file(Path(spec['catalog'])),
        'roots': sorted({str(p.expanduser().resolve()) for p in roots}), 'files_scanned': len(files),
        'packages': package_rows, 'evidence': evidence_rows, 'unresolved': notes,
        'exports': {k: {'path': spec[k], 'status': 'validator_and_provenance_review_required'}
                    for k in ('baseline', 'comparison')}}
    output.mkdir(parents=True)
    for relative, value in sorted({**generated, 'inputs.json': spec, 'discovery.json': report}.items()):
        path = output / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True, indent=2) + '\n')
    (output / 'retrieve-missing.sh').write_text('\n'.join(commands) + '\n')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, default=Path('.'))
    parser.add_argument('--local-root', action='append', type=Path, default=[])
    parser.add_argument('--catalog', type=Path)
    parser.add_argument('--baseline', type=Path)
    parser.add_argument('--comparison', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report = discover(args.repository_root, args.local_root, args.output,
                      catalog=args.catalog, baseline=args.baseline, comparison=args.comparison)
    print(json.dumps({'packages_verified': sum(p['status'] == 'package_verified' for p in report['packages']),
                      'packages_expected': len(report['packages']), 'unresolved': len(report['unresolved'])}, sort_keys=True))
    return 2 if report['unresolved'] or any(p['status'] != 'package_verified' for p in report['packages']) else 0


if __name__ == '__main__':
    raise SystemExit(main())
