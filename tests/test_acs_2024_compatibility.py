"""Offline canonical-row fixtures, never synthetic snapshots, receipts or approvals.

Package fixtures deliberately replace the test-local TAR/catalog anchors. They
exercise the exact August identity route, not certification of production bytes.
The production API has no fixture/anchor injection parameter.
"""
from collections import Counter
import json
from pathlib import Path
import shutil
import socket
import tarfile

import pandas as pd
import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh import acs_2024_compatibility as compat, acs_readiness as readiness
from sources.census_acs.artifact import CANONICAL_COLUMNS, CONFIG_PATHS, PRODUCTS, VARIABLES, geography_plan

ROOT = Path('.').resolve()


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True))


def rows(source):
    pin = compat.read_json(ROOT / f'config/monthly_source_input_pins/{compat.CYCLE}/{source}.json')
    frame = pd.DataFrame([{'geo_id': geo, 'metric_id': source + '_' + suffix,
                          'date': pd.Timestamp('2024-12-31'), 'property_type_id': 'all',
                          'value': 123.0, 'source_id': source, 'property_type': 'all'}
                         for geo in pin['members']['snapshot']['evidence']['available_membership']
                         for suffix in VARIABLES.values()], columns=CANONICAL_COLUMNS)
    absent = pin['members']['snapshot']['evidence']['provider_ineligible']
    coverage = {'available_membership': pin['members']['snapshot']['evidence']['available_membership'],
                'available_geographies': len(frame) // 2,
                'provider_ineligible': [{'geo_id': g, 'classification': 'provider_ineligible_no_content'} for g in absent],
                'excluded_geographies': pin['members']['snapshot']['evidence']['excluded_geographies']}
    return frame, pin, coverage


@pytest.fixture(params=sorted(PRODUCTS))
def source(request):
    return request.param


@pytest.fixture
def retained(tmp_path, monkeypatch, source):
    root = tmp_path / 'repo'
    root.mkdir()
    # git show/ancestry remain real and read-only, with no production authority.
    (root / '.git').write_text('gitdir: ' + str(ROOT / '.git') + '\n')
    copied = [*CONFIG_PATHS, 'config/artifact_catalog.json', compat.FAMILY,
              *compat.CODE, f'config/monthly_source_input_pins/{compat.CYCLE}/{source}.json',
              f'config/monthly_source_cycle_results/{compat.CYCLE}/{source}.json']
    for path in copied:
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, target)
    frame, pin, coverage = rows(source)
    artifact = tmp_path / 'fixture-artifact'
    original_configs = {p: compat.digest(compat.git_bytes(ROOT, compat.ORIGINAL_CODE, p)) for p in CONFIG_PATHS}
    m = create_artifact(artifact, frame, source_id=source, source_family=source, source_type='government_survey',
                        provider='Census', distribution_channel='census_data_api',
                        provider_release_id=PRODUCTS[source] + ':2024', provider_release_timestamp_or_date='2024',
                        retrieved_at=pin['members']['snapshot']['retrieved_at'], target_month='2024-12',
                        source_request_identity=pin['members']['snapshot']['evidence']['request_identity'],
                        source_urls_or_endpoint_identity=[pin['members']['snapshot']['url']],
                        raw_source_lineage={'schema_version': 'census_acs_candidate_evidence_v1',
                            'cycle_id': compat.CYCLE, 'physical_source_id': source, 'provider_pin': pin, 'coverage': coverage},
                        config_hashes=original_configs, git_sha=compat.ORIGINAL_CODE)
    cat = compat.read_json(root / 'config/artifact_catalog.json')
    rec = next(r for r in cat['immutable_records'] if r['object_id'] == compat.ELIGIBLE[source][0])
    m['artifact_id'], m['artifact_content_hash'] = rec['object_id'], rec['artifact_content_hash']
    dump(artifact / 'manifest.json', m)
    package = tmp_path / 'original-fixture.tar'
    anchored = {}
    original_git_bytes = compat.git_bytes

    def repack():
        # Test-only bypass of builder validation allows independently testing bad
        # rows behind a valid TAR hash; production always uses exact frozen hashes.
        manifest = compat.read_json(artifact / 'manifest.json')
        data = pd.read_parquet(artifact / 'data.parquet')
        manifest.update(data_sha256=sha256_file(artifact / 'data.parquet'), row_count=len(data),
                        observation_min=str(pd.to_datetime(data.date).dt.date.min()),
                        observation_max=str(pd.to_datetime(data.date).dt.date.max()))
        dump(artifact / 'manifest.json', manifest)
        with tarfile.open(package, 'w') as archive:
            for name in ['manifest.json', 'data.parquet', 'validation.json']:
                archive.add(artifact / name, arcname=name)
        rec['metadata']['data_sha256'] = manifest['data_sha256']
        rec['package_sha256'] = sha256_file(package)
        dump(root / 'config/artifact_catalog.json', cat)
        result_path = f'config/monthly_source_cycle_results/{compat.CYCLE}/{source}.json'
        result = compat.read_json(root / result_path)
        result['result']['package_sha256'] = rec['package_sha256']
        dump(root / result_path, result)
        family = compat.read_json(root / compat.FAMILY)
        parent = next(p for p in family['parents'] if p['source_id'] == source)
        parent.update(data_sha256=manifest['data_sha256'], package_sha256=rec['package_sha256'])
        dump(root / compat.FAMILY, family)
        for path in ['config/artifact_catalog.json', result_path, compat.FAMILY]:
            anchored[path] = (root / path).read_bytes()
    repack()

    def test_git_bytes(where, revision, path):
        if revision == compat.BASE and path in anchored:
            return anchored[path]
        return original_git_bytes(ROOT, revision, path)
    monkeypatch.setattr(compat, 'git_bytes', test_git_bytes)
    monkeypatch.setattr(socket.socket, 'connect', lambda *a: pytest.fail('network access'))
    def run():
        return compat.qualify(root=root, source_id=source, package=package)
    return root, artifact, package, repack, run


def test_exact_august_identity_technical_success_without_approval(retained, source):
    root, _, package, _, run = retained
    originals = {p: p.read_bytes() for p in [package, *root.rglob('*.json')]}
    report = run()
    assert report['technical_reconciliation'] == 'passed', report
    assert report['artifact_id'] == compat.ELIGIBLE[source][0]
    assert report['reconciliation']['accounted_cells'] == 446
    assert report['reconciliation']['retained_rows'] == compat.ELIGIBLE[source][3]
    assert report['compatibility_governance'] == 'not_approved'
    assert report['original_provider_response_independently_verified'] == 'not_established'
    assert report['historical_scope']['status'] == 'missing_authority'
    assert report['disposition'] == 'blocked_pending_governance'
    assert report == run()
    assert report['qualification_id'] == compat.VERSION + '__' + sha256_json({k:v for k,v in report.items() if k != 'qualification_id'})
    assert originals == {p:p.read_bytes() for p in originals}


def test_wrong_tar_hash(retained):
    _, _, package, _, run = retained
    with package.open('ab') as stream:
        stream.write(b'corruption')
    assert run()['technical_reconciliation'] == 'failed'


@pytest.mark.parametrize('defect', ['september', 'pin', 'snapshot_digest', 'snapshot_size', 'request', 'retrieval', 'product', 'validation'])
def test_identity_and_pin_substitution_rejected(retained, defect):
    _, artifact, _, repack, run = retained
    path = artifact / 'manifest.json'
    m = compat.read_json(path)
    pin = m['raw_source_lineage']['provider_pin']
    if defect == 'september':
        m['artifact_id'] = 'src__census_acs1__2024-12__r1__september'
    elif defect == 'pin':
        pin['cycle_id'] = 'September'
    elif defect == 'snapshot_digest':
        pin['members']['snapshot']['sha256'] = '0'*64
    elif defect == 'snapshot_size':
        pin['members']['snapshot']['size_bytes'] += 1
    elif defect == 'request':
        m['source_request_identity'] = '0'*64
    elif defect == 'retrieval':
        m['retrieved_at'] = '2026-09-25T00:00:00Z'
    elif defect == 'product':
        m['provider_release_id'] = 'acs/acs5:2023'
    else:
        dump(artifact / 'validation.json', {'status': 'failed'})
        m['validation_sha256'] = sha256_file(artifact / 'validation.json')
    dump(path, m)
    repack()
    assert run()['technical_reconciliation'] == 'failed'


@pytest.mark.parametrize('defect', ['duplicate', 'missing', 'extra_geo', 'extra_metric', 'nan', 'inf', 'sentinel', 'date', 'property', 'source'])
def test_actual_tar_cells_fail_closed(retained, defect):
    _, artifact, _, repack, run = retained
    path = artifact / 'data.parquet'
    frame = pd.read_parquet(path)
    if defect == 'duplicate':
        frame = pd.concat([frame, frame.iloc[:1]], ignore_index=True)
    elif defect == 'missing':
        frame = frame.iloc[1:]
    elif defect == 'extra_geo':
        frame.loc[0, 'geo_id'] = 'not_requested'
    elif defect == 'extra_metric':
        frame.loc[0, 'metric_id'] = 'not_governed'
    elif defect in {'nan', 'inf', 'sentinel'}:
        frame.loc[0, 'value'] = {'nan': float('nan'), 'inf': float('inf'), 'sentinel': -666666666}[defect]
    elif defect == 'date':
        frame['date'] = pd.to_datetime(frame.date)
        frame.loc[0, 'date'] = pd.Timestamp('2024-12-30')
    elif defect == 'property':
        frame.loc[0, 'property_type'] = 'single_family'
    else:
        frame.loc[0, 'source_id'] = 'other_source'
    frame.to_parquet(path, index=False)
    repack()
    assert run()['technical_reconciliation'] == 'failed'


@pytest.mark.parametrize('defect', ['overlap', 'incomplete', 'duplicate', 'excluded', 'request'])
def test_membership_partition_and_request_identity(source, defect):
    frame, pin, coverage = rows(source)
    plan, excluded = geography_plan()
    e = pin['members']['snapshot']['evidence']
    if defect == 'overlap':
        e['provider_ineligible'][0] = e['available_membership'][0]
    elif defect == 'incomplete':
        e['provider_ineligible'].pop()
    elif defect == 'duplicate':
        e['available_membership'][0] = e['available_membership'][1]
    elif defect == 'excluded':
        excluded = excluded[:-1]
    else:
        e['request_identity'] = '0'*64
    with pytest.raises(ValueError):
        compat.reconcile(frame, pin, coverage, plan, excluded, source)


@pytest.mark.parametrize('path', CONFIG_PATHS)
def test_configuration_incompatibility(retained, path):
    root, _, _, _, run = retained
    with (root / path).open('a') as stream:
        stream.write('\nconfiguration drift\n')
    assert run()['technical_reconciliation'] == 'failed'


def test_original_provenance_and_current_code_drift(retained):
    root, artifact, _, repack, run = retained
    m = compat.read_json(artifact / 'manifest.json')
    m['git_sha'] = 'unknown'
    dump(artifact / 'manifest.json', m)
    repack()
    assert run()['technical_reconciliation'] == 'failed'
    m['git_sha'] = compat.ORIGINAL_CODE
    dump(artifact / 'manifest.json', m)
    repack()
    path = root / 'sources/census_acs/artifact.py'
    path.write_text(path.read_text().replace('value in SENTINELS', 'value == 0'))
    assert run()['technical_reconciliation'] == 'failed'


def test_cli_cannot_claim_response_verification_or_approval(retained, monkeypatch, tmp_path):
    root, _, package, _, _ = retained
    for claim in ['--original-provider-response-independently-verified', '--compatibility-governance']:
        monkeypatch.setattr('sys.argv', ['compatibility', '--source-id', 'census_acs1', '--package', str(package),
                            '--output', str(tmp_path/'report'), claim, 'passed'])
        with pytest.raises(SystemExit) as exc:
            compat.main()
        assert exc.value.code == 2
    assert not (tmp_path/'report').exists()


def test_readiness_compatibility_lane_is_distinct(retained, source, monkeypatch):
    root, _, package, _, run = retained
    spec = {'schema_version': readiness.VERSION, 'ranges': {s:[2024,2024] for s in PRODUCTS},
            'packages': {compat.ELIGIBLE[source][0]: str(package)}, 'evidence': {}, 'selected_parents': {},
            'accepted_august': {}, 'monthly_compatibility': [source+':2024'],
            'catalog': str(root / 'config/artifact_catalog.json')}
    cat = compat.read_json(root / 'config/artifact_catalog.json')
    for s in PRODUCTS:
        rec = next(r for r in cat['immutable_records'] if r['object_id'] == compat.ELIGIBLE[s][0])
        spec['accepted_august'][s] = {'artifact_id':rec['object_id'], 'artifact_content_hash':rec['artifact_content_hash'],
                                     'package_sha256':rec['package_sha256']}
    monkeypatch.setattr(readiness, 'repository_inputs', lambda _: spec)
    report = readiness.evaluate(spec, root)
    cells = [c for c in report['coverage'] if c['source_id'] == source and c['availability'] != 'explicitly_excluded']
    assert Counter(c['availability'] for c in cells) == {
        'available_compatibility_reconciled': compat.ELIGIBLE[source][3],
        'monthly_no_content_attested': 2*compat.ELIGIBLE[source][2]}
    assert all(c['independent_provider_verification'] == 'not_established' for c in cells)
    assert report['monthly_compatibility_qualifications'][source+':2024'] == run()
    assert report['validation']['source_availability'] == 'blocked'
    assert report['validation']['historical_acceptance'] == 'blocked'
    assert not report['proposed_inventories'][source]['parents']
    del spec['monthly_compatibility']
    assert 'missing_independent_provider_evidence' in {b['code'] for b in readiness.evaluate(spec,root)['blockers']}


@pytest.mark.parametrize('lane', [['census_acs5:2023'], ['census_acs1:2020'],
                                 {'census_acs1:2024': {'original_provider_response_independently_verified':'passed'}},
                                 ['census_acs1:2024','census_acs1:2024']])
def test_readiness_rejects_widening_and_status_injection(lane):
    with pytest.raises(ValueError, match='compatibility lane'):
        readiness.evaluate({'schema_version': readiness.VERSION, 'ranges': dict.fromkeys(PRODUCTS, [2024,2024]),
                            'monthly_compatibility': lane}, ROOT)


def test_production_anchors_are_not_caller_configurable(tmp_path):
    source = 'census_acs1'
    fake = compat.qualify(root=ROOT, source_id=source, package=tmp_path/'absent.tar')
    assert fake['technical_reconciliation'] == 'failed'
    with pytest.raises(TypeError):
        compat.qualify(root=ROOT, source_id=source, package=tmp_path/'absent.tar', original_provider_response_independently_verified='passed')


@pytest.mark.parametrize('record', ['pin', 'result', 'family', 'catalog'])
def test_repository_evidence_tampering(retained, source, record):
    root, _, _, _, run = retained
    if record == 'pin':
        path = root / f'config/monthly_source_input_pins/{compat.CYCLE}/{source}.json'
        value = compat.read_json(path)
        value['members']['snapshot']['sha256'] = '0'*64
    elif record == 'result':
        path = root / f'config/monthly_source_cycle_results/{compat.CYCLE}/{source}.json'
        value = compat.read_json(path)
        value['result']['candidate_artifact_id'] = 'September'
    elif record == 'family':
        path = root / compat.FAMILY
        value = compat.read_json(path)
        next(p for p in value['parents'] if p['source_id'] == source)['artifact_id'] = 'September'
    else:
        path = root / 'config/artifact_catalog.json'
        value = compat.read_json(path)
        rec = next(r for r in value['immutable_records'] if r['object_id'] == compat.ELIGIBLE[source][0])
        rec['metadata']['data_sha256'] = '0'*64
    dump(path, value)
    assert run()['technical_reconciliation'] == 'failed'


def test_untrusted_governance_cannot_approve(retained, source, tmp_path):
    root, _, package, _, _ = retained
    path = tmp_path / 'claimed-approval.json'
    dump(path, {'compatibility_governance': 'approved',
                'original_provider_response_independently_verified': 'passed'})
    report = compat.qualify(root=root, source_id=source, package=package, governance=path)
    assert report['technical_reconciliation'] == 'failed'
    assert report['compatibility_governance'] == 'not_approved'
    assert report['original_provider_response_independently_verified'] == 'not_established'


def test_cli_writes_content_addressed_report_and_refuses_overwrite(retained, source, monkeypatch, tmp_path):
    root, _, package, _, _ = retained
    output = tmp_path / 'qualification'
    monkeypatch.setattr('sys.argv', ['compatibility', '--repository-root', str(root),
        '--source-id', source, '--package', str(package), '--output', str(output)])
    assert compat.main() == 2
    files = list(output.iterdir())
    assert len(files) == 1
    report = compat.read_json(files[0])
    assert files[0].name == report['qualification_id'] + '.json'
    with pytest.raises(SystemExit):
        compat.main()
