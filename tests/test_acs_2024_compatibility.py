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
              f'config/monthly_source_cycle_results/{compat.CYCLE}/{source}.json',
              f'config/monthly_source_input_pins/{compat.PUBLICATION_CYCLE}/{source}.json',
              f'config/monthly_source_cycle_results/{compat.PUBLICATION_CYCLE}/{source}.json']
    for path in copied:
        target = root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / path, target)
    frame, _, coverage = rows(source)
    pin = compat.read_json(root / f'config/monthly_source_input_pins/{compat.PUBLICATION_CYCLE}/{source}.json')
    artifact = tmp_path / 'fixture-artifact'
    original_configs = {p: compat.digest(compat.git_bytes(ROOT, compat.ORIGINAL_CODE, p)) for p in CONFIG_PATHS}
    m = create_artifact(artifact, frame, source_id=source, source_family=source, source_type='government_survey',
                        provider='Census', distribution_channel='census_data_api',
                        provider_release_id=PRODUCTS[source] + ':2024', provider_release_timestamp_or_date='2024',
                        retrieved_at=pin['members']['snapshot']['retrieved_at'], target_month='2024-12',
                        source_request_identity=pin['members']['snapshot']['evidence']['request_identity'],
                        source_urls_or_endpoint_identity=[pin['members']['snapshot']['url']],
                        raw_source_lineage={'schema_version': 'census_acs_candidate_evidence_v1',
                            'cycle_id': compat.PUBLICATION_CYCLE, 'physical_source_id': source, 'provider_pin': pin, 'coverage': coverage},
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
        publication_result_path = f'config/monthly_source_cycle_results/{compat.PUBLICATION_CYCLE}/{source}.json'
        publication_result = compat.read_json(root / publication_result_path)
        publication_result['result']['package_sha256'] = rec['package_sha256']
        dump(root / publication_result_path, publication_result)
        family = compat.read_json(root / compat.FAMILY)
        parent = next(p for p in family['parents'] if p['source_id'] == source)
        parent.update(data_sha256=manifest['data_sha256'], package_sha256=rec['package_sha256'])
        dump(root / compat.FAMILY, family)
        for path in ['config/artifact_catalog.json', result_path, publication_result_path, compat.FAMILY]:
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
    provenance = report['provenance']
    assert provenance['original_publication']['cycle_id'] == compat.PUBLICATION_CYCLE
    assert provenance['august_acceptance']['cycle_id'] == compat.CYCLE
    july = provenance['original_publication']['pin']
    august = provenance['august_acceptance']['pin']
    assert july != august and july['pin_id'] != august['pin_id']
    assert july['members']['snapshot']['retrieved_at'] != august['members']['snapshot']['retrieved_at']
    assert report['original_pin'] == july
    assert provenance['august_acceptance']['same_snapshot_commitments_and_evidence']
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


@pytest.mark.parametrize('cycle', [compat.PUBLICATION_CYCLE, compat.CYCLE])
@pytest.mark.parametrize('field', ['candidate_artifact_id', 'artifact_content_hash', 'package_sha256',
                                   'source_id', 'cycle_id', 'provider_release_id'])
def test_both_cycle_result_bindings_are_exact(retained, source, monkeypatch, cycle, field):
    root, _, _, _, run = retained
    path = f'config/monthly_source_cycle_results/{cycle}/{source}.json'
    value = compat.read_json(root / path)
    value['result'][field] = 'changed'
    dump(root / path, value)
    # Isolate result-binding checks behind a test-local matching repository anchor.
    previous = compat.git_bytes
    monkeypatch.setattr(compat, 'git_bytes', lambda r, rev, p:
        (root / path).read_bytes() if rev == compat.BASE and p == path else previous(r, rev, p))
    report = run()
    assert report['technical_reconciliation'] == 'failed'
    assert report['failure']['predicate'] == ('august_result_binding' if cycle == compat.CYCLE else 'july_publication_result_binding')


@pytest.mark.parametrize('field', ['sha256', 'size_bytes', 'url', 'request_identity',
                                   'available_membership', 'provider_ineligible', 'excluded_geographies'])
def test_august_must_retain_all_original_snapshot_and_geographic_evidence(retained, source, monkeypatch, field):
    from jobs.monthly_refresh.source_inputs import provider_pin
    root, _, _, _, run = retained
    path = f'config/monthly_source_input_pins/{compat.CYCLE}/{source}.json'
    value = compat.read_json(root / path)
    snapshot = value['members']['snapshot']
    if field in {'sha256', 'url'}:
        snapshot[field] = '0'*64 if field == 'sha256' else 'https://example.invalid/changed'
    elif field == 'size_bytes':
        snapshot[field] += 1
    elif field == 'request_identity':
        snapshot['evidence'][field] = '0'*64
    else:
        snapshot['evidence'][field] = snapshot['evidence'][field][1:]
    value = provider_pin(cycle_id=compat.CYCLE, source_id=source,
                         provider_release_id=value['provider_release_id'], members=value['members'])
    dump(root / path, value)
    previous = compat.git_bytes
    monkeypatch.setattr(compat, 'git_bytes', lambda r, rev, p:
        (root / path).read_bytes() if rev == compat.BASE and p == path else previous(r, rev, p))
    report = run()
    assert report['technical_reconciliation'] == 'failed'
    assert report['failure']['predicate'] == 'exact_august_snapshot_and_geographic_evidence_binding'


def test_rebinding_original_manifest_to_august_is_rejected(retained, source):
    root, artifact, _, repack, run = retained
    pin = compat.read_json(root / f'config/monthly_source_input_pins/{compat.CYCLE}/{source}.json')
    path = artifact / 'manifest.json'
    manifest = compat.read_json(path)
    manifest['raw_source_lineage'].update(provider_pin=pin, cycle_id=compat.CYCLE)
    manifest['retrieved_at'] = pin['members']['snapshot']['retrieved_at']
    dump(path, manifest)
    repack()
    report = run()
    assert report['technical_reconciliation'] == 'failed'
    assert report['failure']['predicate'] == 'exact_original_pin_binding'


def original_production_code_inputs(source):
    result = compat.read_json(ROOT / f'config/monthly_source_cycle_results/{compat.PUBLICATION_CYCLE}/{source}.json')['result']
    manifest = {'git_sha': 'unknown', 'source_id': source, 'artifact_id': result['candidate_artifact_id'],
                'artifact_content_hash': result['artifact_content_hash']}
    return manifest, result


@pytest.fixture
def original_code_objects(monkeypatch):
    # Exact original Git blob captured from the execution commit. This policy
    # differs from later main history, so offline tests must not require an
    # operator's extra Git fetch. Production still requires local Git objects.
    original = compat.subprocess.check_output
    def output(args, **kwargs):
        if args[-3:] == ['cat-file', 'blob', '17a682cc0ee4889d812a0757706893a11038823a']:
            return (ROOT / 'tests/fixtures/acs_2024_original_code/monthly_refresh_policy.json').read_bytes()
        return original(args, **kwargs)
    monkeypatch.setattr(compat.subprocess, 'check_output', output)


def test_independent_original_workflow_records_exact_execution_commit(source, original_code_objects):
    manifest, result = original_production_code_inputs(source)
    commit, basis, files = compat.original_code_provenance(ROOT, manifest, result)
    assert commit == '9131dc8ad2626f393e7975411693908d43445173'
    assert manifest['git_sha'] == 'unknown'
    assert basis['manifest_code_commit_recorded'] is False
    assert basis['basis'] == 'independent_original_workflow_checkout_and_publication'
    assert basis['execution_commit_relationship_to_baseline']['status'] == 'diverged'
    assert set(files) == {*CONFIG_PATHS, *compat.CODE, 'jobs/monthly_refresh/acs_hosted.py'}
    assert 'git_sha: str = "unknown"' in files['jobs/monthly_refresh/acs_monthly.py'].decode()


@pytest.mark.parametrize('field', ['candidate_artifact_id', 'artifact_content_hash', 'package_sha256', 'cycle_id', 'source_id'])
def test_workflow_code_cannot_authorize_another_publication(source, field):
    manifest, result = original_production_code_inputs(source)
    result[field] = 'changed'
    with pytest.raises(ValueError, match='checkout/publication binding'):
        compat.original_code_provenance(ROOT, manifest, result)


@pytest.mark.parametrize('recorded', [None, '', 'missing', 'UNKNOWN'])
def test_no_general_missing_code_fallback(source, recorded):
    manifest, result = original_production_code_inputs(source)
    manifest['git_sha'] = recorded
    with pytest.raises(ValueError, match='known monthly omission'):
        compat.original_code_provenance(ROOT, manifest, result)


def test_modified_or_missing_workflow_evidence_fails_closed(source, tmp_path):
    manifest, result = original_production_code_inputs(source)
    with pytest.raises(FileNotFoundError):
        compat.original_code_provenance(tmp_path, manifest, result)
    path = tmp_path / compat.CODE_EVIDENCE_PATH
    path.parent.mkdir(parents=True)
    path.write_bytes((ROOT / compat.CODE_EVIDENCE_PATH).read_bytes() + b'\n')
    with pytest.raises(ValueError, match='evidence identity mismatch'):
        compat.original_code_provenance(tmp_path, manifest, result)


@pytest.fixture
def independently_recorded_code(retained, source, monkeypatch, original_code_objects):
    """Test-local workflow result binding for canonical-row fixture TARs only.

    Production captures are unchanged; no provider snapshots/receipts or approval
    records are created. The test explicitly replaces the evidence-file digest.
    """
    root, artifact, package, repack, run = retained
    _, basis, files = compat.original_code_provenance(ROOT, *original_production_code_inputs(source))
    path = artifact / 'manifest.json'
    manifest = compat.read_json(path)
    manifest['git_sha'] = 'unknown'
    manifest['config_hashes'] = {p: compat.digest(files[p]) for p in CONFIG_PATHS}
    dump(path, manifest)
    repack()
    evidence = compat.read_json(ROOT / compat.CODE_EVIDENCE_PATH)
    result = compat.read_json(root / f'config/monthly_source_cycle_results/{compat.PUBLICATION_CYCLE}/{source}.json')['result']
    evidence['jobs'][source]['result'] = result
    line = evidence['jobs'][source]['result_log_line']
    evidence['jobs'][source]['result_log_line'] = line.split(' ', 1)[0] + ' ' + json.dumps(result)
    evidence_path = root / compat.CODE_EVIDENCE_PATH
    dump(evidence_path, evidence)
    monkeypatch.setattr(compat, 'CODE_EVIDENCE_SHA256', sha256_file(evidence_path))
    return retained


def test_workflow_code_evidence_preserves_all_downstream_qualification(independently_recorded_code):
    root, artifact, _, _, run = independently_recorded_code
    before = (artifact / 'manifest.json').read_bytes()
    report = run()
    assert report['technical_reconciliation'] == 'passed', report
    assert report['code_provenance']['manifest_git_sha'] == 'unknown'
    assert report['code_provenance']['manifest_code_commit_recorded'] is False
    assert report['code_provenance']['original_commit'] == '9131dc8ad2626f393e7975411693908d43445173'
    assert report['compatibility_governance'] == 'not_approved'
    assert report['original_provider_response_independently_verified'] == 'not_established'
    assert (artifact / 'manifest.json').read_bytes() == before
    assert report['reconciliation']['accounted_cells'] == 446


def test_workflow_code_evidence_does_not_bypass_manifest_configuration(independently_recorded_code):
    _, artifact, _, repack, run = independently_recorded_code
    path = artifact / 'manifest.json'
    manifest = compat.read_json(path)
    manifest['config_hashes'][CONFIG_PATHS[0]] = '0'*64
    dump(path, manifest)
    repack()
    # Keep fixture workflow/package result binding current, so the exact config
    # predicate is exercised rather than an earlier package binding failure.
    root = independently_recorded_code[0]
    evidence_path = root / compat.CODE_EVIDENCE_PATH
    evidence = compat.read_json(evidence_path)
    source = manifest['source_id']
    result = compat.read_json(root / f'config/monthly_source_cycle_results/{compat.PUBLICATION_CYCLE}/{source}.json')['result']
    evidence['jobs'][source]['result'] = result
    evidence['jobs'][source]['result_log_line'] = evidence['jobs'][source]['result_log_line'].split(' ', 1)[0] + ' ' + json.dumps(result)
    dump(evidence_path, evidence)
    from unittest.mock import patch
    with patch.object(compat, 'CODE_EVIDENCE_SHA256', sha256_file(evidence_path)):
        report = run()
    assert report['technical_reconciliation'] == 'failed'
    assert report['failure']['predicate'] == 'original_configuration_provenance'


def test_original_workflow_requires_exact_git_blob_bytes(source, monkeypatch):
    original = compat.subprocess.check_output
    def output(args, **kwargs):
        if args[-3:] == ['cat-file', 'blob', '17a682cc0ee4889d812a0757706893a11038823a']:
            return b'changed original configuration'
        return original(args, **kwargs)
    monkeypatch.setattr(compat.subprocess, 'check_output', output)
    with pytest.raises(ValueError, match='Git blob bytes differ'):
        compat.original_code_provenance(ROOT, *original_production_code_inputs(source))


def test_authorized_readiness_preserves_evidence_classification(retained, source, monkeypatch):
    """Consumer integration only; authority verification is tested independently."""
    root, _, package, _, run = retained
    qualification = run()
    assert qualification['technical_reconciliation'] == 'passed'
    spec = {'schema_version': readiness.VERSION, 'ranges': {s: [2024, 2024] for s in PRODUCTS},
        'packages': {compat.ELIGIBLE[s][0]: str(package) for s in PRODUCTS}, 'evidence': {},
        'selected_parents': {}, 'accepted_august': {}, 'monthly_compatibility': [s + ':2024' for s in PRODUCTS],
        'catalog': str(root / 'config/artifact_catalog.json'),
        'compatibility_authorization': {'authorization_id': 'offline-consumer-integration', 'reports': {}}}
    cat = compat.read_json(root / 'config/artifact_catalog.json')
    for s in PRODUCTS:
        rec = next(r for r in cat['immutable_records'] if r['object_id'] == compat.ELIGIBLE[s][0])
        spec['accepted_august'][s] = {'artifact_id': rec['object_id'], 'artifact_content_hash': rec['artifact_content_hash'],
                                     'package_sha256': rec['package_sha256']}
    monkeypatch.setattr(readiness, 'repository_inputs', lambda _: spec)
    monkeypatch.setattr(readiness, 'verify_compatibility', lambda **kw: {'qualifications': {source: qualification}})
    class EndFence:
        def current_commit(self, repository): pass
    report = readiness.evaluate(spec, root, production_governance=EndFence())
    cells = [r for r in report['coverage'] if r['source_id'] == source and r['availability'] != 'explicitly_excluded']
    assert Counter(r['availability'] for r in cells) == {
        'available_compatibility_authorized': compat.ELIGIBLE[source][3],
        'monthly_no_content_authorized': compat.ELIGIBLE[source][2] * 2}
    assert all(r['independent_provider_verification'] == 'not_established' and
               r['original_provider_response_independently_verified'] == 'not_established' for r in cells)
    assert all(r['compatibility_governance'] == 'approved_bounded_readiness' for r in cells)
    assert report['validation']['historical_acceptance'] == 'blocked'
    assert report['validation']['downstream_activation'] == 'blocked'
    assert len(report['proposed_inventories'][source]['parents']) == 1
    assert all(r['proposed_source'] == source for r in report['logical_potential'])
    assert any(b['code'] == 'incomplete_historical_logical_inputs' for b in report['blockers'])
