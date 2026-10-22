"""Discovery reads explicit local roots only; retrieval scripts never run in tests."""
from copy import deepcopy
import json
from pathlib import Path
import socket

import pytest

from test_acs_readiness import annual, dump
from jobs.monthly_refresh import acs_readiness, acs_readiness_discover as discovery
from sources.census_acs.artifact import PRODUCTS


@pytest.fixture
def local(annual, tmp_path, monkeypatch):
    spec, records, add = annual
    def prepare():
        for index, rec in enumerate(records):
            source = rec['metadata']['source_id']
            rec.update(logical_artifact_uri=f"artifact://source/{source}/{rec['object_id']}",
                remote_repository='owner/repository', release_id=100+index, asset_id=1000+index,
                release_tag='source-artifact/'+source+'/'+rec['object_id'],
                asset_filename=rec['object_id']+'.tar', publication_receipt_id='publication_receipt__fixture')
            rec['metadata']['observation_max'] = rec['metadata']['provider_release_id'].rsplit(':',1)[1]+'-12-31'
        dump(Path(spec['catalog']), {'immutable_records': records})
    prepare()
    monkeypatch.setattr(discovery, 'repository_inputs', lambda root: deepcopy(spec))
    monkeypatch.setattr(discovery, 'geography_plan', acs_readiness.geography_plan)
    monkeypatch.setattr(socket.socket, 'connect', lambda *args: pytest.fail('network access during discovery'))
    return spec, records, add, prepare


def run(local, tmp_path, name='discovery'):
    report = discovery.discover(Path('.'), [tmp_path], tmp_path/name)
    return report, acs_readiness.read_json(tmp_path/name/'inputs.json')


def test_discovers_all_exact_packages_without_mappings(local, tmp_path):
    spec, records, _, _ = local
    before = {p: Path(p).read_bytes() for p in spec['packages'].values()}
    report, inputs = run(local, tmp_path)
    assert len(inputs['packages']) == len(records) == 4
    assert all(Path(path).is_absolute() for path in inputs['packages'].values())
    assert all(p['status'] == 'package_verified' for p in report['packages'])
    assert len(inputs['evidence']) == 4
    assert before == {p: Path(p).read_bytes() for p in before}
    result = acs_readiness.evaluate(inputs, Path('.'))
    assert result['validation']['source_availability'] == 'pass'
    assert result['validation']['historical_acceptance'] == 'blocked'


def test_missing_package_exact_authenticated_retrieval(local, tmp_path):
    spec, records, _, _ = local
    first = records[0]
    Path(spec['packages'][first['object_id']]).unlink()
    report, inputs = run(local, tmp_path)
    assert first['object_id'] not in inputs['packages']
    assert report['packages'][0]['status'] == 'missing_local_package'
    script = (tmp_path/'discovery/retrieve-missing.sh').read_text()
    assert 'repos/owner/repository/releases/assets/1000' in script
    assert 'shasum -a 256 -c -' in script and 'set -C' in script
    assert 'api.census.gov' not in script
    assert not (tmp_path/'discovery/retrieved-packages').exists()


def test_hash_matching_not_filename_matching(local, tmp_path):
    spec, records, _, _ = local
    path = Path(spec['packages'][records[0]['object_id']])
    path.rename(tmp_path/'renamed-original.tar')
    report, inputs = run(local, tmp_path)
    assert inputs['packages'][records[0]['object_id']] == str(tmp_path/'renamed-original.tar')
    assert report['packages'][0]['status'] == 'package_verified'


def test_corrupt_filename_match_and_extracted_cache_are_not_verified(local, tmp_path):
    spec, records, _, _ = local
    rec = records[0]
    path = Path(spec['packages'][rec['object_id']])
    path.write_bytes(b'wrong package')
    report, inputs = run(local, tmp_path)
    row = report['packages'][0]
    assert rec['object_id'] not in inputs['packages']
    assert row['status'] == 'missing_local_package'
    assert row['extracted_caches_not_original_packages']


def test_duplicate_vintage_does_not_choose_arbitrary_parent(local, tmp_path):
    spec, _, add, prepare = local
    add('census_acs1', 2022, suffix='another')
    prepare()
    report, inputs = run(local, tmp_path)
    assert any(n['code'] == 'duplicate_same_vintage_identities' for n in report['unresolved'])
    assert 'census_acs1:2022' not in inputs['selected_parents']


def test_exact_august_parent_wins_over_another_2024_parent(local, tmp_path):
    spec, records, add, prepare = local
    for source in PRODUCTS:
        rec = add(source, 2024)
        spec['ranges'][source] = [2022, 2024]
        spec['accepted_august'][source] = {'artifact_id': rec['object_id'],
            'artifact_content_hash': rec['artifact_content_hash'], 'package_sha256': rec['package_sha256']}
    add('census_acs1', 2024, suffix='different')
    prepare()
    report, inputs = run(local, tmp_path)
    assert inputs['selected_parents']['census_acs1:2024'] == spec['accepted_august']['census_acs1']['artifact_id']
    assert any(n['code']=='duplicate_same_vintage_identities' for n in report['unresolved'])


def test_snapshots_require_existing_exact_receipts(local, tmp_path):
    spec, _, _, _ = local
    key = 'census_acs1:2023'
    value = acs_readiness.read_json(spec['evidence'][key])
    Path(spec['evidence'][key]).unlink()
    del spec['evidence'][key]
    dump(tmp_path/'snapshot.json', value['snapshot'])
    report, inputs = run(local, tmp_path)
    assert key not in inputs['evidence']
    assert any(n['code']=='retained_snapshot_missing_exact_omission_receipts' for n in report['unresolved'])
    dump(tmp_path/'omissions.json', value['omission_evidence'])
    report, inputs = run(local, tmp_path, 'with-receipts')
    # Exclude prior discovery outputs in real usage by choosing narrow roots.
    assert key in inputs['evidence']
    assert acs_readiness.read_json(inputs['evidence'][key])['omission_evidence'] == value['omission_evidence']


def test_conflicting_provider_evidence_is_unresolved(local, tmp_path):
    spec, _, _, _ = local
    value = acs_readiness.read_json(spec['evidence']['census_acs5:2023'])
    m = next(m for m in value['snapshot']['members'] if m['status']=='available')
    m['payload'][1][m['payload'][0].index('B01003_001E')] = '98765'
    dump(tmp_path/'conflicting-evidence.json', value)
    report, inputs = run(local, tmp_path)
    assert 'census_acs5:2023' not in inputs['evidence']
    assert any(n['code']=='conflicting_provider_evidence' for n in report['unresolved'])


def test_baseline_convention_and_ambiguity(local, tmp_path):
    fixture = Path('tests/fixtures/acs_readiness/annual-baseline.csv')
    (tmp_path/'macro-baseline.csv').write_bytes(fixture.read_bytes())
    _, inputs = run(local, tmp_path)
    assert inputs['baseline'] == str(tmp_path/'macro-baseline.csv')
    (tmp_path/'other').mkdir()
    (tmp_path/'other/macro-baseline.csv').write_bytes(fixture.read_bytes())
    report, inputs = run(local, tmp_path, 'ambiguous')
    assert inputs['baseline'] is None
    assert any(n['code']=='unresolved_baseline' for n in report['unresolved'])


def test_bounds_symlinks_and_external_outputs(local, tmp_path, monkeypatch):
    monkeypatch.setattr(discovery, 'MAX_FILES', 1)
    with pytest.raises(ValueError, match='file limit'):
        run(local, tmp_path)
    assert not (tmp_path/'discovery').exists()
    with pytest.raises(ValueError, match='outside the repository'):
        discovery.local_files([Path('.')], Path('.').resolve(),tmp_path/'outside')


def test_repeatable_discovery(local, tmp_path):
    report, inputs = run(local, tmp_path)
    first = {p.relative_to(tmp_path/'discovery'):p.read_bytes() for p in (tmp_path/'discovery').rglob('*') if p.is_file()}
    # Re-run at the same output path with identical source roots.
    import shutil
    shutil.rmtree(tmp_path/'discovery')
    report2, inputs2 = run(local, tmp_path)
    assert report == report2 and inputs == inputs2
    assert first == {p.relative_to(tmp_path/'discovery'):p.read_bytes() for p in (tmp_path/'discovery').rglob('*') if p.is_file()}


def test_retained_pin_requires_original_snapshot_bytes(local, tmp_path):
    from jobs.monthly_refresh.acs_monthly import snapshot_to_pin
    from core.source_artifacts.package import build_publication_package
    spec, records, _, prepare = local
    rec = records[0]
    snapshot = acs_readiness.read_json(spec['evidence']['census_acs1:2022'])['snapshot']
    snapshot_path = tmp_path/'snapshot.json'
    dump(snapshot_path, snapshot)
    pin, _ = snapshot_to_pin(cycle_id='fixture-2022', source_id='census_acs1', vintage=2022,
        snapshot=snapshot, path=snapshot_path, retrieved_at='2026-10-22T00:00:00Z')
    dump(tmp_path/'retained-pin.json', pin)
    artifact = tmp_path/'artifact-census_acs1-2022'
    manifest = acs_readiness.read_json(artifact/'manifest.json')
    manifest['raw_source_lineage'] = {'provider_pin': pin}
    dump(artifact/'manifest.json', manifest)
    built = build_publication_package(artifact, Path(spec['packages'][rec['object_id']]))
    rec['package_sha256'] = built['package_sha256']
    prepare()
    report, inputs = run(local, tmp_path)
    assert len(inputs['pins']) == 1
    assert inputs['pins'][0]['members']['snapshot'] == str(snapshot_path)
    assert not any(b['code']=='invalid_source_pin_evidence' for b in acs_readiness.evaluate(inputs,Path('.'))['blockers'])
    snapshot_path.unlink()
    report, inputs = run(local,tmp_path,'without-snapshot')
    assert not inputs['pins']
    assert any(n['code']=='pin_snapshot_bytes_missing' for n in report['unresolved'])


def test_invalid_baseline_schema_is_not_silently_selected(local, tmp_path):
    (tmp_path/'macro-baseline.csv').write_text('geo_id,score\ndc,1\n')
    report, inputs = run(local,tmp_path)
    assert inputs['baseline'] is None
    assert any(n['code']=='export_schema_unresolved' for n in report['unresolved'])


def test_symlink_and_scan_byte_limits(local, tmp_path, monkeypatch):
    spec, records, _, _ = local
    target = Path(spec['packages'][records[0]['object_id']])
    linked = tmp_path/'only-links'
    linked.mkdir()
    (linked/'package.tar').symlink_to(target)
    assert discovery.local_files([linked],Path('.').resolve(),tmp_path/'result') == []
    monkeypatch.setattr(discovery,'MAX_TOTAL',1)
    with pytest.raises(ValueError,match='byte limit'):
        run(local,tmp_path)
    assert not (tmp_path/'discovery').exists()


def test_repository_discovery_never_retrieves(tmp_path, monkeypatch):
    monkeypatch.setattr(socket.socket,'connect',lambda *args: pytest.fail('network access'))
    report = discovery.discover(Path('.'),[],tmp_path/'repository')
    inputs = acs_readiness.read_json(tmp_path/'repository/inputs.json')
    assert report['files_scanned'] == 0
    assert len(report['packages']) == 34
    assert not inputs['packages']
    assert len(inputs['evidence']) == 33
    assert inputs['selected_parents']['census_acs1:2024'] == inputs['accepted_august']['census_acs1']['artifact_id']
    assert inputs['selected_parents']['census_acs5:2024'] == inputs['accepted_august']['census_acs5']['artifact_id']
    assert not (tmp_path/'repository/retrieved-packages').exists()
