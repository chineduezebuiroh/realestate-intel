"""Annual provider fixtures exercise offline readiness, never activation."""
from copy import deepcopy
import json
from pathlib import Path

import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.package import build_publication_package
from jobs.monthly_refresh import acs_readiness as readiness
from sources.census_acs.artifact import PRODUCTS, canonicalize, geography_plan
from sources.census_acs.historical import omission_receipts

ROOT = Path('.')


def dump(path, value):
    path.write_text(json.dumps(value, sort_keys=True))
    return str(path)


@pytest.fixture
def annual(tmp_path, monkeypatch):
    full, _ = geography_plan()
    plan = [g for g in full if g['geo_id'] in {'district_of_columbia__state', 'amelia_county_va__county', 'alameda_county_ca__county'}]
    monkeypatch.setattr(readiness, 'geography_plan', lambda path: (plan, []))
    spec = {'schema_version': readiness.VERSION, 'ranges': {s: [2022, 2023] for s in PRODUCTS},
            'packages': {}, 'evidence': {}, 'accepted_august': {}, 'selected_parents': {}}
    records = []
    monkeypatch.setattr(readiness, 'repository_inputs', lambda root: {'ranges': {s: [2022, 2023] for s in PRODUCTS}, 'accepted_august': spec['accepted_august']})

    def add(source, year, *, omit=False, suffix=''):
        original = readiness.read_json(f'config/acs_backfill_reviews/provider-{source[7:]}-2023.json')['2023']
        snapshot = deepcopy(original['snapshot'])
        snapshot['vintage'] = year
        snapshot['excluded_geographies'] = []
        snapshot['members'] = [m for m in snapshot['members'] if m['geo_id'] in {g['geo_id'] for g in plan}]
        for member in snapshot['members']:
            member['request_url'] = member['request_url'].replace('/2023/', f'/{year}/')
            # The small county is unavailable in ACS1 2023; exercise changing
            # membership with actual returned state evidence for the other cell.
            if member['geo_id'] == 'district_of_columbia__state' and omit:
                member.update(status='provider_ineligible_no_content', payload=None)
        evidence = {'snapshot': snapshot, 'omission_evidence': omission_receipts(source_id=source, vintage=year, snapshot=snapshot)}
        spec['evidence'][f'{source}:{year}'] = dump(tmp_path / f'evidence-{source}-{year}{suffix}.json', evidence)
        frame, _ = canonicalize(source, year, snapshot)
        artifact = tmp_path / f'artifact-{source}-{year}{suffix}'
        manifest = create_artifact(artifact, frame, source_id=source, source_family=source,
            source_type='government_survey', provider='Census', distribution_channel='api',
            provider_release_id=f'{PRODUCTS[source]}:{year}', provider_release_timestamp_or_date=str(year),
            retrieved_at=None, target_month=f'{year}-12', source_request_identity=source+str(year)+suffix,
            source_urls_or_endpoint_identity=['fixture://acs'], artifact_created_at='fixed',
            source_contract_version='census_acs_physical_source_v1', config_hashes={'fixture': sha256_json(suffix)})
        package = tmp_path / f'{source}-{year}{suffix}.tar'
        built = build_publication_package(artifact, package)
        record = {'object_id': manifest['artifact_id'], 'object_type': 'source',
            'artifact_content_hash': manifest['artifact_content_hash'], 'package_sha256': built['package_sha256'],
            'publication_state': 'published_immutable_verified', 'metadata': {'source_id': source,
                'provider_release_id': f'{PRODUCTS[source]}:{year}', 'data_sha256': manifest['data_sha256']}}
        records.append(record)
        spec['packages'][record['object_id']] = str(package)
        spec['catalog'] = dump(tmp_path / 'catalog.json', {'immutable_records': records})
        return record
    for source in PRODUCTS:
        for year in (2022, 2023):
            add(source, year)
    return spec, records, add


def codes(report):
    return {b['code'] for b in report['blockers']}


def test_annual_eligibility_and_fallback(annual):
    spec, _, add = annual
    # DC is available in one vintage and specifically omitted in the next.
    add('census_acs1', 2023, omit=True, suffix='omitted')
    spec['selected_parents']['census_acs1:2023'] = _last_id(annual)
    result = readiness.evaluate(spec, ROOT)
    dc = [r for r in result['logical_potential'] if r['geo_id'] == 'district_of_columbia__state' and r['metric'] == 'pop_total']
    assert [(r['vintage'], r['proposed_source']) for r in dc] == [(2022, 'census_acs1'), (2023, 'census_acs5')]
    assert all(r['acs1_evidence_resolved'] for r in dc)
    assert result['validation']['historical_acceptance'] == 'blocked'


def _last_id(annual):
    return annual[1][-1]['object_id']


def test_missing_2023_and_2020_gap(annual):
    spec, records, _ = annual
    spec['ranges']['census_acs1'] = [2020, 2023]
    records[:] = [r for r in records if r['metadata']['provider_release_id'] != 'acs/acs5:2023']
    dump(Path(spec['catalog']), {'immutable_records': records})
    result = readiness.evaluate(spec, ROOT)
    assert {'missing_acs5_2023_acquisition', 'acs1_2020_whole_vintage_gap_requires_governance',
            'incomplete_historical_logical_inputs'} <= codes(result)
    gap = next(b for b in result['blockers'] if b['code'] == 'missing_acs5_2023_acquisition')
    assert gap['provider_evidence_present']


@pytest.mark.parametrize('duplicate', [True, False])
def test_duplicate_or_conflicting_packages_fail(annual, duplicate):
    spec, records, add = annual
    if duplicate:
        records.append(deepcopy(records[0]))
        dump(Path(spec['catalog']), {'immutable_records': records})
    else:
        add('census_acs1', 2022, suffix='conflict')
    result = readiness.evaluate(spec, ROOT)
    assert result['validation']['disposition'] == 'fail'
    assert 'ambiguous_same_vintage_parent' in codes(result)


def test_exact_august_parent_preservation(annual):
    spec, _, add = annual
    for source in PRODUCTS:
        rec = add(source, 2024)
        spec['ranges'][source] = [2022, 2024]
        spec['accepted_august'][source] = {'artifact_id': rec['object_id'],
            'artifact_content_hash': rec['artifact_content_hash'], 'package_sha256': rec['package_sha256']}
    add('census_acs1', 2024, suffix='other')
    result = readiness.evaluate(spec, ROOT)
    assert result['proposed_inventories']['census_acs1']['parents'][-1]['artifact_id'] == spec['accepted_august']['census_acs1']['artifact_id'], result['blockers']
    spec['selected_parents']['census_acs1:2024'] = _last_id(annual)
    assert 'august_parent_replacement' in codes(readiness.evaluate(spec, ROOT))


def test_missing_or_ambiguous_evidence_fails_closed(annual):
    spec, _, _ = annual
    path = Path(spec['evidence']['census_acs1:2022'])
    evidence = readiness.read_json(path)
    evidence['omission_evidence'] = []
    dump(path, evidence)
    result = readiness.evaluate(spec, ROOT)
    assert 'invalid_provider_accounting_or_omissions' in codes(result)
    del spec['evidence']['census_acs5:2022']
    assert 'missing_independent_provider_evidence' in codes(readiness.evaluate(spec, ROOT))
    path.write_text('{"identity":1,"identity":2}')
    with pytest.raises(ValueError, match='duplicate JSON'):
        readiness.read_json(path)


def test_package_corruption_is_failure(annual):
    spec, records, _ = annual
    Path(spec['packages'][records[0]['object_id']]).write_bytes(b'corrupt')
    assert 'invalid_acquisition_identity' in codes(readiness.evaluate(spec, ROOT))


def test_annual_continuity_dc_and_contraction(annual):
    spec, _, _ = annual
    spec['baseline'] = 'tests/fixtures/acs_readiness/annual-baseline.csv'
    result = readiness.evaluate(spec, ROOT)
    assert all(r['dc_regression'] for r in result['continuity'])
    assert all(len(r['comparison_dates']) == 2 for r in result['continuity'] if r['series'] in {'income', 'population'})
    assert 'continuity_evidence_missing' in codes(result)
    spec['ranges'] = {s: [2023, 2023] for s in PRODUCTS}
    assert 'historical_contraction' in codes(readiness.evaluate(spec, ROOT))


def test_deterministic_reports(annual, tmp_path):
    spec, _, _ = annual
    first = readiness.evaluate(spec, ROOT)
    assert first == readiness.evaluate(spec, ROOT)
    for name in ('a', 'b'):
        readiness.write_reports(first, tmp_path / name)
    assert {p.name: p.read_bytes() for p in (tmp_path/'a').iterdir()} == {p.name: p.read_bytes() for p in (tmp_path/'b').iterdir()}


def test_repository_accounting():
    spec = readiness.repository_inputs(ROOT)
    plan, excluded = geography_plan()
    assert len(plan) == 223 and len(excluded) == 10
    assert spec['ranges'] == {'census_acs1': [2005, 2024], 'census_acs5': [2009, 2024]}
    assert 'census_acs5:2023' in spec['evidence']


def test_single_vintage_cannot_claim_complete_history(annual):
    spec, _, _ = annual
    spec['ranges'] = {source: [2023, 2023] for source in PRODUCTS}
    result = readiness.evaluate(spec, ROOT)
    assert 'incomplete_configured_historical_range' in codes(result)
    assert result['validation']['disposition'] == 'blocked'


def test_invented_geographic_omission_fails(annual):
    spec, _, _ = annual
    path = Path(spec['evidence']['census_acs5:2023'])
    evidence = readiness.read_json(path)
    evidence['omission_evidence'].append({'evidence_id': 'invented'})
    dump(path, evidence)
    assert 'invalid_provider_accounting_or_omissions' in codes(readiness.evaluate(spec, ROOT))


def test_bad_governance_is_never_authority(annual, tmp_path):
    spec, _, _ = annual
    spec['governance'] = dump(tmp_path / 'governance.json', {'namespace': 'production'})
    result = readiness.evaluate(spec, ROOT)
    assert 'invalid_governance_records' in codes(result)
    assert result['validation']['historical_acceptance'] == 'blocked'


def test_missing_input_cli_emits_fail_report(tmp_path, monkeypatch):
    output = tmp_path / 'reports'
    monkeypatch.setattr('sys.argv', ['acs_readiness', '--inputs', str(tmp_path/'absent.json'), '--output', str(output)])
    assert readiness.main() == 1
    assert readiness.read_json(output/'validation.json')['validation']['disposition'] == 'fail'


def test_read_only_input_bytes(annual):
    spec, _, _ = annual
    paths = [Path(spec['catalog']), *map(Path, spec['packages'].values()), *map(Path, spec['evidence'].values())]
    before = {str(p): p.read_bytes() for p in paths}
    readiness.evaluate(spec, ROOT)
    assert before == {str(p): p.read_bytes() for p in paths}


def test_cataloged_but_unsupplied_package_is_unresolved(annual):
    spec, records, _ = annual
    del spec['packages'][records[0]['object_id']]
    result = readiness.evaluate(spec, ROOT)
    assert 'local_immutable_package_not_supplied' in codes(result)
    assert any(r['availability'] == 'unresolved_evidence' for r in result['coverage'])


def test_verified_availability_is_separate_from_acceptance(annual, monkeypatch):
    import socket
    monkeypatch.setattr(socket.socket, 'connect', lambda *a: pytest.fail('offline validator attempted network'))
    spec, _, _ = annual
    result = readiness.evaluate(spec, ROOT)
    assert result['validation']['source_availability'] == 'pass'
    assert result['validation']['historical_acceptance'] == 'blocked'
    assert result['validation']['downstream_activation'] == 'blocked'


def test_operator_cannot_replace_accepted_identity(annual, monkeypatch):
    spec, _, _ = annual
    authoritative = {'census_acs1': {'artifact_id': 'accepted', 'artifact_content_hash': 'a'*64, 'package_sha256': 'b'*64}}
    monkeypatch.setattr(readiness, 'repository_inputs', lambda root: {'ranges': deepcopy(spec['ranges']), 'accepted_august': authoritative})
    spec['accepted_august'] = {'census_acs1': {'artifact_id': 'different'}}
    assert 'accepted_august_identity_differs_from_repository' in codes(readiness.evaluate(spec, ROOT))


def test_invalid_tar_with_matching_catalog_digest_fails(annual):
    from core.source_artifacts.hashing import sha256_file
    spec, records, _ = annual
    package = Path(spec['packages'][records[0]['object_id']])
    package.write_bytes(b'invalid tar bytes')
    records[0]['package_sha256'] = sha256_file(package)
    dump(Path(spec['catalog']), {'immutable_records': records})
    assert 'invalid_acquisition_identity' in codes(readiness.evaluate(spec, ROOT))
