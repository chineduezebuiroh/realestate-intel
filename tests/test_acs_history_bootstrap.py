"""Offline 35-parent fixtures never establish real production verification."""
from copy import deepcopy
import json
from pathlib import Path
import shutil
from types import SimpleNamespace

import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file, write_canonical_json
from core.source_artifacts.validation import ArtifactValidationError
from core.source_artifacts.package import build_publication_package
from jobs.monthly_refresh import acs_history_bootstrap as boot
from jobs.monthly_refresh.acs_monthly import snapshot_to_pin
from jobs.monthly_refresh.acs_scope import ACSGovernance, identify, GAP_VERSION
from sources.census_acs.artifact import canonicalize
from test_acs_historical_materialization import registry, scope

ROOT = Path('.').resolve()


@pytest.fixture(scope='module')
def baseline(tmp_path_factory):
    directory = tmp_path_factory.mktemp('bootstrap-fixture')
    reg = registry()
    actual = json.loads((ROOT/'config/acs_historical_governance.json').read_text())
    gap = deepcopy(next(iter(actual['gap_evidence'].values())))
    gap.update(namespace='test_only', approval_provenance={'review':'offline fixture'})
    gap = identify(GAP_VERSION, gap, 'evidence_id'); reg['gap_evidence'][gap['evidence_id']] = gap
    scope(reg,'census_acs1',start=2005,end=2024,gaps=[{'vintage':2020,'evidence_id':gap['evidence_id']}])
    scope(reg,'census_acs5',start=2009,end=2024)
    records, parents, packages, evidence, eligible = [], [], {}, {}, {}
    for source,years in boot.YEARS.items():
        for year in years:
            # 2024 uses a separate fixture acquisition; the real compatibility
            # consumer is tested by its adversarial suite, never bypassed in CLI.
            evidence_year = 2023 if year == 2024 else year
            path = ROOT/f'config/acs_backfill_reviews/provider-{source[7:]}-{evidence_year}.json'
            value = json.loads(path.read_text())[str(evidence_year)]
            frame,_ = canonicalize(source,evidence_year,value['snapshot'])
            if year == 2024: frame['date'] = __import__('pandas').Timestamp('2024-12-31')
            snapshot = directory/f'{source}-{year}.json'
            write_canonical_json(snapshot,value['snapshot'])
            cycle=f'fixture-{source}-{year}'
            if year != 2024:
                pin,_=snapshot_to_pin(cycle_id=cycle,source_id=source,vintage=year,snapshot=value['snapshot'],
                    path=snapshot,retrieved_at='2026-11-05T00:00:00Z',historical_evidence=value)
            else: pin={}
            artifact=directory/f'{source}-{year}'
            manifest=create_artifact(artifact,frame,source_id=source,source_family=source,
                source_type='government_survey',provider='fixture',distribution_channel='fixture',
                provider_release_id=f'{boot.PRODUCTS[source]}:{year}',provider_release_timestamp_or_date=None,
                retrieved_at=None,target_month=f'{year}-12',source_request_identity=cycle,
                source_urls_or_endpoint_identity=['fixture://acs'],artifact_created_at='2026-11-05T00:00:00Z',
                raw_source_lineage={'cycle_id':cycle,'provider_pin':pin})
            tar=directory/(manifest['artifact_id']+'.tar');package=build_publication_package(artifact,tar)
            record={'object_id':manifest['artifact_id'],'object_type':'source','publication_state':'published_immutable_verified',
                'artifact_content_hash':manifest['artifact_content_hash'],'package_sha256':package['package_sha256'],
                'metadata':{'source_id':source,'provider_release_id':f'{boot.PRODUCTS[source]}:{year}',
                            'data_sha256':manifest['data_sha256']}}
            records.append(record);parents.append({'source_id':source,'vintage':year,'artifact_id':record['object_id'],
                'artifact_content_hash':record['artifact_content_hash'],'package_sha256':record['package_sha256'],
                'data_sha256':record['metadata']['data_sha256']})
            packages[record['object_id']]=str(tar)
            if year != 2024:evidence[f'{source}:{year}']=str(path)
            if year == 2024:eligible[source]=(record['object_id'],)
            if source=='census_acs5' and year==2023:
                pin_path=directory/'2023-pin.json';write_canonical_json(pin_path,pin)
    return {'schema_version':boot.INPUT_VERSION,'catalog':None,'governance':None,'scope_authorities':reg['current_scope'],
        'parents':parents,'packages':packages,'evidence':evidence,'acs5_2023_pin':str(pin_path),
        'compatibility_authorization':'fixture-only','reports':{},'artifact_created_at':'2026-11-05T00:00:00Z'},reg,{'immutable_records':records},eligible


@pytest.fixture
def inputs(baseline,tmp_path,monkeypatch):
    spec,registry,catalog,eligible=deepcopy(baseline)
    for key,value in [('governance',registry),('catalog',catalog)]:
        path=tmp_path/(key+'.json');write_canonical_json(path,value);spec[key]=str(path)
    pin = tmp_path/'pin.json'; shutil.copyfile(spec['acs5_2023_pin'],pin); spec['acs5_2023_pin']=str(pin)
    monkeypatch.setattr(boot.compatibility.qualification,'ELIGIBLE',eligible)
    monkeypatch.setattr(boot.compatibility.qualification,'git_bytes',lambda *args:json.dumps(catalog).encode())
    def verify(**kwargs):
        assert kwargs['governance'].namespace=='test_only'
        return {'authorization_id':'fixture-only','acs5_only_geographies':['fixture-five-only'],
                'jointly_unobserved_geographies':['fixture-missing']}
    monkeypatch.setattr(boot.compatibility,'verify',verify)
    return spec,ACSGovernance.from_fixture(registry)


def reconcile(spec,g,tmp_path):
    return boot.reconcile_inventory(spec,governance=g,root=ROOT,workspace=tmp_path/'extract')


def test_deterministic_complete_preparation_and_independent_verification(inputs,tmp_path):
    spec,g=inputs
    a=boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=tmp_path/'one')
    b=boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=tmp_path/'two')
    assert a==b and (tmp_path/'one/bundle.json').read_bytes()==(tmp_path/'two/bundle.json').read_bytes()
    for source in boot.YEARS:
        assert (tmp_path/'one'/a['candidates'][source]['package_path']).read_bytes()==(tmp_path/'two'/b['candidates'][source]['package_path']).read_bytes()
    result=boot.verify_bootstrap(spec,a,governance=g,root=ROOT,directory=tmp_path/'one')
    assert result['verification']=='passed' and result['production_inputs_verified'] is False
    assert a['verification']['evidence_kind']=='test_fixture' and a['installation_readiness']=='blocked'
    assert a['active'] is False and a['approval_provenance'] is None
    assert set(a['proposed_acceptance']['set_current_state'])==set(boot.YEARS)
    assert len(a['candidates']['census_acs1']['state']['inventory']['parents'])==19
    assert len(a['candidates']['census_acs5']['state']['inventory']['parents'])==16
    assert not g._registry['states'] and not g._registry['current_state']


@pytest.mark.parametrize('mutation',['missing_tar','hash','missing_parent','duplicate_year','alternative_2024','catalog_hash','missing_gap','existing_state','pin','omission'])
def test_inputs_fail_closed(inputs,tmp_path,mutation):
    spec,g=inputs
    if mutation=='missing_tar':spec['packages'][spec['parents'][0]['artifact_id']]=str(tmp_path/'missing.tar')
    if mutation=='hash':spec['parents'][0]['package_sha256']='0'*64
    if mutation=='missing_parent':spec['parents'].pop()
    if mutation=='duplicate_year':spec['parents'][1]=deepcopy(spec['parents'][0])
    if mutation=='alternative_2024':
        p=next(p for p in spec['parents'] if p['vintage']==2024); old=p['artifact_id'];p['artifact_id']='alternative'
        spec['packages']['alternative']=spec['packages'].pop(old)
    if mutation=='catalog_hash':
        c=boot.read_json(Path(spec['catalog']));c['immutable_records'][0]['artifact_content_hash']='0'*64;write_canonical_json(Path(spec['catalog']),c)
    if mutation=='missing_gap':
        r=boot.read_json(Path(spec['governance']));r['gap_evidence']={};write_canonical_json(Path(spec['governance']),r)
    if mutation=='existing_state':
        r=boot.read_json(Path(spec['governance']));r['current_state']={'census_acs1':'unexpected'};write_canonical_json(Path(spec['governance']),r)
    if mutation=='pin':write_canonical_json(Path(spec['acs5_2023_pin']),{})
    if mutation=='omission':
        key='census_acs5:2023';v=boot.read_json(Path(spec['evidence'][key]));v['2023']['omission_evidence'].pop()
        path=tmp_path/'omission.json';write_canonical_json(path,v);spec['evidence'][key]=str(path)
    with pytest.raises((ValueError,KeyError,FileNotFoundError,ArtifactValidationError)):reconcile(spec,g,tmp_path)


@pytest.mark.parametrize('mutation',['package','delta','partial','published','paths','input'])
def test_verifier_does_not_trust_retained_pass_or_partial_outputs(inputs,tmp_path,mutation):
    spec,g=inputs;out=tmp_path/'prepared';bundle=boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=out)
    if mutation=='package':(out/'packages/census_acs1.tar').write_bytes(b'changed')
    if mutation=='delta':bundle['proposed_acceptance']['active']=True
    if mutation=='partial':(out/'packages/census_acs5.tar').unlink()
    if mutation=='published':bundle['publication']='published'
    if mutation=='paths':bundle['candidates']['census_acs1']['package_path']='../outside.tar'
    if mutation=='input':spec['artifact_created_at']='2026-11-06T00:00:00Z'
    with pytest.raises((ValueError,FileNotFoundError,ArtifactValidationError)):boot.verify_bootstrap(spec,bundle,governance=g,root=ROOT,directory=out)


def test_stale_or_concurrent_authority_stops_preparation(inputs,tmp_path,monkeypatch):
    spec,g=inputs;original=boot._fresh;calls=[]
    def fresh(*args):
        value=original(*args);calls.append(1)
        if len(calls)>1:raise ValueError('concurrent registry change')
        return value
    monkeypatch.setattr(boot,'_fresh',fresh)
    with pytest.raises(ValueError,match='concurrent'):boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=tmp_path/'partial')
    assert not (tmp_path/'partial/bundle.json').exists()


def test_fixture_cannot_be_relabelled_production(inputs,tmp_path):
    spec,g=inputs;g.namespace='production';g.provenance='main:'+'a'*40
    with pytest.raises(ValueError,match='test_only'):reconcile(spec,g,tmp_path)


def test_cli_reports_real_inputs_and_network_gate_without_fabricating_bundle(tmp_path,monkeypatch):
    spec=boot.input_template(ROOT,tmp_path/'absent-packages');input_path=tmp_path/'inputs.json';write_canonical_json(input_path,spec)
    def denied(*args):raise OSError('transport unavailable')
    monkeypatch.setattr(boot.ACSGovernance,'load_production',denied)
    monkeypatch.setattr('sys.argv',['bootstrap','--inputs',str(input_path),'--prepare','--output',str(tmp_path/'report')])
    assert boot.main()==2
    report=boot.read_json(tmp_path/'report/verification.json')
    assert report['actual_35_package_verification']=='not_established' and report['verified_original_parents']==0
    assert report['local_tar_files_present']==0 and report['installation_readiness']=='blocked'
    assert not (tmp_path/'report/bundle.json').exists()


@pytest.mark.parametrize('mutation',['none','local_registry','catalog','registry_advance','main_advance'])
def test_production_reader_uses_live_capability_and_only_gets(inputs,monkeypatch,mutation):
    import base64
    from unittest.mock import patch
    spec,_=inputs
    registry=boot.read_json(Path(spec['governance']));registry['namespace']='production'
    write_canonical_json(Path(spec['governance']),registry)
    catalog=boot.read_json(Path(spec['catalog']));calls=[]
    class Response:
        headers={}
        def __init__(self,v):self.value=v
        def __enter__(self):return self
        def __exit__(self,*args):pass
        def getcode(self):return 200
        def read(self):return json.dumps(self.value).encode()
    class Opener:
        def open(self,request):
            assert request.method=='GET';calls.append(request.full_url)
            if request.full_url.endswith('/commits/main'):
                count=sum(x.endswith('/commits/main') for x in calls)
                return Response({'sha': ('b' if mutation=='main_advance' and count>=3 else 'a')*40})
            if 'artifact_catalog.json' in request.full_url:
                value={**catalog,'changed':True} if mutation=='catalog' else catalog
            else:
                value={**registry,'changed':True} if mutation=='registry_advance' and len(calls)>2 else registry
            return Response({'content':base64.b64encode(json.dumps(value).encode()).decode()})
    with patch('urllib.request.build_opener',return_value=Opener()):
        gov=ACSGovernance.load_production(boot.REPOSITORY,'fixture-token')
        if mutation=='local_registry':
            write_canonical_json(Path(spec['governance']),{**registry,'changed':True})
        if mutation=='none':
            assert boot._fresh(spec,gov)['governance']=='live_verified_for_preparation_only'
        else:
            with pytest.raises(ValueError):boot._fresh(spec,gov)
    assert calls and all('https://api.github.com/repos/' in x for x in calls)


def test_loose_candidate_cannot_diverge_from_verified_tar(inputs,tmp_path):
    spec,g=inputs;out=tmp_path/'out';bundle=boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=out)
    (out/'candidates/census_acs1/lineage.parquet').write_bytes(b'tampered')
    with pytest.raises(Exception):boot.verify_bootstrap(spec,bundle,governance=g,root=ROOT,directory=out)


def test_acceptance_delta_cannot_install_one_product(inputs,tmp_path):
    spec,g=inputs;bundle=boot.prepare_bootstrap(spec,governance=g,root=ROOT,output=tmp_path/'out')
    state=bundle['candidates']['census_acs1']['state']
    with pytest.raises(ValueError,match='two-state'):boot.propose_acceptance({'census_acs1':state},baseline_registry=boot.read_json(Path(spec['governance'])))


def test_original_qualification_catalog_remains_separate_from_live_catalog(tmp_path):
    catalog=boot.read_json(ROOT/'config/artifact_catalog.json')
    records={r['object_id']:r for r in catalog['immutable_records']}
    path=boot._qualification_catalog(ROOT,records,tmp_path)
    assert sha256_file(path)=='2b49256a75802f8290094c5841e6015ced4fc09cfc6dd7680b4def267978bfa9'
    assert sha256_file(path)!=sha256_file(ROOT/'config/artifact_catalog.json')
    record=records[boot.compatibility.qualification.ELIGIBLE['census_acs1'][0]]
    record['release_id']='altered'
    with pytest.raises(ValueError,match='2024 catalog record changed'):boot._qualification_catalog(ROOT,records,tmp_path)
