"""Bounded ACS correction: exact facts, write isolation and atomic crash recovery."""
import base64
from copy import deepcopy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from core.source_artifacts.hashing import canonical_json_bytes, sha256_json
from core.source_artifacts.publication import PublicationError, TransientPublicationError
from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes
from jobs.monthly_refresh import august_acs_correction as c
from jobs.monthly_refresh import august_acs_correction_hosted as h

ROOT=Path('.')


def load(path): return json.loads((ROOT/path).read_text())


def changed_record(record, target):
    r=deepcopy(record);r.update(target)
    r['asset_id'] += 10000000
    r['logical_artifact_uri']=f"artifact://{r['object_type']}/{r['object_id']}"
    r['asset_filename']=r['object_id']+'.tar'
    return r


@pytest.fixture
def fixture(tmp_path,monkeypatch):
    from jobs.monthly_refresh import acs_authoritative_write
    monkeypatch.setattr(acs_authoritative_write.time,'sleep',lambda *_:None)
    catalog=load('config/artifact_catalog.json')
    original=load(f'config/monthly_logical_cohort_plans/{c.CYCLE}.json')
    resolution=load(c.RESOLUTION_PATH)
    entries=[]
    for source,identity in catalog['accepted']['source'].items():
        r=c.exact(catalog,'source',identity)
        entries.append(dict(source_id=source,artifact_id=identity,logical_artifact_uri=r['logical_artifact_uri'],
            package_sha256=r['package_sha256'],artifact_content_hash=r['artifact_content_hash'],
            provider_release_id=r['metadata']['provider_release_id'],observation_max=r['metadata']['observation_max'],
            validation_status='passed',monthly_status='refreshed',release_tag=r['release_tag'],asset_id=r['asset_id'],
            publication_receipt_id=r['publication_receipt_id'],cycle_check_succeeded=True,
            carried_forward=False,carry_forward_policy_allowed=False))
    families=[]
    for logical,r in original['family_resolutions'].items():
        families.append(dict(logical_source_id=logical,resolution_id=r['resolution_id'],
            output_artifact_id=r['output_artifact_id'],output_content_hash=r['output_content_hash'],
            output_package_sha256=r['output_package_sha256'],physical_sources=sorted([
                {k:p[k] for k in ('source_id','artifact_id','artifact_content_hash','package_sha256')} for p in r['parents']],key=lambda p:p['source_id'])))
    family={'schema_version':'source_family_resolution_map_v1','cycle_id':c.CYCLE,
        'logical_source_inventory':sorted(c.SOURCES),
        'physical_source_inventory':sorted((c.SOURCES-{'acs','bps'})|{'census_acs1','census_acs5','census_bps','census_bps_provisional'}),
        'families':families}
    old=create_source_set_v2(tmp_path/'old-set.json',target_month='2026-08',created_at='old',builder_git_sha='old',
        entries=entries,config_hashes=governed_config_hashes(),family_resolution=family)
    old_record=c.exact(catalog,'source_set',c.OLD_SET)
    catalog['immutable_records'].remove(old_record)
    catalog['immutable_records'].append(changed_record(old_record,{'object_id':old['source_set_id']}))
    catalog['immutable_records'].sort(key=lambda r:(r['object_type'],r['object_id']))
    monkeypatch.setattr(c,'OLD_SET',old['source_set_id']);catalog['accepted']['source_set']=c.OLD_SET
    targets={kind:{'object_type':kind,'object_id':identity,'artifact_content_hash':str(i)*64,'package_sha256':str(i+2)*64}
        for i,(kind,identity) in enumerate([('source_set','source_set__2026-08__v2__'+'d'*16),
                                           ('canonical_market','market__2026-08__r3__'+'e'*16)],1)}
    for kind,target in targets.items():
        target['metadata']=deepcopy(c.exact(catalog,kind,c.OLD_SET if kind=='source_set' else c.OLD_MARKET)['metadata'])
    protected={p:load(p) for p in h.PROTECTED}
    review=c.seal(dict(schema_version=c.VERSION,repository=c.REPOSITORY,resolution_id=c.RESOLUTION,
        acs_package_sha256=c.NEW_SHA,requirements_sha256=h.sha256_file(Path('requirements.txt')),
        successor_manifests={k:{'fixture':k,'database_sha256':'a'*64} for k in targets},
        config_hashes=governed_config_hashes(),predecessor=dict(commit='1'*40,accepted=deepcopy(catalog['accepted']),
            registry_sha256='a'*64,immutable_records=deepcopy(catalog['immutable_records']),
            protected_sha256={p:sha256_json(v) for p,v in protected.items()}),targets=targets),
        'review_id',c.VERSION+'_review')
    published={kind:changed_record(c.exact(catalog,kind,c.OLD_SET if kind=='source_set' else c.OLD_MARKET),target)
               for kind,target in targets.items()}
    return SimpleNamespace(catalog=catalog,old=old,resolution=resolution,review=review,
        published=published,protected=protected,tmp=tmp_path,targets=targets)


def test_successor_preserves_nine_entries_and_bps(fixture,tmp_path):
    f=fixture;r=c.exact(f.catalog,'source',c.NEW_ACS)
    manifest={'artifact_id':c.NEW_ACS,'artifact_content_hash':r['artifact_content_hash'],
              'provider_release_id':r['metadata']['provider_release_id'],'observation_max':r['metadata']['observation_max']}
    new=c.successor(tmp_path/'new.json',old=f.old,catalog=f.catalog,resolution=f.resolution,
                    manifest=manifest,config_hashes=governed_config_hashes())
    assert [e for e in new['sources'] if e['source_id']!='acs']==[e for e in f.old['sources'] if e['source_id']!='acs']
    assert next(x for x in new['family_resolution']['families'] if x['logical_source_id']=='bps')==next(x for x in f.old['family_resolution']['families'] if x['logical_source_id']=='bps')
    assert next(x for x in new['family_resolution']['families'] if x['logical_source_id']=='acs')['resolution_id']==c.RESOLUTION
    assert new['source_set_id']!=f.old['source_set_id']
    bad=deepcopy(f.resolution);bad['output_package_sha256']='0'*64
    with pytest.raises(PublicationError):
        c.successor(tmp_path/'bad.json',old=f.old,catalog=f.catalog,resolution=bad,manifest=manifest,config_hashes=governed_config_hashes())


def test_atomic_three_pointer_delta_and_replay(fixture):
    f=fixture;catalog=deepcopy(f.catalog);catalog['immutable_records']+=list(f.published.values())
    catalog['immutable_records'].sort(key=lambda r:(r['object_type'],r['object_id']))
    new,changed=c.atomic_delta(catalog,f.review)
    assert changed and new['accepted']==c.target_accepted(f.review)
    assert new['immutable_records']==catalog['immutable_records']
    assert new['accepted']['serving_market']==catalog['accepted']['serving_market']
    assert {k:v for k,v in new['accepted']['source'].items() if k!='acs'}=={k:v for k,v in catalog['accepted']['source'].items() if k!='acs'}
    assert c.atomic_delta(new,f.review)==(new,False)
    for field in ('source_set','canonical_market','acs','serving_market'):
        bad=deepcopy(catalog)
        if field=='acs':bad['accepted']['source']['acs']=c.NEW_ACS
        elif field=='serving_market':bad['accepted'][field]=None
        else:bad['accepted'][field]=new['accepted'][field]
        with pytest.raises(PublicationError):c.atomic_delta(bad,f.review)


def test_factual_delta_rejects_nonacs_change(tmp_path):
    for name,value in [('old',1),('new',1),('bad',2)]:
        with duckdb.connect(str(tmp_path/name)) as db:
            db.execute('CREATE TABLE fact_timeseries(source_id VARCHAR, value DOUBLE)')
            db.execute("INSERT INTO fact_timeseries VALUES ('bps', ?), ('acs', ?)",[value,10 if name=='old' else 99])
    c.factual_delta(tmp_path/'old',tmp_path/'new')
    with pytest.raises(PublicationError):c.factual_delta(tmp_path/'old',tmp_path/'bad')


class MemoryAPI:
    repository=c.REPOSITORY;token='fixture';read_only=False
    def __init__(self,f):
        self.tip='1'*40;self.next=2;self.trees={};self.commits={};self.calls=[];self.outcome=None;self.race=False
        self.snapshots={self.tip:{c.CATALOG_PATH:deepcopy(f.catalog),**deepcopy(f.protected)}}
        self.changes={}
    def advance(self,updates):
        updates=deepcopy(updates)
        if c.CATALOG_PATH in updates:
            updates[c.CATALOG_PATH]['immutable_records'].sort(key=lambda r:(r['object_type'],r['object_id']))
        tip=f'{self.next:040x}';self.next+=1
        self.snapshots[tip]={**deepcopy(self.snapshots[self.tip]),**deepcopy(updates)}
        self.changes[tip]=set(updates);self.tip=tip;return tip
    def request(self,method,url,*,payload=None,**kwargs):
        self.calls.append((method,url,deepcopy(payload)))
        if self.read_only and method!='GET':raise AssertionError('validation attempted remote write')
        if method=='GET' and url.startswith('/contents/'):
            path,tip=url[len('/contents/'):].split('?ref=')
            if tip=='main':tip=self.tip
            value=self.snapshots[tip].get(unquote(path))
            if value is None:return None,{}
            raw=canonical_json_bytes(value);return {'sha':hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest(),
                'content':base64.b64encode(raw).decode()},{}
        if url.startswith('/compare/'):
            base,tip=url.split('/compare/')[1].split('...')
            paths=set().union(*(v for k,v in self.changes.items() if k!=base)) if self.changes else set()
            return {'merge_base_commit':{'sha':base},'files':[{'filename':p} for p in paths]},{}
        if url.startswith('/git/commits/') and method=='GET':return {'sha':self.tip,'tree':{'sha':self.tip}},{}
        if url=='/git/ref/heads/main':return {'object':{'sha':self.tip}},{}
        if method=='POST' and url=='/git/trees':
            self.trees['tree']=payload
            if self.race:
                bad=deepcopy(self.snapshots[self.tip][c.CATALOG_PATH]);bad['accepted']['serving_market']=None
                self.advance({c.CATALOG_PATH:bad});self.race=False
            return {'sha':'tree'},{}
        if method=='POST' and url=='/git/commits':
            sha=f'{100+self.next:040x}';self.next+=1;self.commits[sha]=(payload,self.trees['tree']);return {'sha':sha},{}
        if method=='PATCH' and url=='/git/refs/heads/main':
            if self.outcome=='before':raise TransientPublicationError('fixture lost before commit')
            commit,tree=self.commits[payload['sha']]
            if commit['parents']!=[self.tip]:raise PublicationError('non-fast-forward fixture')
            value={x['path']:json.loads(x['content']) for x in tree['tree']}
            self.snapshots[payload['sha']]={**deepcopy(self.snapshots[self.tip]),**value}
            self.tip=payload['sha'];self.changes[self.tip]=set(value)
            if self.outcome=='after':raise TransientPublicationError('fixture lost after commit')
            return {'object':{'sha':self.tip}},{}
        raise AssertionError((method,url))

from urllib.parse import unquote


class Governance:
    def __init__(self,api):self.api=api
    def require_destination(self,repository):assert repository==c.REPOSITORY
    def current_commit(self,repository):self.require_destination(repository);return self.api.tip
    def production_snapshot(self):return {'registry_sha256':'a'*64}


@pytest.fixture
def harness(fixture,monkeypatch):
    f=fixture;api=MemoryAPI(f);g=Governance(api)
    monkeypatch.setattr(h.ACSGovernance,'load_production',lambda *args:g)
    monkeypatch.setattr(h,'clean_code',lambda base:None)
    monkeypatch.setattr(h,'construct',lambda *args:(deepcopy(f.targets),{k:{'member_hashes':{},'manifest':{'fixture':k,'database_sha256':'a'*64}} for k in f.targets}))
    monkeypatch.setattr(h,'_verify_object_record',lambda *args:None)
    monkeypatch.setattr(h,'retain_bundle',lambda *args:None)
    monkeypatch.setattr(h,'retained_candidates',lambda *args:{})
    monkeypatch.setattr(h,'verify_candidates',lambda *args:None)
    f.fail_publication=None
    def publish(**kwargs):
        kwargs['api'].guard()
        kind=kwargs['object_type'];record=deepcopy(f.published[kind]);catalog=deepcopy(api.snapshots[api.tip][c.CATALOG_PATH])
        matches=[r for r in catalog['immutable_records'] if r['object_id']==record['object_id']]
        if not matches:
            catalog['immutable_records'].append(record);api.advance({c.CATALOG_PATH:catalog})
        elif matches!=[record]:raise PublicationError('fixture identity collision')
        if f.fail_publication==kind:raise TransientPublicationError('fixture partial publication')
        return record
    monkeypatch.setattr(h,'_publish_object',publish)
    f.api=api;f.g=g
    return f


def run(f,mode,record=None):
    return h.execute(mode=mode,api=f.api,workspace=f.tmp/mode,
        review=f.review,record=record,supplied=c.authorization(mode,record if record else f.review))


def test_full_lifecycle_readonly_prepare_atomic_accept_duplicate(harness):
    f=harness;f.api.read_only=True
    validation=h.execute(mode='validate',api=f.api,workspace=f.tmp/'validate')
    assert validation['production_writes'] is False and all(m=='GET' for m,_,_ in f.api.calls)
    f.api.read_only=False;f.api.calls=[]
    record=run(f,'prepare')['record']
    assert f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']==f.review['predecessor']['accepted']
    assert run(f,'prepare')['record']==record
    f.api.calls=[]
    result=run(f,'accept',record)
    assert result['atomic'] and result['accepted_state_changed']
    patches=[x for x in f.api.calls if x[0]=='PATCH']
    assert len(patches)==1
    assert c.CATALOG_PATH in f.api.changes[f.api.tip] and len(f.api.changes[f.api.tip])==1
    assert not run(f,'accept',record)['accepted_state_changed']
    assert f.api.snapshots[f.api.tip][c.READINESS_PATH]==f.protected[c.READINESS_PATH]


@pytest.mark.parametrize('boundary',['source_set','canonical_market'])
def test_partial_publication_recovers_same_review(harness,boundary):
    f=harness;f.fail_publication=boundary
    with pytest.raises(TransientPublicationError):run(f,'prepare')
    assert f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']==f.review['predecessor']['accepted']
    f.fail_publication=None;record=run(f,'prepare')['record']
    assert run(f,'prepare')['record']==record


@pytest.mark.parametrize('outcome',['before','after'])
def test_uncertain_atomic_write_never_leaves_partial_pointers(harness,outcome):
    f=harness;record=run(f,'prepare')['record'];f.api.outcome=outcome
    if outcome=='before':
        with pytest.raises(PublicationError):run(f,'accept',record)
        assert f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']==f.review['predecessor']['accepted']
    else:assert run(f,'accept',record)['result']=='PASS'
    f.api.outcome=None;run(f,'accept',record)
    assert f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']==c.target_accepted(f.review)


@pytest.mark.parametrize('attack',['accepted','serving','readiness','resolution','code','conflicting_record','catalog_record','unreviewed_publication'])
def test_stale_or_conflicting_state_fails_before_any_write(harness,attack):
    f=harness;snapshot=f.api.snapshots[f.api.tip]
    if attack in ('accepted','serving','catalog_record','unreviewed_publication'):
        value=deepcopy(snapshot[c.CATALOG_PATH])
        if attack=='accepted':value['accepted']['source']['acs']=c.NEW_ACS
        elif attack=='serving':value['accepted']['serving_market']=None
        elif attack=='catalog_record':value['immutable_records'][0]['asset_id']+=1
        else:
            extra=deepcopy(f.published['source_set']);extra['object_id']+='unreviewed';value['immutable_records'].append(extra)
        f.api.advance({c.CATALOG_PATH:value})
    elif attack=='conflicting_record':
        record=run(f,'prepare')['record'];bad=deepcopy(record);bad['review']['predecessor']['commit']='9'*40
        bad['review']=c.seal(bad['review'],'review_id',c.VERSION+'_review');bad=c.seal(bad,'correction_id',c.VERSION)
        f.api.advance({c.RECORD_PATH:bad})
    else:
        path={'readiness':c.READINESS_PATH,'resolution':c.RESOLUTION_PATH,'code':'jobs/changed.py'}[attack]
        f.api.advance({path:{'changed':True}})
    f.api.calls=[]
    with pytest.raises((PublicationError,KeyError)):run(f,'prepare')
    assert all(method=='GET' for method,_,_ in f.api.calls)


def test_concurrent_acceptance_change_rejected_before_catalog_commit(harness):
    f=harness;record=run(f,'prepare')['record'];f.api.race=True;f.api.calls=[]
    with pytest.raises(PublicationError):run(f,'accept',record)
    assert not any(m=='PATCH' for m,_,_ in f.api.calls)
    accepted=f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']
    assert accepted['source_set']==c.OLD_SET and accepted['canonical_market']==c.OLD_MARKET
    assert accepted['source']['acs']==c.OLD_ACS


def test_no_authorization_no_writes(harness):
    f=harness
    with pytest.raises(PublicationError):h.execute(mode='prepare',api=f.api,workspace=f.tmp,review=f.review,supplied='wrong')
    assert not f.api.calls


def test_reviewed_binary_package_drift_stops_preparation(harness,monkeypatch):
    f=harness;bad=deepcopy(f.targets);bad['source_set']['package_sha256']='0'*64
    monkeypatch.setattr(h,'retained_candidates',lambda *a: (_ for _ in ()).throw(PublicationError('reviewed packages changed')))
    with pytest.raises(PublicationError,match='reviewed packages'):run(f,'prepare')
    assert all(m=='GET' for m,_,_ in f.api.calls)


@pytest.mark.parametrize('duplicate,published,tagged',[ (False,False,False),(True,False,False),(False,True,False),(False,True,True)])
def test_exhaustive_draft_discovery_and_conflicts(duplicate,published,tagged):
    release={'id':437,'tag_name':'source-set/exact','draft':not published}
    class API:
        repository=c.REPOSITORY;token='fixture'
        calls=[]
        def request(self,method,url,**kwargs):
            self.calls.append((method,url))
            if url.endswith('page=1'):return [{'id':i,'tag_name':'other'} for i in range(100)],{}
            if url.endswith('page=2'):return [release]+([dict(release,id=438)] if duplicate else []),{}
            if '/tags/' in url:return release if tagged else None,{}
            if url=='/releases/437':return release,{}
            raise AssertionError(url)
    api=API();wrapper=h.FencedAPI(api,lambda:None)
    if duplicate or (published and not tagged):
        with pytest.raises(PublicationError):wrapper.request('GET','/releases/tags/source-set%2Fexact',expected=(200,404))
    else:assert wrapper.request('GET','/releases/tags/source-set%2Fexact',expected=(200,404))[0]['id']==437
    assert all(m=='GET' for m,_ in api.calls)


@pytest.mark.parametrize('lost',['create','upload','verify','finalize','catalog_before','catalog_after',None])
def test_real_publisher_restart_never_duplicates_or_replaces_release(fixture,tmp_path,lost):
    """Use the actual publisher/package validator with an offline remote service."""
    from core.source_artifacts.catalog import empty_catalog, add_record
    from core.source_artifacts.object_package import build_object_package
    from core.source_artifacts.hashing import write_canonical_json
    f=fixture
    write_canonical_json(tmp_path/'source-set.json',f.old)
    package=tmp_path/'source-set.tar'
    info=build_object_package({'source-set.json':tmp_path/'source-set.json'},package)
    class Remote:
        repository=c.REPOSITORY;token='fixture'
        def __init__(self):self.release=None;self.bytes=None;self.lose=lost;self.creates=0;self.uploads=0
        def once(self,stage):
            if self.lose==stage:self.lose=None;raise TransientPublicationError('fixture lost acknowledgement')
        def request(self,method,url,*,payload=None,**kwargs):
            if method=='GET':
                if url.startswith('/releases?'):return [deepcopy(self.release)] if self.release else [],{}
                if url.startswith('/releases/tags/'):
                    return (deepcopy(self.release) if self.release and not self.release['draft'] else None),{}
                if url=='/releases/17':return deepcopy(self.release),{}
            if method=='POST' and url=='/releases':
                assert self.release is None
                self.creates+=1;self.release={'id':17,'tag_name':payload['tag_name'],'draft':True,'assets':[],
                    'upload_url':'https://uploads.github.com/17{?name,label}'}
                self.once('create');return deepcopy(self.release),{}
            if method=='POST' and url.startswith('https://uploads.github.com/17'):
                assert self.bytes is None
                self.uploads+=1;self.bytes=payload
                self.release['assets']=[{'id':23,'name':f.old['source_set_id']+'.tar'}]
                self.once('upload');return deepcopy(self.release['assets'][0]),{}
            if method=='PATCH' and url=='/releases/17':
                self.release.update(draft=False,published_at='2026-11-17T00:00:00Z')
                self.once('finalize');return deepcopy(self.release),{}
            raise AssertionError((method,url))
        def download_asset(self,asset_id,path):
            assert asset_id==23
            path.write_bytes(self.bytes);self.once('verify')
    remote=Remote()
    review=deepcopy(f.review)
    review['targets']['source_set']={'object_type':'source_set','object_id':f.old['source_set_id'],
        'artifact_content_hash':sha256_json(f.old),'package_sha256':info['package_sha256'],'metadata':{'cycle_id':c.CYCLE}}
    review=c.seal(review,'review_id',c.VERSION+'_review')
    git=MemoryAPI(f);gov=Governance(git)
    journal=h.PublicationJournal(git,gov,review)
    wrapper=h.PublicationAPI(remote,lambda:h.fence(git,gov,review),journal,'source_set')
    class CAS:
        def __init__(self):self.catalog=empty_catalog();self.lose=lost
        def read(self):return deepcopy(self.catalog),None
        def add(self,record,receipt):
            if self.lose=='catalog_before':self.lose=None;raise TransientPublicationError('fixture lost before CAS')
            self.catalog=add_record(self.catalog,record,receipt)
            if self.lose=='catalog_after':self.lose=None;raise TransientPublicationError('fixture lost after CAS')
    cas=CAS()
    def publish(attempt):
        directory=tmp_path/str(attempt);directory.mkdir()
        retained=directory/'source-set.tar';retained.write_bytes(package.read_bytes())
        return h._publish_object(api=wrapper,cas=cas,package=retained,object_id=f.old['source_set_id'],
            object_type='source_set',content_hash=sha256_json(f.old),metadata={'cycle_id':c.CYCLE},
            members=info['member_hashes'],git_sha='1'*40)
    if lost:
        with pytest.raises(TransientPublicationError):publish(1)
    result=publish(2)
    assert result['release_id']==17 and result['asset_id']==23
    assert remote.creates==remote.uploads==1
    assert len(cas.catalog['immutable_records'])==1
    assert cas.catalog['accepted']==empty_catalog()['accepted']
    assert publish(3)==result


@pytest.mark.parametrize('outcome',['before','after'])
def test_prepared_record_write_acknowledgement_recovers_without_acceptance(harness,outcome):
    f=harness;f.api.outcome=outcome
    if outcome=='before':
        with pytest.raises(PublicationError):run(f,'prepare')
    else:assert run(f,'prepare')['result']=='PASS'
    f.api.outcome=None
    record=run(f,'prepare')['record']
    assert run(f,'prepare')['record']==record
    assert f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']==f.review['predecessor']['accepted']


def test_sparse_release_pages_follow_next_link():
    class API:
        repository=c.REPOSITORY;token='fixture'
        def __init__(self):self.pages=[]
        def request(self,method,url,**kwargs):
            if 'page=' in url:
                page=int(url.split('page=')[-1]);self.pages.append(page)
                if page==1:return [{'id':1,'tag_name':'other'}],{'Link':'<https://api.github.com/releases?page=2>; rel="next"'}
                return [{'id':17,'tag_name':'exact','draft':True}],{}
            if '/tags/' in url:return None,{}
            return {'id':17,'tag_name':'exact','draft':True},{}
    api=API()
    assert h.FencedAPI(api,lambda:None).request('GET','/releases/tags/exact')[0]['id']==17
    assert api.pages==[1,2]


class RealRemote(MemoryAPI):
    """Offline Git/release service; actual publisher, package validation and CAS."""
    def __init__(self,f):
        super().__init__(f);self.releases={};self.assets={};self.release_calls=[]
        self.lose=None;self.hidden=False;self.counter=900000
    def seed(self,record,package):
        self.releases[record['release_id']]={'id':record['release_id'],'tag_name':record['release_tag'],
            'draft':False,'assets':[{'id':record['asset_id'],'name':record['asset_filename']}],
            'upload_url':f"https://uploads.github.com/repos/fixture/releases/{record['release_id']}/assets{{?name,label}}"}
        self.assets[record['asset_id']]=package.read_bytes()
    def lost(self,stage):
        if self.lose==stage:self.lose=None;raise TransientPublicationError('offline uncertain '+stage)
    def request(self,method,url,*,payload=None,**kwargs):
        from urllib.parse import urlsplit,parse_qs
        if not (url.startswith('/releases') or url.startswith('https://uploads.github.com/')):
            return super().request(method,url,payload=payload,**kwargs)
        self.release_calls.append((method,url))
        if self.read_only and method!='GET':raise AssertionError('read-only remote mutation')
        if method=='GET':
            if url.startswith('/releases?'):
                if self.hidden:return [],{}
                page=int(parse_qs(urlsplit(url).query)['page'][0]);return list(deepcopy(self.releases).values())[(page-1)*100:page*100],{}
            if url.startswith('/releases/tags/'):
                tag=unquote(url.split('/tags/')[1])
                return next((deepcopy(r) for r in self.releases.values() if r['tag_name']==tag and not r['draft'] and not self.hidden),None),{}
            return deepcopy(self.releases.get(int(url.split('/')[-1]))),{}
        if method=='POST' and url=='/releases':
            self.counter+=1;rid=self.counter
            result={'id':rid,'tag_name':payload['tag_name'],'draft':True,'assets':[],
                'upload_url':f'https://uploads.github.com/repos/fixture/releases/{rid}/assets{{?name,label}}'}
            self.releases[rid]=result;self.lost('create');return deepcopy(result),{}
        if method=='POST' and url.startswith('https://uploads.github.com/'):
            parsed=urlsplit(url);rid=int(parsed.path.split('/releases/')[1].split('/')[0])
            name=parse_qs(parsed.query)['name'][0];self.counter+=1;aid=self.counter
            asset={'id':aid,'name':name};self.assets[aid]=payload;self.releases[rid]['assets'].append(asset)
            self.lost('upload');return deepcopy(asset),{}
        if method=='PATCH' and url.startswith('/releases/'):
            rid=int(url.split('/')[-1]);self.releases[rid].update(draft=False,published_at='2026-11-18T00:00:00Z')
            self.lost('finalize');return deepcopy(self.releases[rid]),{}
        raise AssertionError((method,url))
    def download_asset(self,identity,destination):destination.write_bytes(self.assets[identity])


@pytest.fixture
def real_lifecycle(fixture,monkeypatch,tmp_path):
    """No mocked constructor/assembler/TAR validator/publisher/CAS/semantic checker.

    The production capability and historical verification inputs are offline
    fixtures; no provider acquisition, real approval or production authority.
    """
    from datetime import date
    import pandas as pd
    from core.source_artifacts.artifact import create_artifact
    from core.source_artifacts.package import build_publication_package
    from core.source_artifacts.object_package import build_object_package
    from core.source_artifacts.market_artifact import create_canonical_market_manifest
    from core.source_artifacts.source_set_v2 import source_set_semantic_sha256
    f=fixture;paths={};counter=1000000
    direct=pd.read_csv('config/source_metric_registry.csv')
    logical=pd.read_csv('config/logical_source_metric_registry.csv')
    def artifact(source,name,values):
        nonlocal counter
        metric=(logical.loc[logical.logical_source_id==source,'metric_id'].iloc[0] if source=='acs'
                else direct.loc[direct.source_id==('census_bps' if source=='bps' else source),'metric_id'].iloc[0])
        frame=pd.DataFrame([dict(geo_id='united_states__nation',metric_id=metric,date=date(2024 if source=='acs' else 2026,8,1),
            property_type_id='all',value=float(v),source_id=source,property_type='all') for v in values])
        if len(frame)>1:frame.loc[0,'date']=date(2005,12,31)
        path=tmp_path/name
        configs=({p:h.sha256_file(Path(p)) for p in ('config/redfin_baseline_manifest.json',
            'config/redfin_metric_domain_contract.json','config/geo_manifest.generated.csv',
            'config/source_refresh_revision_policy_v0_2.json')} if source=='redfin' else {})
        m=create_artifact(path,frame,source_id=source,source_family=source,source_type='offline',provider='fixture',
            distribution_channel='fixture',provider_release_id=name,provider_release_timestamp_or_date=None,
            retrieved_at=None,target_month='2024-12' if source=='acs' else '2026-08',
            source_request_identity=name,source_urls_or_endpoint_identity=['fixture://'+name],artifact_created_at=c.BUILD_TIME,config_hashes=configs)
        package=tmp_path/(name+'.tar');info=build_publication_package(path,package);counter+=1
        record=dict(object_type='source',object_id=m['artifact_id'],logical_artifact_uri=m['artifact_uri'],remote_repository=c.REPOSITORY,
            release_tag='source-artifact/'+source+'/'+m['artifact_id'],release_id=counter,asset_id=counter+100000,
            asset_filename=m['artifact_id']+'.tar',package_sha256=info['package_sha256'],artifact_content_hash=m['artifact_content_hash'],
            publication_receipt_id='offline-'+name,publication_state='published_immutable_verified',
            metadata={'source_id':source,'data_sha256':m['data_sha256'],'provider_release_id':m['provider_release_id'],'observation_max':m['observation_max']})
        paths[m['artifact_uri']]=path
        return record,m
    old_entries=deepcopy(f.old['sources']);old_records=[]
    for entry in old_entries:
        source=entry['source_id'];r,m=artifact(source,'old-'+source,[1])
        entry.update(artifact_id=r['object_id'],logical_artifact_uri=r['logical_artifact_uri'],package_sha256=r['package_sha256'],
            artifact_content_hash=r['artifact_content_hash'],provider_release_id=m['provider_release_id'],observation_max=m['observation_max'],
            release_tag=r['release_tag'],asset_id=r['asset_id'],publication_receipt_id=r['publication_receipt_id'])
        old_records.append(r)
        if source=='acs':monkeypatch.setattr(c,'OLD_ACS',r['object_id'])
    new,m=artifact('acs','new-acs',[20,30]);monkeypatch.setattr(c,'NEW_ACS',new['object_id']);monkeypatch.setattr(c,'NEW_SHA',new['package_sha256'])
    family=deepcopy(f.old['family_resolution'])
    for item in family['families']:
        r=next(x for x in old_records if x['metadata']['source_id']==item['logical_source_id'])
        item.update(output_artifact_id=r['object_id'],output_content_hash=r['artifact_content_hash'],output_package_sha256=r['package_sha256'])
    old_dir=tmp_path/'accepted-set';old_dir.mkdir()
    old=create_source_set_v2(old_dir/'source-set.json',target_month='2026-08',created_at=c.BUILD_TIME,builder_git_sha='offline',
        entries=old_entries,config_hashes=governed_config_hashes(),family_resolution=family)
    old_ss=build_object_package({'source-set.json':old_dir/'source-set.json'},tmp_path/'accepted-set.tar')
    class Resolver:
        def __init__(self,*args):pass
        def resolve(self,uri):return paths[uri]
    db_dir=tmp_path/'accepted-market';db_dir.mkdir()
    validation=h.assemble_source_set_v2(old,db_dir/'market.duckdb',Resolver())
    market=create_canonical_market_manifest(db_dir/'canonical-market.json',database_path=db_dir/'market.duckdb',
        source_set_id=old['source_set_id'],source_set_semantic_sha256=source_set_semantic_sha256(old),source_set_package_sha256=old_ss['package_sha256'],
        canonical_assembly_contract_version='canonical_market_assembly_v1',canonical_schema_identity='canonical-timeseries-v1',config_hashes=old['config_hashes'],
        builder_contract_identity='core-source-artifacts-assembly-v1',dependency_lock_identity='requirements-sha256:'+h.sha256_file(Path('requirements.txt')),
        assembly_revision=2,compressed_package_sha256=h.sha256_file(db_dir/'market.duckdb'),table_inventory=['fact_timeseries','source_artifact_metadata'],
        **{k:validation[k] for k in ('row_count','source_count','geography_count','metric_count','first_date','last_date','duplicate_key_count')},
        validation_status='passed',assembly_warnings=[],builder_git_sha='offline',built_at=c.BUILD_TIME)
    old_mp=build_object_package({'canonical-market.json':db_dir/'canonical-market.json','market.duckdb':db_dir/'market.duckdb'},tmp_path/'accepted-market.tar')
    ssr=changed_record(c.exact(f.catalog,'source_set',c.OLD_SET),{'object_id':old['source_set_id'],'artifact_content_hash':sha256_json(old),'package_sha256':old_ss['package_sha256']})
    mr=changed_record(c.exact(f.catalog,'canonical_market',c.OLD_MARKET),{'object_id':market['market_artifact_id'],'artifact_content_hash':sha256_json(market),'package_sha256':old_mp['package_sha256']})
    remove={c.OLD_SET,c.OLD_MARKET}|set(f.catalog['accepted']['source'].values())
    f.catalog['immutable_records']=[r for r in f.catalog['immutable_records'] if r['object_id'] not in remove]+old_records+[new,ssr,mr]
    f.catalog['immutable_records'].sort(key=lambda r:(r['object_type'],r['object_id']))
    monkeypatch.setattr(c,'OLD_SET',old['source_set_id']);monkeypatch.setattr(c,'OLD_MARKET',market['market_artifact_id'])
    f.catalog['accepted'].update(source_set=c.OLD_SET,canonical_market=c.OLD_MARKET,source={r['metadata']['source_id']:r['object_id'] for r in old_records})
    f.protected[c.RESOLUTION_PATH].update(output_artifact_id=c.NEW_ACS,output_content_hash=new['artifact_content_hash'],output_package_sha256=c.NEW_SHA)
    f.api=RealRemote(f);f.g=Governance(f.api);f.api.seed(ssr,tmp_path/'accepted-set.tar');f.api.seed(mr,tmp_path/'accepted-market.tar')
    monkeypatch.setattr(h.ACSGovernance,'load_production',lambda *args:f.g)
    monkeypatch.setattr(h,'clean_code',lambda base:None)
    monkeypatch.setattr(h,'GitHubReleaseArtifactResolver',Resolver)
    checks=[]
    def historical(record,**kwargs):
        assert record==f.protected[c.RESOLUTION_PATH] and kwargs['governance'] is f.g
        checks.append(True)
    monkeypatch.setattr(h,'validate_cohort',historical)
    f.bundle=tmp_path/'retained';f.api.read_only=True
    report=h.execute(mode='validate',api=f.api,workspace=tmp_path/'validation-work',bundle=f.bundle)
    f.review=report['review'];f.api.read_only=False;f.historical_checks=checks
    return f


def real_run(f,mode,record=None,workspace=None):
    return h.execute(mode=mode,api=f.api,workspace=workspace or f.tmp/(mode+'-work'),
        review=f.review,record=record,bundle=f.bundle if mode=='prepare' else None,
        supplied=c.authorization(mode,record or f.review))


def test_real_assembler_lifecycle_retains_exact_bytes_without_reassembly(real_lifecycle,monkeypatch,tmp_path):
    f=real_lifecycle
    second,_=h.construct(f.api,f.g,f.catalog,f.protected,tmp_path/'independent-build')
    # DuckDB bytes may differ across reconstructions. The reviewed transport,
    # not this independent build's hash, must remain the publication identity.
    assert second['source_set']['package_sha256']==f.review['targets']['source_set']['package_sha256']
    before={k:h.sha256_file(f.bundle/name) for k,name in c.PACKAGES.items()}
    monkeypatch.setattr(h,'assemble_source_set_v2',lambda *a,**kw:pytest.fail('write mode rebuilt DuckDB'))
    record=real_run(f,'prepare',workspace=tmp_path/'prepare-fresh')['record']
    assert {k:record['published'][k]['package_sha256'] for k in c.PACKAGES}==before
    assert {k:h.sha256_file(f.bundle/name) for k,name in c.PACKAGES.items()}==before
    assert real_run(f,'prepare',workspace=tmp_path/'retry-fresh')['record']==record
    f.bundle.rename(tmp_path/'retained-away')
    result=real_run(f,'accept',record,workspace=tmp_path/'accept-fresh')
    assert result['atomic'] and result['accepted_state_changed']
    assert not real_run(f,'accept',record,workspace=tmp_path/'accepted-retry')['accepted_state_changed']
    assert len(f.historical_checks)>=5
    accepted=f.api.snapshots[f.api.tip][c.CATALOG_PATH]['accepted']
    assert accepted==c.target_accepted(f.review)
    assert accepted['serving_market']==f.review['predecessor']['accepted']['serving_market']


@pytest.mark.parametrize('mutation',['missing','altered','wrong_review'])
def test_retained_bundle_drift_blocks_before_any_remote_write(real_lifecycle,mutation):
    f=real_lifecycle
    if mutation=='missing':(f.bundle/c.PACKAGES['canonical_market']).unlink()
    elif mutation=='altered':(f.bundle/c.PACKAGES['source_set']).write_bytes(b'changed')
    else:(f.bundle/'review.json').write_text('{}')
    f.api.calls=[];f.api.release_calls=[]
    with pytest.raises((PublicationError,FileNotFoundError)):real_run(f,'prepare')
    assert all(method=='GET' for method,_,_ in f.api.calls)
    assert all(method=='GET' for method,_ in f.api.release_calls)


@pytest.mark.parametrize('lost',['create','upload','finalize'])
def test_real_publisher_uncertain_outcomes_reconcile_same_numeric_objects(real_lifecycle,lost,tmp_path):
    f=real_lifecycle;f.api.lose=lost
    with pytest.raises(TransientPublicationError):real_run(f,'prepare',workspace=tmp_path/'first')
    journal=f.api.snapshots[f.api.tip][c.JOURNAL_PATH]
    assert journal['events'][-1]['phase']=='intent'
    create_count=sum(m=='POST' and u=='/releases' for m,u in f.api.release_calls)
    if lost=='create':
        f.api.hidden=True
        with pytest.raises(PublicationError,match='uncertain release creation'):real_run(f,'prepare',workspace=tmp_path/'hidden')
        assert sum(m=='POST' and u=='/releases' for m,u in f.api.release_calls)==create_count
        f.api.hidden=False
    record=real_run(f,'prepare',workspace=tmp_path/'reconciled')['record']
    assert sum(m=='POST' and u=='/releases' for m,u in f.api.release_calls)==2
    assert len([r for r in f.api.releases.values() if r['tag_name'].endswith(f.review['targets']['source_set']['object_id'])])==1
    assert real_run(f,'prepare',workspace=tmp_path/'exact-retry')['record']==record


def test_concurrent_publication_intent_cas_has_one_winner(harness,monkeypatch):
    f=harness;one=h.PublicationJournal(f.api,f.g,f.review);two=h.PublicationJournal(f.api,f.g,f.review)
    stale=two.read();one.intent('source_set','create')
    original=two.read;calls=[]
    def stale_read():
        if len(calls)<2:calls.append(True);return deepcopy(stale)
        return original()
    monkeypatch.setattr(two,'read',stale_read)
    with pytest.raises(PublicationError):two.intent('source_set','create')
    events=one.read()[0]['events']
    assert len(events)==1 and events[0]['phase']=='intent'
    # A restarted/concurrent caller cannot claim the outstanding mutation.
    with pytest.raises(PublicationError):two.intent('source_set','create')


@pytest.mark.parametrize('source',['acs','bps'])
def test_rehashed_semantically_changed_database_is_rejected(real_lifecycle,tmp_path,source):
    from core.source_artifacts.object_package import validate_object_package,build_object_package
    from core.source_artifacts.market_artifact import create_canonical_market_manifest
    from core.source_artifacts.hashing import write_canonical_json
    f=real_lifecycle;original=f.review['successor_manifests']['canonical_market'];target=f.review['targets']['canonical_market']
    extracted=validate_object_package(f.bundle/c.PACKAGES['canonical_market'],tmp_path/'changed',object_type='canonical_market',
        expected={**target,'member_hashes':{'canonical-market.json':sha256_json(original),'market.duckdb':original['database_sha256']}})
    with duckdb.connect(str(extracted/'market.duckdb')) as db:
        db.execute('UPDATE fact_timeseries SET value=value+1 WHERE source_id=?',[source])
    values={k:v for k,v in original.items() if k not in {'schema_version','market_artifact_id','database_sha256','supersedes_artifact_id'}}
    values['compressed_package_sha256']=h.sha256_file(extracted/'market.duckdb')
    changed=create_canonical_market_manifest(extracted/'canonical-market.json',database_path=extracted/'market.duckdb',
        supersedes_artifact_id=c.OLD_MARKET,**values)
    info=build_object_package({'canonical-market.json':extracted/'canonical-market.json','market.duckdb':extracted/'market.duckdb'},f.bundle/c.PACKAGES['canonical_market'])
    f.review['successor_manifests']['canonical_market']=changed
    f.review['targets']['canonical_market'].update(object_id=changed['market_artifact_id'],artifact_content_hash=sha256_json(changed),package_sha256=info['package_sha256'])
    f.review['targets']['canonical_market']['metadata']['database_sha256']=changed['database_sha256']
    f.review=c.seal(f.review,'review_id',c.VERSION+'_review');write_canonical_json(f.bundle/'review.json',f.review)
    f.api.calls=[];f.api.release_calls=[]
    with pytest.raises(PublicationError,match='ACS facts|non-ACS canonical facts'):real_run(f,'prepare')
    assert all(method=='GET' for method,_,_ in f.api.calls)
    assert all(method=='GET' for method,_ in f.api.release_calls)


def test_publication_transport_cannot_mutate_without_journal():
    class API:
        repository=c.REPOSITORY;token='fixture'
        def request(self,*args,**kwargs):pytest.fail('unjournaled remote mutation')
    with pytest.raises(PublicationError,match='durable publication intent'):
        h.FencedAPI(API(),lambda:None).request('POST','/releases',payload={})


def test_missing_known_journal_cannot_be_overwritten_as_initial_intent(harness,monkeypatch):
    f=harness;journal=h.PublicationJournal(f.api,f.g,f.review);journal.intent('source_set','create')
    retained=deepcopy(f.api.snapshots[f.api.tip][c.JOURNAL_PATH]);original=f.api.request
    def hidden(method,url,**kwargs):
        if method=='GET' and url.startswith('/contents/'+c.JOURNAL_PATH+'?ref='):return None,{}
        return original(method,url,**kwargs)
    monkeypatch.setattr(f.api,'request',hidden);f.api.calls=[]
    with pytest.raises(PublicationError,match='intent unavailable'):
        h.PublicationJournal(f.api,f.g,f.review).intent('source_set','create')
    assert f.api.snapshots[f.api.tip][c.JOURNAL_PATH]==retained
    assert all(method=='GET' for method,_,_ in f.api.calls)
