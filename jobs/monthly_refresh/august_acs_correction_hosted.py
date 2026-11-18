"""Three explicit modes for the single August historical ACS correction.

validate: GET-only remote transport, temporary local reconstruction.
prepare: inactive publication plus create-once evidence, never acceptance.
accept: one fenced catalog commit, never serving/Macro or Redfin consumption.
"""
import argparse
from copy import deepcopy
import json
import os
from pathlib import Path
import subprocess
import tempfile
import shutil
import uuid
from urllib.parse import unquote, quote

from core.source_artifacts.assembly_v2 import assemble_source_set_v2
from core.source_artifacts.catalog import validate_catalog
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactResolver
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.market_artifact import create_canonical_market_manifest
from core.source_artifacts.market_artifact import validate_canonical_market_artifact
from core.source_artifacts.object_package import build_object_package, validate_object_package
from core.source_artifacts.publication import PublicationError
from core.source_artifacts.source_set_v2 import governed_config_hashes, source_set_semantic_sha256
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh import august_acs_correction as contract
from jobs.monthly_refresh.acs_authoritative_write import CurrentCatalog, read_at_commit, write_current, verify_at_commit
from jobs.monthly_refresh.acs_history_family import validate_cohort
from jobs.monthly_refresh.acs_scope import ACSGovernance
from jobs.monthly_refresh.cohort_promotion_hosted import _verify_object_record, _publish_object

PROTECTED = (contract.READINESS_PATH, contract.RESOLUTION_PATH,
    f'config/cohort_promotion_records/{contract.CYCLE}.json',
    f'config/cohort_correction_records/{contract.CYCLE}.json')
CODE = ('core', 'jobs', 'sources', 'requirements.txt')


def clean_code(base):
    if subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip() != base:
        raise PublicationError('checkout must remain at the reviewed production commit')
    subprocess.run(['git','diff','--exit-code',base,'--',*CODE,'config'], check=True,
                   stdout=subprocess.DEVNULL)
    # New/untracked adapters cannot borrow the provenance of an older main.
    for path in ('jobs/monthly_refresh/august_acs_correction.py',
                 'jobs/monthly_refresh/august_acs_correction_hosted.py'):
        subprocess.run(['git','cat-file','-e',base+':'+path],check=True,
                       stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)


def context(api):
    governance = ACSGovernance.load_production(api.repository, api.token)
    governance.require_destination(contract.REPOSITORY)
    tip = governance.current_commit(api.repository)
    catalog, oid = read_at_commit(api, contract.CATALOG_PATH, tip)
    validate_catalog(catalog)
    protected = {p:read_at_commit(api,p,tip)[0] for p in PROTECTED}
    return governance, tip, catalog, oid, protected


def review_record(record):
    contract.check(record, 'correction_id', contract.VERSION)
    if (record.get('schema_version') != contract.VERSION or record.get('status') != 'prepared_inactive'
            or record.get('acceptance_contract') != 'atomic_catalog_three_pointer_cas_v1'):
        raise PublicationError('invalid immutable ACS correction record')
    contract.review_check(record['review'])
    check_journal(record['publication_journal'],record['review'],complete=True)
    for kind, target in record['review']['targets'].items():
        published = record['published'][kind]
        if any(published[k] != target[k] for k in ('object_id','object_type','artifact_content_hash','package_sha256','metadata')):
            raise PublicationError('correction receipt/review mismatch')
        events=record['publication_journal']['events']
        observed=[e for e in events if e['kind']==kind and e['phase']=='observed']
        if (observed[-1]['release_id']!=published['release_id'] or observed[-1]['asset_id']!=published['asset_id']):
            raise PublicationError('publication journal/receipt identity mismatch')
    return record


def fence(api, governance, review, *, accepting=False, record=None):
    """Only this review's append-only publications/record may advance main."""
    contract.review_check(review); governance.require_destination(contract.REPOSITORY)
    if set(review['predecessor']['protected_sha256']) != set(PROTECTED):
        raise PublicationError('reviewed August evidence bindings incomplete')
    if governance.production_snapshot()['registry_sha256'] != review['predecessor']['registry_sha256']:
        raise PublicationError('production ACS governance changed')
    tip = governance.current_commit(api.repository)
    base = review['predecessor']['commit']
    journal_changed=False
    predecessor,_ = read_at_commit(api,contract.CATALOG_PATH,base)
    if (predecessor['accepted'] != review['predecessor']['accepted']
            or predecessor['immutable_records'] != review['predecessor']['immutable_records']):
        raise PublicationError('review does not match its immutable predecessor snapshot')
    if tip != base:
        comparison,_ = api.request('GET',f'/compare/{base}...{tip}')
        journal_changed=any(f['filename']==contract.JOURNAL_PATH for f in comparison.get('files',[]))
        if (comparison.get('merge_base_commit',{}).get('sha') != base
                or not comparison.get('files')
                or any(f['filename'] not in {contract.CATALOG_PATH,contract.RECORD_PATH,contract.JOURNAL_PATH}
                       for f in comparison['files'])):
            raise PublicationError('unreviewed production main advance')
    catalog,oid = read_at_commit(api,contract.CATALOG_PATH,tip)
    validate_catalog(catalog)
    allowed = [review['predecessor']['accepted']]
    if accepting: allowed.append(contract.target_accepted(review))
    if catalog['accepted'] not in allowed:
        raise PublicationError('accepted predecessor changed or partial transition')
    baseline = {(r['object_type'],r['object_id']):r for r in review['predecessor']['immutable_records']}
    current = {(r['object_type'],r['object_id']):r for r in catalog['immutable_records']}
    if any(current.get(k) != v for k,v in baseline.items()):
        raise PublicationError('predecessor immutable catalog changed')
    allowed_new = {(v['object_type'],v['object_id']):v for v in review['targets'].values()}
    for identity in current.keys()-baseline.keys():
        r=current[identity]; t=allowed_new.get(identity)
        if t is None or any(r[k]!=t[k] for k in ('object_type','object_id','artifact_content_hash','package_sha256','metadata')):
            raise PublicationError('unreviewed immutable publication')
    for p,digest in review['predecessor']['protected_sha256'].items():
        if sha256_json(read_at_commit(api,p,tip)[0]) != digest:
            raise PublicationError('protected August evidence changed')
    stored,_ = read_at_commit(api,contract.RECORD_PATH,tip,allow_missing=True)
    journal,_ = read_at_commit(api,contract.JOURNAL_PATH,tip,allow_missing=True)
    if journal_changed and journal is None:
        raise PublicationError('durable publication intent unavailable; cannot restart as absent')
    if journal is not None: check_journal(journal,review)
    if stored is not None:
        review_record(stored)
        if stored['review'] != review or (record is not None and stored != record):
            raise PublicationError('conflicting ACS successor attempt')
        if stored['publication_journal']!=journal:
            raise PublicationError('prepared publication journal changed')
    elif record is not None:
        raise PublicationError('durable correction record missing')
    if governance.current_commit(api.repository) != tip:
        raise PublicationError('production main advanced during correction fence')
    return catalog,oid,tip,stored


class FencedAPI:
    """Local publication guard; exhaustive draft discovery prevents replacement.

    Lost mutation responses propagate. On restart exact remote objects are
    rediscovered; existing asset bytes are verified by the shared publisher.
    No uncertain mutation is automatically replayed and no deletion is exposed.
    """
    def __init__(self, api, guard):
        self.api,self.guard=api,guard
        self.repository,self.token=api.repository,api.token

    def request(self, method, url, **kwargs):
        if method == 'DELETE': raise PublicationError('ACS correction cannot delete remote objects')
        if method!='GET' and (url.startswith('/releases') or url.startswith('https://uploads.')):
            raise PublicationError('release mutations require durable publication intent')
        if method != 'GET': self.guard()
        if method == 'GET' and url.startswith('/releases/tags/'):
            tag=unquote(url.split('/releases/tags/',1)[1]); matches=[]
            for page in range(1,1001):
                items,page_headers=self.api.request('GET',f'/releases?per_page=100&page={page}')
                if not isinstance(items,list): raise PublicationError('incomplete release discovery')
                matches.extend(r for r in items if r.get('tag_name')==tag)
                has_next='rel="next"' in page_headers.get('Link',page_headers.get('link',''))
                if not items and has_next: raise PublicationError('release pagination returned an empty intermediate page')
                if len(items)<100 and not has_next: break
            else: raise PublicationError('release discovery pagination incomplete')
            tagged,headers=self.api.request(method,url,**kwargs)
            ids={r['id'] for r in matches}
            if tagged is not None:
                if tagged['id'] not in ids: raise PublicationError('release listing/tag lookup inconsistent')
                ids.add(tagged['id'])
            if len(ids)>1: raise PublicationError('duplicate/conflicting correction releases')
            if not ids: return None,headers
            rid=ids.pop(); release,_=self.api.request('GET',f'/releases/{rid}')
            if release.get('id')!=rid or release.get('tag_name')!=tag:
                raise PublicationError('numeric correction release identity mismatch')
            if not release.get('draft') and (tagged is None or tagged.get('id')!=rid):
                raise PublicationError('published correction tag lookup inconsistent')
            return release,headers
        return self.api.request(method,url,**kwargs)

    def download_asset(self,*args,**kwargs): return self.api.download_asset(*args,**kwargs)



def check_journal(value, review, *, complete=False):
    if (set(value)!={'schema_version','review_id','targets_sha256','events'}
            or value['schema_version']!='august_acs_publication_intent_v1'
            or value['review_id']!=review['review_id']
            or value['targets_sha256']!=sha256_json(review['targets'])):
        raise PublicationError('conflicting publication intent binding')
    expected=[(k,op,phase) for k in ('source_set','canonical_market')
              for op in ('create','upload','finalize') for phase in ('intent','observed')]
    events=value['events']
    if len(events)>len(expected) or (complete and len(events)!=len(expected)):
        raise PublicationError('incomplete publication intent history')
    release_ids={};asset_ids={}
    for i,event in enumerate(events):
        if (set(event)!={'kind','operation','phase','intent_id','release_id','asset_id'}
                or (event['kind'],event['operation'],event['phase'])!=expected[i]
                or not isinstance(event['intent_id'],str) or not event['intent_id']):
            raise PublicationError('invalid publication intent sequence')
        kind,op,phase=expected[i]
        rid,aid=event['release_id'],event['asset_id']
        if phase=='observed' and event['intent_id']!=events[i-1]['intent_id']:
            raise PublicationError('publication observation has no matching intent')
        if op=='create' and phase=='intent':
            if rid is not None or aid is not None: raise PublicationError('invalid creation intent')
        else:
            if type(rid) is not int or rid<=0 or rid!=release_ids.setdefault(kind,rid):
                raise PublicationError('publication release identity changed')
            if op=='create':
                if aid is not None: raise PublicationError('invalid creation observation')
            elif op=='upload' and phase=='intent':
                if aid is not None: raise PublicationError('invalid upload intent')
            elif type(aid) is not int or aid<=0 or aid!=asset_ids.setdefault(kind,aid):
                raise PublicationError('publication asset identity changed')
    return value


class PublicationJournal:
    """Append-only, review-bound Git CAS intents; one winner before each POST.

    A pending intent never grants a restarted/concurrent process permission to
    repeat its mutation. It must first observe the exact authoritative outcome.
    """
    def __init__(self,api,governance,review):
        self.api,self.governance,self.review=api,governance,review
    def read(self):
        tip=self.governance.current_commit(self.api.repository)
        value,oid=read_at_commit(self.api,contract.JOURNAL_PATH,tip,allow_missing=True)
        if value is None:
            value={'schema_version':'august_acs_publication_intent_v1','review_id':self.review['review_id'],
                   'targets_sha256':sha256_json(self.review['targets']),'events':[]}
        return check_journal(value,self.review),oid
    def event(self,kind,operation):
        value,_=self.read()
        return next((e for e in reversed(value['events']) if e['kind']==kind and e['operation']==operation),None)
    def append(self,event):
        value,oid=self.read();value=deepcopy(value);value['events'].append(event)
        check_journal(value,self.review)
        api=FencedAPI(self.api,lambda:fence(self.api,self.governance,self.review))
        written=write_current(api=api,branch='main',path=contract.JOURNAL_PATH,value=value,expected_sha=oid,
            message='Record ACS publication '+event['operation']+' '+event['phase'],governance=self.governance)
        verify_at_commit(self.api,contract.JOURNAL_PATH,written,value)
    def intent(self,kind,operation):
        if self.event(kind,operation) is not None:
            raise PublicationError('publication outcome already pending/observed; mutation cannot be repeated')
        rid=aid=None
        if operation!='create':rid=self.event(kind,'create')['release_id']
        if operation=='finalize':aid=self.event(kind,'upload')['asset_id']
        self.append(dict(kind=kind,operation=operation,phase='intent',intent_id=uuid.uuid4().hex,
                         release_id=rid,asset_id=aid))
    def observe(self,kind,operation,rid,aid=None):
        event=self.event(kind,operation)
        if event is None:raise PublicationError('unrecorded remote publication outcome')
        observed={**event,'phase':'observed','release_id':rid,'asset_id':aid}
        if event['phase']=='observed':
            if event!=observed:raise PublicationError('inconsistent remote re-adoption')
            return
        self.append(observed)
    def cataloged(self,kind,record):
        # Called only after the existing immutable publisher verifies the exact
        # cataloged package. No remote release mutation occurs on this path.
        for op,aid in [('create',None),('upload',record['asset_id']),('finalize',record['asset_id'])]:
            event=self.event(kind,op)
            if event is None:self.intent(kind,op)
            self.observe(kind,op,record['release_id'],aid)


class PublicationAPI(FencedAPI):
    def __init__(self,api,guard,journal,kind):
        super().__init__(api,guard);self.journal,self.kind=journal,kind
        self.target=journal.review['targets'][kind]
        self.tag={'source_set':'source-set/','canonical_market':'canonical-market/'}[kind]+self.target['object_id']
        self.name=self.target['object_id']+'.tar'
    def reconcile(self,release):
        event=self.journal.event(self.kind,'create')
        if event is None:return
        rid=release.get('id')
        if release.get('tag_name')!=self.tag or (event['release_id'] is not None and event['release_id']!=rid):
            raise PublicationError('pinned publication release changed')
        self.journal.observe(self.kind,'create',rid)
        assets=release.get('assets',[])
        if len(assets)>1 or any(a.get('name')!=self.name for a in assets):
            raise PublicationError('unexpected/conflicting publication assets')
        upload=self.journal.event(self.kind,'upload')
        if assets and upload is None:
            raise PublicationError('unrecorded publication asset cannot be adopted')
        if assets and upload is not None:
            aid=assets[0]['id']
            with tempfile.TemporaryDirectory() as d:
                path=Path(d)/'asset.tar';self.api.download_asset(aid,path)
                if sha256_file(path)!=self.target['package_sha256']:
                    raise PublicationError('observed publication asset bytes changed')
            self.journal.observe(self.kind,'upload',rid,aid)
        final=self.journal.event(self.kind,'finalize')
        if not release.get('draft'):
            tagged,_=self.api.request('GET','/releases/tags/'+quote(self.tag,safe=''),expected=(200,404))
            if tagged is None or tagged.get('id')!=rid:
                raise PublicationError('published release tag identity unavailable')
            if final is not None:
                if not assets:raise PublicationError('published asset missing')
                self.journal.observe(self.kind,'finalize',rid,assets[0]['id'])
    def request(self,method,url,**kwargs):
        if method=='GET' and url.startswith('/releases/tags/'):
            release,headers=super().request(method,url,**kwargs)
            event=self.journal.event(self.kind,'create')
            if event is not None and event['release_id'] is not None:
                pinned,_=self.api.request('GET',f"/releases/{event['release_id']}",expected=(200,404))
                if pinned is None:raise PublicationError('pinned release missing; replacement prohibited')
                if release is not None and release['id']!=pinned['id']:raise PublicationError('conflicting pinned release')
                release=pinned
            elif event is not None and release is None:
                raise PublicationError('uncertain release creation cannot be established; replacement prohibited')
            elif event is None and release is not None:
                raise PublicationError('unrecorded release cannot be silently adopted')
            if release is not None:self.reconcile(release)
            return release,headers
        if method=='GET' and url.startswith('/releases/') and url.split('/')[-1].isdigit():
            release,headers=self.api.request(method,url,**kwargs)
            if release is not None:self.reconcile(release)
            return release,headers
        operation=None
        if method=='POST' and url=='/releases':operation='create'
        elif method=='POST' and url.startswith('https://uploads.github.com/'):operation='upload'
        elif method=='PATCH' and url.startswith('/releases/'):
            if kwargs.get('payload',{}).get('draft') is not False:raise PublicationError('invalid finalization')
            operation='finalize'
        if operation is not None:
            self.guard();self.journal.intent(self.kind,operation)
            # Do not retry any uncertain remote mutation. Its intent is already
            # durable, even when the response never reaches this process.
            result,headers=self.api.request(method,url,**kwargs)
            if operation=='create':self.reconcile(result)
            elif operation=='upload':
                if result.get('name')!=self.name:raise PublicationError('unexpected upload identity')
                self.journal.observe(self.kind,'upload',self.journal.event(self.kind,'create')['release_id'],result['id'])
            else:self.reconcile(result)
            return result,headers
        return super().request(method,url,**kwargs)

def verified_inputs(api, governance, catalog, protected, workspace):
    """Revalidate published ACS, reuse the nine exact entries and assemble locally."""
    workspace.mkdir(parents=True,exist_ok=True)
    resolution=protected[contract.RESOLUTION_PATH]
    resolver=GitHubReleaseArtifactResolver(catalog,api,workspace/'sources')
    validate_cohort(resolution,catalog=catalog,resolver=resolver,governance=governance)
    acs_path=resolver.resolve(f'artifact://source/acs/{contract.NEW_ACS}')
    manifest=validate_artifact(acs_path,expected_source_id='acs')['manifest']
    old_set_dir=_verify_object_record(api,contract.exact(catalog,'source_set',contract.OLD_SET),workspace/'old-set')
    old_market_dir=_verify_object_record(api,contract.exact(catalog,'canonical_market',contract.OLD_MARKET),workspace/'old-market')
    old=json.loads((old_set_dir/'source-set.json').read_text())
    old_market=validate_canonical_market_artifact(json.loads((old_market_dir/'canonical-market.json').read_text()))
    if (old_market['market_artifact_id'] != contract.OLD_MARKET
            or old_market['source_set_id'] != contract.OLD_SET
            or old_market['source_set_semantic_sha256'] != source_set_semantic_sha256(old)
            or old_market['source_set_package_sha256'] != contract.exact(catalog,'source_set',contract.OLD_SET)['package_sha256']):
        raise PublicationError('accepted canonical/Source Set binding mismatch')
    if {s['source_id']:s['artifact_id'] for s in old['sources']} != {
            **catalog['accepted']['source'],'acs':contract.OLD_ACS}:
        raise PublicationError('accepted Source Set/source pointer mismatch')
    return old,old_market_dir,resolver,manifest,resolution


def construct(api, governance, catalog, protected, workspace):
    old,old_market_dir,resolver,manifest,resolution=verified_inputs(api,governance,catalog,protected,workspace)
    source_set=contract.successor(workspace/'source-set.json',old=old,catalog=catalog,
        resolution=resolution,manifest=manifest,config_hashes=governed_config_hashes())
    ss=build_object_package({'source-set.json':workspace/'source-set.json'},workspace/'source-set.tar')
    validation=assemble_source_set_v2(source_set,workspace/'market.duckdb',resolver)
    contract.factual_delta(old_market_dir/'market.duckdb',workspace/'market.duckdb')
    market=create_canonical_market_manifest(workspace/'canonical-market.json',
        database_path=workspace/'market.duckdb',supersedes_artifact_id=contract.OLD_MARKET,
        source_set_id=source_set['source_set_id'],source_set_semantic_sha256=source_set_semantic_sha256(source_set),
        source_set_package_sha256=ss['package_sha256'],canonical_assembly_contract_version='canonical_market_assembly_v1',
        canonical_schema_identity='canonical-timeseries-v1',config_hashes=source_set['config_hashes'],
        builder_contract_identity='core-source-artifacts-assembly-v1',
        dependency_lock_identity='requirements-sha256:'+sha256_file(Path('requirements.txt')),
        assembly_revision=3,compressed_package_sha256=sha256_file(workspace/'market.duckdb'),
        table_inventory=['fact_timeseries','source_artifact_metadata'],
        **{k:validation[k] for k in ('row_count','source_count','geography_count','metric_count','first_date','last_date','duplicate_key_count')},
        validation_status='passed',assembly_warnings=[],builder_git_sha=contract.BUILD_ID,built_at=contract.BUILD_TIME)
    mp=build_object_package({'canonical-market.json':workspace/'canonical-market.json',
        'market.duckdb':workspace/'market.duckdb'},workspace/'canonical-market.tar')
    targets={kind:{'object_type':kind,'object_id':identity,'artifact_content_hash':sha256_json(value),
                    'package_sha256':package['package_sha256'],
                    'metadata':({'cycle_id':contract.CYCLE,'source_set_semantic_sha256':source_set_semantic_sha256(source_set),
                                 'supersedes_artifact_id':contract.OLD_SET} if kind=='source_set' else
                                {'source_set_id':source_set['source_set_id'],'database_sha256':market['database_sha256'],
                                 'supersedes_artifact_id':contract.OLD_MARKET})}
             for kind,identity,value,package in (
                ('source_set',source_set['source_set_id'],source_set,ss),
                ('canonical_market',market['market_artifact_id'],market,mp))}
    return targets,{'source_set':{**ss,'manifest':source_set},'canonical_market':{**mp,'manifest':market}}



def retain_bundle(review,workspace,bundle):
    if bundle is None:raise PublicationError('validation requires a retained bundle directory')
    bundle.mkdir(parents=True,exist_ok=False)
    for kind,name in contract.PACKAGES.items():
        with (workspace/name).open('rb') as src,(bundle/name).open('xb') as dst:
            shutil.copyfileobj(src,dst);dst.flush();os.fsync(dst.fileno())
        if sha256_file(bundle/name)!=review['targets'][kind]['package_sha256']:
            raise PublicationError('retained candidate hash mismatch')
    write_canonical_json(bundle/'review.json',review)


def retained_candidates(review,bundle,workspace):
    if bundle is None or json.loads((bundle/'review.json').read_text())!=review:
        raise PublicationError('retained review missing or mismatched')
    result={}
    for kind,name in contract.PACKAGES.items():
        source=bundle/name
        if source.is_symlink() or not source.is_file():raise PublicationError('reviewed TAR missing or unsafe')
        copy=workspace/name;shutil.copyfile(source,copy)
        target=review['targets'][kind];manifest=review['successor_manifests'][kind]
        if sha256_file(copy)!=target['package_sha256']:raise PublicationError('reviewed TAR bytes changed')
        members=({'source-set.json':sha256_json(manifest)} if kind=='source_set' else
                 {'canonical-market.json':sha256_json(manifest),'market.duckdb':manifest['database_sha256']})
        result[kind]=validate_object_package(copy,workspace/('retained-'+kind),object_type=kind,
            expected={**target,'member_hashes':members})
    return result


def verify_candidates(review,paths,api,governance,catalog,protected,workspace):
    """Query retained/published DBs; never reconstruct DuckDB in write modes."""
    import duckdb
    import pandas as pd
    from core.source_artifacts.models import CANONICAL_COLUMNS,CANONICAL_KEY
    old,old_market_dir,resolver,acs_manifest,resolution=verified_inputs(api,governance,catalog,protected,workspace)
    expected=contract.successor(workspace/'expected-source-set.json',old=old,catalog=catalog,
        resolution=resolution,manifest=acs_manifest,config_hashes=governed_config_hashes())
    source_set=json.loads((paths['source_set']/'source-set.json').read_text())
    market=validate_canonical_market_artifact(json.loads((paths['canonical_market']/'canonical-market.json').read_text()))
    if (source_set!=expected or source_set!=review['successor_manifests']['source_set']
            or market!=review['successor_manifests']['canonical_market']
            or market['source_set_id']!=source_set['source_set_id']
            or market['source_set_semantic_sha256']!=source_set_semantic_sha256(source_set)
            or market['source_set_package_sha256']!=review['targets']['source_set']['package_sha256']
            or market.get('supersedes_artifact_id')!=contract.OLD_MARKET or market['assembly_revision']!=3
            or market['config_hashes']!=review['config_hashes']):
        raise PublicationError('reviewed successor semantic bindings changed')
    database=paths['canonical_market']/'market.duckdb'
    contract.factual_delta(old_market_dir/'market.duckdb',database)
    acs=resolver.resolve(f'artifact://source/acs/{contract.NEW_ACS}')
    expected_acs=pd.read_parquet(acs/acs_manifest['data_filename'])[CANONICAL_COLUMNS]
    with duckdb.connect(str(database),read_only=True) as db:
        schema=[(row[0],row[1]) for row in db.execute('DESCRIBE fact_timeseries').fetchall()]
        if schema!=list(zip(CANONICAL_COLUMNS,['VARCHAR','VARCHAR','DATE','VARCHAR','DOUBLE','VARCHAR','VARCHAR'])):
            raise PublicationError('reviewed canonical schema changed')
        actual=db.execute("SELECT * FROM fact_timeseries WHERE source_id='acs'").fetchdf()
        inputs=db.execute('SELECT * FROM source_artifact_metadata ORDER BY source_id').fetchall()
        counts=db.execute('SELECT count(*),count(DISTINCT source_id),count(DISTINCT geo_id),count(DISTINCT metric_id),min(date),max(date) FROM fact_timeseries').fetchone()
        duplicates=db.execute('SELECT count(*) FROM (SELECT geo_id,metric_id,date,property_type_id FROM fact_timeseries GROUP BY ALL HAVING count(*)>1)').fetchone()[0]
    for frame in (expected_acs,actual):
        frame['date']=pd.to_datetime(frame['date']).dt.date
        for name in set(CANONICAL_COLUMNS)-{'date','value'}:frame[name]=frame[name].astype('string')
    try:
        pd.testing.assert_frame_equal(expected_acs.sort_values(CANONICAL_KEY).reset_index(drop=True),
            actual.sort_values(CANONICAL_KEY).reset_index(drop=True),check_exact=True)
    except AssertionError as exc:raise PublicationError('canonical ACS facts differ from published logical ACS') from exc
    expected_inputs=sorted((e['source_id'],e['artifact_id'],source_set['source_set_id']) for e in source_set['sources'])
    stats=(*[market[k] for k in ('row_count','source_count','geography_count','metric_count')],market['first_date'],market['last_date'])
    if inputs!=expected_inputs or (*counts[:4],str(counts[4]),str(counts[5]))!=stats or duplicates:
        raise PublicationError('reviewed canonical input inventory/statistics mismatch')

def execute(*, mode, api, workspace, supplied='', review=None, record=None, bundle=None):
    if api.repository != contract.REPOSITORY: raise PublicationError('wrong correction repository')
    if mode not in {'validate','prepare','accept'}: raise PublicationError('unknown correction mode')
    if mode=='validate' and not api.read_only: raise PublicationError('validation requires GET-only transport')
    if mode!='validate':
        if mode=='prepare': contract.review_check(review); value=review
        else: review_record(record); review=record['review']; value=record
        if supplied != contract.authorization(mode,value):
            raise PublicationError('explicit exact reviewed correction authorization required')
        clean_code(review['predecessor']['commit'])
        if sha256_file(Path('requirements.txt')) != review['requirements_sha256']:
            raise PublicationError('reviewed dependency requirements changed')
    governance,tip,catalog,oid,protected=context(api)
    configs=governed_config_hashes()
    if mode=='validate':
        clean_code(tip); accepted=contract.origin(catalog)
        existing,_=read_at_commit(api,contract.RECORD_PATH,tip,allow_missing=True)
        if existing is not None: raise PublicationError('correction already prepared; reuse its original review')
    else:
        if configs != review['config_hashes']: raise PublicationError('reviewed configuration drift')
        catalog,oid,tip,stored=fence(api,governance,review,accepting=mode=='accept',record=record)
    workspace.mkdir(parents=True,exist_ok=True)
    if mode=='validate':
        targets,packages=construct(api,governance,catalog,protected,workspace)
        manifests={kind:value['manifest'] for kind,value in packages.items()}
        review=contract.seal({'schema_version':contract.VERSION,'repository':contract.REPOSITORY,
            'resolution_id':contract.RESOLUTION,'acs_package_sha256':contract.NEW_SHA,'config_hashes':configs,
            'requirements_sha256':sha256_file(Path('requirements.txt')),
            'successor_manifests':manifests,
            'predecessor':{'commit':tip,'accepted':accepted,'registry_sha256':governance.production_snapshot()['registry_sha256'],
              'immutable_records':deepcopy(catalog['immutable_records']),
              'protected_sha256':{p:sha256_json(v) for p,v in protected.items()}},
            'targets':targets},'review_id',contract.VERSION+'_review')
        if read_at_commit(api,contract.JOURNAL_PATH,tip,allow_missing=True)[0] is not None:
            raise PublicationError('publication already started; retain and reuse its original review')
        fence(api,governance,review)
        retain_bundle(review,workspace,bundle)
        return {'result':'PASS','mode':mode,'review':review,
                'preparation_authorization':contract.authorization('prepare',review),'production_writes':False}
    targets=review['targets']
    if mode=='prepare':
        paths=retained_candidates(review,bundle,workspace)
        verify_candidates(review,paths,api,governance,catalog,protected,workspace/'semantic-check')
        journal=PublicationJournal(api,governance,review)
        published={}
        for kind,filename in [('source_set','source-set'),('canonical_market','canonical-market')]:
            fence(api,governance,review)
            target=targets[kind]
            guarded=PublicationAPI(api,lambda:fence(api,governance,review),journal,kind)
            cas=CurrentCatalog(GitHubCatalogCAS(guarded,contract.CATALOG_PATH,'main'),governance)
            manifest=review['successor_manifests'][kind]
            members=({'source-set.json':sha256_json(manifest)} if kind=='source_set' else
                     {'canonical-market.json':sha256_json(manifest),'market.duckdb':manifest['database_sha256']})
            published[kind]=_publish_object(api=guarded,cas=cas,package=workspace/(filename+'.tar'),
                object_id=target['object_id'],object_type=kind,content_hash=target['artifact_content_hash'],
                metadata=target['metadata'],
                members=members,git_sha=review['predecessor']['commit'])
            journal.cataloged(kind,published[kind])
        proposed=contract.seal({'schema_version':contract.VERSION,'status':'prepared_inactive',
            'acceptance_contract':'atomic_catalog_three_pointer_cas_v1','review':review,'published':published,'publication_journal':journal.read()[0]},
            'correction_id',contract.VERSION)
        _,_,tip,existing=fence(api,governance,review)
        if existing is not None:
            if existing!=proposed: raise PublicationError('contradictory immutable correction record')
        else:
            write_current(api=guarded,branch='main',path=contract.RECORD_PATH,value=proposed,expected_sha=None,
                message='Prepare '+proposed['correction_id'],governance=governance)
        _,_,_,durable=fence(api,governance,review,record=proposed)
        return {'result':'PASS','mode':mode,'record':durable,
                'acceptance_authorization':contract.authorization('accept',durable),'accepted_state_changed':False}
    # Acceptance uses only durable records and published packages, not local
    # retained files or a newly assembled database.
    paths={}
    for kind,target in targets.items():
        published=contract.exact(catalog,kind,target['object_id'])
        if published != record['published'][kind]: raise PublicationError('durable publication record changed')
        paths[kind]=_verify_object_record(api,published,workspace/('published-'+kind))
    verify_candidates(review,paths,api,governance,catalog,protected,workspace/'semantic-check')
    catalog,oid,tip,_=fence(api,governance,review,accepting=True,record=record)
    updated,changed=contract.atomic_delta(catalog,review)
    if changed:
        updated['compare_and_swap']['expected_git_blob_sha']=oid
        guarded=FencedAPI(api,lambda:fence(api,governance,review,accepting=True,record=record))
        written=write_current(api=guarded,branch='main',path=contract.CATALOG_PATH,value=updated,expected_sha=oid,
            message='Accept atomic '+record['correction_id'],governance=governance)
        verify_at_commit(api,contract.CATALOG_PATH,written,updated)
    final,_,_,_=fence(api,governance,review,accepting=True,record=record)
    if final['accepted']!=contract.target_accepted(review): raise PublicationError('atomic acceptance not confirmed')
    return {'result':'PASS','mode':mode,'correction_id':record['correction_id'],
            'accepted_state_changed':changed,'atomic':True,'serving_changed':False,'readiness_changed':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode',choices=['validate','prepare','accept'])
    parser.add_argument('--review',type=Path);parser.add_argument('--record',type=Path)
    parser.add_argument('--bundle-dir',type=Path)
    parser.add_argument('--authorization',default='');parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    if ((args.mode=='prepare' and (args.review is None or args.record is not None))
            or (args.mode=='accept' and (args.record is None or args.review is not None))
            or (args.mode=='validate' and (args.review or args.record or args.authorization))):
        parser.error('validate takes no authorization/input; prepare requires --review; accept requires --record')
    if (args.mode in {'validate','prepare'} and args.bundle_dir is None) or (args.mode=='accept' and args.bundle_dir is not None):
        parser.error('validate/prepare require --bundle-dir; accept reads published bytes only')
    root=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],text=True).strip()).resolve()
    if args.output.resolve().is_relative_to(root) or (args.bundle_dir and args.bundle_dir.resolve().is_relative_to(root)):
        parser.error('--output must be outside the repository; production files are never report destinations')
    api=GitHubAPI(contract.REPOSITORY,os.environ.get('GITHUB_TOKEN',''),read_only=args.mode=='validate')
    with tempfile.TemporaryDirectory(prefix='august-acs-correction-') as directory:
        report=execute(mode=args.mode,api=api,workspace=Path(directory),supplied=args.authorization,
            review=json.loads(args.review.read_text())['review'] if args.review else None,
            record=json.loads(args.record.read_text())['record'] if args.record else None,bundle=args.bundle_dir)
    write_canonical_json(args.output,report)
    print(json.dumps(report,sort_keys=True));return 0


if __name__=='__main__': raise SystemExit(main())
