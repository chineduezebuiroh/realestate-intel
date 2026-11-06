"""All HTTP and owner decisions below are offline fixtures, never approvals."""
from copy import deepcopy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import shutil
from types import SimpleNamespace
import urllib.error
from urllib.parse import urlparse, parse_qs, unquote

import pytest

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.hashing import canonical_json_bytes, sha256_file, sha256_json
from core.source_artifacts.publication import PublicationError
from jobs.monthly_refresh import acs_history_bootstrap_publication as pub
from jobs.monthly_refresh import acs_history_bootstrap as boot
# Reuse the existing real, offline 35-parent package/evidence fixture.
from test_acs_history_bootstrap import baseline, inputs, ROOT  # noqa: F401


def identify_bundle(bundle):
    bundle.pop('bundle_id', None)
    bundle['bundle_id'] = boot.BUNDLE_VERSION + '__' + sha256_json(bundle)
    return bundle


@pytest.fixture
def prepared(inputs, tmp_path):
    spec, governance = inputs
    directory = tmp_path / 'phase1'
    bundle = boot.prepare_bootstrap(spec, governance=governance, root=ROOT, output=directory)
    return spec, governance, directory, bundle


def test_independent_all_35_reconstruction_and_retained_candidate_bytes(prepared):
    spec, gov, directory, bundle = prepared
    # verify_local is a reconstruction primitive; Session separately requires a
    # genuine production capability, exact production bundle and live consent.
    bundle = deepcopy(bundle)
    bundle['verification']['evidence_kind'] = 'production_inputs'
    catalog = pub.read(spec['catalog'])
    before = {p: sha256_file(p) for p in directory.rglob('*') if p.is_file()}
    metadata = pub.verify_local(spec, bundle, directory, ROOT, gov, catalog)
    assert set(metadata) == set(pub.CANDIDATES)
    assert before == {p: sha256_file(p) for p in directory.rglob('*') if p.is_file()}
    for source in metadata:
        assert metadata[source]['object_metadata']['provider_release_id'] == bundle['candidates'][source]['state']['state_id']
        assert len(metadata[source]['member_hashes']) == 4


@pytest.mark.parametrize('mutation', ['parent', 'omission', 'state', 'lineage', 'delta', 'report', 'scope'])
def test_independent_reconstruction_fails_closed(prepared, tmp_path, mutation):
    spec, gov, directory, bundle = prepared
    bundle = deepcopy(bundle); bundle['verification']['evidence_kind'] = 'production_inputs'
    if mutation == 'parent': spec['parents'][0]['artifact_content_hash'] = '0' * 64
    if mutation == 'omission':
        value = pub.read(spec['evidence']['census_acs5:2023']); value['2023']['omission_evidence'].pop()
        path = tmp_path/'changed.json'; path.write_bytes(canonical_json_bytes(value)); spec['evidence']['census_acs5:2023'] = str(path)
    if mutation == 'state': bundle['candidates']['census_acs1']['state']['state_id'] = 'changed'
    if mutation == 'lineage': (directory/'candidates/census_acs1/lineage.parquet').write_bytes(b'changed')
    if mutation == 'delta': bundle['proposed_acceptance']['set_current_state'].pop('census_acs5')
    if mutation == 'report': bundle['compatibility']['limitations']['imputation'] = True
    if mutation == 'scope': spec['scope_authorities']['census_acs1'] = 'forged'
    with pytest.raises(Exception): pub.verify_local(spec, bundle, directory, ROOT, gov, pub.read(spec['catalog']))


class Remote:
    """Stdlib HTTP interception, including real publisher and GitHub CAS paths."""
    def __init__(self, request, registry, commit, baseline):
        self.review = request; self.registry = registry; self.commit = commit; self.baseline = baseline
        self.tip = commit; self.catalogs = {baseline: empty_catalog(), commit: empty_catalog()}
        self.owner = {'id': 7, 'login': 'chineduezebuiroh', 'type': 'User'}
        self.locator = {'reviewed_pr': 999, 'reviewed_commit': commit, 'comment_id': 100}
        self.repo = {'id': 5, 'full_name': pub.REPOSITORY, 'owner': self.owner}
        self.pull = {'number': 999, 'state': 'open', 'merged': False, 'base': {'ref': 'main', 'repo': {'full_name': pub.REPOSITORY}},
            'head': {'sha': commit, 'repo': {'full_name': pub.REPOSITORY}}, 'comments': 1, 'review_comments': 0}
        body = {'schema_version': 'acs_history_bootstrap_publication_decision_v1', 'repository': pub.REPOSITORY,
            'request_id': request['request_id'], 'request_sha256': sha256_json(request),
            'reviewed_pr': 999, 'reviewed_commit': commit, 'decision': 'authorize_exact_publication_only',
            'personally_reviewed_and_authorized': True, 'authorship': 'human_personal',
            'acknowledgements': pub.ACKNOWLEDGEMENTS, 'decision_rationale': 'Offline test only'}
        self.comment = {'id': 100, 'user': self.owner, 'html_url': f'https://github.com/{pub.REPOSITORY}/pull/999#issuecomment-100',
            'issue_url': f'https://api.github.com/repos/{pub.REPOSITORY}/issues/999',
            'created_at': '2026-11-06T00:00:00Z', 'updated_at': '2026-11-06T00:00:00Z',
            'body': json.dumps(body), 'performed_via_github_app': None}
        self.comments = [self.comment]; self.reviews = []; self.inline = []
        self.releases = {}; self.assets = {}; self.trees = {}; self.commits = {}
        self.calls = []; self.lose = None; self.code_drift = False; self.scan_drift = False
        self.scans = 0; self.omit_pages = False; self.concurrency = False; self.after = None

    def failure(self, url, status=404):
        raise urllib.error.HTTPError(url, status, 'fixture', {}, io.BytesIO(b'fixture rejection'))

    def open(self, req):
        url = urlparse(req.full_url); path = unquote(url.path.split('/realestate-intel', 1)[-1]); q = parse_qs(url.query)
        method = req.get_method(); self.calls.append((method, path))
        payload = json.loads(req.data) if req.data and req.get_header('Content-type') == 'application/json' else req.data
        value = None; status = 200; binary = False
        if method == 'GET':
            if path == '': value = self.repo
            elif path == '/commits/main': value = {'sha': self.tip}
            elif path == '/git/ref/heads/main': value = {'object': {'sha': self.tip}}
            elif path.startswith('/compare/'): value = {'merge_base_commit': {'sha': self.baseline}}
            elif path.startswith('/git/trees/'):
                value = {'truncated': False, 'tree': [{'path': p, 'type': 'blob', 'sha': ('0'*40 if self.code_drift else s)} for p, s in self.review['code_blobs'].items()]}
            elif path.startswith('/git/commits/'):
                bound = path.rsplit('/', 1)[-1]; value = {'sha': bound, 'tree': {'sha': 'tree-' + bound}}
            elif path == '/pulls/999':
                self.scans += 1; value = deepcopy(self.pull)
                if self.scan_drift and self.scans > 1: value['comments'] += 1
            elif path == '/issues/comments/100': value = self.comment
            elif path in ('/issues/999/comments', '/pulls/999/reviews', '/pulls/999/comments'):
                stream = self.comments if path.startswith('/issues/') else (self.inline if path.endswith('comments') else self.reviews)
                page = int(q['page'][0]); value = stream[(page-1)*100:page*100]
                if self.omit_pages: value = []
            elif path.startswith('/collaborators/'): value = {'permission': 'write'}
            elif path.startswith('/contents/'):
                name = path[len('/contents/'):]; ref = q['ref'][0]; ref = self.tip if ref == 'main' else ref
                if name == pub.CATALOG: data = self.catalogs[ref]
                elif name == 'config/acs_historical_governance.json': data = self.registry
                elif name == pub.REQUEST_PATH: data = self.review
                else: raise AssertionError(path)
                raw = canonical_json_bytes(data)
                value = {'content': __import__('base64').b64encode(raw).decode(), 'sha': pub._blob(raw)}
            elif path.startswith('/releases/tags/'):
                tag = path[len('/releases/tags/'):]
                value = next((v for v in self.releases.values() if v['tag_name'] == tag), None)
            elif path.startswith('/releases/assets/'):
                value = self.assets.get(int(path.rsplit('/', 1)[-1])); binary = True
            elif path.startswith('/releases/'): value = self.releases.get(int(path.rsplit('/', 1)[-1]))
            else: raise AssertionError((method, path))
            if value is None: self.failure(req.full_url)
        elif method == 'POST' and path == '/releases':
            rid = len(self.releases) + 1
            value = {'id': rid, 'tag_name': payload['tag_name'], 'assets': [], 'draft': True,
                'published_at': None, 'upload_url': f'https://api.github.com/repos/{pub.REPOSITORY}/uploads/{rid}'}
            self.releases[rid] = value; status = 201
        elif method == 'POST' and path.startswith('/uploads/'):
            rid = int(path.rsplit('/', 1)[-1]); aid = len(self.assets) + 1000
            value = {'id': aid, 'name': q['name'][0]}; self.assets[aid] = payload
            self.releases[rid]['assets'].append(value); status = 201
        elif method == 'PATCH' and path.startswith('/releases/'):
            value = self.releases[int(path.rsplit('/', 1)[-1])]
            value['draft'] = False; value['published_at'] = '2026-11-06T00:01:00Z'
        elif method == 'POST' and path == '/git/trees':
            tid = 'tree' + str(len(self.trees)); self.trees[tid] = json.loads(payload['tree'][0]['content'])
            value = {'sha': tid}; status = 201
        elif method == 'POST' and path == '/git/commits':
            sha = hashlib.sha1(str(len(self.commits)).encode()).hexdigest()
            self.commits[sha] = payload; self.catalogs[sha] = self.trees[payload['tree']]
            value = {'sha': sha}; status = 201
        elif method == 'PATCH' and path == '/git/refs/heads/main':
            sha = payload['sha']; assert payload['force'] is False
            if self.commits[sha]['parents'] != [self.tip] or self.concurrency: self.failure(req.full_url, 422)
            self.tip = sha; value = {'object': {'sha': sha}}
        else: raise AssertionError((method, path))
        if self.after is not None:
            self.after(method, path)
        if method != 'GET' and self.lose == path:
            self.lose = None
            raise urllib.error.URLError('lost response after committed mutation')
        raw = value if binary else json.dumps(value).encode()
        class Response:
            headers = {}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def getcode(self): return status
            def read(self, *args):
                if hasattr(self, 'sent'): return b''
                self.sent = True; return raw
            def close(self): pass
        return Response()


@pytest.fixture(scope='module')
def publication_fixture(baseline, tmp_path_factory):
    # The control-plane tests do not repeatedly rebuild the same 35-parent
    # fixture. Independent reconstruction has its own intact adversarial cases.
    directory = tmp_path_factory.mktemp('publication-fixture')
    with pytest.MonkeyPatch.context() as patch:
        spec, governance = inputs.__wrapped__(baseline, directory, patch)
        prepared = directory / 'phase1'
        bundle = boot.prepare_bootstrap(spec, governance=governance, root=ROOT, output=prepared)
    return spec, governance, prepared, bundle


@pytest.fixture
def lifecycle(publication_fixture, tmp_path, monkeypatch):
    spec, fixture_gov, original_directory, bundle = publication_fixture
    directory = tmp_path/'phase1'
    shutil.copytree(original_directory, directory)
    registry = pub.read(ROOT/'config/acs_historical_governance.json')
    catalog = tmp_path/'original-catalog.json'; catalog.write_bytes(canonical_json_bytes(empty_catalog()))
    spec = deepcopy(spec); spec['catalog'] = str(catalog)
    spec['governance'] = str(ROOT/'config/acs_historical_governance.json'); spec['scope_authorities'] = registry['current_scope']
    bundle = deepcopy(bundle); bundle.update(namespace='production', input_sha256=sha256_json(spec))
    commit = subprocess.check_output(['git','rev-parse','HEAD'], text=True).strip()
    baseline_commit = 'a'*40
    bundle['baseline'] = {'namespace': 'production', 'validated_main_commit': baseline_commit,
        'registry_sha256': sha256_json(registry), 'catalog_sha256': sha256_json(empty_catalog())}
    identify_bundle(bundle); (directory/'bundle.json').write_bytes(canonical_json_bytes(bundle))
    # Test-only candidate identities; production constants are never changed on disk.
    monkeypatch.setattr(pub, 'BUNDLE', bundle['bundle_id']); monkeypatch.setattr(pub, 'BASELINE', baseline_commit)
    monkeypatch.setattr(pub, 'CANDIDATES', {s: {k: c[k] for k in ('artifact_id','package_sha256')} | {'state_id': c['state']['state_id']} for s,c in bundle['candidates'].items()})
    review = pub.request(spec, bundle, directory, ROOT)
    remote = Remote(review, registry, commit, baseline_commit)
    monkeypatch.setattr('urllib.request.build_opener', lambda *args: remote)
    monkeypatch.setenv('GITHUB_TOKEN', 'offline-fixture-token')
    # The independent 35-parent reconstruction is tested intact above. Here only
    # that expensive step is isolated; all authority, publisher, remote receipt,
    # catalog CAS, retry and installation-evidence boundaries execute normally.
    metadata = {s: pub._metadata(directory/c['artifact_path'], c, commit) for s,c in bundle['candidates'].items()}
    calls = []
    def verified(*args): calls.append(1); return metadata
    monkeypatch.setattr(pub, 'verify_local', verified)
    def session():
        return pub.Session(spec=spec, directory=directory, root=ROOT, review=review,
            locator=remote.locator, journal=tmp_path/'journal')
    return SimpleNamespace(spec=spec, directory=directory, bundle=bundle, review=review, remote=remote,
        session=session, calls=calls, tmp=tmp_path)


def test_complete_publication_then_independent_verification_without_installation(lifecycle):
    f = lifecycle; original_registry = deepcopy(f.remote.registry)
    retained = {p: sha256_file(p) for p in f.directory.rglob('*') if p.is_file()}
    with f.session() as session: result = session.publish()
    assert result['verification'] == 'passed_publication_only'
    assert result['installation_authorization'] == 'required_separately'
    assert result['original_parents_verified'] == 35 and len(f.calls) == 2
    assert result['registry_installation'] == 'not_performed' and result['activation'] is False
    assert len(f.remote.releases) == 2 and len(f.remote.assets) == 2
    catalog = f.remote.catalogs[f.remote.tip]
    assert len(catalog['immutable_records']) == 2 and catalog['accepted'] == empty_catalog()['accepted']
    assert f.remote.registry == original_registry and not f.remote.registry['states']
    assert retained == {p: sha256_file(p) for p in f.directory.rglob('*') if p.is_file()}
    mutations = [v for v in f.remote.calls if v[0] != 'GET']
    with f.session() as session: second = session.verify_installation()
    assert second['records'] == result['records']
    assert mutations == [v for v in f.remote.calls if v[0] != 'GET']
    with f.session() as session: session.publish()
    assert mutations == [v for v in f.remote.calls if v[0] != 'GET']


@pytest.mark.parametrize('path', ['/releases', '/uploads/1', '/releases/1', '/git/refs/heads/main'])
def test_lost_response_recovery_never_replaces_packages_or_installs(lifecycle, path):
    f = lifecycle; f.remote.lose = path
    try:
        with f.session() as session: session.publish()
    except PublicationError: pass
    with f.session() as session: result = session.publish()
    assert result['verification'] == 'passed_publication_only'
    assert len(f.remote.releases) == 2 and len(f.remote.assets) == 2
    assert len(f.remote.catalogs[f.remote.tip]['immutable_records']) == 2
    assert not f.remote.registry['states'] and not f.remote.registry['current_state']


@pytest.mark.parametrize('mutation', ['missing', 'bot', 'app', 'edited', 'wrong_body', 'wrong_head', 'fork', 'closed', 'false_attestation', 'superseded', 'review_superseded', 'omitted', 'concurrent_scan', 'code', 'governance', 'accepted', 'catalog', 'package', 'parent_bytes', 'request'])
def test_no_production_write_on_failed_preflight(lifecycle, mutation):
    f = lifecycle; r = f.remote
    if mutation == 'missing': r.comment = None
    if mutation == 'bot': r.comment['user'] = {'id':7,'login':r.owner['login'],'type':'Bot'}
    if mutation == 'app': r.comment['performed_via_github_app'] = {'id':1}
    if mutation == 'edited': r.comment['updated_at'] = '2026-11-06T01:00:00Z'
    if mutation == 'wrong_body': r.comment['body'] = '{}'
    if mutation == 'false_attestation':
        body = json.loads(r.comment['body']); body['personally_reviewed_and_authorized'] = 1; r.comment['body'] = json.dumps(body)
    if mutation == 'closed': r.pull.update(state='closed', merged=False)
    if mutation == 'wrong_head': r.pull['head']['sha'] = 'b'*40
    if mutation == 'fork': r.pull['head']['repo']['full_name'] = 'attacker/fork'
    if mutation in {'superseded','review_superseded'}:
        later = {'id':101,'user':r.owner,'updated_at':'2026-11-06T01:00:00Z','body':'Withdraw approval'}
        if mutation == 'superseded': r.comments.append(later); r.pull['comments'] += 1
        else: r.reviews.append(later)
    if mutation == 'omitted': r.omit_pages = True
    if mutation == 'concurrent_scan': r.scan_drift = True
    if mutation == 'code': r.code_drift = True
    if mutation == 'governance': r.registry = {**r.registry,'changed':True}
    if mutation == 'accepted': r.catalogs[r.tip]['accepted']['source_set'] = 'unexpected'
    if mutation == 'catalog': r.catalogs[r.tip]['extra'] = 'unexpected'
    if mutation == 'package': (f.directory/'packages/census_acs1.tar').write_bytes(b'changed')
    if mutation == 'parent_bytes':
        # Do not mutate the shared module fixture; point at changed bytes instead.
        path = f.tmp/'bad-parent.tar'; path.write_bytes(b'changed')
        f.spec['packages'][next(iter(f.spec['packages']))] = str(path)
    if mutation == 'request': f.review['registry_installation_authorized'] = True
    with pytest.raises(Exception):
        with f.session() as session: session.publish()
    assert not [v for v in r.calls if v[0] != 'GET']


@pytest.mark.parametrize('mutation', ['asset', 'deleted_release', 'tag_reassigned', 'receipt', 'extra_catalog', 'missing_second'])
def test_recovery_rejects_conflicting_remote_or_catalog_evidence(lifecycle, mutation):
    f = lifecycle
    with f.session() as session: session.publish()
    if mutation == 'asset': f.remote.assets[1000] = b'corrupt'
    if mutation == 'deleted_release': del f.remote.releases[1]
    if mutation == 'tag_reassigned': f.remote.releases[1]['tag_name'] = 'wrong'
    if mutation == 'receipt':
        path = f.tmp/'journal/census_acs1.receipt.json'; value = pub.read(path); value['asset_id'] = 666; path.write_bytes(canonical_json_bytes(value))
    if mutation == 'extra_catalog': f.remote.catalogs[f.remote.tip]['accepted']['source']['census_acs1'] = 'wrong'
    if mutation == 'missing_second':
        f.remote.catalogs[f.remote.tip]['immutable_records'].pop()
    before = len([v for v in f.remote.calls if v[0] != 'GET'])
    with pytest.raises(Exception):
        with f.session() as session: session.verify_installation()
    assert len([v for v in f.remote.calls if v[0] != 'GET']) == before


def test_concurrent_catalog_advance_fails_closed(lifecycle):
    f = lifecycle; f.remote.concurrency = True
    with pytest.raises(PublicationError):
        with f.session() as session: session.publish()
    assert f.remote.tip == f.remote.commit and not f.remote.registry['states']
    f.remote.concurrency = False
    with f.session() as session: session.publish()
    assert len(f.remote.assets) == 2


def test_two_local_publishers_cannot_share_journal(lifecycle):
    f = lifecycle
    with f.session():
        with pytest.raises(BlockingIOError): f.session()
    with f.session(): pass


def test_no_network_or_approval_when_generating_review_request(lifecycle, monkeypatch):
    f = lifecycle
    before = len(f.remote.calls)
    value = pub.request(f.spec, f.bundle, f.directory, ROOT)
    assert value['approval_provenance'] is None and value['status'] == 'proposed'
    assert value['registry_installation_authorized'] is False
    assert value['operations'] == ['publish_exact_retained_packages','add_two_immutable_catalog_records']
    assert len(f.remote.calls) == before


def test_cli_errors_are_redacted(tmp_path, monkeypatch, capsys):
    p = tmp_path/'input.json'; p.write_text('{"secret-private-path":true}')
    monkeypatch.setattr('sys.argv', ['publication', '--inputs', str(p), '--bundle-directory', str(tmp_path),
        '--request', str(tmp_path/'request.json'), '--publish'])
    assert pub.main() == 2
    output = capsys.readouterr()
    assert 'secret-private-path' not in output.out + output.err
    assert json.loads(output.out)['activation'] is False


@pytest.mark.parametrize('drift', ['authorization', 'code', 'governance', 'catalog', 'provider'])
def test_fresh_checks_at_each_write_boundary(lifecycle, drift):
    f = lifecycle
    def changed(method, path):
        if method != 'POST' or path != '/releases':
            return
        f.remote.after = None
        if drift == 'authorization': f.remote.comment['updated_at'] = '2026-11-06T01:00:00Z'
        if drift == 'code': f.remote.code_drift = True
        if drift == 'governance': f.remote.registry = {**f.remote.registry, 'changed': True}
        if drift == 'catalog': f.remote.catalogs[f.remote.tip]['accepted']['source_set'] = 'wrong'
        if drift == 'provider':
            path = f.tmp/'changed-provider.json'; path.write_text('{}')
            f.spec['evidence']['census_acs5:2023'] = str(path)
    f.remote.after = changed
    with pytest.raises(Exception):
        with f.session() as session: session.publish()
    assert [(m,p) for m,p in f.remote.calls if m != 'GET'] == [('POST','/releases')]
    assert not f.remote.assets and not f.remote.registry['states']


def test_copied_audit_cannot_replace_live_comment(lifecycle):
    f = lifecycle
    f.remote.locator['snapshot_sha256'] = '0'*64
    with pytest.raises(ValueError, match='locator'):
        with f.session() as session: session.publish()
    assert not [v for v in f.remote.calls if v[0] != 'GET']


def test_fixture_governance_cannot_publish(lifecycle, monkeypatch):
    from jobs.monthly_refresh.acs_scope import ACSGovernance
    from test_acs_historical_materialization import registry
    f = lifecycle
    monkeypatch.setattr(pub.ACSGovernance, 'load_production', lambda *args: ACSGovernance.from_fixture(registry()))
    with pytest.raises(ValueError, match='test_only'):
        with f.session() as session: session.publish()
    assert not [v for v in f.remote.calls if v[0] != 'GET']


def test_repository_argument_cannot_verify_a_different_executing_checkout(tmp_path):
    with pytest.raises(ValueError, match='reviewed repository checkout'):
        pub.code_blobs(tmp_path)
