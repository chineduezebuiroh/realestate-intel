"""Synthetic HTTP evidence only. No real owner authorization is created."""
import base64
from copy import deepcopy
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import pytest

from core.source_artifacts.hashing import sha256_json
from jobs.monthly_refresh import acs_governance_installation as install
from jobs.monthly_refresh import acs_governance_provenance as auth
from jobs.monthly_refresh.acs_scope import identify, GAP_VERSION, SCOPE_VERSION

ROOT = Path('.').resolve()
SHA = 'f' * 40
PR = 999999
OWNER = {'id': 240767978, 'login': 'chineduezebuiroh', 'type': 'User'}
STAMP = '2026-10-24T00:00:00Z'
BASE = f'https://api.github.com/repos/{install.REPOSITORY}'


class Fixture:
    def __init__(self, monkeypatch, *, review=False):
        proposal = install.generate(ROOT)
        rationales = {k: 'Synthetic HTTP fixture; never a human approval.' for k in proposal['records']}
        self.body = auth.decision_body(proposal, PR, SHA, rationales)
        self.kind = 'github_approved_review' if review else 'github_owner_decision_comment'
        self.user = {'id': 12345, 'login': 'synthetic-reviewer', 'type': 'User'} if review else deepcopy(OWNER)
        anchor = 'pullrequestreview' if review else 'issuecomment'
        self.obj = {'id': 100, 'html_url': f'https://github.com/{install.REPOSITORY}/pull/{PR}#{anchor}-100',
                    'user': self.user, 'body': json.dumps(self.body, indent=2),
                    'performed_via_github_app': None}
        if review:
            self.obj.update(state='APPROVED', commit_id=SHA, submitted_at=STAMP,
                            pull_request_url=f'{BASE}/pulls/{PR}')
        else:
            self.obj.update(created_at=STAMP, updated_at=STAMP, issue_url=f'{BASE}/issues/{PR}')
        self.comments = [] if review else [self.obj]
        self.reviews = [self.obj] if review else []; self.inline = []
        self.provenances = {}
        for name, template in proposal['approval_templates'].items():
            v = deepcopy(template)
            v.update(provenance_kind=self.kind, decision='approved', reviewer_login=self.user['login'],
                     reviewed_at=STAMP, reviewed_pr=PR, reviewed_commit=SHA,
                     review_url=self.obj['html_url'], decision_rationale=rationales[name])
            if not review:
                v['owner_comment'] = {'comment_id': 100, 'comment_url': self.obj['html_url'],
                    'author_login': self.user['login'], 'author_user_id': self.user['id'],
                    'created_at': STAMP, 'updated_at': STAMP, 'body_sha256': auth.body_hash(self.obj['body']),
                    'snapshot_sha256': sha256_json(auth.comment_snapshot(self.obj))}
            self.provenances[name] = v
        self.pull = {'number': PR, 'comments': len(self.comments), 'review_comments': 0, 'user': OWNER,
                     'base': {'ref': 'main', 'repo': {'full_name': install.REPOSITORY}},
                     'head': {'sha': SHA, 'repo': {'full_name': install.REPOSITORY}}}
        self.repo = {'id': 1085190171, 'full_name': install.REPOSITORY, 'owner': deepcopy(OWNER)}
        self.calls = []; self.headers = {}; self.page_size = 100; self.omit = False
        self.deleted = False; self.drift = False; self.scans = 0; self.numeric_links = False
        self.reviewed_proposal = proposal
        self.registry = None
        fixture = self
        class Response:
            def __init__(self, value, headers): self.value = value; self.headers = headers
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def getcode(self): return 200
            def read(self): return json.dumps(self.value).encode()
        class Opener:
            def open(self, request):
                assert request.method == 'GET'
                assert request.full_url.startswith(BASE)
                fixture.calls.append(request.full_url)
                parsed = urlparse(request.full_url); path = parsed.path.removeprefix('/repos/' + install.REPOSITORY)
                query = parse_qs(parsed.query)
                if path == '': value = fixture.repo
                elif path == '/commits/main': value = {'sha': 'a' * 40}
                elif path == '/contents/config/acs_historical_governance.json':
                    value = {'content': base64.b64encode(json.dumps(fixture.registry).encode()).decode()}
                elif path == f'/pulls/{PR}':
                    fixture.scans += 1
                    value = deepcopy(fixture.pull)
                    if fixture.drift and fixture.scans > 1: value['comments'] += 1
                elif path.startswith('/collaborators/'):
                    value = {'permission': 'write'}
                elif path == f'/contents/{install.PROPOSAL_PATH}':
                    assert query == {'ref': [SHA]}
                    value = {'content': base64.b64encode(json.dumps(fixture.reviewed_proposal).encode()).decode()}
                elif path in [f'/issues/{PR}/comments', f'/pulls/{PR}/reviews', f'/pulls/{PR}/comments']:
                    stream = fixture.comments if '/issues/' in path else (fixture.inline if path.endswith('/comments') else fixture.reviews)
                    page = int(query['page'][0]); assert query['per_page'] == ['100']
                    lo = (page - 1) * fixture.page_size
                    value = stream[lo:lo+fixture.page_size]
                    if fixture.omit and page > 1: value = []
                elif path in ['/issues/comments/100', f'/pulls/{PR}/reviews/100']:
                    value = None if fixture.deleted else fixture.obj
                else: raise AssertionError(request.full_url)
                headers = fixture.headers
                if fixture.numeric_links and path in [f'/issues/{PR}/comments', f'/pulls/{PR}/reviews', f'/pulls/{PR}/comments'] and value and lo+fixture.page_size < len(stream):
                    headers = {'Link': f'<https://api.github.com/repositories/1085190171{path}?page={page+1}&per_page=100>; rel="next"'}
                return Response(deepcopy(value), headers)
        monkeypatch.setattr('urllib.request.build_opener', lambda *a, **k: Opener())

    def verify(self, **kwargs):
        return auth.verify_authorizations(self.provenances, 'synthetic-token', ROOT, **kwargs)

    def resign_fixture(self):
        """Recompute forged hashes to prove bindings, not hash syntax, reject it."""
        self.obj['body'] = json.dumps(self.body)
        for v in self.provenances.values():
            if v['owner_comment']:
                v['owner_comment'].update(body_sha256=auth.body_hash(self.obj['body']),
                    snapshot_sha256=sha256_json(auth.comment_snapshot(self.obj)))

    def registry_with_fixture_provenance(self):
        p = install.generate(ROOT); registry = deepcopy(p['initial_registry'])
        gap = deepcopy(p['records']['acs1_2020_gap'])
        gap.update(status='approved', approval_provenance=self.provenances['acs1_2020_gap'])
        gap = identify(GAP_VERSION, gap, 'evidence_id'); registry['gap_evidence'][gap['evidence_id']] = gap
        for name in ('acs1_scope', 'acs5_scope'):
            scope = deepcopy(p['records'][name]); scope['approval_provenance'] = self.provenances[name]
            if name == 'acs1_scope': scope['governed_gaps'][0]['evidence_id'] = gap['evidence_id']
            scope = identify(SCOPE_VERSION, scope, 'authority_id')
            registry['scopes'][scope['authority_id']] = scope
            registry['current_scope'][scope['source_id']] = scope['authority_id']
        return registry


@pytest.mark.parametrize('review', [False, True])
def test_real_transport_shape_both_paths(monkeypatch, review):
    f = Fixture(monkeypatch, review=review)
    result = f.verify()
    assert set(result['evidence']) == set(f.provenances)
    assert result['production_authority'] is result['activation'] is False
    assert result['namespace'] == 'audit_only'
    assert all(result['checks'].values())
    assert f.verify(prior_snapshot=result)['evidence'] == result['evidence']
    assert any('page=2' in call for call in f.calls)
    if not review:
        assert len({v['owner_comment']['comment_id'] for v in f.provenances.values()}) == 1
        f.registry = f.registry_with_fixture_provenance()
        checked = install.verify_remote(install.REPOSITORY, 'synthetic-token', ROOT)
        assert checked['production_authority'] is True and checked['activation'] is False
        assert checked['authorization_audit']['namespace'] == 'audit_only'


@pytest.mark.parametrize('attack', ['bot', 'app', 'llm', 'fixture', 'author', 'user_id', 'repo', 'pr', 'sha', 'stale',
    'deleted', 'edited', 'hash', 'snapshot_hash', 'incomplete', 'rationale', 'subject', 'evidence', 'config', 'geo',
    'limitations', 'bool_coercion', 'ambiguous_json', 'wrong_url', 'wrong_pr_url', 'timestamp', 'proposal_drift', 'partial_provenance', 'kind', 'activation', 'race'])
def test_owner_authorization_rejections(monkeypatch, attack):
    f = Fixture(monkeypatch)
    if attack == 'bot': f.obj['user']['type'] = 'Bot'
    elif attack == 'app': f.obj['performed_via_github_app'] = {'id': 1}
    elif attack == 'llm': f.body['authorship'] = 'llm_generated'
    elif attack == 'fixture': f.body['namespace'] = 'test_only'
    elif attack == 'author': f.obj['user']['login'] = 'synthetic-stranger'
    elif attack == 'user_id': f.obj['user']['id'] = 999
    elif attack == 'repo': f.repo['full_name'] = 'other/repo'
    elif attack == 'pr': f.pull['number'] = 1
    elif attack == 'sha': f.body['reviewed_commit'] = '0' * 40
    elif attack == 'stale': f.pull['head']['sha'] = '0' * 40
    elif attack == 'deleted': f.deleted = True
    elif attack == 'edited': f.obj['updated_at'] = '2026-10-24T01:00:00Z'
    elif attack == 'hash': f.obj['body'] += '\n'
    elif attack == 'snapshot_hash':
        for v in f.provenances.values(): v['owner_comment']['snapshot_sha256'] = '0' * 64
    elif attack == 'incomplete': del f.body['decisions']['acs5_scope']
    elif attack == 'rationale': f.body['decisions']['acs5_scope']['decision_rationale'] = 'different'
    elif attack == 'subject': f.body['decisions']['acs1_scope']['subject']['proposal_identity'] = 'forged'
    elif attack == 'evidence': f.body['decisions']['acs1_2020_gap']['documentary_evidence']['sha256'] = '0' * 64
    elif attack == 'config': f.body['decisions']['acs5_scope']['config_hashes'] = {}
    elif attack == 'geo': f.body['decisions']['acs1_scope']['approved_request_scope']['requested_geographies'] = 224
    elif attack == 'limitations': f.body['decisions']['acs1_scope']['limitations'] = []
    elif attack == 'bool_coercion': f.body['personally_reviewed_and_authorized'] = 1
    elif attack == 'ambiguous_json': f.obj['body'] = f.obj['body'][:-1] + ', "reviewed_pr": 1}'
    elif attack == 'wrong_url': f.obj['html_url'] = 'https://github.com/other/repo/pull/1#issuecomment-100'
    elif attack == 'wrong_pr_url': f.obj['issue_url'] = BASE + '/issues/1'
    elif attack == 'timestamp': f.obj['created_at'] = '2026-10-24T00:00:01Z'
    elif attack == 'proposal_drift': f.reviewed_proposal['active'] = True
    elif attack == 'partial_provenance': del f.provenances['acs5_scope']
    elif attack == 'kind': f.provenances['acs5_scope']['provenance_kind'] = 'fixture'
    elif attack == 'activation': f.body['activate'] = True
    elif attack == 'race': f.drift = True
    if attack in {'llm','fixture','sha','incomplete','rationale','subject','evidence','config','geo','limitations','activation','bool_coercion'}:
        f.resign_fixture()
    with pytest.raises((ValueError, KeyError)): f.verify()


@pytest.mark.parametrize('stream', ['comments', 'reviews', 'inline'])
@pytest.mark.parametrize('body', ['I withdraw authorization.', 'Supersedes the prior decision.', 'Do not install: scope is incorrect.'])
def test_later_decisions_on_second_page_block(monkeypatch, stream, body):
    f = Fixture(monkeypatch); f.page_size = 1
    later = {'id': 101, 'user': OWNER, 'body': body, 'created_at': '2026-10-24T01:00:00Z',
             'updated_at': '2026-10-24T01:00:00Z', 'submitted_at': '2026-10-24T01:00:00Z'}
    if stream == 'comments': f.comments.append(later); f.pull['comments'] = 2
    else:
        getattr(f, stream).extend([{'id': 90, 'user': OWNER, 'submitted_at': '2026-10-23T00:00:00Z'}, later])
        if stream == 'inline': f.pull['review_comments'] = 2
    with pytest.raises(ValueError, match='subsequent'): f.verify()
    assert any('page=2' in call for call in f.calls)


def test_pagination_omissions_and_unsafe_next_link(monkeypatch):
    f = Fixture(monkeypatch); f.page_size = 1; f.omit = True
    f.comments.append({'id': 101}); f.pull['comments'] = 2
    with pytest.raises(ValueError, match='pagination'): f.verify()
    f.omit = False
    f.headers = {'Link': f'<{BASE}/issues/{PR}/comments?per_page=100&page=3>; rel="next"'}
    with pytest.raises(ValueError, match='pagination'): f.verify()


@pytest.mark.parametrize('change', ['hash', 'namespace', 'record'])
def test_retained_fixture_or_forged_snapshot_never_authorizes(monkeypatch, change):
    f = Fixture(monkeypatch); snapshot = f.verify()
    if change == 'hash': snapshot['snapshot_sha256'] = '0' * 64
    elif change == 'namespace':
        snapshot['schema_version'] = 'fixture'
        snapshot['snapshot_sha256'] = sha256_json({k:v for k,v in snapshot.items() if k != 'snapshot_sha256'})
    else:
        snapshot['evidence']['acs1_scope']['github_record']['body'] = 'forged'
        snapshot['snapshot_sha256'] = sha256_json({k:v for k,v in snapshot.items() if k != 'snapshot_sha256'})
    with pytest.raises(ValueError, match='snapshot'): f.verify(prior_snapshot=snapshot)
    f.deleted = True
    with pytest.raises(ValueError): f.verify(prior_snapshot=snapshot)


@pytest.mark.parametrize('attack', ['dismissed', 'self', 'sha', 'author'])
def test_approved_review_still_requires_actual_review(monkeypatch, attack):
    f = Fixture(monkeypatch, review=True)
    if attack == 'dismissed': f.obj['state'] = 'DISMISSED'
    elif attack == 'self': f.pull['user'] = f.user
    elif attack == 'sha': f.obj['commit_id'] = '0' * 40
    else: f.obj['user']['type'] = 'Bot'
    with pytest.raises(ValueError): f.verify()


def test_raw_fixture_not_accepted_as_transport():
    with pytest.raises(TypeError): auth.verify_authorizations({}, '', ROOT, api={})


def test_cli_preserves_audit_and_never_overwrites(monkeypatch, tmp_path):
    f = Fixture(monkeypatch)
    source = tmp_path / 'synthetic-provenance.json'; source.write_text(json.dumps(f.provenances))
    destination = tmp_path / 'synthetic-audit.json'
    argv = ['verify', '--provenance', str(source), '--audit-output', str(destination)]
    monkeypatch.setattr('sys.argv', argv); auth.main()
    snapshot = json.loads(destination.read_text()); assert snapshot['namespace'] == 'audit_only'
    with pytest.raises(SystemExit): auth.main()
    assert json.loads(destination.read_text()) == snapshot


def test_canonical_numeric_pagination_and_query_order(monkeypatch):
    f = Fixture(monkeypatch); f.page_size = 1; f.numeric_links = True
    f.comments.insert(0, {'id': 90, 'user': OWNER, 'body': 'Earlier discussion.',
                         'created_at': '2026-10-23T00:00:00Z', 'updated_at': '2026-10-23T00:00:00Z'})
    f.pull['comments'] = 2
    assert f.verify()['checks']['pagination_exhausted']
