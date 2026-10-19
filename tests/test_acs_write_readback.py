"""B3/B5-shaped durable commit followed by unavailable mutable readback.

The incidents establish false negatives, not a particular cache or HTTP cause.
All transports, provider bytes and commit histories here are offline fixtures.
"""
import base64
import copy
import hashlib
import json
from urllib.parse import unquote

import pytest

from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import PublicationError, TransientPublicationError
from jobs.monthly_refresh import acs_backfill as backfill
from jobs.monthly_refresh import acs_authoritative_write as writes
from jobs.monthly_refresh.source_inputs import discover_persist_execute
from test_acs_backfill import Harness, FencedContentsAPI, FixturePlanFence


class CommitAPI(FencedContentsAPI):
    def __init__(self):
        super().__init__()
        self.snapshots = {self.tip: {}}
        self.mutable_reads = []
        self.reads = []
        self.bad_content = None
        self.missing_reads = 0
        self.transient_reads = 0
        self.ref_outcome = None
        self.drift_after_verification = False

    def request(self, method, url, *, payload=None, **kwargs):
        if method == 'GET' and url == '/git/ref/heads/main':
            return {'object': {'sha': self.tip}}, {}
        if method == 'GET' and url.startswith('/contents/'):
            path, ref = url[len('/contents/'):].split('?ref=')
            path = unquote(path)
            if ref == 'main':
                self.mutable_reads.append(url)
                return None, {}  # Historical mutable readback failure.
            self.reads.append((path, ref))
            snapshot = self.snapshots[ref]
            if path not in snapshot:
                return None, {}
            if self.transient_reads:
                self.transient_reads -= 1
                raise TransientPublicationError('fixture read unavailable')
            if self.missing_reads:
                self.missing_reads -= 1
                return None, {}
            value = snapshot[path]
            if self.bad_content == 'contradiction': value = {'contradiction': True}
            raw = canonical_json_bytes(value)
            blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
            if self.bad_content == 'blob': blob = '0' * 40
            if self.drift_after_verification:
                self.governance_changed = True
            return {'sha': blob, 'content': base64.b64encode(raw).decode()}, {}
        if method == 'PATCH' and self.ref_outcome == 'lost_before':
            self.writes.append((method, url, copy.deepcopy(payload)))
            raise TransientPublicationError('fixture response lost')
        response = super().request(method, url, payload=payload, **kwargs)
        if method == 'POST' and url == '/git/commits':
            return {'sha': hashlib.sha1(canonical_json_bytes(payload)).hexdigest()}, {}
        if method == 'PATCH':
            self.snapshots[self.tip] = copy.deepcopy(self.objects)
            if self.ref_outcome == 'lost_after':
                raise TransientPublicationError('fixture response lost')
            if self.ref_outcome == 'unexpected':
                return {'object': {'sha': 'f' * 40}}, {}
            if self.ref_outcome == 'advanced_after':
                self.tip = 'e' * 40
                raise TransientPublicationError('fixture response lost')
        return response


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(writes.time, 'sleep', lambda delay: None)


@pytest.fixture(scope='module', params=[2015, 2010], ids=['B3-2015', 'B5-2010'])
def acquired(request, tmp_path_factory):
    h = Harness(tmp_path_factory.mktemp(str(request.param)), (request.param,))
    output = h.execute()[0]
    return h, output['pin'], h.results[0]


def setup_store(acquired, kind):
    h, pin, result = acquired
    api = CommitAPI(); fence = FixturePlanFence(api); fence.plan = h.plan
    fence.write_events = []
    value = pin if kind == 'pin' else result
    store = (backfill.FencedPinStore if kind == 'pin' else backfill.FencedResultStore)(api, fence)
    return api, fence, store, copy.deepcopy(value)


@pytest.mark.parametrize('kind', ['pin', 'result'])
def test_exact_commit_verification_ignores_missing_mutable_read(acquired, kind):
    api, fence, store, value = setup_store(acquired, kind)
    assert store.put(value) == (value, True)
    written = store.last_commit_sha
    assert store.get(value['cycle_id'], value['source_id']) == value
    assert written == api.tip
    assert fence.write_events[-1]['state'] == 'verified_durable'
    assert any(ref == written for _, ref in api.reads)
    assert not api.mutable_reads
    before = copy.deepcopy(api.writes)
    assert store.put(value) == (value, False)
    assert api.writes == before


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('failure', ['contradiction', 'blob', 'missing', 'transient'])
def test_commit_content_fails_closed_without_repeating_mutation(acquired, kind, failure):
    api, fence, store, value = setup_store(acquired, kind)
    api.bad_content = failure
    api.missing_reads = 4 if failure == 'missing' else 0
    api.transient_reads = 4 if failure == 'transient' else 0
    with pytest.raises(PublicationError): store.put(value)
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1
    assert fence.write_events[-1]['state'] == 'ref_update_confirmed'
    # Readback failures did not undo durable progress. A subsequent attempt
    # reconciles that exact object and performs no second publication/write.
    api.bad_content = None
    before = copy.deepcopy(api.writes)
    assert store.put(value) == (value, False)
    assert api.writes == before


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('failure', ['missing', 'transient'])
def test_transient_commit_reads_have_bounded_backoff(acquired, kind, failure, monkeypatch):
    api, fence, store, value = setup_store(acquired, kind)
    delays = []; monkeypatch.setattr(writes.time, 'sleep', delays.append)
    # Exclude legitimate initial absence from the recorded retry delays.
    original = api.request
    def request(method, url, **kwargs):
        result = original(method, url, **kwargs)
        if method == 'PATCH':
            api.missing_reads = 2 if failure == 'missing' else 0
            api.transient_reads = 2 if failure == 'transient' else 0
            delays.clear()
        return result
    api.request = request
    assert store.put(value)[1]
    assert delays == [0.1, 0.2]


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('outcome', ['lost_before', 'lost_after', 'unexpected', 'advanced_after'])
def test_ambiguous_ref_update_is_reconciled_without_repeat(acquired, kind, outcome):
    api, fence, store, value = setup_store(acquired, kind)
    api.ref_outcome = outcome
    if outcome == 'lost_after':
        assert store.put(value) == (value, True)
        assert fence.write_events[-1]['state'] == 'verified_durable'
    else:
        with pytest.raises(PublicationError): store.put(value)
        assert fence.write_events[-1]['state'] == 'ref_update_uncertain'
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1
    assert all(w[2]['force'] is False for w in api.writes if w[0] == 'PATCH')


@pytest.mark.parametrize('kind', ['pin', 'result'])
def test_governance_drift_after_commit_verification_is_rejected(acquired, kind):
    api, fence, store, value = setup_store(acquired, kind)
    api.drift_after_verification = True
    with pytest.raises(ValueError, match='governance drift'): store.put(value)
    assert fence.write_events[-1]['state'] == 'verified_durable'
    assert store.last_commit_sha == api.tip


def test_lifecycle_reads_exact_durable_pin_before_execution(acquired):
    api, fence, store, value = setup_store(acquired, 'pin')
    calls = []
    result = discover_persist_execute(mode='normal', store=store,
        cycle_id=value['cycle_id'], source_id=value['source_id'], required_members={'snapshot'},
        discover_and_retrieve=lambda: calls.append('discover') or value,
        execute=lambda durable: calls.append('execute') or durable)
    assert result == value and calls == ['discover', 'execute']
    assert not api.mutable_reads


def test_lifecycle_rejects_current_contradiction_after_verified_write(acquired):
    api, fence, store, value = setup_store(acquired, 'pin')
    original = store.put
    def put(pin):
        result = original(pin)
        api.bad_content = 'contradiction'
        return result
    store.put = put
    with pytest.raises((ValueError, PublicationError)):
        discover_persist_execute(mode='normal', store=store, cycle_id=value['cycle_id'],
            source_id=value['source_id'], required_members={'snapshot'},
            discover_and_retrieve=lambda: value, execute=lambda pin: pytest.fail('must not execute'))


@pytest.mark.parametrize('boundary', ['prepare', 'upload', 'finalize', 'cas', 'result'])
def test_partial_failure_diagnostics_and_idempotent_recovery(tmp_path, boundary):
    h = Harness(tmp_path); progress = {}
    if boundary == 'result': h.result_failure = True
    else: h.api.fail_at = boundary
    with pytest.raises(PublicationError): h.execute(progress=progress)
    diagnostic = backfill.failure_diagnostic(progress, [])
    assert diagnostic['boundary'] == ('result_write' if boundary == 'result' else 'publication')
    assert diagnostic['completed_vintages'] == []
    assert 'secret' not in json.dumps(diagnostic)
    h.execute(progress=progress)
    assert progress['completed_vintages'] == [2019]
    assert h.acquisitions == [2019]
    assert len(h.api.release['assets']) == len(h.results) == 1


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('boundary', ['/git/trees', '/git/commits', '/git/refs/heads/main'])
def test_response_loss_after_each_git_boundary_reconciles_before_next_mutation(acquired, kind, boundary):
    api, fence, store, value = setup_store(acquired, kind)
    original = api.request
    failed = False
    def request(method, url, **kwargs):
        nonlocal failed
        response = original(method, url, **kwargs)
        if method != 'GET' and url == boundary and not failed:
            failed = True
            raise TransientPublicationError('fixture lost response containing signature=secret')
        return response
    api.request = request
    if boundary.endswith('/main'):
        assert store.put(value)[1]  # Reconcile a successful authoritative update.
        before = copy.deepcopy(api.writes)
        assert not store.put(value)[1]
        assert api.writes == before
    else:
        with pytest.raises(TransientPublicationError): store.put(value)
        assert api.tip == 'a' * 40  # Only unreferenced Git objects may exist.
        assert not any(w[0] == 'PATCH' for w in api.writes)
        assert store.put(value)[1]
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1
    assert 'secret' not in json.dumps(backfill.failure_diagnostic({}, fence.write_events))


def test_partial_multi_vintage_execution_retains_confirmed_progress(tmp_path):
    from core.source_artifacts.package import build_publication_package
    h = Harness(tmp_path, (2015, 2016))
    catalog = {'immutable_records': []}; publications = []; fail = True
    def publish(path, source):
        nonlocal fail
        manifest = json.loads((path / 'manifest.json').read_text())
        release = manifest['provider_release_id']
        if release.endswith(':2016') and fail:
            fail = False
            raise PublicationError('fixture failure signature=secret')
        existing = next((r for r in catalog['immutable_records'] if r['object_id'] == manifest['artifact_id']), None)
        if existing is not None:
            return {'record': existing, 'catalog': catalog}
        package = build_publication_package(path, tmp_path / (manifest['artifact_id'] + '.tar'))
        item = {'object_id': manifest['artifact_id'], 'object_type': 'source',
            'artifact_content_hash': manifest['artifact_content_hash'], 'package_sha256': package['package_sha256'],
            'publication_state': 'published_immutable_verified', 'logical_artifact_uri': manifest['artifact_uri'],
            'metadata': {'source_id': source, 'provider_release_id': release,
                         'observation_max': manifest['observation_max']}}
        catalog['immutable_records'].append(item); publications.append(release)
        return {'record': item, 'catalog': catalog}
    progress = {}
    with pytest.raises(PublicationError): h.execute(publish=publish, progress=progress)
    diagnostic = backfill.failure_diagnostic(progress, [])
    assert diagnostic['completed_vintages'] == [2015]
    assert diagnostic['vintage'] == 2016 and diagnostic['boundary'] == 'publication'
    assert 'secret' not in json.dumps(diagnostic)
    h.execute(publish=publish, progress=progress)
    assert publications == ['acs/acs1:2015', 'acs/acs1:2016']
    assert h.acquisitions == [2015, 2016] and len(h.results) == 2


@pytest.mark.parametrize('field', ['code_sha256', 'config_hashes', 'serialization_runtime'])
def test_previously_adopted_authorization_does_not_permit_identity_drift(tmp_path, field):
    from test_acs_backfill import IDENTITY
    h = Harness(tmp_path)
    changed = copy.deepcopy(IDENTITY); changed[field] = {'drift': True}
    with pytest.raises(ValueError, match='drift'): h.execute(identity=changed)
    assert not h.acquisitions and not h.api.writes


def test_cli_emits_sanitized_artifact_when_batch_fails(acquired, tmp_path, monkeypatch):
    import sys
    from test_acs_backfill import IDENTITY, GEOGRAPHY, auth
    h, _, _ = acquired
    plan_path = tmp_path / 'plan.json'; plan_path.write_text(json.dumps(h.plan))
    auth_path = tmp_path / 'auth.json'; auth_path.write_text(json.dumps(auth(h.plan)))
    output = tmp_path / 'diagnostic.json'
    monkeypatch.setattr(sys, 'argv', ['acs_backfill', '--action', 'execute',
        '--workspace', str(tmp_path), '--output', str(output), '--plan', str(plan_path),
        '--authorization', str(auth_path), '--reviewed-plan-id', h.plan['plan_id'],
        '--authorization-run-id', '123'])
    monkeypatch.setattr(backfill, 'execution_context', lambda: None)
    monkeypatch.setattr(backfill, 'repository_identity', lambda root: IDENTITY)
    monkeypatch.setattr(backfill, '_main_snapshot', lambda api: 'a' * 40)
    monkeypatch.setattr(backfill, '_authority_files', lambda *args: None)
    monkeypatch.setattr(backfill, '_code_authority', lambda *args: None)
    monkeypatch.setattr(backfill, 'geography_plan', lambda: (GEOGRAPHY, []))
    monkeypatch.setattr(backfill, 'verify_authorization_run', lambda *args, **kwargs: None)
    monkeypatch.setattr(backfill, 'GitHubAPI', lambda *args, **kwargs: CommitAPI())
    def fail(**kwargs):
        kwargs['progress'].update(boundary='result_write', vintage=2010, completed_vintages=[2009])
        kwargs['pin_store'].authority.write_events.append({'path': 'config/fixture.json',
            'commit_sha': 'c' * 40, 'state': 'verified_durable', 'blob_sha': 'd' * 40,
            'unsafe_extra': 'signature=secret'})
        raise RuntimeError('https://fixture.invalid?signature=secret')
    monkeypatch.setattr(backfill, 'execute_batch', fail)
    with pytest.raises(RuntimeError): backfill.main()
    diagnostic = json.loads(output.read_text())
    assert diagnostic['boundary'] == 'result_write' and diagnostic['completed_vintages'] == [2009]
    assert diagnostic['writes'][0]['state'] == 'verified_durable'
    assert 'secret' not in output.read_text() and 'unsafe_extra' not in output.read_text()


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('race', ['governance', 'advance'])
def test_postverification_current_read_is_fenced_on_both_sides(acquired, kind, race):
    api, fence, store, value = setup_store(acquired, kind)
    original = api.request; reads = 0
    def request(method, url, **kwargs):
        nonlocal reads
        response = original(method, url, **kwargs)
        if method == 'GET' and url.startswith('/contents/') and api.tip != 'a' * 40:
            reads += 1
            if reads == 2:
                if race == 'governance': api.governance_changed = True
                else:
                    api.tip = 'd' * 40
                    api.snapshots[api.tip] = copy.deepcopy(api.objects)
        return response
    api.request = request
    with pytest.raises(ValueError, match='governance drift|snapshot changed'): store.put(value)
    assert fence.write_events[-1]['state'] == 'verified_durable'
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1


@pytest.mark.parametrize('race', ['governance', 'replacement'])
def test_final_lifecycle_get_cannot_start_provider_retrieval(acquired, tmp_path, race):
    from jobs.monthly_refresh.acs_hosted import execute_product
    from jobs.monthly_refresh.source_inputs import pin_path
    api, fence, store, value = setup_store(acquired, 'pin'); store.put(value)
    original = api.request; reads = 0; calls = []
    def request(method, url, **kwargs):
        nonlocal reads
        response = original(method, url, **kwargs)
        if method == 'GET' and url.startswith('/contents/'):
            reads += 1
            if reads == 2:
                if race == 'governance': api.governance_changed = True
                else:
                    api.tip = 'd' * 40; api.snapshots[api.tip] = copy.deepcopy(api.objects)
                    api.snapshots[api.tip][pin_path(value['cycle_id'], value['source_id'])] = {'replacement': True}
        return response
    api.request = request
    def forbidden(*args, **kwargs): calls.append('continued'); pytest.fail('superseded lifecycle continued')
    with pytest.raises(ValueError, match='governance drift|snapshot changed'):
        execute_product(source_id=value['source_id'], cycle_id=value['cycle_id'], mode='resume',
            workspace=tmp_path, pin_store=store, discover=forbidden, retrieve=forbidden,
            build=forbidden, publish=forbidden, record=forbidden)
    assert calls == []


@pytest.mark.parametrize('kind', ['pin', 'result'])
@pytest.mark.parametrize('failure', ['before_content', 'duplicate_content'])
def test_reconciled_diagnostics_preserve_strongest_proven_boundary(acquired, kind, failure):
    api, fence, store, value = setup_store(acquired, kind); api.ref_outcome = 'lost_after'
    if failure == 'before_content': api.missing_reads = 4
    else:
        original = api.request; reads = 0
        def request(method, url, **kwargs):
            nonlocal reads
            if method == 'GET' and url.startswith('/contents/') and api.tip != 'a' * 40:
                reads += 1
                if reads == 2: api.missing_reads = 4
            return original(method, url, **kwargs)
        api.request = request
    with pytest.raises(PublicationError): store.put(value)
    assert fence.write_events[-1]['state'] == ('ref_update_confirmed' if failure == 'before_content' else 'verified_durable')
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1


def test_reconciliation_rejects_head_advance_during_content_verification(acquired):
    api, fence, store, value = setup_store(acquired, 'pin'); api.ref_outcome = 'lost_after'
    original = api.request
    def request(method, url, **kwargs):
        response = original(method, url, **kwargs)
        if method == 'GET' and url.startswith('/contents/') and api.tip != 'a' * 40:
            api.tip = 'd' * 40; api.snapshots[api.tip] = copy.deepcopy(api.objects)
        return response
    api.request = request
    with pytest.raises(PublicationError, match='head advanced'): store.put(value)
    assert fence.write_events[-1]['state'] == 'verified_durable'
    assert len([w for w in api.writes if w[0] == 'PATCH']) == 1
