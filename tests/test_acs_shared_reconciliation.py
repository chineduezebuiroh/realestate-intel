"""Real reconstructed ACS capabilities at shared ambiguous-write boundaries."""
import base64
import hashlib

import pytest

from core.source_artifacts.publication import TransientPublicationError
from jobs.monthly_refresh import acs_authoritative_write as writes
from jobs.monthly_refresh.acs_attestation import verify
from jobs.monthly_refresh.cohort_plan_store import GitHubCohortPlanStore
from jobs.monthly_refresh.acs_family_resolution import GitHubFamilyResolutionStore
from jobs.monthly_refresh.acs_history_family import resolve_inputs
from jobs.monthly_refresh import cohort_promotion_hosted as hosted
from test_acs_boundary_hardening import (durable_fixture, promotion_fixture, AuthorityAPI,
                                         advance_remote, JSONCAS)


class ReconciliationAPI(AuthorityAPI):
    def __init__(self, governance, failure='drift'):
        super().__init__(); self.governance = governance; self.failure = failure
        self.failed_reads = 0
    def request(self, method, url, *, payload=None, **kwargs):
        if method == 'GET' and url == '/git/ref/heads/main':
            return {'object': {'sha': self.tip}}, {}
        if method == 'GET' and url.startswith('/contents/'):
            if self.value is None: return None, {}
            if '?ref=' + self.tip in url and self.tip == 'c' * 40:
                if self.failure in {'drift', 'transient'}: advance_remote(self.governance)
                if self.failure == 'transient' or (self.failure == 'recover' and self.failed_reads < 4):
                    self.failed_reads += 1
                    raise TransientPublicationError('fixture unavailable immutable read')
            raw = writes.canonical_json_bytes(self.value)
            blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
            return {'content': base64.b64encode(raw).decode(), 'sha': blob}, {}
        response = super().request(method, url, payload=payload, **kwargs)
        if method == 'PATCH':
            from jobs.monthly_refresh.acs_scope import _READERS
            _READERS[self.governance].opener.tip = self.tip
            raise TransientPublicationError('fixture lost PATCH response')
        return response


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch): monkeypatch.setattr(writes.time, 'sleep', lambda delay: None)


@pytest.mark.parametrize('failure', ['drift', 'transient', 'none'])
@pytest.mark.parametrize('caller', ['cohort', 'family'])
def test_shared_reconciliation_rechecks_current_governance(tmp_path, caller, failure):
    plan, validation = durable_fixture(tmp_path)
    api = ReconciliationAPI(validation['governance'], failure)
    if caller == 'cohort':
        proof = verify(plan, **validation)
        operation = lambda: GitHubCohortPlanStore(api, 'main').put(plan, acs_proof=proof)
    else:
        record = plan['acs_validation']['resolution']
        inputs = resolve_inputs(parents=record['parents'], **validation)
        artifact = validation['resolver'].resolve(f"artifact://source/acs/{record['output_artifact_id']}")
        operation = lambda: GitHubFamilyResolutionStore(api, 'main').put(record, artifact=artifact,
            catalog_record=inputs['records'][record['output_artifact_id']], validation_inputs=inputs)
    if failure == 'none':
        assert operation()[1]
        assert not operation()[1]
    elif failure == 'transient' and caller == 'cohort':
        with pytest.raises(TransientPublicationError): operation()
    else:
        with pytest.raises(ValueError, match='superseded'): operation()
    assert sum(method == 'PATCH' for method, _ in api.calls) == 1
    if failure == 'transient': assert api.failed_reads == 4


@pytest.mark.parametrize('caller', ['cohort', 'prepared'])
def test_shared_existing_record_reread_revalidates_governance(tmp_path, caller):
    if caller == 'cohort':
        target, validation = durable_fixture(tmp_path); proof = verify(target, **validation)
        api = AuthorityAPI(target)
        original = api.request
        def request(method, url, **kwargs):
            response = original(method, url, **kwargs)
            if method == 'GET' and url.startswith('/contents/'):
                advance_remote(validation['governance'])
            return response
        api.request = request
        operation = lambda: GitHubCohortPlanStore(api, 'main').put(target, acs_proof=proof)
    else:
        target, proof, _, _, _, validation = promotion_fixture(tmp_path)
        store = JSONCAS(target); store.api = AuthorityAPI()
        original = store.read
        def read():
            result = original(); advance_remote(validation['governance']); return result
        store.read = read
        operation = lambda: hosted.persist_prepared(store, target, acs_proof=proof)
    with pytest.raises(ValueError, match='superseded'): operation()


def test_family_transient_recovery_reuses_exact_object_without_second_patch(tmp_path):
    plan, validation = durable_fixture(tmp_path)
    record = plan['acs_validation']['resolution']
    inputs = resolve_inputs(parents=record['parents'], **validation)
    artifact = validation['resolver'].resolve(f"artifact://source/acs/{record['output_artifact_id']}")
    api = ReconciliationAPI(validation['governance'], 'recover')
    stored, changed = GitHubFamilyResolutionStore(api, 'main').put(record, artifact=artifact,
        catalog_record=inputs['records'][record['output_artifact_id']], validation_inputs=inputs)
    assert stored == record and changed is False
    assert api.failed_reads == 4
    assert sum(method == 'PATCH' for method, _ in api.calls) == 1
