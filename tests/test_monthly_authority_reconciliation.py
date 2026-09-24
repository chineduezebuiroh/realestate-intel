from __future__ import annotations

import base64
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from urllib.parse import unquote

import pytest
import yaml

from core.source_artifacts.github_release import GitHubCatalogCAS
from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh import august_source_result_reconciliation as subject
from jobs.monthly_refresh.acs_family_resolution import GitHubFamilyResolutionStore as ACSResolutionStore
from jobs.monthly_refresh.bps_family_resolution import GitHubFamilyResolutionStore as BPSResolutionStore
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, record_path


class ContentsAPI:
    repository = "fixture/repo"
    def __init__(self, objects):
        self.objects = deepcopy(objects); self.generation = {key: 1 for key in objects}; self.puts = []
        self.conflict_once = None; self.inject = None

    def _sha(self, key): return f"{self.generation.get(key, 0):040x}"
    def request(self, method, url, *, payload=None, expected=(200,), **_kwargs):
        path = unquote(url.split("/contents/", 1)[1].split("?", 1)[0])
        if method == "GET":
            branch = unquote(url.split("?ref=", 1)[1]); key = (branch, path)
            value = self.objects.get(key)
            if value is None: return None, {}
            return {"content":base64.b64encode(canonical_json_bytes(value)).decode(), "sha":self._sha(key)}, {}
        key = (payload["branch"], path)
        if self.conflict_once == key:
            self.conflict_once = None
            if self.inject: self.inject(self)
            raise PublicationError("GitHub API PUT failed with HTTP 409")
        if key in self.objects and payload.get("sha") != self._sha(key):
            raise PublicationError("GitHub API PUT failed with HTTP 409")
        self.objects[key] = json.loads(base64.b64decode(payload["content"]))
        self.generation[key] = self.generation.get(key, 0) + 1; self.puts.append(key)
        return {"commit":{"sha":f"commit-{len(self.puts)}"}}, {}


def fixtures():
    source_catalog = json.loads(Path("config/artifact_catalog.json").read_text())
    target_catalog = deepcopy(source_catalog)
    target_catalog["immutable_records"] = [item for item in target_catalog["immutable_records"]
        if item["object_id"] not in subject.EXPECTED_IDS.values()]
    objects = {(subject.SOURCE_BRANCH, subject.CATALOG_PATH):source_catalog,
               (subject.TARGET_BRANCH, subject.CATALOG_PATH):target_catalog}
    for source in subject.EXPECTED_IDS:
        value = json.loads(Path(record_path(subject.CYCLE_ID, source)).read_text())
        objects[(subject.SOURCE_BRANCH, record_path(subject.CYCLE_ID, source))] = value
    return objects


def test_legacy_source_workflows_use_main_for_every_durable_operation():
    for source in ("ces", "fred", "laus"):
        path = Path(f".github/workflows/{source}-monthly-source.yml")
        workflow = yaml.safe_load(path.read_text()); job = workflow["jobs"]["source"]
        assert job["env"]["DURABLE_AUTHORITY_BRANCH"] == "main"
        commands = "\n".join(step.get("run", "") for step in job["steps"])
        assert 'GITHUB_REF_NAME' not in commands
        assert commands.count('--branch "$DURABLE_AUTHORITY_BRANCH"') >= 3
    laus = Path(".github/workflows/laus-monthly-source.yml").read_text()
    assert 'laus_routine satisfy' in laus and '--branch "$DURABLE_AUTHORITY_BRANCH"' in laus
    for source in ("ces", "fred", "laus"):
        text = Path(f"jobs/monthly_refresh/{source}_durable.py").read_text()
        assert 'monthly-refresh-orchestration' not in text
        assert 'add_argument("--branch", required=True)' in text
    fred_artifact = yaml.safe_load(Path(".github/workflows/fred-monthly-artifact.yml").read_text())
    assert set(fred_artifact.get(True, fred_artifact.get("on"))) == {"workflow_dispatch"}


def test_cycle_result_put_rereads_and_reports_commit():
    record = json.loads(Path(record_path(subject.CYCLE_ID, "ces")).read_text())
    api = ContentsAPI({}); store = GitHubCycleResultStore(api, "main")
    assert store.put(record) == (record, True)
    assert store.last_commit_sha == "commit-1"


def test_cycle_result_read_after_write_mismatch_fails():
    record = json.loads(Path(record_path(subject.CYCLE_ID, "ces")).read_text())
    api = ContentsAPI({})
    original = api.request
    def corrupt(method, url, **kwargs):
        result = original(method, url, **kwargs)
        if method == "PUT":
            key = (kwargs["payload"]["branch"], unquote(url.split("/contents/",1)[1]))
            api.objects[key] = {"wrong": True}
        return result
    api.request = corrupt
    with pytest.raises(PublicationError, match="read-after-write"):
        GitHubCycleResultStore(api, "main", attempts=1).put(record)


def test_concurrent_different_source_results_retry_and_preserve_both():
    ces = json.loads(Path(record_path(subject.CYCLE_ID, "ces")).read_text())
    fred = json.loads(Path(record_path(subject.CYCLE_ID, "fred_macro")).read_text())
    api = ContentsAPI({}); ces_key = ("main", record_path(subject.CYCLE_ID, "ces"))
    api.conflict_once = ces_key
    api.inject = lambda state: GitHubCycleResultStore(state, "main").put(fred)
    GitHubCycleResultStore(api, "main").put(ces)
    assert api.objects[ces_key] == ces
    assert api.objects[("main", record_path(subject.CYCLE_ID, "fred_macro"))] == fred


def test_cycle_result_exhausted_conflicts_and_contradiction_fail_closed():
    record = json.loads(Path(record_path(subject.CYCLE_ID, "ces")).read_text())
    class AlwaysConflict(ContentsAPI):
        def request(self, method, url, **kwargs):
            if method == "PUT": raise PublicationError("409")
            return super().request(method, url, **kwargs)
    with pytest.raises(PublicationError, match="retries exhausted"):
        GitHubCycleResultStore(AlwaysConflict({}), "main", attempts=2).put(record)
    bad = deepcopy(record); bad["result"]["candidate_artifact_id"] = "different"
    api = ContentsAPI({("main", record_path(subject.CYCLE_ID, "ces")):record})
    with pytest.raises(IdentityCollisionError): GitHubCycleResultStore(api, "main").put(bad)


def test_august_preflight_live_copy_and_noop(monkeypatch):
    api = ContentsAPI(fixtures()); before = deepcopy(api.objects)
    monkeypatch.setattr(subject, "_verify_remote", lambda *_args: None)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    preflight = subject.run(api=api, policy=policy, cycle_id=subject.CYCLE_ID,
                            expected_ids=subject.EXPECTED_IDS, live=False)
    assert set(k for k,v in preflight["catalog_actions"].items() if v == "add") == set(subject.EXPECTED_IDS)
    assert set(k for k,v in preflight["cycle_result_actions"].items() if v == "add") == set(subject.EXPECTED_IDS)
    assert api.objects == before and not api.puts
    assert all(preflight[key] is False for key in ("provider_discovery_performed",
        "provider_acquisition_performed", "source_artifact_publication_performed",
        "accepted_pointers_changed", "redfin_readiness_consumed", "source_set_created",
        "canonical_market_created", "serving_market_created"))
    accepted = deepcopy(api.objects[(subject.TARGET_BRANCH, subject.CATALOG_PATH)]["accepted"])
    first = subject.run(api=api, policy=policy, cycle_id=subject.CYCLE_ID,
                        expected_ids=subject.EXPECTED_IDS, live=True)
    assert first["mutation_performed"]
    assert api.objects[(subject.TARGET_BRANCH, subject.CATALOG_PATH)]["accepted"] == accepted
    second = subject.run(api=api, policy=policy, cycle_id=subject.CYCLE_ID,
                         expected_ids=subject.EXPECTED_IDS, live=True)
    assert not second["mutation_performed"]
    assert set(second["catalog_actions"].values()) == {"reuse"}
    assert set(second["cycle_result_actions"].values()) == {"reuse"}


def test_august_reconciliation_identity_mismatch_fails_without_mutation(monkeypatch):
    api = ContentsAPI(fixtures()); before = deepcopy(api.objects)
    monkeypatch.setattr(subject, "_verify_remote", lambda *_args: None)
    policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
    wrong = dict(subject.EXPECTED_IDS, ces="wrong")
    with pytest.raises(PublicationError, match="candidate identities"):
        subject.run(api=api, policy=policy, cycle_id=subject.CYCLE_ID, expected_ids=wrong, live=True)
    assert api.objects == before and not api.puts

    api = ContentsAPI(fixtures()); catalog = api.objects[(subject.SOURCE_BRANCH, subject.CATALOG_PATH)]
    next(item for item in catalog["immutable_records"]
         if item["object_id"] == subject.EXPECTED_IDS["ces"])["release_id"] += 1
    with pytest.raises(PublicationError, match="exact immutable catalog identity"):
        subject.run(api=api, policy=policy, cycle_id=subject.CYCLE_ID,
                    expected_ids=subject.EXPECTED_IDS, live=True)
    assert not api.puts


def test_shared_catalog_conflict_is_explicit_and_resumable():
    catalog = json.loads(Path("config/artifact_catalog.json").read_text())
    records = [item for item in catalog["immutable_records"]
               if item["object_id"] in subject.EXPECTED_IDS.values()][:2]
    base = deepcopy(catalog); base["immutable_records"] = [item for item in base["immutable_records"]
        if item["object_id"] not in {r["object_id"] for r in records}]
    api = ContentsAPI({("main", subject.CATALOG_PATH):base})
    first = deepcopy(base); first["immutable_records"].append(records[0]); first["immutable_records"].sort(key=lambda x:(x["object_type"],x["object_id"]))
    second = deepcopy(base); second["immutable_records"].append(records[1]); second["immutable_records"].sort(key=lambda x:(x["object_type"],x["object_id"]))
    cas = GitHubCatalogCAS(api, subject.CATALOG_PATH, "main", fixture=False)
    _, stale = cas.read(); cas._write(first, stale, "first family candidate")
    with pytest.raises(PublicationError): cas._write(second, stale, "stale second family candidate")
    current, oid = cas.read(); current["immutable_records"].append(records[1]); current["immutable_records"].sort(key=lambda x:(x["object_type"],x["object_id"]))
    cas._write(current, oid, "resumed second family candidate")
    durable, _ = cas.read()
    assert {r["object_id"] for r in records}.issubset({r["object_id"] for r in durable["immutable_records"]})


def test_parallel_family_resolution_record_conflict_is_explicit_and_resumable():
    bps = {"schema_version":"bps_family_resolution_record_v1", "resolution_id":"bps-fixture",
        "accepted_pointer_changed":False, "source_set_created":False, "duckdb_mutated":False,
        "redfin_consumed":False, "provider_discovery_performed":False}
    acs = {"schema_version":"acs_family_resolution_record_v1", "resolution_id":"acs-fixture",
        "accepted_pointer_changed":False, "source_set_created":False, "duckdb_mutated":False,
        "serving_db_mutated":False, "provider_discovery_performed":False}
    api = ContentsAPI({})
    BPSResolutionStore(api, "main").put(bps)
    acs_key = ("main", "config/acs_family_resolutions/acs-fixture.json")
    api.conflict_once = acs_key
    with pytest.raises(PublicationError): ACSResolutionStore(api, "main").put(acs)
    ACSResolutionStore(api, "main").put(acs)
    assert api.objects[("main", "config/bps_family_resolutions/bps-fixture.json")] == bps
    assert api.objects[acs_key] == acs
