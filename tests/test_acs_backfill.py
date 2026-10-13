"""Offline historical acquisition, review boundaries, and recovery fixtures."""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path
from urllib.parse import unquote, urlparse, parse_qs

import pytest
import yaml

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.github_release import GitHubCatalogCAS
from core.source_artifacts.hashing import sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh import acs_backfill as backfill
from jobs.monthly_refresh.acs_monthly import candidate, snapshot_to_pin
from jobs.monthly_refresh.source_inputs import FilePinStore
from sources.census_acs.historical import (VerificationRequired, exact_dataset, geography_identity,
                                         omission_receipts, preflight, validate_evidence, validate_snapshot)

FIXTURE = json.loads(Path("tests/fixtures/census_acs_historical/provider.json").read_text())
GEOGRAPHY = FIXTURE["geography"]
IDENTITY = {"repository": backfill.REPOSITORY, "baseline": backfill.BASELINE,
            "code_sha256": "c" * 64, "config_hashes": {"governance": "absent"}}


class Response:
    status_code = 200
    def __init__(self, payload): self.payload = payload
    def json(self): return copy.deepcopy(self.payload)
    def raise_for_status(self): pass


class Transport:
    def __init__(self, source="census_acs1", year=2019):
        self.source, self.year, self.calls = source, year, []
    def get(self, url, *, params, timeout):
        self.calls.append((url, params))
        if url.endswith("data.json"):
            return Response({"dataset": [{"c_vintage": self.year,
                "c_dataset": backfill.PRODUCTS[self.source].split("/"),
                "identifier": f"http://api.census.gov/data/{self.year}/{backfill.PRODUCTS[self.source]}"}]})
        if url.endswith("variables.json"): return Response(FIXTURE["metadata"])
        fields = dict(part.rsplit(":", 1) for part in [params["for"], *([params["in"]] if "in" in params else [])])
        return Response([["NAME", "B01003_001E", "B19013_001E", *fields],
                         ["fixture geography", "100", "70000", *fields.values()]])


def evidence(source="census_acs1", year=2019):
    return preflight(source_id=source, vintage=year, transport=Transport(source, year), plan=GEOGRAPHY, excluded=[])


def missing(year=2019):
    return {"vintage": year, "classification": "missing_acquisition", "parents": [],
            "verification_requirements": [], "approved_gap": None}


def plan(years=(2019,), source="census_acs1"):
    return backfill.make_plan(source_id=source, vintages=list(years), batch_size=len(years), identity=copy.deepcopy(IDENTITY),
        code_commit="a" * 40, inventory=[missing(y) for y in years], evidence={str(y): evidence(source, y) for y in years},
        geography=GEOGRAPHY, excluded=[], authority={"repository": backfill.REPOSITORY, "branch": "main", "commit": "a" * 40})


def auth(value):
    return backfill.authorize(value, reviewed_plan_id=value["plan_id"], identity=IDENTITY, actor="reviewer")


@pytest.mark.parametrize("source", ["census_acs1", "census_acs5"])
@pytest.mark.parametrize("year", [2009, 2019, 2024])
def test_exact_provider_preflight_and_all_geographies(source, year):
    value = evidence(source, year)
    assert len(value["accounting"]) == 8
    assert validate_evidence(value, source_id=source, vintage=year, plan=GEOGRAPHY, excluded=[])
    assert value["snapshot"]["vintage"] == year


@pytest.mark.parametrize("mutation", ["experimental", "substring", "wrong_vintage", "wrong_components", "duplicate"])
def test_dataset_identity_never_falls_back(mutation):
    discovery = copy.deepcopy(FIXTURE)
    row = discovery["dataset"][0]
    if mutation == "experimental": row["c_dataset"].append("experimental")
    if mutation == "substring": row["identifier"] += "/experimental"
    if mutation == "wrong_vintage": row["c_vintage"] = 2020
    if mutation == "wrong_components": row["c_dataset"] = ["acs", "acs5"]
    if mutation == "duplicate": discovery["dataset"].append(copy.deepcopy(row))
    with pytest.raises(VerificationRequired): exact_dataset("census_acs1", 2019, discovery)


@pytest.mark.parametrize("mutation", ["duplicate_header", "missing_metric", "unexpected_header", "wrong_geo",
    "wrong_product", "wrong_year", "wrong_source", "missing_member", "duplicate_member", "extra_member",
    "no_content", "bad_number", "nonfinite", "unknown_sentinel", "malformed"])
def test_invalid_historical_snapshot_fails_closed(mutation):
    snapshot = evidence()["snapshot"]
    member = snapshot["members"][0]; header, values = member["payload"]
    if mutation == "duplicate_header": header.append(header[0]); values.append(values[0])
    if mutation == "missing_metric": header.pop(2); values.pop(2)
    if mutation == "unexpected_header": header.append("extra"); values.append("1")
    if mutation == "wrong_geo": values[-1] = "2"
    if mutation == "wrong_product": snapshot["product"] = "acs/acs5"
    if mutation == "wrong_year": snapshot["vintage"] = 2020
    if mutation == "wrong_source": snapshot["source_id"] = "census_acs5"
    if mutation == "missing_member": snapshot["members"].pop()
    if mutation == "duplicate_member": snapshot["members"].append(copy.deepcopy(member))
    if mutation == "extra_member": snapshot["members"].append({**member, "geo_id": "unknown"})
    if mutation == "no_content": member["status"] = "provider_ineligible_no_content"; member["payload"] = None
    if mutation == "bad_number": values[1] = "garbage"
    if mutation == "nonfinite": values[1] = "NaN"
    if mutation == "unknown_sentinel": values[1] = "-222222222"
    if mutation == "malformed": member["payload"] = {"error": "invalid"}
    with pytest.raises(ValueError):
        validate_snapshot(source_id="census_acs1", vintage=2019, snapshot=snapshot, plan=GEOGRAPHY, excluded=[])


@pytest.mark.parametrize("raw,classification", [(None, "provider_null"), ("null", "provider_null"),
    ("-666666666", "provider_sentinel:-666666666"), ("-888888888", "provider_sentinel:-888888888"),
    ("-999999999", "provider_sentinel:-999999999")])
def test_complete_null_and_known_sentinel_accounting(raw, classification):
    snapshot = evidence()["snapshot"]; snapshot["members"][0]["payload"][1][1] = raw
    omissions = omission_receipts(source_id="census_acs1", vintage=2019, snapshot=snapshot)
    checked = validate_snapshot(source_id="census_acs1", vintage=2019, snapshot=snapshot, plan=GEOGRAPHY, excluded=[], omissions=omissions)
    assert checked["accounting"][0]["classification"] == classification
    assert len(checked["accounting"]) == 8


@pytest.mark.parametrize("failure", ["metadata", "no_content", "inaccessible"])
def test_provider_failure_redaction_and_verification_requirement(failure):
    class Broken(Transport):
        def get(self, url, **kwargs):
            if failure == "inaccessible": raise RuntimeError("https://provider?key=secret-value")
            if failure == "metadata" and url.endswith("variables.json"): return Response({"variables": {}})
            if failure == "no_content" and not url.endswith(".json"):
                result = Response(None); result.status_code = 204; return result
            return super().get(url, **kwargs)
    with pytest.raises(VerificationRequired) as exc:
        preflight(source_id="census_acs1", vintage=2019, transport=Broken(), plan=GEOGRAPHY, excluded=[])
    assert "secret-value" not in str(exc.value)


def test_plan_binding_authorization_and_code_governance_drift():
    value = plan(); authorization = auth(value)
    assert authorization["plan_id"] == value["plan_id"]
    assert backfill.cycle_identity(value["plan_id"], "census_acs1", 2019) != backfill.cycle_identity(value["plan_id"], "census_acs5", 2019)
    for field in ("vintages", "repository_identity", "geography", "entries", "code_commit"):
        changed = copy.deepcopy(value); changed[field] = None
        with pytest.raises(ValueError): backfill.verify_plan(changed, IDENTITY)
    with pytest.raises(ValueError): backfill.authorize(value, reviewed_plan_id="different", identity=IDENTITY, actor="reviewer")
    with pytest.raises(ValueError): backfill.verify_plan(value, {**IDENTITY, "config_hashes": {"governance": "new"}})
    with pytest.raises(ValueError): backfill.verify_plan(value, {**IDENTITY, "code_sha256": "d" * 64})
    # Missing official facts preserve a reviewable plan but cannot authorize.
    unknown = backfill.make_plan(source_id="census_acs1", vintages=[2019], batch_size=1, identity=IDENTITY,
        code_commit="a" * 40, inventory=[missing()], evidence={}, geography=GEOGRAPHY, excluded=[], authority={})
    assert unknown["entries"][0]["verification_requirement"]
    with pytest.raises(ValueError): auth(unknown)


@pytest.mark.parametrize("years,batch", [([], 1), ([2019, 2019], 2), ([2019, 2020], 1),
    (list(range(2010, 2016)), 6), ([1999], 1), ([True], 1), ([2019], 0)])
def test_scope_bounds(years, batch):
    with pytest.raises(ValueError): backfill.scope("census_acs1", years, batch)


class MemoryAPI:
    """Only fixture Release/Contents operations; never delegates to a transport."""
    repository = backfill.REPOSITORY
    def __init__(self):
        self.release = None; self.asset_bytes = {}; self.catalog = empty_catalog()
        self.fail_at = None; self.writes = []
    def request(self, method, url, *, payload=None, **kwargs):
        if method != "GET": self.writes.append((method, url))
        if url.startswith("/releases/tags/"): return copy.deepcopy(self.release), {}
        if method == "POST" and url == "/releases":
            self.release = {"id": 11, "tag_name": payload["tag_name"], "draft": True,
                "prerelease": False, "assets": [], "upload_url": "https://fixture.invalid/upload{?name}", "published_at": None}
            if self.fail_at == "prepare": self.fail_at = None; raise PublicationError("fixture prepare response lost")
            return copy.deepcopy(self.release), {}
        if method == "POST" and url.startswith("https://fixture.invalid/upload"):
            name = parse_qs(urlparse(url).query)["name"][0]
            self.asset_bytes[22] = payload
            self.release["assets"].append({"id": 22, "name": name, "state": "uploaded"})
            if self.fail_at == "upload": self.fail_at = None; raise PublicationError("fixture upload response lost")
            return copy.deepcopy(self.release["assets"][0]), {}
        if method == "GET" and url == "/releases/11": return copy.deepcopy(self.release), {}
        if method == "PATCH" and url == "/releases/11":
            self.release["draft"] = False; self.release["published_at"] = "2026-01-01T00:00:00Z"
            if self.fail_at == "finalize": self.fail_at = None; raise PublicationError("fixture finalize response lost")
            return copy.deepcopy(self.release), {}
        if method == "GET" and url.startswith("/contents/"):
            return {"sha": "a" * 40, "content": base64.b64encode(json.dumps(self.catalog).encode()).decode()}, {}
        if method == "PUT" and url.startswith("/contents/"):
            if self.fail_at == "cas": self.fail_at = None; raise PublicationError("fixture CAS conflict")
            self.catalog = json.loads(base64.b64decode(payload["content"]))
            return {}, {}
        raise AssertionError((method, url))
    def download_asset(self, asset_id, destination): destination.write_bytes(self.asset_bytes[asset_id])


class Harness:
    def __init__(self, root, years=(2019,)):
        self.root = root; self.plan = plan(years); self.api = MemoryAPI()
        self.pins = FilePinStore(root / "pins"); self.acquisitions = []; self.results = []
        self.saved = {}; self.result_failure = False; self.drift = False
    def acquire(self, *, source_id, vintage, output):
        self.acquisitions.append(vintage); snapshot = evidence(source_id, vintage)["snapshot"]
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(snapshot, sort_keys=True, separators=(",", ":")) + "\n")
        self.saved[vintage] = output.read_bytes(); return snapshot
    def retrieve(self, pin, output):
        year = int(pin["provider_release_id"].rsplit(":", 1)[1])
        output.write_bytes(self.saved[year] + (b" " if self.drift else b""))
    def publish(self, path, source):
        return backfill.exact_publish(artifact=path, source_id=source, api=self.api,
            cas=GitHubCatalogCAS(self.api, backfill.CATALOG_PATH, "main"),
            workspace=self.root / "publication", git_sha=self.plan["code_commit"])
    def record(self, result, catalog):
        from jobs.monthly_refresh.cycle_results import add_record, governed_record
        item = governed_record(result, json.loads(Path("config/monthly_refresh_policy.json").read_text()), catalog)
        if self.result_failure:
            self.result_failure = False; raise PublicationError("fixture result write failure")
        prior = next((r for r in self.results if r["cycle_id"] == item["cycle_id"]), None)
        value, changed = add_record(prior, item)
        if changed: self.results.append(value)
    def execute(self, **overrides):
        kwargs = dict(plan=self.plan, authorization=auth(self.plan), reviewed_plan_id=self.plan["plan_id"],
            identity=IDENTITY, workspace=self.root / "work", pin_store=self.pins,
            current_inventory=lambda: [missing(y) for y in self.plan["vintages"]],
            acquire=self.acquire, retrieve=self.retrieve, publish=self.publish, record=self.record)
        kwargs.update(overrides)
        return backfill.execute_batch(**kwargs)


@pytest.mark.parametrize("boundary", ["prepare", "upload", "finalize", "cas", "result"])
def test_exact_byte_recovery_at_publication_boundaries(tmp_path, boundary):
    h = Harness(tmp_path)
    if boundary == "result": h.result_failure = True
    else: h.api.fail_at = boundary
    with pytest.raises(PublicationError): h.execute()
    original_bytes = h.api.asset_bytes.get(22)
    out = h.execute()
    if original_bytes: assert h.api.asset_bytes[22] == original_bytes
    assert len(h.api.release["assets"]) == 1
    assert len(h.api.catalog["immutable_records"]) == len(h.results) == 1
    assert h.acquisitions == [2019]
    assert out[0]["result"]["accepted_pointer_changed"] is False
    assert all(method != "DELETE" for method, _ in h.api.writes)
    # Full success repeats preserve the durable result/package identities.
    repeated = h.execute()
    assert repeated[0]["result"] == out[0]["result"] and len(h.results) == 1


def test_failure_before_pin_persistence_cannot_publish(tmp_path):
    h = Harness(tmp_path)
    class FailBefore(FilePinStore):
        def put(self, pin): raise PublicationError("pin persistence failed")
    with pytest.raises(PublicationError): h.execute(pin_store=FailBefore(tmp_path / "failed"))
    assert h.api.release is None and not h.api.writes
    h.execute(); assert len(h.results) == 1


def test_failure_after_pin_persistence_provider_drift_fails_closed(tmp_path):
    h = Harness(tmp_path); h.api.fail_at = "prepare"
    with pytest.raises(PublicationError): h.execute()
    h.drift = True; writes = list(h.api.writes)
    with pytest.raises(ValueError, match="pin mismatch"): h.execute()
    assert h.api.writes == writes and h.acquisitions == [2019]


def test_conflicting_existing_release_bytes_never_overwritten(tmp_path):
    h = Harness(tmp_path); h.api.fail_at = "upload"
    with pytest.raises(PublicationError): h.execute()
    h.api.asset_bytes[22] += b"conflict"; original = h.api.asset_bytes[22]
    with pytest.raises(IdentityCollisionError): h.execute()
    assert h.api.asset_bytes[22] == original and h.api.release["draft"]
    assert not h.api.catalog["immutable_records"] and not h.results


def test_cataloged_package_collision_and_timestamp_identity_defect(tmp_path):
    h = Harness(tmp_path); out = h.execute(); pin = out[0]["pin"]
    raw = tmp_path / "original"; raw.write_bytes(h.saved[2019])
    rebuilt = candidate(pin=pin, paths={"snapshot": raw}, output=tmp_path / "changed", cycle_id=pin["cycle_id"],
        git_sha=h.plan["code_commit"], artifact_created_at="2000-01-01T00:00:00Z")
    assert rebuilt["manifest"]["artifact_id"] == out[0]["candidate"]["manifest"]["artifact_id"]
    writes = list(h.api.writes)
    with pytest.raises(IdentityCollisionError, match="exact-byte recovery"): h.publish(tmp_path / "changed", "census_acs1")
    assert h.api.writes == writes


def test_authoritative_inventory_existing_missing_conflict_unknown_gap(tmp_path):
    h = Harness(tmp_path); h.execute(); record = h.api.catalog["immutable_records"][0]
    from core.source_artifacts.github_release import GitHubReleaseArtifactResolver
    def inventory(catalog, resolve, gaps=None):
        return backfill.reconcile(catalog=catalog, source_id="census_acs1", vintages=[2019, 2020], resolve=resolve,
                                  approved_gaps=gaps)
    resolver = GitHubReleaseArtifactResolver(h.api.catalog, h.api, tmp_path / "resolve")
    result = inventory(h.api.catalog, resolver.resolve)
    assert [r["classification"] for r in result] == ["verified_existing_parent", "missing_acquisition"]
    def inaccessible(uri): raise PublicationError("credential=must-not-leak")
    unknown = inventory(h.api.catalog, inaccessible)
    assert unknown[0]["classification"] == "unknown_unverified" and "must-not-leak" not in json.dumps(unknown)
    duplicated = {"immutable_records": [record, copy.deepcopy(record)]}
    assert inventory(duplicated, resolver.resolve)[0]["classification"] == "conflict"
    assert inventory(h.api.catalog, resolver.resolve, {2020: {"evidence_id": "approved-main-fixture"}})[1]["classification"] == "approved_gap"
    assert inventory(h.api.catalog, resolver.resolve, {2019: {"evidence_id": "approved-main-fixture"}})[0]["classification"] == "conflict"


def test_sequential_batch_stops_before_next_vintage(tmp_path):
    h = Harness(tmp_path, (2019, 2020)); h.api.fail_at = "upload"
    with pytest.raises(PublicationError): h.execute()
    assert h.acquisitions == [2019]


def test_execution_auth_mismatch_precedes_every_side_effect(tmp_path):
    h = Harness(tmp_path)
    for override in ({"reviewed_plan_id": "wrong"}, {"authorization": auth(plan((2020,)))},
                     {"identity": {**IDENTITY, "code_sha256": "changed"}}):
        with pytest.raises(ValueError): h.execute(**override)
    assert not h.acquisitions and not h.api.writes


def test_inventory_drift_blocks_acquisition(tmp_path):
    h = Harness(tmp_path)
    with pytest.raises(ValueError, match="inventory changed"):
        h.execute(current_inventory=lambda: [{**missing(), "classification": "conflict"}])
    assert not h.acquisitions


def test_monthly_candidate_rejects_snapshot_pin_identity_mismatch(tmp_path):
    snapshot = evidence()["snapshot"]; path = tmp_path / "raw"
    path.write_text(json.dumps(snapshot))
    pin, _ = snapshot_to_pin(cycle_id="cycle", source_id="census_acs1", vintage=2019, snapshot=snapshot, path=path)
    snapshot["product"] = "acs/acs5"; path.write_text(json.dumps(snapshot))
    from jobs.monthly_refresh.source_inputs import provider_pin
    from core.source_artifacts.hashing import sha256_file
    pin["members"]["snapshot"]["sha256"] = sha256_file(path)
    pin = provider_pin(cycle_id="cycle", source_id="census_acs1", provider_release_id="acs/acs1:2019", members=pin["members"])
    with pytest.raises(ValueError, match="persisted product/vintage"):
        candidate(pin=pin, paths={"snapshot": path}, output=tmp_path / "candidate", cycle_id="cycle")


def test_workflow_manual_read_only_default_and_injection_resistance():
    path = Path(".github/workflows/acs-historical-backfill.yml")
    workflow = yaml.safe_load(path.read_text()); triggers = workflow.get(True, workflow.get("on"))
    assert set(triggers) == {"workflow_dispatch"}
    assert triggers["workflow_dispatch"]["inputs"]["action"]["default"] == "preflight"
    assert workflow["permissions"] == {"contents": "read"}
    assert workflow["jobs"]["review"]["permissions"] == {"contents": "read"}
    assert workflow["jobs"]["execute"]["permissions"] == {"contents": "write", "actions": "read"}
    assert workflow["concurrency"]["cancel-in-progress"] is False
    for job in workflow["jobs"].values():
        assert "refs/heads/main" in job["if"]
        for step in job["steps"]:
            assert "${{ inputs." not in step.get("run", "")
    assert "acs-backfill-authorize" in path.read_text()


def test_main_code_drift_and_truncated_evidence_fail_closed():
    class CompareAPI:
        def __init__(self, files): self.files = files
        def request(self, *args): return {"merge_base_commit": {"sha": "a" * 40}, "files": self.files}, {}
    backfill._code_authority(CompareAPI([{ "filename": "config/monthly_source_input_pins/cycle/source.json"}]), "a" * 40, "b" * 40)
    for files in (None, [{"filename": "jobs/monthly_refresh/acs_backfill.py"}], [{}] * 300):
        with pytest.raises(ValueError): backfill._code_authority(CompareAPI(files), "a" * 40, "b" * 40)


def test_verified_catalog_package_recovers_result_without_provider(tmp_path):
    h = Harness(tmp_path); h.result_failure = True
    with pytest.raises(PublicationError): h.execute()
    from core.source_artifacts.github_release import GitHubReleaseArtifactResolver
    resolver = GitHubReleaseArtifactResolver(h.api.catalog, h.api, tmp_path / "recovery")
    def current():
        return backfill.reconcile(catalog=h.api.catalog, source_id="census_acs1", vintages=[2019], resolve=resolver.resolve)
    def no_provider(*args, **kwargs): raise AssertionError("provider should not be called for verified recovery")
    recovered = h.execute(current_inventory=current, retrieve=no_provider, acquire=no_provider,
                         existing_publication=lambda uri: (resolver.resolve(uri), h.api.catalog))
    assert recovered[0]["reused_verified_package"] is True and len(h.results) == 1
    h.execute(current_inventory=current, retrieve=no_provider, acquire=no_provider,
              existing_publication=lambda uri: (resolver.resolve(uri), h.api.catalog))
    assert len(h.results) == 1 and h.acquisitions == [2019]


def test_pin_write_response_lost_resumes_recorded_pin(tmp_path):
    h = Harness(tmp_path)
    class PersistThenFail(FilePinStore):
        def put(self, pin):
            super().put(pin)
            raise PublicationError("fixture pin response lost")
    store = PersistThenFail(tmp_path / "pins")
    with pytest.raises(PublicationError): h.execute(pin_store=store)
    assert not h.api.writes
    h.execute()
    assert h.acquisitions == [2019] and len(h.results) == 1


def test_preflight_response_drift_requires_new_review_before_pin(tmp_path):
    h = Harness(tmp_path)
    def drift(**kwargs):
        snapshot = h.acquire(**kwargs)
        snapshot["members"][0]["payload"][1][1] = "101"
        kwargs["output"].write_text(json.dumps(snapshot))
        return snapshot
    with pytest.raises(ValueError, match="provider response drift"): h.execute(acquire=drift)
    cycle = backfill.cycle_identity(h.plan["plan_id"], "census_acs1", 2019)
    assert h.pins.get(cycle, "census_acs1") is None and not h.api.writes


def test_bounded_successful_batch_uses_independent_cycles_and_exact_pins(tmp_path):
    h = Harness(tmp_path, (2018, 2019, 2020))
    publications, catalog = [], {"immutable_records": []}
    def publish(path, source):
        manifest = json.loads((path / "manifest.json").read_text())
        package = build_publication_package(path, tmp_path / (manifest["artifact_id"] + ".tar"))
        item = {"object_id": manifest["artifact_id"], "object_type": "source",
            "artifact_content_hash": manifest["artifact_content_hash"], "package_sha256": package["package_sha256"],
            "publication_state": "published_immutable_verified", "logical_artifact_uri": manifest["artifact_uri"],
            "metadata": {"source_id": source, "provider_release_id": manifest["provider_release_id"],
                         "observation_max": manifest["observation_max"]}}
        catalog["immutable_records"].append(item)
        publications.append(manifest["provider_release_id"])
        return {"record": item, "catalog": catalog}
    out = h.execute(publish=publish)
    assert h.acquisitions == [2018, 2019, 2020]
    assert publications == ["acs/acs1:2018", "acs/acs1:2019", "acs/acs1:2020"]
    assert len({x["pin"]["cycle_id"] for x in out}) == 3
    assert len(h.results) == 3 and all(x["result"]["accepted_pointer_changed"] is False for x in out)


def test_conflicting_durable_pin_evidence_stops_before_package_reuse(tmp_path):
    h = Harness(tmp_path); out = h.execute()
    pin = out[0]["pin"]
    from jobs.monthly_refresh.source_inputs import pin_path, provider_pin
    altered = copy.deepcopy(pin["members"]); altered["snapshot"]["evidence"]["product"] = "acs/acs5"
    contradictory = provider_pin(cycle_id=pin["cycle_id"], source_id=pin["source_id"],
        provider_release_id=pin["provider_release_id"], members=altered)
    # Simulate a corrupted authority fixture, not a supported store overwrite.
    write_canonical_json(h.pins.root / pin_path(pin["cycle_id"], pin["source_id"]), contradictory)
    writes = list(h.api.writes)
    with pytest.raises(ValueError, match="durable pin differs"): h.execute()
    assert h.api.writes == writes


def test_serialization_runtime_identity_is_plan_bound():
    runtime = backfill.runtime_identity()
    assert set(runtime["implementations"]) == {"pandas", "numpy", "pyarrow"}
    identity = {**IDENTITY, "serialization_runtime": runtime}
    value = plan()
    body = {k: v for k, v in value.items() if k != "plan_id"}; body["repository_identity"] = identity
    value = backfill._identified(backfill.PLAN_VERSION, body, "plan_id")
    backfill.verify_plan(value, identity)
    changed = copy.deepcopy(identity); changed["serialization_runtime"]["implementations"]["pyarrow"]["version"] = "changed"
    with pytest.raises(ValueError, match="drift"): backfill.verify_plan(value, changed)


@pytest.mark.parametrize("field,bad", [("head_branch", "feature"), ("event", "push"), ("conclusion", "failure"),
    ("path", ".github/workflows/other.yml"), ("repository", {"full_name": "other/repo"}),
    ("actor", {"login": "other"}), ("head_sha", "invalid")])
def test_authorization_run_requires_successful_matching_main_workflow(field, bad):
    value = plan(); authorization = auth(value)
    run = {"head_branch": "main", "event": "workflow_dispatch", "conclusion": "success",
           "path": ".github/workflows/acs-historical-backfill.yml", "repository": {"full_name": backfill.REPOSITORY},
           "actor": {"login": "reviewer"}, "head_sha": "a" * 40}
    import hashlib, io, zipfile
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("plan.json", json.dumps(value))
        archive.writestr("authorization.json", json.dumps(authorization))
    raw = buffer.getvalue()
    class RunAPI:
        def request(self, method, url):
            if url.endswith("/artifacts?per_page=100"):
                return {"total_count": 1, "artifacts": [{"name": "acs-backfill-authorize", "id": 44,
                    "expired": False, "digest": "sha256:" + hashlib.sha256(raw).hexdigest(),
                    "workflow_run": {"id": 123, "head_sha": "a" * 40}}]}, {}
            if url.startswith("/actions/runs/"): return copy.deepcopy(run), {}
            return {"merge_base_commit": {"sha": "a" * 40}, "files": []}, {}
    def verify(run_id="123"):
        backfill.verify_authorization_run(RunAPI(), run_id=run_id, plan=value, authorization=authorization,
                                         archive_reader=lambda api, artifact: raw)
    verify()
    run[field] = bad
    with pytest.raises(ValueError): verify()
    with pytest.raises(ValueError): verify("1;echo bad")


@pytest.mark.parametrize("remote", [f"https://github.com/{backfill.REPOSITORY}",
    f"https://github.com/{backfill.REPOSITORY}.git", f"git@github.com:{backfill.REPOSITORY}",
    f"git@github.com:{backfill.REPOSITORY}.git", f"https://github.com/{backfill.REPOSITORY}-fork.git"])
def test_repository_remote_identity_including_actions_checkout_format(monkeypatch, remote):
    original = backfill.subprocess.check_output
    def checkout_output(args, **kwargs):
        if args[-3:] == ["remote", "get-url", "origin"]: return remote
        return original(args, **kwargs)
    monkeypatch.setattr(backfill.subprocess, "check_output", checkout_output)
    if "-fork" in remote:
        with pytest.raises(ValueError, match="repository identity"): backfill.repository_identity(Path.cwd())
    else:
        assert backfill.repository_identity(Path.cwd())["baseline"] == backfill.BASELINE


class OmissionTransport(Transport):
    def __init__(self, *, omitted='metro', status=204, body=b'', null_variable=None):
        super().__init__(); self.omitted, self.status, self.body, self.null_variable = omitted, status, body, null_variable
    def get(self, url, *, params, timeout):
        result = super().get(url, params=params, timeout=timeout)
        if params and params['for'].endswith('12345') and self.omitted == 'metro':
            result = Response(None); result.status_code = self.status; result.content = self.body
        if params and params['for'] == 'us:1' and self.null_variable:
            index = result.payload[0].index(self.null_variable); result.payload[1][index] = None
        return result


def omitted_evidence(**kwargs):
    return preflight(source_id='census_acs1', vintage=2019, transport=OmissionTransport(**kwargs),
                     plan=GEOGRAPHY, excluded=[])


def reidentify(value, field, kind):
    return backfill._identified(kind, {k: v for k, v in value.items() if k != field}, field)


def test_explicit_member_and_variable_omissions_preserve_unequal_coverage(tmp_path):
    value = omitted_evidence(null_variable='B19013_001E')
    assert len(value['omission_evidence']) == 2
    assert len(value['accounting']) == 8
    validate_evidence(value, source_id='census_acs1', vintage=2019, plan=GEOGRAPHY, excluded=[])
    h = Harness(tmp_path)
    h.plan['entries'][0]['provider_evidence'] = value
    h.plan = reidentify(h.plan, 'plan_id', backfill.PLAN_VERSION)
    def acquire(*, source_id, vintage, output):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(value['snapshot'], sort_keys=True, separators=(',', ':')) + '\n')
        h.saved[vintage] = output.read_bytes()
        return copy.deepcopy(value['snapshot'])
    out = h.execute(acquire=acquire)
    import pandas as pd
    data = pd.read_parquet(h.root / 'work' / out[0]['pin']['cycle_id'] / 'artifact/data.parquet')
    assert 'metro' not in set(data.geo_id)
    assert len(data) == 5
    assert len(data[data.geo_id == 'nation']) == 1
    assert set(data.date.astype(str)) == {'2019-12-31'}
    persisted = out[0]['pin']['members']['snapshot']['evidence']
    assert persisted['provider_ineligible'] == ['metro']
    assert len(persisted['historical_omission_evidence_ids']) == 2
    assert persisted['historical_preflight_sha256'] == sha256_json(value)
    assert out[0]['result']['accepted_pointer_changed'] is False


@pytest.mark.parametrize('field,bad', [('product', 'acs/acs5'), ('source_id', 'census_acs5'), ('vintage', 2018),
    ('geography', {'geo_id': 'wrong', 'level': 'cbsa_metro', 'census_code': '99999'}),
    ('variables', ['B01003_001E']), ('request_url', 'https://api.census.gov/data/2019/acs/acs1?for=us:1'),
    ('http_status', 404), ('classification', 'request_failure'), ('response_sha256', 'f' * 64)])
def test_rehashed_omission_receipt_cannot_change_its_exact_observation_binding(field, bad):
    value = omitted_evidence(); receipt = value['omission_evidence'][0]
    receipt[field] = bad
    value['omission_evidence'][0] = reidentify(receipt, 'evidence_id', 'acs_historical_omission_evidence_v1')
    with pytest.raises(ValueError): validate_evidence(value, source_id='census_acs1', vintage=2019, plan=GEOGRAPHY, excluded=[])


@pytest.mark.parametrize('mutation', ['undocumented', 'duplicate_receipt', 'extra_receipt', 'missing_member',
    'unexpected_member', 'missing_variable_header', 'variable_receipt_without_null'])
def test_evidence_cannot_waive_undocumented_or_missing_responses(mutation):
    value = omitted_evidence(null_variable='B19013_001E')
    if mutation == 'undocumented': value['omission_evidence'] = []
    if mutation == 'duplicate_receipt': value['omission_evidence'].append(copy.deepcopy(value['omission_evidence'][0]))
    if mutation == 'extra_receipt':
        extra = copy.deepcopy(value['omission_evidence'][0]); extra['geography']['geo_id'] = 'extra'
        value['omission_evidence'].append(reidentify(extra, 'evidence_id', 'acs_historical_omission_evidence_v1'))
    if mutation == 'missing_member': value['snapshot']['members'].pop()
    if mutation == 'unexpected_member': value['snapshot']['members'].append({'geo_id': 'unexpected'})
    if mutation == 'missing_variable_header':
        member = value['snapshot']['members'][0]; member['payload'][0].pop(2); member['payload'][1].pop(2)
    if mutation == 'variable_receipt_without_null': value['snapshot']['members'][0]['payload'][1][2] = '70000'
    with pytest.raises(ValueError): validate_evidence(value, source_id='census_acs1', vintage=2019, plan=GEOGRAPHY, excluded=[])


@pytest.mark.parametrize('status,body', [(400, b''), (404, b''), (500, b''), (206, b''), (204, b'error'), (200, b'')])
def test_no_content_never_masks_request_status_body_or_parsing_failures(status, body):
    with pytest.raises(ValueError): omitted_evidence(status=status, body=body)


def test_historical_acquisition_checks_empty_204_body_without_changing_monthly(tmp_path, monkeypatch):
    from sources.census_acs.historical import acquire_historical_snapshot
    monkeypatch.setattr('jobs.monthly_refresh.acs_monthly.geography_plan', lambda: (GEOGRAPHY, []))
    snapshot = acquire_historical_snapshot(source_id='census_acs1', vintage=2019, output=tmp_path / 'valid',
                                          session=OmissionTransport(), key='never-persist-this-secret')
    assert snapshot == omitted_evidence()['snapshot']
    assert 'never-persist-this-secret' not in (tmp_path / 'valid').read_text()
    with pytest.raises(ValueError):
        acquire_historical_snapshot(source_id='census_acs1', vintage=2019, output=tmp_path / 'invalid',
                                    session=OmissionTransport(body=b'error'), key='runtime')
    assert not (tmp_path / 'invalid').exists()


def review_archive(value, authorization, *, extra=None):
    import io, zipfile
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as archive:
        archive.writestr('plan.json', json.dumps(value))
        archive.writestr('authorization.json', json.dumps(authorization))
        if extra: archive.writestr(extra, '{}')
    return buffer.getvalue()


class ReviewAPI:
    repository = backfill.REPOSITORY
    def __init__(self, value, authorization):
        import hashlib
        self.raw = review_archive(value, authorization)
        self.run = {'head_branch': 'main', 'event': 'workflow_dispatch', 'conclusion': 'success',
            'path': '.github/workflows/acs-historical-backfill.yml', 'repository': {'full_name': backfill.REPOSITORY},
            'actor': {'login': 'reviewer'}, 'head_sha': 'a' * 40}
        self.artifact = {'id': 44, 'name': 'acs-backfill-authorize', 'expired': False,
            'digest': 'sha256:' + hashlib.sha256(self.raw).hexdigest(),
            'workflow_run': {'id': 123, 'head_sha': 'a' * 40}}
        self.listing = {'total_count': 1, 'artifacts': [self.artifact]}
    def request(self, method, url):
        assert method == 'GET'
        if url.endswith('/artifacts?per_page=100'): return copy.deepcopy(self.listing), {}
        if url.startswith('/actions/runs/'): return copy.deepcopy(self.run), {}
        return {'merge_base_commit': {'sha': 'a' * 40}, 'files': []}, {}
    def verify(self, value, authorization):
        backfill.verify_authorization_run(self, run_id='123', plan=value, authorization=authorization,
                                         archive_reader=lambda api, artifact: self.raw)


@pytest.mark.parametrize('mutation', ['modified_evidence', 'expanded_batch', 'wrong_product', 'wrong_vintage',
    'changed_governance', 'wrong_authorization'])
def test_successful_run_id_cannot_authorize_forged_rehashed_plan_or_authorization(mutation):
    value = plan(); authorization = auth(value); api = ReviewAPI(value, authorization)
    api.verify(value, authorization)
    if mutation == 'modified_evidence':
        value['entries'][0]['provider_evidence']['snapshot']['members'][0]['payload'][1][1] = '101'
    if mutation == 'expanded_batch': value['batch_size'] = 5; value['vintages'].append(2020)
    if mutation == 'wrong_product': value['source_id'] = 'census_acs5'
    if mutation == 'wrong_vintage': value['vintages'] = [2020]
    if mutation == 'changed_governance': value['repository_identity']['config_hashes'] = {'governance': 'new'}
    value = reidentify(value, 'plan_id', backfill.PLAN_VERSION)
    authorization['plan_id'] = value['plan_id']
    if mutation == 'wrong_authorization': authorization['action'] = 'allow_other_write'
    authorization = reidentify(authorization, 'authorization_id', backfill.AUTH_VERSION)
    with pytest.raises(ValueError, match='independently authorized run artifact'): api.verify(value, authorization)


@pytest.mark.parametrize('mutation', ['expired', 'bad_digest', 'missing_digest', 'wrong_run', 'wrong_head',
    'incomplete_listing', 'duplicate', 'missing', 'traversal', 'duplicate_member'])
def test_authorization_artifact_requires_complete_immutable_provenance(mutation):
    import hashlib
    value = plan(); authorization = auth(value); api = ReviewAPI(value, authorization)
    if mutation == 'expired': api.artifact['expired'] = True
    if mutation == 'bad_digest': api.artifact['digest'] = 'sha256:' + '0' * 64
    if mutation == 'missing_digest': api.artifact.pop('digest')
    if mutation == 'wrong_run': api.artifact['workflow_run']['id'] = 124
    if mutation == 'wrong_head': api.artifact['workflow_run']['head_sha'] = 'b' * 40
    if mutation == 'incomplete_listing': api.listing['total_count'] = 2
    if mutation == 'duplicate': api.listing['artifacts'].append(copy.deepcopy(api.artifact)); api.listing['total_count'] = 2
    if mutation == 'missing': api.listing['artifacts'] = []; api.listing['total_count'] = 0
    if mutation in {'traversal', 'duplicate_member'}:
        if mutation == 'duplicate_member':
            with pytest.warns(UserWarning, match='Duplicate name'):
                api.raw = review_archive(value, authorization, extra='plan.json')
        else:
            api.raw = review_archive(value, authorization, extra='../escape')
        api.artifact['digest'] = 'sha256:' + hashlib.sha256(api.raw).hexdigest()
    with pytest.raises(ValueError): api.verify(value, authorization)


@pytest.mark.parametrize('actions', [None, 'false', 'true'])
def test_execution_cli_cannot_bypass_hosted_preflight_authorization(monkeypatch, tmp_path, actions):
    import sys
    monkeypatch.delenv('GITHUB_WORKFLOW_REF', raising=False)
    if actions is None: monkeypatch.delenv('GITHUB_ACTIONS', raising=False)
    else: monkeypatch.setenv('GITHUB_ACTIONS', actions)
    monkeypatch.setattr(sys, 'argv', ['acs_backfill', '--action', 'execute', '--workspace', str(tmp_path), '--output', str(tmp_path / 'out')])
    monkeypatch.setattr(backfill, 'GitHubAPI', lambda *args, **kwargs: pytest.fail('API must not be constructed before execution gate'))
    with pytest.raises(ValueError, match='dedicated manual main workflow'): backfill.main()
    assert not (tmp_path / 'out').exists()


def test_modified_plan_and_unknown_rehashed_classification_fail_before_any_acquisition(tmp_path):
    h = Harness(tmp_path)
    h.plan['entries'][0]['classification'] = 'skip_everything'
    h.plan = reidentify(h.plan, 'plan_id', backfill.PLAN_VERSION)
    with pytest.raises(ValueError, match='unknown plan inventory'): h.execute()
    assert not h.acquisitions and not h.api.writes


def test_reconciliation_race_after_acquisition_stops_before_publication(tmp_path):
    h = Harness(tmp_path); changed = False
    def acquire(**kwargs):
        nonlocal changed
        snapshot = h.acquire(**kwargs); changed = True; return snapshot
    def current(): return [{**missing(), 'classification': 'conflict'}] if changed else [missing()]
    with pytest.raises(ValueError, match='drift before publication'): h.execute(acquire=acquire, current_inventory=current)
    assert h.acquisitions == [2019] and not h.api.writes and not h.results


def test_conflicting_concurrent_pin_proposals_never_publish(tmp_path):
    h = Harness(tmp_path)
    class ConcurrentPins(FilePinStore):
        def put(self, proposed):
            other = copy.deepcopy(proposed)
            other['members']['snapshot']['retrieved_at'] = '2000-01-01T00:00:00Z'
            from jobs.monthly_refresh.source_inputs import provider_pin
            other = provider_pin(cycle_id=other['cycle_id'], source_id=other['source_id'],
                provider_release_id=other['provider_release_id'], members=other['members'])
            super().put(other)
            return super().put(proposed)
    with pytest.raises(IdentityCollisionError): h.execute(pin_store=ConcurrentPins(tmp_path / 'pins'))
    assert not h.api.writes and not h.results


def test_same_pin_and_plan_produce_byte_identical_packages_across_processes(tmp_path):
    import os, subprocess, sys, tarfile
    h = Harness(tmp_path); output = h.execute()[0]
    pin_path = tmp_path / 'pin.json'; write_canonical_json(pin_path, output['pin'])
    snapshot_path = tmp_path / 'snapshot.json'; snapshot_path.write_bytes(h.saved[2019])
    code = '''
import json, sys
from pathlib import Path
from jobs.monthly_refresh.acs_monthly import candidate
from core.source_artifacts.package import build_publication_package
pin = json.loads(Path(sys.argv[1]).read_text())
output = Path(sys.argv[3])
candidate(pin=pin, paths={'snapshot': Path(sys.argv[2])}, output=output,
          cycle_id=pin['cycle_id'], git_sha='a'*40,
          artifact_created_at=pin['members']['snapshot']['retrieved_at'])
build_publication_package(output, Path(sys.argv[4]))
'''
    packages = []
    for index, (seed, zone) in enumerate([('1', 'UTC'), ('777', 'America/New_York')]):
        package = tmp_path / f'package-{index}.tar'; packages.append(package)
        env = {**os.environ, 'PYTHONHASHSEED': seed, 'TZ': zone}
        subprocess.run([sys.executable, '-c', code, str(pin_path), str(snapshot_path),
                        str(tmp_path / f'build-{index}'), str(package)], env=env, check=True)
    assert packages[0].read_bytes() == packages[1].read_bytes() == h.api.asset_bytes[22]
    artifact = tmp_path / 'build-0'
    for path in artifact.iterdir(): os.utime(path, (123456, 987654))
    build_publication_package(artifact, tmp_path / 'new-mtimes.tar')
    assert (tmp_path / 'new-mtimes.tar').read_bytes() == packages[0].read_bytes()
    with tarfile.open(packages[0]) as archive:
        members = archive.getmembers()
        assert [m.name for m in members] == sorted(m.name for m in members)
        assert all(m.mtime == m.uid == m.gid == 0 and m.mode == 0o644 and not m.uname and not m.gname for m in members)


def test_runtime_verifies_actual_installed_bytes_not_just_metadata(monkeypatch, tmp_path):
    import hashlib, importlib.metadata
    from types import SimpleNamespace
    binary = tmp_path / 'implementation.so'; binary.write_bytes(b'original')
    member = SimpleNamespace(hash=SimpleNamespace(mode='sha256', value=base64.urlsafe_b64encode(
        hashlib.sha256(binary.read_bytes()).digest()).decode().rstrip('=')))
    class Distribution:
        version = 'fixed'; files = [member]
        def read_text(self, name): return 'fixed-RECORD'
        def locate_file(self, entry): return binary
    monkeypatch.setattr(importlib.metadata, 'distribution', lambda name: Distribution())
    backfill.runtime_identity()
    binary.write_bytes(b'drifted implementation, identical version and RECORD')
    with pytest.raises(ValueError, match='implementation bytes drifted'): backfill.runtime_identity()


class FencedContentsAPI:
    """Offline main Git tree/ref model, including non-fast-forward rejection."""
    repository = backfill.REPOSITORY
    def __init__(self):
        self.tip = 'a' * 40; self.objects = {}; self.tree = None; self.commit = None
        self.advance_at = None; self.governance_changed = False; self.writes = []
    def request(self, method, url, *, payload=None, **kwargs):
        import hashlib
        from urllib.parse import unquote
        if method == 'GET' and url.startswith('/contents/'):
            path = unquote(url[len('/contents/'):].split('?')[0])
            if path not in self.objects: return None, {}
            raw = json.dumps(self.objects[path], sort_keys=True, separators=(',', ':')).encode() + b'\n'
            return {'sha': hashlib.sha1(raw).hexdigest(), 'content': base64.b64encode(raw).decode()}, {}
        if method == 'GET' and url.startswith('/git/commits/'):
            return {'sha': url.rsplit('/', 1)[1], 'tree': {'sha': '1' * 40}}, {}
        self.writes.append((method, url, copy.deepcopy(payload)))
        if method == 'POST' and url == '/git/trees':
            self.tree = copy.deepcopy(payload['tree'])
            if self.advance_at == 'tree': self.tip = 'b' * 40; self.governance_changed = True
            return {'sha': '2' * 40}, {}
        if method == 'POST' and url == '/git/commits':
            self.commit = copy.deepcopy(payload)
            return {'sha': 'c' * 40}, {}
        if method == 'PATCH' and url == '/git/refs/heads/main':
            assert payload['force'] is False
            if self.commit['parents'] != [self.tip]: raise PublicationError('fixture non-fast-forward conflict')
            for item in self.tree: self.objects[item['path']] = json.loads(item['content'])
            self.tip = payload['sha']; return {'object': {'sha': self.tip}}, {}
        raise AssertionError((method, url))


class FixturePlanFence:
    def __init__(self, api): self.api = api; self.plan = plan()
    def require_destination(self, repository):
        if repository != backfill.REPOSITORY: raise ValueError('wrong repository')
    def current_commit(self, repository):
        self.require_destination(repository)
        if self.api.governance_changed: raise ValueError('governance drift')
        return self.api.tip


@pytest.mark.parametrize('boundary', ['pin', 'catalog', 'result'])
@pytest.mark.parametrize('conflict', [False, True])
def test_physical_authoritative_writes_fence_concurrent_main_advance(tmp_path, monkeypatch, boundary, conflict):
    from core.source_artifacts.github_release import GitHubReleaseArtifactPublisher
    api = FencedContentsAPI(); fence = FixturePlanFence(api)
    receipts = []
    original = GitHubReleaseArtifactPublisher.finalize
    def capture(self, uri):
        receipt = original(self, uri); receipts.append(receipt); return receipt
    monkeypatch.setattr(GitHubReleaseArtifactPublisher, 'finalize', capture)
    h = Harness(tmp_path); published = h.execute()[0]
    api.advance_at = 'tree' if conflict else None
    if boundary == 'pin':
        store = backfill.FencedPinStore(api, fence)
        operation = lambda: store.put(published['pin'])
    elif boundary == 'result':
        store = backfill.FencedResultStore(api, fence)
        operation = lambda: store.put(h.results[0])
    else:
        api.objects[backfill.CATALOG_PATH] = empty_catalog()
        store = backfill.FencedPhysicalCatalog(GitHubCatalogCAS(api, backfill.CATALOG_PATH, 'main'), fence)
        operation = lambda: store.add(published['publication']['record'], receipts[0])
    before = copy.deepcopy(api.objects)
    if conflict:
        with pytest.raises(PublicationError, match='non-fast-forward'): operation()
        assert api.objects == before and api.tip == 'b' * 40
        with pytest.raises(ValueError, match='governance drift'): operation()
    else:
        value, changed = operation(); assert changed
        assert api.tip == 'c' * 40
        assert operation()[1] is False
    assert all(method != 'PUT' and method != 'DELETE' for method, _, _ in api.writes)
    patches = [payload for method, path, payload in api.writes if method == 'PATCH']
    assert patches and all(payload['force'] is False for payload in patches)


def test_plan_authority_revalidates_local_identity_and_remote_governance_at_each_write(monkeypatch):
    value = plan(); api = FencedContentsAPI(); changed = False
    monkeypatch.setattr(backfill, 'repository_identity', lambda root: IDENTITY)
    monkeypatch.setattr(backfill, '_main_snapshot', lambda api: api.tip)
    def check(api, root, tip):
        if changed: raise ValueError('main configuration/governance drift')
    monkeypatch.setattr(backfill, '_authority_files', check)
    monkeypatch.setattr(backfill, '_code_authority', lambda api, commit, tip: None)
    authority = backfill.PlanAuthority(api=api, plan=value, root=Path.cwd())
    assert authority.current_commit(backfill.REPOSITORY) == api.tip
    changed = True
    with pytest.raises(ValueError, match='governance drift'): authority.current_commit(backfill.REPOSITORY)
    with pytest.raises(ValueError, match='destination'): authority.current_commit('other/repo')
    monkeypatch.setattr(backfill, 'repository_identity', lambda root: {**IDENTITY, 'code_sha256': 'new'})
    with pytest.raises(ValueError, match='drift'): authority.current_commit(backfill.REPOSITORY)


@pytest.mark.parametrize('authorization', [None, {}, {'authorization_id': 'invented'}, []])
def test_missing_invalid_execution_authorization_rejected_before_inventory(tmp_path, authorization):
    h = Harness(tmp_path)
    def no_inventory(): pytest.fail('invalid authorization must precede inventory')
    with pytest.raises(ValueError): h.execute(authorization=authorization, current_inventory=no_inventory)
    assert not h.acquisitions and not h.api.writes


def test_archive_download_strips_credentials_and_redacts_signed_url_errors(monkeypatch):
    import io, urllib.request
    class RedirectAPI:
        token = 'never-forward'
        def request(self, *args, **kwargs):
            return None, {'Location': 'https://fixture.invalid/zip?signature=secret'}
    class Download(io.BytesIO):
        def __enter__(self): return self
        def __exit__(self, *args): self.close()
    def opener(request, **kwargs):
        assert request.get_header('Authorization') is None
        return Download(b'archive')
    monkeypatch.setattr(urllib.request, 'urlopen', opener)
    assert backfill.download_review_archive(RedirectAPI(), 44) == b'archive'
    def broken(*args, **kwargs): raise RuntimeError('signature=secret; never-forward')
    monkeypatch.setattr(urllib.request, 'urlopen', broken)
    with pytest.raises(ValueError) as exc: backfill.download_review_archive(RedirectAPI(), 44)
    assert 'secret' not in str(exc.value) and 'never-forward' not in str(exc.value)


def test_workflow_write_surface_is_physical_only_and_globally_serialized():
    import inspect
    source = inspect.getsource(backfill.main)
    assert 'FencedPinStore' in source and 'FencedResultStore' in source and 'FencedPhysicalCatalog' in source
    assert 'verify_authorization_run' in source
    forbidden = ('activate_source(', 'activate_object(', 'materialize_current(', 'bootstrap(',
                 'promote(', 'accept(', 'store_prepared(', 'resolve_frames(')
    assert all(token not in source for token in forbidden)
    workflow = yaml.safe_load(Path('.github/workflows/acs-historical-backfill.yml').read_text())
    assert workflow['concurrency'] == {'group': 'acs-historical-acquisition-main', 'cancel-in-progress': False}
    assert 'CENSUS_API_KEY' not in json.dumps(workflow['jobs']['review'])


@pytest.mark.parametrize('arguments', [['--source-id', 'census_acs5'], ['--vintages', '2018,2019,2020'],
    ['--batch-size', '5'], ['--evidence', 'some-evidence.json']])
def test_execution_cli_rejects_scope_overrides_before_destination_io(monkeypatch, tmp_path, arguments):
    import sys
    monkeypatch.setattr(sys, 'argv', ['acs_backfill', '--action', 'execute', '--workspace', str(tmp_path),
                                    '--output', str(tmp_path / 'out'), *arguments])
    monkeypatch.setattr(backfill, 'GitHubAPI', lambda *args, **kwargs: pytest.fail('scope override caused I/O'))
    with pytest.raises(ValueError, match='scope overrides forbidden'): backfill.main()


def test_provider_fixture_and_omission_observations_are_explicitly_test_only():
    assert FIXTURE['fixture_namespace'] == 'test_only'
    value = omitted_evidence(null_variable='B19013_001E')
    receipts = value['omission_evidence']
    for case in FIXTURE['omission_cases']:
        assert any(r['geography']['geo_id'] == case['geo_id'] and r['http_status'] == case['http_status']
                   and r['classification'] == case['classification'] for r in receipts)


def test_same_vintage_parent_arriving_during_upload_blocks_catalog_registration(tmp_path, monkeypatch):
    from core.source_artifacts.github_release import GitHubReleaseArtifactPublisher
    receipts = []; original = GitHubReleaseArtifactPublisher.finalize
    def capture(self, uri):
        result = original(self, uri); receipts.append(result); return result
    monkeypatch.setattr(GitHubReleaseArtifactPublisher, 'finalize', capture)
    h = Harness(tmp_path); published = h.execute()[0]
    api = FencedContentsAPI(); existing = copy.deepcopy(published['publication']['record'])
    existing['object_id'] += '_concurrent'
    # This models a newly read conflicting physical identity; it is rejected
    # before generic catalog construction or any write, not picked as a winner.
    api.objects[backfill.CATALOG_PATH] = {'immutable_records': [existing]}
    class Store:
        branch = 'main'; path = backfill.CATALOG_PATH; fixture = False
        def __init__(self): self.api = api
        def read(self): return api.objects[backfill.CATALOG_PATH], 'd' * 40
    fenced = backfill.FencedPhysicalCatalog(Store(), FixturePlanFence(api))
    with pytest.raises(IdentityCollisionError, match='concurrent same-vintage'):
        fenced.add(published['publication']['record'], receipts[0])
    assert not api.writes
    assert h.api.asset_bytes[22] and h.api.release['draft'] is False


def test_write_fence_reconciles_all_selected_vintages_at_exact_main_tip(monkeypatch):
    value = plan(); api = FencedContentsAPI(); observed = [missing()]
    monkeypatch.setattr(backfill, 'repository_identity', lambda root: IDENTITY)
    monkeypatch.setattr(backfill, '_main_snapshot', lambda api: api.tip)
    monkeypatch.setattr(backfill, '_authority_files', lambda *args: None)
    monkeypatch.setattr(backfill, '_code_authority', lambda *args: None)
    tips = []
    def inventory(tip): tips.append(tip); return observed
    authority = backfill.PlanAuthority(api=api, plan=value, root=Path.cwd(), inventory_check=inventory)
    authority.current_commit(backfill.REPOSITORY)
    observed[0]['classification'] = 'conflict'
    with pytest.raises(ValueError, match='inventory changed'): authority.current_commit(backfill.REPOSITORY)
    assert tips == ['a' * 40, 'a' * 40] and not api.writes


@pytest.mark.parametrize('object_type', ['source_set', 'canonical_market', 'serving_market'])
def test_catalog_write_capability_is_confined_to_physical_source_objects(object_type):
    api = FencedContentsAPI(); fence = FixturePlanFence(api)
    store = backfill.FencedPhysicalCatalog(GitHubCatalogCAS(api, backfill.CATALOG_PATH, 'main'), fence)
    record = {'object_type': object_type, 'metadata': {'source_id': 'census_acs1', 'provider_release_id': 'acs/acs1:2019'}}
    with pytest.raises(ValueError, match='cannot register'): store.add(record, {})
    assert not api.writes


@pytest.mark.parametrize('boundary', ['pin', 'result', 'catalog'])
def test_physical_write_scope_cannot_be_expanded_by_calling_store_directly(tmp_path, boundary):
    h = Harness(tmp_path); published = h.execute()[0]
    api = FencedContentsAPI(); fence = FixturePlanFence(api)
    if boundary == 'pin':
        pin = copy.deepcopy(published['pin']); pin['provider_release_id'] = 'acs/acs1:2020'
        operation = lambda: backfill.FencedPinStore(api, fence).put(pin)
    elif boundary == 'result':
        result = copy.deepcopy(h.results[0]); result['result']['provider_release_id'] = 'acs/acs1:2020'
        operation = lambda: backfill.FencedResultStore(api, fence).put(result)
    else:
        record = copy.deepcopy(published['publication']['record']); record['metadata']['source_id'] = 'census_acs5'
        store = backfill.FencedPhysicalCatalog(GitHubCatalogCAS(api, backfill.CATALOG_PATH, 'main'), fence)
        operation = lambda: store.add(record, {})
    with pytest.raises(ValueError): operation()
    assert not api.writes


@pytest.mark.parametrize('mutation', [None, 'missing_access', 'wrong_product_access', 'wrong_year_access',
                                    'wrong_variables_link', 'ambiguous_identity', 'foreign_identifier'])
def test_opaque_provider_identifiers_require_exact_advertised_dataset_binding(mutation):
    dataset = copy.deepcopy(FIXTURE['opaque_dataset_case'])
    discovery = {'dataset': [dataset]}
    if mutation == 'missing_access': dataset.pop('distribution')
    if mutation == 'wrong_product_access': dataset['distribution'][0]['accessURL'] += '/experimental'
    if mutation == 'wrong_year_access': dataset['distribution'][0]['accessURL'] = 'https://api.census.gov/data/2018/acs/acs1'
    if mutation == 'wrong_variables_link': dataset['c_variablesLink'] = 'https://api.census.gov/data/2019/acs/acs5/variables.json'
    if mutation == 'ambiguous_identity':
        duplicate = copy.deepcopy(dataset); duplicate['identifier'] = 'https://api.census.gov/data/id/another_fixture'
        duplicate['distribution'] = []
        discovery['dataset'].append(duplicate)
    if mutation == 'foreign_identifier': dataset['identifier'] = 'https://other.invalid/data/id/fixture'
    if mutation is None:
        assert exact_dataset('census_acs1', 2019, discovery) == dataset
    else:
        with pytest.raises(VerificationRequired): exact_dataset('census_acs1', 2019, discovery)
