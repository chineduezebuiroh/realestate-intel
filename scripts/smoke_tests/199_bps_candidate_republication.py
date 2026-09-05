"""Smoke 199: BPS-only immutable r2 republication from an exact durable pin."""
from __future__ import annotations

import copy
import csv
import hashlib
import json
import tempfile
from pathlib import Path

from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.bps_republication import (
    REPUBLICATION_VERSION, add_record, execute_republication, republication_request,
)
from jobs.monthly_refresh.source_inputs import provider_pin
from sources.census_bps.artifact import ADAPTER_CONTRACT_VERSION, governed_config_hashes

CYCLE = "monthly_cycle__2026-07__7cab1c5df177a1e4"
SOURCE = "census_bps"
PARENT = "src__census_bps__2026-04__r1__3f74c0ab0828b6e6"


def monthly_result() -> dict:
    result = {"schema_version":"monthly_source_execution_result_v1", "source_id":SOURCE,
        "cycle_id":CYCLE, "status":"succeeded", "candidate_artifact_id":PARENT,
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":"bps-compiled:202604", "observation_max":"2026-04-01",
        "prior_artifact_id":None, "source_change_detected":True, "retryability":"not_applicable",
        "accepted_pointer_changed":False, "evidence_uri":f"artifact://source/{SOURCE}/{PARENT}"}
    return {"schema_version":"monthly_source_cycle_result_v1", "cycle_id":CYCLE,
        "source_id":SOURCE, "result_contract":"monthly_source_execution_result_v1",
        "policy_schema_version":"monthly_refresh_policy_v2", "result":result}


def catalog() -> dict:
    return {"accepted":{"source":{"redfin":"unchanged"}}, "immutable_records":[{
        "object_type":"source", "object_id":PARENT, "artifact_content_hash":"a"*64,
        "package_sha256":"b"*64, "publication_state":"published_immutable_verified",
        "publication_receipt_id":"old-receipt", "metadata":{"source_id":SOURCE,
        "provider_release_id":"bps-compiled:202604"}}]}


def pin(raw: Path) -> dict:
    return provider_pin(cycle_id=CYCLE, source_id=SOURCE, provider_release_id="202604",
        members={"compiled_zip":{"url":"https://provider.invalid/BPS_Compiled_File_202604.zip",
        "retrieved_at":"2026-09-03T00:00:00Z", "sha256":hashlib.sha256(raw.read_bytes()).hexdigest()}})


class Store:
    def __init__(self): self.value = None
    def get(self, *_): return copy.deepcopy(self.value)
    def put(self, proposed):
        self.value, changed = add_record(self.value, proposed)
        return copy.deepcopy(self.value), changed


with tempfile.TemporaryDirectory() as td:
    root = Path(td); raw = root/"raw"; raw.write_bytes(b"exact provider bytes")
    source_pin = pin(raw); configs = governed_config_hashes()
    request = republication_request(parent_record=monthly_result(), parent_catalog=catalog(), pin=source_pin,
        expected_parent_artifact_id=PARENT, expected_pin_id=source_pin["pin_id"],
        source_contract_version=ADAPTER_CONTRACT_VERSION, config_hashes=configs, revision=2)
    repeated = republication_request(parent_record=monthly_result(), parent_catalog=catalog(), pin=source_pin,
        expected_parent_artifact_id=PARENT, expected_pin_id=source_pin["pin_id"],
        source_contract_version=ADAPTER_CONTRACT_VERSION, config_hashes=configs, revision=2)
    assert request == repeated and request["republication_id"].startswith("source_republication__census_bps__")
    assert request["provider_release_id"] == "202604"

    # All parent/pin failures occur while forming the request, before retrieval.
    cases = []
    missing = catalog(); missing["immutable_records"] = []
    cases.append(dict(parent_catalog=missing, expected_parent_artifact_id=PARENT,
                      expected_pin_id=source_pin["pin_id"], pin=source_pin))
    cases.append(dict(parent_catalog=catalog(), expected_parent_artifact_id=PARENT+"wrong",
                      expected_pin_id=source_pin["pin_id"], pin=source_pin))
    cases.append(dict(parent_catalog=catalog(), expected_parent_artifact_id=PARENT,
                      expected_pin_id="wrong-pin", pin=source_pin))
    bad_parent = monthly_result(); bad_parent["result"]["artifact_content_hash"] = "f"*64
    for index, case in enumerate(cases):
        try:
            republication_request(parent_record=monthly_result(), source_contract_version=ADAPTER_CONTRACT_VERSION,
                                  config_hashes=configs, revision=2, **case)
        except (ValueError, IdentityCollisionError): pass
        else: raise AssertionError(f"invalid parent/pin case {index} was accepted")
    try:
        republication_request(parent_record=bad_parent, parent_catalog=catalog(), pin=source_pin,
            expected_parent_artifact_id=PARENT, expected_pin_id=source_pin["pin_id"],
            source_contract_version=ADAPTER_CONTRACT_VERSION, config_hashes=configs, revision=2)
    except ValueError: pass
    else: raise AssertionError("wrong parent hash was accepted")
    try:
        republication_request(parent_record=monthly_result(), parent_catalog=catalog(), pin=None,
            expected_parent_artifact_id=PARENT, expected_pin_id=source_pin["pin_id"],
            source_contract_version=ADAPTER_CONTRACT_VERSION, config_hashes=configs, revision=2)
    except ValueError as exc: assert "missing" in str(exc)
    else: raise AssertionError("missing provider pin was accepted")
    for revision, parent in ((1, PARENT), (2, PARENT.replace("__r1__", "__r2__"))):
        try:
            republication_request(parent_record=monthly_result(), parent_catalog=catalog(), pin=source_pin,
                expected_parent_artifact_id=parent, expected_pin_id=source_pin["pin_id"],
                source_contract_version=ADAPTER_CONTRACT_VERSION, config_hashes=configs, revision=revision)
        except ValueError: pass
        else: raise AssertionError("republication accepted invalid revision/parent")

    calls = []; store = Store(); output_catalog = catalog()
    new_id = "src__census_bps__2026-04__r2__c0ffee0123456789"
    new_item = {"object_type":"source", "object_id":new_id, "artifact_content_hash":"c"*64,
        "package_sha256":"d"*64, "publication_state":"published_immutable_verified",
        "publication_receipt_id":"new-receipt", "metadata":{"source_id":SOURCE,
        "provider_release_id":"bps-compiled:202604"}}
    output_catalog["immutable_records"].append(new_item)
    def retrieve(url, path): calls.append(("retrieve", url)); path.write_bytes(raw.read_bytes())
    def build(**kwargs):
        calls.append(("build", kwargs["pin"]["provider_release_id"])); kwargs["output"].mkdir()
        return {"manifest":{"artifact_id":new_id, "supersedes_artifact_id":PARENT,
            "prior_artifact_id":PARENT, "prior_artifact_sha256":"a"*64,
            "republication_id":request["republication_id"],
            "source_contract_version":ADAPTER_CONTRACT_VERSION,
            "config_hashes":configs, "provider_release_id":"bps-compiled:202604"}}
    def publish(path, source):
        calls.append(("publish", source)); return {"record":new_item, "catalog":output_catalog, "reused":False}
    first = execute_republication(request=request, pin=source_pin, catalog=catalog(), workspace=root/"first",
        retrieve=retrieve, build=build, publish=publish, store=store)
    assert first["record_changed"] and first["record"]["schema_version"] == REPUBLICATION_VERSION
    assert first["record"]["supersedes_artifact_id"] == PARENT
    assert first["record"]["prior_artifact_id"] == PARENT
    assert not first["record"]["accepted_pointer_changed"]
    assert not first["record"]["source_set_created"] and not first["record"]["family_resolution_created"]
    assert {x[0] for x in calls} == {"retrieve", "build", "publish"}
    assert len(output_catalog["immutable_records"]) == 2  # r1 remains resolvable beside r2.
    calls.clear()
    second = execute_republication(request=request, pin=source_pin, catalog=output_catalog,
        workspace=root/"second", retrieve=lambda *_: calls.append("retrieved"), build=build,
        publish=publish, store=store)
    assert not second["record_changed"] and second["candidate_reused"] and calls == []
    contradiction = copy.deepcopy(first["record"]); contradiction["candidate_content_hash"] = "e"*64
    try: add_record(first["record"], contradiction)
    except IdentityCollisionError: pass
    else: raise AssertionError("contradictory republication record was accepted")

    # Hash mismatch is terminal before the builder/canonicalizer is invoked.
    mismatch_calls = []
    try:
        execute_republication(request=request, pin=source_pin, catalog=catalog(), workspace=root/"mismatch",
            retrieve=lambda url, path: path.write_bytes(b"different"),
            build=lambda **_: mismatch_calls.append("build"), publish=publish, store=Store())
    except ValueError as exc: assert "mismatch" in str(exc)
    else: raise AssertionError("provider SHA mismatch was accepted")
    assert mismatch_calls == []

# Promoted geography identity is exact and excludes unsupported concepts/placeholders.
with Path("config/bps_governed_geographies_v1.csv").open() as stream:
    governed = list(csv.DictReader(stream))
with Path("config/bps_cbsa_canonical_concepts_v1.csv").open() as stream:
    concepts = list(csv.DictReader(stream))
metros = [row for row in governed if row["level"] == "cbsa_metro"]
compatible = [row for row in concepts if row["bps_compatibility"] == "compatible"]
assert len(governed) == 221 and len(metros) == len(compatible) == 53
assert not any(row["canonical_concept"] == "metropolitan_division" for row in compatible)
assert not any(row["provider_identifier"] == "09999" for row in governed)

# Static boundary: manual-only, absent from monthly fan-out, and no forbidden control-plane APIs.
workflow = Path(".github/workflows/bps-candidate-republication.yml").read_text()
master = Path(".github/workflows/monthly-refresh-production.yml").read_text()
module = Path("jobs/monthly_refresh/bps_republication.py").read_text()
assert "workflow_dispatch:" in workflow and "schedule:" not in workflow and "push:" not in workflow
assert "bps-candidate-republication" not in master
for forbidden in ("discover_latest", "githubcycleresultstore", "import duckdb"):
    assert forbidden not in module.lower()

# The implementation pass does not mutate either authoritative July monthly result.
for source in ("census_bps", "census_bps_provisional"):
    path = Path("config/monthly_source_cycle_results")/CYCLE/f"{source}.json"
    expected = {"census_bps":"f76cb232f46c89d29224812fb39c85c81909be50c4b894943e9c0137c6a20c14",
                "census_bps_provisional":"849d71ef59e065debfe7ebcf322180f90cb47d90b10a6a948ff8e03cd8a1f7e8"}
    assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == expected[source]

print("Smoke 199 BPS candidate republication passed")
