"""Smoke 197: normal re-entry resolves durable completion before fan-out."""
from __future__ import annotations

import base64
import copy
import json
from pathlib import Path

from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.cohort import (resolve_invocation, resolve_resume_results,
                                         resume_plan)
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, load_registry

catalog = json.loads(Path("config/artifact_catalog.json").read_text())
readiness = json.loads(Path("config/monthly_refresh_readiness.json").read_text())
policy = json.loads(Path("config/monthly_refresh_policy.json").read_text())
loaded_registry = load_registry(Path("config/monthly_source_cycle_results.json"))
cycle = resolve_invocation(mode="normal", policy_path=Path("config/monthly_refresh_policy.json"),
    readiness=readiness, catalog=catalog)

# The fixture explicitly owns automated completion authority.  Later checked-in
# cycle results must not silently alter any scenario below; Redfin remains the
# production planner's readiness-derived completion.
base_sources = {"fred_macro", "ces", "laus"}
base_records = [copy.deepcopy(record) for record in loaded_registry["records"]
                if record["cycle_id"] == cycle["cycle_id"] and record["source_id"] in base_sources]
assert {record["source_id"] for record in base_records} == base_sources

def fixture_completion(source: str) -> tuple[dict, dict]:
    artifact_id = f"fixture-{source}"
    result = {"schema_version":"monthly_source_execution_result_v1", "source_id":source,
        "cycle_id":cycle["cycle_id"], "status":"succeeded", "candidate_artifact_id":artifact_id,
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":f"release-{source}", "observation_max":"2026-07-31",
        "prior_artifact_id":None, "source_change_detected":True, "retryability":"not_applicable",
        "accepted_pointer_changed":False, "evidence_uri":f"artifact://evidence/{source}"}
    durable = {"schema_version":"monthly_source_cycle_result_v1", "cycle_id":cycle["cycle_id"],
        "source_id":source, "result_contract":"monthly_source_execution_result_v1",
        "policy_schema_version":policy["schema_version"], "result":result}
    catalog_item = {"object_type":"source", "object_id":artifact_id,
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_immutable_verified",
        "metadata":{"source_id":source, "provider_release_id":f"release-{source}"}}
    return durable, catalog_item

provisional, provisional_catalog = fixture_completion("census_bps_provisional")
compiled, compiled_catalog = fixture_completion("census_bps")
fixture_catalog = copy.deepcopy(catalog)
fixture_catalog["immutable_records"].extend([provisional_catalog, compiled_catalog])

def fixture_registry(*additional: dict) -> dict:
    return {"schema_version":loaded_registry["schema_version"],
            "records":[*copy.deepcopy(base_records), *copy.deepcopy(additional)]}

# Reproduce the live re-entry: provider state may now be newer, but completed
# FRED/CES/LAUS members are removed from fan-out before any source callback.
four = fixture_registry()
plan = resolve_resume_results(cycle=cycle, catalog=fixture_catalog, registry=four, policy=policy)
assert plan["reuse"] == ["ces", "fred_macro", "laus", "redfin"]
assert plan["run"] == ["census_bps", "census_bps_provisional"]
events = []
def forbidden_completed_execution(source: str) -> None:
    raise AssertionError(f"completed {source} acquisition/publication/result write was invoked")
for source in plan["run"]:
    if source in {"fred_macro", "laus"}: forbidden_completed_execution(source)
    events.append((source, "execute"))
assert not any(source in {"fred_macro", "laus"} for source, _ in events)
assert {r["source_id"] for r in plan["results"]} == {"redfin", "fred_macro", "ces", "laus"}

five = resolve_resume_results(cycle=cycle, catalog=fixture_catalog,
    registry=fixture_registry(provisional), policy=policy)
assert five["reuse"] == ["census_bps_provisional", "ces", "fred_macro", "laus", "redfin"]
assert five["run"] == ["census_bps"]

six = resolve_resume_results(cycle=cycle, catalog=fixture_catalog,
    registry=fixture_registry(provisional, compiled), policy=policy)
assert six["reuse"] == ["census_bps", "census_bps_provisional", "ces", "fred_macro", "laus", "redfin"]
assert six["run"] == []

# The common planner handles all-complete and one-missing states by physical
# source identity, including the two independent future BPS members.
required = ("redfin", "fred_macro", "ces", "laus", "census_bps", "census_bps_provisional")
def successful(source: str) -> dict:
    return {"schema_version":"monthly_source_execution_result_v1", "source_id":source,
        "cycle_id":cycle["cycle_id"], "status":"succeeded", "candidate_artifact_id":f"id-{source}",
        "artifact_content_hash":"a"*64, "package_sha256":"b"*64,
        "publication_state":"published_verified", "validation_status":"passed",
        "provider_release_id":f"release-{source}", "observation_max":"2026-07-31",
        "prior_artifact_id":None, "source_change_detected":True, "retryability":"not_applicable",
        "accepted_pointer_changed":False, "evidence_uri":f"artifact://evidence/{source}"}
completed = [successful(source) for source in required]
assert resume_plan(required, completed, expected_cycle_id=cycle["cycle_id"])["run"] == []
missing = resume_plan(required, completed[:-1], expected_cycle_id=cycle["cycle_id"])
assert missing["run"] == ["census_bps_provisional"]

# Exercise the actual Contents store: its contradictory-write fail-closed
# policy remains unchanged by the upstream planning correction.
class ContentsAPI:
    def __init__(self): self.value = None; self.sha = None
    def request(self, method, path, payload=None, expected=()):
        if method == "GET":
            if self.value is None: return None, {}
            return {"content": base64.b64encode(self.value).decode(), "sha": self.sha}, {}
        self.value = base64.b64decode(payload["content"]); self.sha = "stored-sha"; return {}, {}

api = ContentsAPI(); store = GitHubCycleResultStore(api, "fixture")
record = next(r for r in base_records if r["source_id"] == "fred_macro")
store.put(record)
contradiction = copy.deepcopy(record)
contradiction["result"]["candidate_artifact_id"] = "different-candidate"
try: store.put(contradiction)
except IdentityCollisionError: pass
else: raise AssertionError("contradictory durable cycle result was accepted")

print("Smoke 197 normal re-entry completion passed")
