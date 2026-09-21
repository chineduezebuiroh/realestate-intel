"""Create-once durable handoff for Phase 2 logical cohort evidence."""
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.parse
from copy import deepcopy
from pathlib import Path
from typing import Any

from core.source_artifacts.github_release import GitHubAPI
from core.source_artifacts.hashing import canonical_json_bytes, sha256_json, write_canonical_json
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.cohort import REQUIRED_SOURCES
from jobs.monthly_refresh.phase3 import validate_logical_plan

ROOT = "config/monthly_logical_cohort_plans"
NON_MUTATING_FLAGS = ("accepted_pointers_advanced", "source_set_created",
                      "canonical_market_created", "serving_market_created",
                      "redfin_consumption_committed")


def validate_durable_plan(plan: dict[str, Any]) -> dict[str, Any]:
    validate_logical_plan(plan)
    if any(plan.get(flag) is not False for flag in NON_MUTATING_FLAGS):
        raise PublicationError("logical cohort plan contains forbidden mutation")
    physical = plan.get("physical_candidates", [])
    physical_by_source = {item.get("source_id"): item for item in physical}
    if len(physical_by_source) != len(physical) or tuple(plan.get("physical_source_inventory", ())) != REQUIRED_SOURCES \
            or set(physical_by_source) != set(REQUIRED_SOURCES):
        raise PublicationError("durable plan physical inventory mismatch")
    results=plan.get("physical_results",[]); results_by_source={item.get("source_id"):item for item in results}
    if len(results_by_source)!=len(results) or set(results_by_source)!=set(REQUIRED_SOURCES):
        raise PublicationError("durable plan physical result inventory mismatch")
    for source,pin in physical_by_source.items():
        result=results_by_source[source]
        if (result.get("candidate_artifact_id"),result.get("artifact_content_hash"),result.get("package_sha256")) != \
                (pin["artifact_id"],pin["artifact_content_hash"],pin["package_sha256"]):
            raise PublicationError(f"durable plan physical result identity drift: {source}")
    for item in [*physical, *plan["sources"]]:
        if not {"source_id", "artifact_id", "artifact_content_hash", "package_sha256"} <= set(item):
            raise PublicationError("durable plan artifact identity incomplete")
    families = plan.get("family_resolutions")
    if not isinstance(families, dict) or set(families) != {"bps", "acs"}:
        raise PublicationError("durable plan family resolution inventory mismatch")
    for logical, parents in (("bps", {"census_bps", "census_bps_provisional"}),
                             ("acs", {"census_acs1", "census_acs5"})):
        resolution = families[logical]
        if resolution.get("resolution_id") != next(s["resolution_id"] for s in plan["sources"] if s["source_id"] == logical):
            raise PublicationError(f"{logical} durable resolution identity mismatch")
        if resolution.get("output_artifact_id") != next(s["artifact_id"] for s in plan["sources"] if s["source_id"] == logical):
            raise PublicationError(f"{logical} durable output identity mismatch")
        actual = {p.get("source_id") for p in resolution.get("parents", [])}
        if actual != parents:
            raise PublicationError(f"{logical} durable parent inventory mismatch")
        if any(resolution.get(flag) is not False for flag in
               ("accepted_pointer_changed", "source_set_created", "provider_discovery_performed")):
            raise PublicationError(f"{logical} durable resolution contains mutation")
    semantic = {key:value for key,value in plan.items() if key != "plan_id"}
    if plan.get("plan_id") != "logical_cohort_plan__" + sha256_json(semantic)[:24]:
        raise PublicationError("durable logical cohort plan identity mismatch")
    return plan


def add_plan(existing: dict[str, Any] | None, proposed: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    validate_durable_plan(proposed)
    if existing is None: return deepcopy(proposed), True
    validate_durable_plan(existing)
    if existing == proposed: return deepcopy(existing), False
    raise IdentityCollisionError("contradictory durable logical cohort plan")


class GitHubCohortPlanStore:
    def __init__(self, api: GitHubAPI, branch: str): self.api, self.branch = api, branch
    def path(self, cycle_id: str) -> str: return f"{ROOT}/{cycle_id}.json"
    def get(self, cycle_id: str) -> dict[str, Any]:
        path = urllib.parse.quote(self.path(cycle_id), safe="/")
        item, _ = self.api.request("GET", f"/contents/{path}?ref={urllib.parse.quote(self.branch)}", expected=(200,404))
        if item is None: raise PublicationError("durable logical cohort plan is absent")
        value = json.loads(base64.b64decode(item["content"]))
        if value.get("cycle_id") != cycle_id: raise PublicationError("durable logical cohort cycle mismatch")
        return validate_durable_plan(value)
    def put(self, plan: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        path = urllib.parse.quote(self.path(plan["cycle_id"]), safe="/")
        item, _ = self.api.request("GET", f"/contents/{path}?ref={urllib.parse.quote(self.branch)}", expected=(200,404))
        existing = json.loads(base64.b64decode(item["content"])) if item else None
        value, changed = add_plan(existing, plan)
        if changed:
            payload={"message":f"Record {value['plan_id']}","content":base64.b64encode(canonical_json_bytes(value)).decode(),"branch":self.branch}
            if item: payload["sha"]=item["sha"]
            self.api.request("PUT",f"/contents/{path}",payload=payload,expected=(200,201))
        durable=self.get(plan["cycle_id"])
        if durable != value: raise PublicationError("durable logical cohort plan reread contradiction")
        return durable, changed


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--repository",required=True)
    parser.add_argument("--branch",required=True); parser.add_argument("--plan",type=Path,required=True)
    parser.add_argument("--output",type=Path,required=True); args=parser.parse_args()
    api=GitHubAPI(args.repository,os.environ.get("GITHUB_TOKEN","")); plan=json.loads(args.plan.read_text())
    durable,changed=GitHubCohortPlanStore(api,args.branch).put(plan)
    report={"plan":durable,"created":changed,"provider_discovery_performed":False,"accepted_state_mutated":False}
    write_canonical_json(args.output,report); print(json.dumps(report,sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
