"""Smoke 181: generic durable governed-source resume classification."""
import copy, json
from pathlib import Path
import yaml
from jobs.monthly_refresh.cohort import barrier_evidence, resolve_invocation, resolve_resume_results
from jobs.monthly_refresh.cycle_results import load_registry

catalog=json.loads(Path("config/artifact_catalog.json").read_text())
readiness=json.loads(Path("config/monthly_refresh_readiness.json").read_text())
policy=json.loads(Path("config/monthly_refresh_policy.json").read_text())
registry=load_registry(Path("config/monthly_source_cycle_results.json"))
cycle=resolve_invocation(mode="resume",policy_path=Path("config/monthly_refresh_policy.json"),readiness=readiness,catalog=catalog,supplied_cycle_id="monthly_cycle__2026-07__7cab1c5df177a1e4")
plan=resolve_resume_results(cycle=cycle,catalog=catalog,registry=registry,policy=policy)
assert plan["reuse"]==["ces","fred_macro","redfin"] and plan["run"]==[]
evidence=barrier_evidence(cycle=cycle,results=[],reused_results=plan["results"],pins=plan["pins"],github={})
assert evidence["barrier_status"]=="ready" and evidence["reused_source_ids"]==["ces","fred_macro","redfin"] and evidence["retry_source_ids"]==[]
assert not evidence["accepted_pointers_advanced"] and not evidence["source_set_created"] and not evidence["redfin_consumption_committed"]

def without(source_id):
 changed=copy.deepcopy(registry); changed["records"]=[r for r in changed["records"] if r["source_id"]!=source_id]
 return resolve_resume_results(cycle=cycle,catalog=catalog,registry=changed,policy=policy)
fred_failed=without("fred_macro"); assert fred_failed["reuse"]==["ces","redfin"] and fred_failed["run"]==["fred_macro"]
ces_failed=without("ces"); assert ces_failed["reuse"]==["fred_macro","redfin"] and ces_failed["run"]==["ces"]

missing_catalog=copy.deepcopy(catalog)
fred_id=next(r["result"]["candidate_artifact_id"] for r in registry["records"] if r["source_id"]=="fred_macro")
missing_catalog["immutable_records"]=[r for r in missing_catalog["immutable_records"] if r["object_id"]!=fred_id]
missing=resolve_resume_results(cycle=cycle,catalog=missing_catalog,registry=registry,policy=policy)
assert missing["reuse"]==["ces","redfin"] and missing["run"]==["fred_macro"]
drift=copy.deepcopy(registry); drift["records"][0]["result"]["package_sha256"]="0"*64
try: resolve_resume_results(cycle=cycle,catalog=catalog,registry=drift,policy=policy)
except ValueError as exc: assert "identity drift" in str(exc)
else: raise AssertionError("mismatched durable package was reused")

replay_plan=resolve_resume_results(cycle=dict(cycle,invocation_mode="replay"),catalog=catalog,registry=registry,policy=policy)
assert replay_plan["reuse"]==[] and replay_plan["run"]==["redfin","fred_macro","ces"]
workflow=yaml.safe_load(Path(".github/workflows/monthly-refresh-production.yml").read_text()); jobs=workflow["jobs"]
assert "run_fred == 'true'" in jobs["fred"]["if"] and "run_ces == 'true'" in jobs["ces"]["if"]
barrier_run=next(s for s in jobs["barrier"]["steps"] if "CYCLE_JSON" in s.get("env",{}))
assert "RESUME_FRED" in barrier_run["env"] and "RESUME_CES" in barrier_run["env"]
assert "request_end_year" not in jobs["ces"]["with"] and jobs["ces"]["with"]["target_month"]
print("Smoke 181 monthly resume semantics passed")
