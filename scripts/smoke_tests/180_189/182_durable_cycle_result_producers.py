"""Smoke 182: parallel-safe automated-source durable result producers."""
import copy, json
from pathlib import Path
import yaml
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.cohort import resolve_invocation, resolve_resume_results
from jobs.monthly_refresh.cycle_results import add_record, governed_record, load_registry, record_path, semantic_identity

catalog=json.loads(Path("config/artifact_catalog.json").read_text()); policy=json.loads(Path("config/monthly_refresh_policy.json").read_text())
readiness=json.loads(Path("config/monthly_refresh_readiness.json").read_text()); registry=load_registry(Path("config/monthly_source_cycle_results.json"))
cycle_id="monthly_cycle__2026-07__7cab1c5df177a1e4"; records={r["source_id"]:r for r in registry["records"]}
for source in ("fred_macro","ces"):
 proposed=governed_record(records[source]["result"],policy,catalog); assert proposed==records[source]
 assert record_path(cycle_id,source).endswith(f"/{source}.json")
 same,changed=add_record(proposed,copy.deepcopy(proposed)); assert not changed and semantic_identity(same)==semantic_identity(proposed)
 conflict=copy.deepcopy(proposed); conflict["result"]["package_sha256"]="0"*64
 try: add_record(proposed,conflict)
 except IdentityCollisionError as exc: assert "collision" in str(exc)
 else: raise AssertionError("contradictory repeat did not fail closed")

fred,ces=records["fred_macro"],records["ces"]
for order in ((fred,ces),(ces,fred)):
 store={}
 for proposed in order:
  path=record_path(proposed["cycle_id"],proposed["source_id"]); store[path],changed=add_record(store.get(path),proposed); assert changed
 assert {r["source_id"]:semantic_identity(r) for r in store.values()}=={"fred_macro":semantic_identity(fred),"ces":semantic_identity(ces)}

def reject(result,cat=catalog):
 try: governed_record(result,policy,cat)
 except ValueError: return
 raise AssertionError("invalid evidence became durable")
unverified=copy.deepcopy(fred["result"]); unverified["publication_state"]="not_published"; reject(unverified)
missing=copy.deepcopy(catalog); missing["immutable_records"]=[r for r in missing["immutable_records"] if r["object_id"]!=fred["result"]["candidate_artifact_id"]]; reject(fred["result"],missing)
for field in ("artifact_content_hash","package_sha256","provider_release_id"):
 bad=copy.deepcopy(fred["result"]); bad[field]="0"*64; reject(bad)
wrong=copy.deepcopy(catalog); next(r for r in wrong["immutable_records"] if r["object_id"]==fred["result"]["candidate_artifact_id"])["metadata"]["source_id"]="ces"; reject(fred["result"],wrong)

cycle=resolve_invocation(mode="resume",policy_path=Path("config/monthly_refresh_policy.json"),readiness=readiness,catalog=catalog,supplied_cycle_id=cycle_id)
for retained,reuse,run in ((fred,["fred_macro","redfin"],["ces"]),(ces,["ces","redfin"],["fred_macro"])):
 plan=resolve_resume_results(cycle=cycle,catalog=catalog,registry={"schema_version":"monthly_source_cycle_results_v1","records":[retained]},policy=policy)
 assert plan["reuse"]==reuse and plan["run"]==run
replay=resolve_resume_results(cycle=dict(cycle,invocation_mode="replay"),catalog=catalog,registry=registry,policy=policy)
assert replay["reuse"]==[] and replay["run"]==["redfin","fred_macro","ces"]

for path in (Path(".github/workflows/fred-monthly-source.yml"),Path(".github/workflows/ces-monthly-source.yml")):
 workflow=yaml.safe_load(path.read_text()); steps=workflow["jobs"]["source"]["steps"]; ids=[s.get("id") for s in steps]
 assert ids.index("publish")<ids.index("result")<ids.index("record")<ids.index("final")
 assert workflow["jobs"]["source"]["outputs"]["result_json"]=="${{ steps.final.outputs.result_json }}"
 record=next(s for s in steps if s.get("id")=="record"); assert "cycle_results" in record["run"] and record["continue-on-error"] is True
assert all(r["result"]["accepted_pointer_changed"] is False for r in records.values())
master=Path(".github/workflows/monthly-refresh-production.yml").read_text(); assert "source_set_created" not in master and "redfin_consumption_committed" not in master
print("Smoke 182 durable automated-source cycle results passed")
