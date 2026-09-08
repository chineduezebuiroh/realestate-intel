"""Smoke 205: one-time July control-plane migration is closed and recoverable."""
import copy
import json
from pathlib import Path

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.control_plane_migration import (CATALOG_PATH, CYCLE_ID,
    EVIDENCE_PATHS, READINESS_PATH, build_plan, execute_plan, migration_path)


class Store:
    def __init__(self, value=None, oid=None):
        self.value=copy.deepcopy(value); self.oid=oid; self.writes=0
    def read(self): return copy.deepcopy(self.value),self.oid
    def write(self,value,oid,message):
        if oid != self.oid: raise PublicationError("stale JSON SHA")
        self.value=copy.deepcopy(value); self.oid=f"{self.writes+2}"*40; self.writes+=1


class CatalogStore(Store):
    def _write(self,value,oid,message): self.write(value,oid,message)


source_catalog=json.loads(Path(CATALOG_PATH).read_text())
source_objects={p:json.loads(Path(p).read_text()) for p in (*EVIDENCE_PATHS,READINESS_PATH)}
source_snapshot=copy.deepcopy(source_objects)

# Retain an unrelated existing record and accepted pointer from main.
old_redfin=source_catalog["accepted"]["source"]["redfin"]
old_record=next(r for r in source_catalog["immutable_records"] if r["object_id"]==old_redfin)
target_catalog=empty_catalog(); target_catalog["immutable_records"]=[copy.deepcopy(old_record)]
target_catalog["accepted"]["source"]["redfin"]=old_redfin
target_catalog["compare_and_swap"]["expected_git_blob_sha"]="1"*40
accepted_before=copy.deepcopy(target_catalog["accepted"])
target_objects={p:None for p in (*EVIDENCE_PATHS,READINESS_PATH)}

plan=build_plan(source_catalog=source_catalog,target_catalog=target_catalog,
    source_objects=source_objects,target_objects=target_objects,
    source_catalog_sha="a"*40,target_catalog_sha="1"*40)
assert len(plan["objects"])==9 and all(x["action"]=="create" for x in plan["objects"])
assert old_redfin not in plan["record"]["catalog_records_added"]
assert plan["merged_catalog"]["accepted"]==accepted_before
assert next(r for r in plan["merged_catalog"]["immutable_records"] if r["object_id"]==old_redfin)==old_record

catalog_store=CatalogStore(target_catalog,"1"*40)
stores={p:Store() for p in target_objects}
migration=Store()
out=execute_plan(plan=plan,target_catalog_store=catalog_store,target_stores=stores,migration_store=migration)
assert out["changed"] and migration.value["completion_state"]=="complete"
assert catalog_store.value["accepted"]==accepted_before and catalog_store.writes==1
assert stores[READINESS_PATH].value["records"][0]["consumed"] is False
candidate=stores[READINESS_PATH].value["records"][0]["candidate_artifact_id"]
assert len([r for r in catalog_store.value["immutable_records"] if r["object_id"]==candidate])==1
assert source_objects==source_snapshot
assert not Path(f"config/cohort_promotion_records/{CYCLE_ID}.json").exists()
assert not any(r["object_type"] in {"source_set","canonical_market"}
               for r in catalog_store.value["immutable_records"])

# Rebuild from durable target state: every object/catalog record reuses exactly;
# the completed migration record makes the live exact repeat a no-op.
second=build_plan(source_catalog=source_catalog,target_catalog=catalog_store.value,
    source_objects=source_objects,target_objects={p:s.value for p,s in stores.items()},
    source_catalog_sha="a"*40,target_catalog_sha=catalog_store.oid)
assert not second["catalog_changed"] and all(x["action"]=="reuse" for x in second["objects"])
repeat=execute_plan(plan=second,target_catalog_store=catalog_store,target_stores=stores,migration_store=migration)
assert repeat=={"migration_id":plan["migration_id"],"changed":False,"completion_state":"complete"}
assert catalog_store.writes==1 and all(s.writes==1 for s in stores.values())

# Same catalog identity/different content and same evidence path/different bytes fail closed.
bad_catalog=copy.deepcopy(target_catalog)
required_id=plan["required_artifact_ids"][0]
bad=copy.deepcopy(next(r for r in source_catalog["immutable_records"] if r["object_id"]==required_id))
bad["package_sha256"]="f"*64
bad_catalog["immutable_records"].append(bad)
bad_catalog["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
try:
    build_plan(source_catalog=source_catalog,target_catalog=bad_catalog,source_objects=source_objects,
        target_objects=target_objects,source_catalog_sha="a"*40,target_catalog_sha="1"*40)
except IdentityCollisionError: pass
else: raise AssertionError("catalog identity contradiction accepted")
bad_objects=copy.deepcopy(target_objects); bad_objects[EVIDENCE_PATHS[0]]={"contradiction":True}
try:
    build_plan(source_catalog=source_catalog,target_catalog=target_catalog,source_objects=source_objects,
        target_objects=bad_objects,source_catalog_sha="a"*40,target_catalog_sha="1"*40)
except IdentityCollisionError: pass
else: raise AssertionError("evidence contradiction accepted")

# A stale main catalog precondition performs no catalog, evidence, or record write.
stale_catalog=CatalogStore(target_catalog,"9"*40); stale_stores={p:Store() for p in target_objects}; stale_record=Store()
try: execute_plan(plan=plan,target_catalog_store=stale_catalog,target_stores=stale_stores,migration_store=stale_record)
except PublicationError: pass
else: raise AssertionError("stale catalog SHA accepted")
assert stale_catalog.writes==0 and stale_record.writes==0 and all(s.writes==0 for s in stale_stores.values())

print("Smoke 205 control-plane migration passed")
