"""Smoke 207: arbitrary durable handoff and hosted serving boundaries, offline."""
from __future__ import annotations

import copy
import json
import tempfile
import socket
from pathlib import Path

import duckdb
import yaml

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.hashing import sha256_file, sha256_json
from core.source_artifacts.object_package import build_object_package, validate_object_package
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.serving_market import create_serving_manifest, promote_serving
from jobs.monthly_refresh.cohort import LOGICAL_DIRECT_SOURCES, REQUIRED_SOURCES
from jobs.monthly_refresh.cohort_plan_store import add_plan, validate_durable_plan
from jobs.monthly_refresh.serving_hosted import authorization_token, create_serving_plan

CYCLE="monthly_cycle__2031-04__offlineproof"
socket.create_connection=lambda *_args,**_kwargs: (_ for _ in ()).throw(AssertionError("network forbidden in Phase 3 proof"))
def pin(source, n):
    return {"source_id":source,"artifact_id":f"src__{source}__2031-04__r1__{n:016x}",
            "artifact_content_hash":f"{n%10}"*64,"package_sha256":f"{(n+1)%10}"*64}
physical=[pin(source,n) for n,source in enumerate(REQUIRED_SOURCES,1)]
physical_results=[{"source_id":p["source_id"],"candidate_artifact_id":p["artifact_id"],
    "artifact_content_hash":p["artifact_content_hash"],"package_sha256":p["package_sha256"]} for p in physical]
parents={s:next(p for p in physical if p["source_id"]==s)
         for s in ("census_bps","census_bps_provisional","census_acs1","census_acs5")}
def family(logical, family_parents, n):
    output=pin(logical,n)
    record={"resolution_id":f"{logical}_family_resolution__{n:024x}",
        "parents":[parents[s] for s in family_parents],"output_artifact_id":output["artifact_id"],
        "output_content_hash":output["artifact_content_hash"],"output_package_sha256":output["package_sha256"],
        "accepted_pointer_changed":False,"source_set_created":False,"provider_discovery_performed":False}
    output["resolution_id"]=record["resolution_id"]
    return output,record
bps,bps_record=family("bps",("census_bps","census_bps_provisional"),40)
acs,acs_record=family("acs",("census_acs1","census_acs5"),50)
direct={p["source_id"]:p for p in physical if p["source_id"] not in
        {"census_bps","census_bps_provisional","census_acs1","census_acs5"}}
direct.update(bps=bps,acs=acs)
plan={"schema_version":"monthly_logical_cohort_plan_v1","plan_id":"","cycle_id":CYCLE,
    "physical_source_inventory":list(REQUIRED_SOURCES),"physical_candidates":physical,
    "physical_results":physical_results,
    "logical_source_inventory":list(LOGICAL_DIRECT_SOURCES),"sources":[direct[s] for s in LOGICAL_DIRECT_SOURCES],
    "family_resolutions":{"bps":bps_record,"acs":acs_record},
    "family_resolution_order":["physical_barrier","bps","acs","logical_plan"],
    "accepted_pointers_advanced":False,"source_set_created":False,"canonical_market_created":False,
    "serving_market_created":False,"redfin_consumption_committed":False}
plan["plan_id"]="logical_cohort_plan__"+sha256_json({k:v for k,v in plan.items() if k!="plan_id"})[:24]
assert validate_durable_plan(plan) is plan
assert add_plan(None,plan)[1] and add_plan(plan,plan)[1] is False
bad=copy.deepcopy(plan); bad["sources"][-1]["source_id"]="census_nrc_fred"
bad["plan_id"]="logical_cohort_plan__"+sha256_json({k:v for k,v in bad.items() if k!="plan_id"})[:24]
try: add_plan(plan,bad)
except PublicationError: pass
else: raise AssertionError("legacy NRC entered durable handoff")
stale=copy.deepcopy(plan); stale["family_resolutions"]["bps"]["output_artifact_id"]="changed"
stale["plan_id"]="logical_cohort_plan__"+sha256_json({k:v for k,v in stale.items() if k!="plan_id"})[:24]
try: validate_durable_plan(stale)
except PublicationError: pass
else: raise AssertionError("stale family output entered durable handoff")

with tempfile.TemporaryDirectory() as td:
    root=Path(td); db=root/"market_serving.duckdb"
    con=duckdb.connect(str(db)); con.execute("create table fact_timeseries(i integer)"); con.execute("insert into fact_timeseries values (1)"); con.close()
    canonical="market__2031-04__r1__"+"a"*16
    manifest=create_serving_manifest(root/"serving-market.json",database_path=db,
        canonical_market_artifact_id=canonical,canonical_database_sha256="c"*64,
        validation={"status":"passed","row_count":1,"source_count":9,"duplicate_key_count":0},
        built_at="2031-04-30T23:59:59Z",builder_git_sha="offline")
    package=build_object_package({"serving-market.json":root/"serving-market.json","market_serving.duckdb":db},root/"serving.tar")
    validate_object_package(root/"serving.tar",root/"extracted",object_type="serving_market",
        expected={"object_id":manifest["serving_artifact_id"],"artifact_content_hash":sha256_json(manifest),"member_hashes":package["member_hashes"]})
    plan2=create_serving_plan(canonical_id=canonical,canonical_hash="d"*64,expected_serving=None,
        serving_id=manifest["serving_artifact_id"],serving_hash=sha256_json(manifest))
    token=authorization_token(plan2); changed=copy.deepcopy(plan2); changed["serving_artifact_hash"]="e"*64
    try: authorization_token(changed)
    except PublicationError: pass
    else: raise AssertionError("changed serving plan retained authorization")
    catalog=empty_catalog(); catalog["accepted"]["canonical_market"]=canonical
    def obj(kind,oid,n,meta): return {"object_type":kind,"object_id":oid,"logical_artifact_uri":f"artifact://{kind}/{oid}",
        "remote_repository":"fixture/repo","release_tag":f"{kind}/{oid}","release_id":n,"asset_id":n,
        "asset_filename":oid+".tar","package_sha256":"a"*64,"artifact_content_hash":"b"*64,
        "publication_receipt_id":f"receipt-{n}","publication_state":"published_immutable_verified","metadata":meta}
    catalog["immutable_records"]=[obj("canonical_market",canonical,1,{}),obj("serving_market",manifest["serving_artifact_id"],2,{"canonical_market_artifact_id":canonical})]
    catalog["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
    served,moved=promote_serving(catalog,expected_canonical=canonical,expected_serving=None,serving_artifact_id=manifest["serving_artifact_id"])
    assert moved and promote_serving(served,expected_canonical=canonical,expected_serving=None,serving_artifact_id=manifest["serving_artifact_id"])[1] is False
    drift=copy.deepcopy(catalog); drift["accepted"]["serving_market"]="unexpected"
    try: promote_serving(drift,expected_canonical=canonical,expected_serving=None,serving_artifact_id=manifest["serving_artifact_id"])
    except (IdentityCollisionError,PublicationError): pass
    else: raise AssertionError("serving expected-old drift accepted")

workflow=yaml.safe_load(Path(".github/workflows/serving-market-promotion.yml").read_text())
assert set(workflow.get(True,workflow.get("on")))=={"workflow_dispatch"}
text=Path("jobs/monthly_refresh/serving_hosted.py").read_text()
assert "accepted" in text and "canonical_market" in text and "build_candidate" in text
assert "discover_pin" not in text and "acquire" not in text
print("Smoke 207 arbitrary durable handoff and hosted serving passed")
