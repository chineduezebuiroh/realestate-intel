"""Smoke 164: deterministic governed Redfin + FRED source-artifact vertical slice."""
from __future__ import annotations
import json,tempfile
from pathlib import Path
import duckdb,pandas as pd
from core.source_artifacts import create_artifact,preserve_prior,validate_artifact,ArtifactValidationError
from core.source_artifacts.assembly import assemble
from core.source_artifacts.source_set import create_source_set,validate_source_set
from core.source_artifacts.storage import LocalArtifactResolver
from sources.redfin.governance import FAMILIES,GovernanceError,bootstrap
from sources.redfin.inbox import register_incoming
from sources.redfin.state import bootstrap_state,reconcile_state,emit_artifact
from sources.fred_macro.artifact import produce

def fact(keys,source):
 return pd.DataFrame([{"geo_id":"us","metric_id":m,"date":d,"property_type_id":"all","value":v,"source_id":source,"property_type":"all"} for m,d,v in keys])
def expect(exc,fn):
 try: fn()
 except exc:return
 raise AssertionError("expected fail-closed error")
def raw(path,month): pd.DataFrame([{"period_end":month+"-28","value":1}]).to_csv(path,index=False)

with tempfile.TemporaryDirectory() as td:
 root=Path(td); rawroot=root/"raw"; bootstrap(rawroot)
 names={"nation":"country","state":"states","metro":"metros","county":"counties","city":"cities","neighborhood":"neighborhoods","zip":"zips"}
 for fam in FAMILIES: raw(rawroot/"incoming"/f"redfin_{names[fam]}.csv","2026-08")
 registered=register_incoming(rawroot,clear_incoming=False); assert registered["drop_id"]=="2026-08"
 assert register_incoming(rawroot,clear_incoming=False)["status"]=="already_registered"
 (rawroot/"incoming"/"redfin_states.csv").write_text("period_end,value\n2026-08-28,2\n")
 expect(GovernanceError,lambda:register_incoming(rawroot,clear_incoming=False))
 (rawroot/"incoming"/"redfin_states.csv").unlink(); expect(GovernanceError,lambda:register_incoming(rawroot,clear_incoming=False))
 raw(rawroot/"incoming"/"redfin_states.csv","2026-09"); expect(GovernanceError,lambda:register_incoming(rawroot,clear_incoming=False))
 # Generic and durable reconciliation: A/B + B/C then C/D retains A and revised B after raw evidence disappears.
 baseline=fact([("redfin_a","2026-06-30",1),("redfin_b","2026-06-30",2)],"redfin")
 drop1=fact([("redfin_b","2026-06-30",20),("redfin_c","2026-08-31",3)],"redfin")
 result=preserve_prior(baseline,drop1); assert dict(zip(result.metric_id,result.value))=={"redfin_a":1,"redfin_b":20,"redfin_c":3}
 db=root/"state.duckdb"; assert bootstrap_state(db,baseline,baseline_hash="a"*64)==2
 reconcile_state(db,drop1,drop_id="2026-08",source_hash="b"*64,request_identity="fixture")
 check=duckdb.connect(str(db),read_only=True); before=check.execute("select metric_id,value from canonical_redfin order by 1").fetchall(); check.close()
 expect(RuntimeError,lambda:reconcile_state(db,fact([("redfin_d","2026-09-30",4)],"redfin"),drop_id="2026-09",source_hash="c"*64,request_identity="fixture",fail_after_upsert=True))
 check=duckdb.connect(str(db),read_only=True); assert check.execute("select metric_id,value from canonical_redfin order by 1").fetchall()==before; check.close()
 reconcile_state(db,fact([("redfin_c","2026-08-31",30),("redfin_d","2026-09-30",4)],"redfin"),drop_id="2026-09",source_hash="c"*64,request_identity="fixture")
 red=root/"red"; rm=emit_artifact(db,red,target_month="2026-09",retrieved_at="2026-10-01T00:00:00Z"); assert validate_artifact(red)["rows"]==4
 prior_fred=root/"fred_prior"; create_artifact(prior_fred,fact([("fred_a","2026-06-30",1),("fred_b","2026-06-30",2)],"fred_macro"),source_id="fred_macro",source_family="FRED",source_type="revisionary_current_truth",provider="FRED",distribution_channel="API",provider_release_id="p1",provider_release_timestamp_or_date="2026-06-30",retrieved_at="x",target_month="2026-06",source_request_identity="fixture",source_urls_or_endpoint_identity=["fixture"])
 fred=root/"fred"; fm=produce(fred,fact([("fred_b","2026-06-30",20),("fred_c","2026-09-30",3)],"fred_macro"),target_month="2026-09",provider_release_id="p2",retrieved_at="x",prior_artifact=prior_fred)
 assert dict(zip(pd.read_parquet(fred/"data.parquet").metric_id,pd.read_parquet(fred/"data.parquet").value))=={"fred_a":1,"fred_b":20,"fred_c":3}
 # Tamper, duplicate, source mismatch, deterministic identity and size guardrail.
 original=(fred/"data.parquet").read_bytes(); (fred/"data.parquet").write_bytes(original+b"x"); expect(ArtifactValidationError,lambda:validate_artifact(fred)); (fred/"data.parquet").write_bytes(original)
 duplicate=root/"duplicate"; expect(ValueError,lambda:create_artifact(duplicate,pd.concat([baseline,baseline]),source_id="redfin",source_family="x",source_type="x",provider="x",distribution_channel="x",provider_release_id="x",provider_release_timestamp_or_date="x",retrieved_at="x",target_month="x",source_request_identity="x",source_urls_or_endpoint_identity=[]))
 oversized=root/"oversized"; expect(ValueError,lambda:create_artifact(oversized,baseline,source_id="redfin",source_family="x",source_type="x",provider="x",distribution_channel="x",provider_release_id="x",provider_release_timestamp_or_date="x",retrieved_at="x",target_month="x",source_request_identity="x",source_urls_or_endpoint_identity=[],max_single_asset_bytes=1))
 # Source set, exact hashes, candidate assembly and global collision rejection.
 ss=root/"source_set.json"; create_source_set(ss,[red,fred],target_month="2026-09",created_at="2026-10-01T00:00:00Z")
 resolver=LocalArtifactResolver({rm["artifact_uri"]:red,fm["artifact_uri"]:fred}); assert validate_source_set(ss,resolver)["status"]=="passed"
 candidate=root/"candidate.duckdb"; report=assemble(ss,candidate,resolver); assert report["rows"]==7 and report["sources"]==["fred_macro","redfin"]
 colliding=root/"collision"; coll=fact([("redfin_a","2026-06-30",9)],"fred_macro"); cm=create_artifact(colliding,coll,source_id="fred_macro",source_family="x",source_type="x",provider="x",distribution_channel="x",provider_release_id="x",provider_release_timestamp_or_date="x",retrieved_at="x",target_month="2026-09",source_request_identity="x",source_urls_or_endpoint_identity=[])
 css=root/"collision_set.json"; create_source_set(css,[red,colliding],target_month="2026-09",created_at="x"); cres=LocalArtifactResolver({rm["artifact_uri"]:red,cm["artifact_uri"]:colliding}); expect(ValueError,lambda:assemble(css,root/"collision.duckdb",cres))
 # Missing required artifact and altered package hash fail.
 broken=json.loads(ss.read_text()); broken["included_source_inventory"]=broken["included_source_inventory"][:-1]; (root/"broken.json").write_text(json.dumps(broken)); expect(ArtifactValidationError,lambda:validate_source_set(root/"broken.json",resolver))
 assert rm["artifact_id"]==emit_artifact(db,root/"red_again",target_month="2026-09",retrieved_at="different-time")["artifact_id"]
print("source artifact vertical slice smoke: ok")
