"""Smoke 204: isolated GitHub-backed live-adapter CAS and recovery behavior."""
import copy

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.promotion import create_promotion_record
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.cohort_promotion_hosted import (execute_one,
    execute_to_completion, persist_prepared)

SHA="a"*64

def source_record(source, artifact, n):
    return {"object_type":"source","object_id":artifact,
        "logical_artifact_uri":f"artifact://source/{source}/{artifact}","remote_repository":"fixture/repo",
        "release_tag":f"source-artifact/{source}/{artifact}","release_id":n,"asset_id":n,
        "asset_filename":artifact+".tar","package_sha256":"a"*64,
        "artifact_content_hash":"f"*64,"publication_receipt_id":f"receipt-{n}",
        "publication_state":"published_immutable_verified",
        "metadata":{"source_id":source,"data_sha256":SHA,"provider_release_id":"p","observation_max":"2026-07-31"}}

def object_record(kind, object_id, n):
    return {"object_type":kind,"object_id":object_id,
        "logical_artifact_uri":f"artifact://{kind}/{object_id}","remote_repository":"fixture/repo",
        "release_tag":f"{'source-set' if kind=='source_set' else 'canonical-market'}/{object_id}",
        "release_id":n,"asset_id":n,"asset_filename":object_id+".tar","package_sha256":"b"*64,
        "artifact_content_hash":"c"*64,"publication_receipt_id":f"receipt-{n}",
        "publication_state":"published_immutable_verified","metadata":{}}

class CatalogCAS:
    def __init__(self, value): self.value=copy.deepcopy(value); self.oid="1"*40; self.writes=0; self.fail=False
    def read(self): return copy.deepcopy(self.value),self.oid
    def _write(self,value,oid,message):
        assert oid==self.oid
        if self.fail: raise PublicationError("stale expected-old catalog SHA")
        self.value=copy.deepcopy(value); self.value["compare_and_swap"]["expected_git_blob_sha"]=oid
        self.oid=(hex(self.writes+2)[2:]*40)[:40]; self.writes+=1

class JSONCAS:
    def __init__(self,value=None): self.value=copy.deepcopy(value); self.oid=None if value is None else "2"*40; self.writes=0
    def read(self): return copy.deepcopy(self.value),self.oid
    def write(self,value,oid,message):
        assert oid==self.oid; self.value=copy.deepcopy(value); self.oid="3"*40; self.writes+=1

sources=("bps","ces","fred_macro","laus","redfin")
targets={s:f"src__{s}__2026-07__r1__{'1'*16}" for s in sources}
catalog=empty_catalog()
catalog["immutable_records"]=[source_record(s,a,i+1) for i,(s,a) in enumerate(targets.items())]
source_set_id="source_set__2026-07__v2__"+"2"*16
market_id="market__2026-07__r1__"+"3"*16
catalog["immutable_records"] += [object_record("source_set",source_set_id,20),object_record("canonical_market",market_id,21)]
catalog["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
catalog["accepted"]["serving_market"]=None
readiness={"schema_version":"monthly_refresh_readiness_v1","records":[{
    "readiness_id":"ready-july","cycle_id":"cycle-july","source_id":"redfin",
    "candidate_artifact_id":targets["redfin"],"consumed":False}]}
record=create_promotion_record(cycle_id="cycle-july",source_set_id=source_set_id,
    source_set_semantic_sha256="d"*64,canonical_artifact_id=market_id,canonical_artifact_hash="e"*64,
    expected_source_pointers={s:None for s in sources},target_source_pointers=targets,
    expected_source_set=None,expected_canonical=None,readiness_id="ready-july",resolution_id="resolution-july")

# Prepared record is create-once, exact-repeat reuse, contradiction fail-closed.
prepared=JSONCAS(); assert persist_prepared(prepared,record)[1]
assert not persist_prepared(prepared,record)[1] and prepared.writes==1
bad=copy.deepcopy(record); bad["promotion_id"]="cohort_promotion__"+"f"*24
try: persist_prepared(prepared,bad)
except PublicationError: pass
else: raise AssertionError("prepared-record contradiction accepted")

# First execution, one write per call/reread, exact repeat, and no forbidden state.
cas=CatalogCAS(catalog); ready=JSONCAS(readiness)
outcome=execute_to_completion(record,cas,ready)
assert outcome["progress"]["complete"] and outcome["exact_rerun_noop"]
assert cas.writes==2+len(sources) and ready.writes==1
assert not {"census_bps","census_bps_provisional"}&set(cas.value["accepted"]["source"])
assert cas.value["accepted"]["serving_market"] is None

# Resume after every operation boundary.
for boundary in range(2+len(sources)+1):
    c=CatalogCAS(catalog); r=JSONCAS(readiness)
    for _ in range(boundary): execute_one(record,c,r)
    assert execute_to_completion(record,c,r)["progress"]["complete"]

# Stale blob SHA, pointer drift, and readiness contradiction fail closed.
c=CatalogCAS(catalog); c.fail=True
try: execute_one(record,c,JSONCAS(readiness))
except PublicationError: pass
else: raise AssertionError("stale catalog CAS accepted")
drift=copy.deepcopy(catalog); drift["accepted"]["source"]["ces"]=targets["fred_macro"]
try: execute_one(record,CatalogCAS(drift),JSONCAS(readiness))
except (IdentityCollisionError,PublicationError): pass
else: raise AssertionError("pointer contradiction accepted")
bad_ready=copy.deepcopy(readiness); bad_ready["records"][0]["candidate_artifact_id"]=targets["ces"]
try: execute_one(record,CatalogCAS(catalog),JSONCAS(bad_ready))
except PublicationError: pass
else: raise AssertionError("readiness contradiction accepted")

print("Smoke 204 cohort promotion live adapter passed")
