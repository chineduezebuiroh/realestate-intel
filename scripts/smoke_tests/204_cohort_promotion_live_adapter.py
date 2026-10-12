"""Smoke 204: isolated GitHub-backed live-adapter CAS and recovery behavior."""
import copy

from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from jobs.monthly_refresh.cohort_promotion_hosted import (execute_one,
    execute_to_completion, persist_prepared)

# Real immutable ACS packages are shared with the adversarial boundary tests.
# HTTP is intercepted only in the offline fixture; production has no such hook.
import sys
import tempfile
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
from test_acs_boundary_hardening import promotion_fixture, CatalogCAS, JSONCAS
_fixture = tempfile.TemporaryDirectory()
record, proof, catalog, readiness, api, validation = promotion_fixture(Path(_fixture.name))
sources = tuple(record["target_source_pointers"])
targets = record["target_source_pointers"]
source_set_id = record["source_set_id"]
market_id = record["canonical_artifact_id"]

# Prepared record is create-once, exact-repeat reuse, contradiction fail-closed.
prepared=JSONCAS(); assert persist_prepared(prepared,record,acs_proof=proof)[1]
assert not persist_prepared(prepared,record,acs_proof=proof)[1] and prepared.writes==1
bad=copy.deepcopy(record); bad["promotion_id"]="cohort_promotion__"+"f"*24
try: persist_prepared(prepared,bad,acs_proof=proof)
except (PublicationError, ValueError): pass
else: raise AssertionError("prepared-record contradiction accepted")

# First execution, one write per call/reread, exact repeat, and no forbidden state.
cas=CatalogCAS(catalog); ready=JSONCAS(readiness)
outcome=execute_to_completion(record,cas,ready,acs_proof=proof)
assert outcome["progress"]["complete"] and outcome["exact_rerun_noop"]
assert cas.writes==2+len(sources) and ready.writes==1
assert not {"census_acs1","census_acs5","census_bps","census_bps_provisional"}&set(cas.value["accepted"]["source"])
assert cas.value["accepted"]["serving_market"] is None

# Resume after every operation boundary.
for boundary in range(2+len(sources)+1):
    c=CatalogCAS(catalog); r=JSONCAS(readiness)
    for _ in range(boundary): execute_one(record,c,r,acs_proof=proof)
    assert execute_to_completion(record,c,r,acs_proof=proof)["progress"]["complete"]

# Stale blob SHA, pointer drift, and readiness contradiction fail closed.
c=CatalogCAS(catalog); c.fail=True
try: execute_one(record,c,JSONCAS(readiness),acs_proof=proof)
except (PublicationError, ValueError): pass
else: raise AssertionError("stale catalog CAS accepted")
drift=copy.deepcopy(catalog); drift["accepted"]["source"]["ces"]=targets["fred_macro"]
try: execute_one(record,CatalogCAS(drift),JSONCAS(readiness),acs_proof=proof)
except (IdentityCollisionError,PublicationError): pass
else: raise AssertionError("pointer contradiction accepted")
bad_ready=copy.deepcopy(readiness); bad_ready["records"][0]["candidate_artifact_id"]=targets["ces"]
try: execute_one(record,CatalogCAS(catalog),JSONCAS(bad_ready),acs_proof=proof)
except (PublicationError, ValueError): pass
else: raise AssertionError("readiness contradiction accepted")

_fixture.cleanup()
print("Smoke 204 cohort promotion live adapter passed")
