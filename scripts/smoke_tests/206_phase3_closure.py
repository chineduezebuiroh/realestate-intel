"""Smoke 206: full-cohort authorization, ordered recovery, and serving CAS."""
from __future__ import annotations

import copy

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.promotion import (SOURCE_TRANSITION_ORDER,
    create_promotion_record, recover_promotion)
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.serving_market import promote_serving
from jobs.monthly_refresh.phase3 import authorization_token, authorize, validate_logical_plan


def record(kind, object_id, source=None, canonical=None, number=1):
    metadata = ({"source_id":source, "data_sha256":"d"*64,
                 "provider_release_id":"fixture", "observation_max":"2026-08-31"}
                if kind == "source" else {})
    if canonical: metadata["canonical_market_artifact_id"] = canonical
    return {"object_type":kind,"object_id":object_id,
        "logical_artifact_uri":f"artifact://{kind}/{object_id}","remote_repository":"fixture/repo",
        "release_tag":f"{kind.replace('_','-')}/{object_id}","release_id":1000+number,
        "asset_id":2000+number,"asset_filename":object_id+".tar","package_sha256":"a"*64,
        "artifact_content_hash":"b"*64,"publication_receipt_id":f"publication_receipt__{number}",
        "publication_state":"published_immutable_verified","metadata":metadata}


cycle="monthly_cycle__2026-08__fixture"
targets={source:f"src__{source}__2026-08__r1__{index:016x}"
         for index,source in enumerate(SOURCE_TRANSITION_ORDER,1)}
plan={"schema_version":"monthly_logical_cohort_plan_v1","cycle_id":cycle,
      "logical_source_inventory":list(SOURCE_TRANSITION_ORDER),
      "sources":[{"source_id":s,"artifact_id":targets[s],"artifact_content_hash":"b"*64,
                  "package_sha256":"a"*64} for s in SOURCE_TRANSITION_ORDER]}
validate_logical_plan(plan)
for forbidden in ("census_bps","census_bps_provisional","census_acs1","census_acs5","census_nrc_fred"):
    bad=copy.deepcopy(plan); bad["sources"][-1]["source_id"]=forbidden
    try: validate_logical_plan(bad)
    except PublicationError: pass
    else: raise AssertionError("forbidden physical/legacy source entered plan")

catalog=empty_catalog()
for index,source in enumerate(SOURCE_TRANSITION_ORDER,1):
    catalog["immutable_records"].append(record("source",targets[source],source=source,number=index))
source_set="source_set__2026-08__v2__"+"1"*16
canonical="market__2026-08__r1__"+"2"*16
catalog["immutable_records"] += [record("source_set",source_set,number=20),
                                  record("canonical_market",canonical,number=21)]
catalog["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
readiness={"records":[{"readiness_id":"redfin_readiness__"+cycle,"cycle_id":cycle,
    "source_id":"redfin","candidate_artifact_id":targets["redfin"],"consumed":False}]}
promotion=create_promotion_record(cycle_id=cycle,source_set_id=source_set,
    source_set_semantic_sha256="c"*64,canonical_artifact_id=canonical,
    canonical_artifact_hash="d"*64,
    expected_source_pointers={s:None for s in SOURCE_TRANSITION_ORDER},target_source_pointers=targets,
    expected_source_set=None,expected_canonical=None,readiness_id=readiness["records"][0]["readiness_id"],
    resolution_id="full-family-resolution-fixture")
preflight={"promotion_record":promotion,"authorization_token":authorization_token(promotion)}
assert authorize(preflight,preflight["authorization_token"]) == promotion
changed=copy.deepcopy(promotion); changed["target_source_pointers"]["ces"]="changed"
try: authorize({"promotion_record":changed,"authorization_token":preflight["authorization_token"]},preflight["authorization_token"])
except PublicationError: pass
else: raise AssertionError("stale authorization accepted a changed plan")

# The two object transitions, ten sources in governed order, then Redfin last.
state,ready=catalog,readiness
observed=[]
for _ in range(13):
    before=copy.deepcopy(state); before_ready=copy.deepcopy(ready)
    state,ready,progress=recover_promotion(promotion,state,ready,max_operations=1)
    if before["accepted"].get("source_set") != state["accepted"].get("source_set"): observed.append("accepted.source_set")
    elif before["accepted"].get("canonical_market") != state["accepted"].get("canonical_market"): observed.append("accepted.canonical_market")
    else:
        moved=[s for s in SOURCE_TRANSITION_ORDER if before["accepted"]["source"].get(s)!=state["accepted"]["source"].get(s)]
        observed.append("accepted.source."+moved[0] if moved else "consume.redfin")
assert observed == ["accepted.source_set","accepted.canonical_market",*["accepted.source."+s for s in SOURCE_TRANSITION_ORDER],"consume.redfin"]
assert progress["complete"] and ready["records"][0]["consumed"]
repeat=recover_promotion(promotion,state,ready,max_operations=1)
assert repeat[0]==state and repeat[1]==ready

partial_catalog,partial_ready,_=recover_promotion(promotion,catalog,readiness,max_operations=10)
assert not partial_ready["records"][0]["consumed"]
drift=copy.deepcopy(catalog); drift["accepted"]["source"]["ces"]=targets["fred_macro"]
try: recover_promotion(promotion,drift,readiness)
except (IdentityCollisionError, PublicationError): pass
else: raise AssertionError("CAS accepted changed source authority")

serving="serving__fixture"
state["immutable_records"].append(record("serving_market",serving,canonical=canonical,number=30))
state["immutable_records"].sort(key=lambda r:(r["object_type"],r["object_id"]))
served,moved=promote_serving(state,expected_canonical=canonical,expected_serving=None,serving_artifact_id=serving)
assert moved and served["accepted"]["serving_market"]==serving
assert promote_serving(served,expected_canonical=canonical,expected_serving=None,serving_artifact_id=serving)[1] is False
failed=copy.deepcopy(state); failed["accepted"]["canonical_market"]="other"
try: promote_serving(failed,expected_canonical=canonical,expected_serving=None,serving_artifact_id=serving)
except (IdentityCollisionError, PublicationError): pass
else: raise AssertionError("serving CAS accepted changed canonical authority")
# A serving failure leaves the already-complete cohort authority untouched.
assert state["accepted"]["canonical_market"]==canonical and ready["records"][0]["consumed"]

print("Smoke 206 Phase 3 closure passed")
