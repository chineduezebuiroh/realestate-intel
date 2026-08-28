"""Smoke 172: deterministic production cohort gating, barrier, retry and drift."""
from pathlib import Path
from tempfile import TemporaryDirectory
from core.source_artifacts.source_set_v2 import create_source_set_v2, governed_config_hashes, validate_source_set_v2
from jobs.monthly_refresh.production import (CycleState, RESULT_VERSION, cycle_id,
    evaluate_barrier, redfin_ready)

SHA = "a" * 64

def result(source, cycle, status="succeeded", artifact=None, retry="retryable"):
    default_artifact = "src__redfin__2026-08__fixture" if source == "redfin" else f"src__{source}__fixture"
    return {"schema_version":RESULT_VERSION,"source_id":source,"cycle_id":cycle,"status":status,
        "candidate_artifact_id":artifact or default_artifact,"artifact_content_hash":SHA,
        "package_sha256":"b"*64,"publication_state":"published_verified" if status=="succeeded" else "not_published",
        "validation_status":"passed" if status=="succeeded" else "failed","provider_release_id":"release-fixture",
        "observation_max":"2026-08-31","prior_artifact_id":None,"source_change_detected":True,
        "retryability":retry,"accepted_pointer_changed":False,"evidence_uri":f"evidence://{source}"}

def main():
    drop={"drop_id":"2026-08","status":"validated","validation_status":"passed",
          "complete_family_count":7,"required_family_count":7,"drop_content_hash":SHA}
    assert not redfin_ready(None,set())                         # no catalyst: no fan-out
    assert redfin_ready(drop,set())
    assert not redfin_ready(drop,{"2026-08"})                  # duplicate catalyst
    cycle=cycle_id(redfin_drop_id="2026-08",redfin_drop_hash=SHA,target_month="2026-08",policy_sha256="c"*64)
    required=("redfin","fred_macro","source_c")
    success=[result(s,cycle) for s in required]
    complete=evaluate_barrier(expected_cycle_id=cycle,required_source_ids=required,results=success)
    assert complete.state==CycleState.SOURCE_SET_VALIDATED and not complete.retry_source_ids
    entries=[]
    for r in complete.candidates:
        entries.append({"source_id":r["source_id"],"artifact_id":r["candidate_artifact_id"],
          "logical_artifact_uri":f"artifact://source/{r['source_id']}/{r['candidate_artifact_id']}",
          "package_sha256":r["package_sha256"],"artifact_content_hash":r["artifact_content_hash"],
          "provider_release_id":r["provider_release_id"],"observation_max":r["observation_max"],
          "validation_status":"passed","monthly_status":"refreshed","release_tag":"fixture",
          "asset_id":1,"publication_receipt_id":"fixture","cycle_check_succeeded":True,
          "carried_forward":False,"carry_forward_policy_allowed":False})
    with TemporaryDirectory() as temp:
        source_set=create_source_set_v2(Path(temp)/"source-set.json",target_month="2026-08",
          created_at="2026-08-25T00:00:00Z",builder_git_sha="fixture",entries=entries,
          config_hashes=governed_config_hashes())
        assert validate_source_set_v2(source_set)["required_source_inventory"]==sorted(required)
    partial=evaluate_barrier(expected_cycle_id=cycle,required_source_ids=required,
        results=[result("redfin",cycle),result("fred_macro",cycle),result("source_c",cycle,"failed")])
    assert partial.state==CycleState.FAILED_RETRYABLE and partial.reusable_source_ids==("fred_macro","redfin")
    pins={r["source_id"]:r["candidate_artifact_id"] for r in partial.candidates}
    retry=evaluate_barrier(expected_cycle_id=cycle,required_source_ids=required,results=success,pinned_candidates=pins)
    assert retry.state==CycleState.SOURCE_SET_VALIDATED and retry.candidates==complete.candidates
    try:
        drift=[result(s,cycle) for s in required]; drift[1]=result("fred_macro",cycle,artifact="different")
        evaluate_barrier(expected_cycle_id=cycle,required_source_ids=required,results=drift,pinned_candidates=pins)
    except ValueError as exc: assert "drift" in str(exc)
    else: raise AssertionError("pointer/candidate drift was accepted")
    print("[monthly_refresh] smoke 172: OK")
    return 0
if __name__=="__main__": raise SystemExit(main())
