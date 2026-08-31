"""Smoke 185: LAUS equivalence classifications and recoverable workspace."""
from __future__ import annotations
import argparse
import json
import shutil
import tempfile
from pathlib import Path
import pandas as pd
import jobs.monthly_refresh.laus_bootstrap as bootstrap_module

from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.laus_bootstrap import (
    acceptance_gates, equivalence_audit, persist_acquisition, preflight, recover,
    validate_workspace,
)
from sources.bls_laus.artifact import build_request_plan, canonicalize

KEY=["geo_id","metric_id","date","property_type_id"]

def fixture(plan):
    out=[]
    for request in plan["requests"]:
        blocks=[{"seriesID":sid,"data":[{"year":str(request["end_year"]),"period":"M05","value":"100"}]} for sid in request["series_ids"]]
        out.append({"request":dict(request),"response":{"status":"REQUEST_SUCCEEDED","Results":{"series":blocks}}})
    return out

def legacy_row(row,value=None,configured=True,date=None):
    return {"geo_id":row.geo_id,"metric_id":row.metric_id,"date":date or row.date,
            "property_type_id":"all","value":row.value if value is None else value,
            "source_id":"laus","identity_configured":configured}

def expect_error(fn,text):
    try: fn()
    except (RuntimeError,ValueError) as exc: assert text.lower() in str(exc).lower(); return
    raise AssertionError("expected failure")

def main():
    plan=build_request_plan(acquisition_mode="bootstrap",end_year=2026,config_hashes={"fixture":"0"*64})
    frame,diag,obs=canonicalize(plan,fixture(plan)); row=next(frame[frame.date == frame.date.max()].itertuples(index=False))
    provider_one=frame[(frame.geo_id==row.geo_id)&(frame.metric_id==row.metric_id)&(frame.date==row.date)]
    exact=pd.DataFrame([legacy_row(row)])
    _,summary=equivalence_audit(provider_one,exact,plan); assert summary["exact_match_count"]==1
    revised=pd.DataFrame([legacy_row(row,value=99)])
    _,summary=equivalence_audit(provider_one,revised,plan); assert summary["provider_revision_count"]==1
    _,summary=equivalence_audit(provider_one,revised,plan,allow_provider_revisions=False); assert summary["unexplained_numeric_mismatch_count"]==1
    old=pd.DataFrame([legacy_row(row,date=pd.Timestamp("2025-05-31").date())])
    _,summary=equivalence_audit(provider_one,old,plan); assert summary["provider_newer_count"]==1 and summary["legacy_prior_only_count"]==1
    empty=pd.DataFrame(columns=KEY+["value","source_id","identity_configured"])
    _,summary=equivalence_audit(provider_one,empty,plan); assert summary["provider_historical_only_count"]==1
    drift=pd.DataFrame([legacy_row(row,configured=False)])
    _,summary=equivalence_audit(provider_one,drift,plan)
    assert summary["governed_identity_conflict_count"]==1
    conflict=acceptance_gates(frame,{**diag,"missing_series":[]},summary)
    assert not conflict["checks"]["no_governed_identity_conflict"] and conflict["status"]=="failed"
    outside=legacy_row(row,configured=False); outside["geo_id"]="legacy_geo"
    outside_frame=pd.DataFrame([outside])
    detail,summary=equivalence_audit(provider_one,outside_frame,plan)
    assert summary["legacy_out_of_governed_scope_count"]==1
    outside_detail=detail[detail.comparison_category.eq("LEGACY_OUT_OF_GOVERNED_SCOPE")]
    assert len(outside_detail)==1 and outside_detail.iloc[0]["_merge"]=="right_only"
    allowed=acceptance_gates(frame,{**diag,"missing_series":[]},summary)
    assert allowed["checks"]["no_governed_identity_conflict"]
    # The same classification is used for secondary evidence, while only the
    # primary summary is supplied to acceptance_gates.
    _,secondary=equivalence_audit(provider_one,outside_frame,plan)
    assert secondary["legacy_out_of_governed_scope_count"]==1 and allowed["status"]=="passed"
    many=frame.iloc[:5].copy(); scaled=pd.DataFrame([legacy_row(r,value=r.value*1000) for r in many.itertuples(index=False)])
    _,summary=equivalence_audit(many,scaled,plan); assert summary["unit_scale_mismatch_count"]==5 and summary["unit_scale"]["unit_scale_mismatch"]
    bad=acceptance_gates(many,{**diag,"missing_series":[]},summary); assert bad["status"]=="failed"

    catalog={"accepted":{"source":{}},"immutable_records":[]}; assert preflight(catalog)["series_count"]==820
    with tempfile.TemporaryDirectory() as td:
        root=Path(td)/"workspace"; root.mkdir(); write_canonical_json(root/"preflight.json",preflight(catalog)); write_canonical_json(root/"request_plan.json",plan)
        write_canonical_json(root/"acquired.json",fixture(plan))
        persist_acquisition(root,plan,frame,diag,obs)
        validate_workspace(root,expected_end_year=2026)
        args=argparse.Namespace(output_root=root,end_year=2026,legacy_serving=Path(td)/"none.duckdb",
            legacy_secondary=[],retrieved_at="2026-08-30T00:00:00Z")
        first=recover(args); second=recover(args)
        assert first["artifact_id"]==second["artifact_id"] and second["reused"]
        expect_error(lambda:validate_workspace(root,expected_end_year=2025),"contradiction")
        corrupt=Path(td)/"corrupt"; shutil.copytree(root,corrupt); (corrupt/"canonical.parquet").write_bytes(b"bad")
        expect_error(lambda:validate_workspace(corrupt,expected_end_year=2026),"corrupt")
        missing=Path(td)/"missing"; shutil.copytree(root,missing); (missing/"completeness.json").unlink()
        expect_error(lambda:validate_workspace(missing,expected_end_year=2026),"incomplete")
        raw=Path(td)/"raw"; raw.mkdir()
        write_canonical_json(raw/"preflight.json",preflight(catalog)); write_canonical_json(raw/"request_plan.json",plan)
        write_canonical_json(raw/"acquired.json",fixture(plan))
        raw_args=argparse.Namespace(output_root=raw,end_year=2026,legacy_serving=Path(td)/"none.duckdb",
            legacy_secondary=[],retrieved_at="2026-08-30T00:00:00Z")
        original=bootstrap_module.acquire
        bootstrap_module.acquire=lambda *a,**k: (_ for _ in ()).throw(AssertionError("transport invoked"))
        try: recovered=recover(raw_args)
        finally: bootstrap_module.acquire=original
        assert recovered["status"]=="audit_passed" and (raw/"acquisition.json").is_file()
    print("Smoke 185 passed: LAUS equivalence and workspace recovery fail closed.")

if __name__=="__main__": main()
