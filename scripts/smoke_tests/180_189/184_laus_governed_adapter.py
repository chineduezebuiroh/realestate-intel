"""Smoke 184: pure governed LAUS adapter contract."""
from __future__ import annotations
import copy
import os
import random
import tempfile
from pathlib import Path
import pandas as pd

from sources.bls_laus.artifact import (
    CANONICAL_COLUMNS, acquire, build_request_plan, canonicalize, load_registry,
    reconcile, revision_bounds,
)


def acquired_fixture(plan, *, diagnostic_month=6, required_month=7):
    result=[]
    for request in plan["requests"]:
        blocks=[]
        for sid in request["series_ids"]:
            meta=next(row for row in plan["series"] if row["series_id"]==sid)
            month=required_month if meta["target_controlling"] else diagnostic_month
            blocks.append({"seriesID":sid,"data":[{"year":str(plan["end_year"]),"period":f"M{month:02d}","value":"123.4"},
                                                   {"year":str(plan["end_year"]),"period":"M13","value":"120"}]})
        result.append({"request":dict(request),"response":{"status":"REQUEST_SUCCEEDED","Results":{"series":blocks}}})
    return result


def expect_error(fn, text):
    try: fn()
    except (ValueError, RuntimeError) as exc:
        assert text.lower() in str(exc).lower(), (text, str(exc)); return
    raise AssertionError(f"expected error containing {text}")


def main():
    registry=load_registry(); assert len(registry)==820
    assert sum(r["target_controlling"] for r in registry)==615
    assert len({r["geo_id"] for r in registry})==205
    assert all(r["seasonal_adjustment"]=="NSA" and r["scale_transform"]=="none" for r in registry)
    assert not any(r["series_id"].startswith("LAS") for r in registry)
    hashes={"fixture":"0"*64}
    ordinary=build_request_plan(acquisition_mode="ordinary_overlap",end_year=2026,config_hashes=hashes)
    deep=build_request_plan(acquisition_mode="deep_reconciliation",end_year=2026,config_hashes=hashes)
    bootstrap=build_request_plan(acquisition_mode="bootstrap",end_year=2026,config_hashes=hashes)
    assert revision_bounds("ordinary_overlap",2026)==(2024,2026)
    assert revision_bounds("deep_reconciliation",2026)==revision_bounds("bootstrap",2026)==(1976,2026)
    assert len(ordinary["requests"])==17 and len(deep["requests"])==len(bootstrap["requests"])==51
    assert ordinary==build_request_plan(acquisition_mode="ordinary_overlap",end_year=2026,config_hashes=hashes)
    assert "secret" not in str(ordinary).lower()

    acquired=acquired_fixture(ordinary); frame,diag,obs=canonicalize(ordinary,acquired)
    assert list(frame.columns)==list(CANONICAL_COLUMNS) and len(frame)==820
    assert diag["target_month"]=="2026-07" and diag["required_series_count"]==615
    assert diag["diagnostic_series_count"]==205 and diag["m13_discarded_count"]==820
    assert set(frame.date)=={pd.Timestamp("2026-06-30").date(),pd.Timestamp("2026-07-31").date()}
    assert set(frame.value)=={123.4} and all(item["lag_months"]==1 for item in diag["diagnostic_lag"])
    shuffled=copy.deepcopy(acquired); random.Random(7).shuffle(shuffled)
    for item in shuffled: random.Random(item["request"]["batch_index"]).shuffle(item["response"]["Results"]["series"])
    frame2,diag2,obs2=canonicalize(ordinary,shuffled)
    pd.testing.assert_frame_equal(frame,frame2); assert obs==obs2 and diag["provider_release_id"]==diag2["provider_release_id"]
    expect_error(lambda:canonicalize(ordinary,acquired[:-1]),"missing acquired")
    unknown=copy.deepcopy(acquired); unknown[0]["response"]["Results"]["series"][0]["seriesID"]="UNKNOWN"
    expect_error(lambda:canonicalize(ordinary,unknown),"membership mismatch")
    duplicate=copy.deepcopy(acquired); duplicate[0]["response"]["Results"]["series"].append(copy.deepcopy(duplicate[0]["response"]["Results"]["series"][0]))
    expect_error(lambda:canonicalize(ordinary,duplicate),"duplicate")
    expect_error(lambda:canonicalize(ordinary,acquired,prior_target_month="2026-08"),"regression")

    unavailable=copy.deepcopy(acquired)
    for item in unavailable:
        for block in item["response"]["Results"]["series"]:
            block["data"].insert(0,{"year":"2026","period":"M05","value":"-",
                "footnotes":[{"code":"X","text":"Data unavailable due to a lapse in appropriations."}]})
    hole_frame,hole_diag,hole_obs=canonicalize(ordinary,unavailable)
    assert len(hole_frame)==820 and hole_diag["target_month"]=="2026-07"
    assert hole_diag["provider_unavailable_count"]==820
    assert hole_diag["provider_unavailable_series_count"]==820
    assert hole_diag["provider_unavailable_by_period"]=={"2026-05":820}
    assert hole_diag["recognized_unavailable_codes"]==["X"]
    assert sum(item["status"]=="provider_unavailable" for item in hole_obs)==820
    assert hole_diag["provider_release_id"]!=diag["provider_release_id"]
    bare=copy.deepcopy(acquired); bare[0]["response"]["Results"]["series"][0]["data"][0]["value"]="-"
    expect_error(lambda:canonicalize(ordinary,bare),"numeric")
    frontier=copy.deepcopy(acquired)
    for item in frontier:
        for block in item["response"]["Results"]["series"]:
            meta=next(row for row in ordinary["series"] if row["series_id"]==block["seriesID"])
            if meta["target_controlling"]:
                block["data"]=[{"year":"2026","period":"M05","value":"123.4"},
                    {"year":"2026","period":"M06","value":"-","footnotes":[{"code":"X"}]}]
    _,frontier_diag,_=canonicalize(ordinary,frontier)
    assert frontier_diag["target_month"]=="2026-05"  # unavailable M06 cannot advance target

    calls=[]
    def transport(url,*,json,timeout):
        calls.append(json); return {"status":"REQUEST_SUCCEEDED","Results":{"series":[]}}
    one=copy.deepcopy(ordinary); one["requests"]=one["requests"][:1]
    acquire(one,api_key="secret",transport=transport,sleep=lambda _:None)
    assert calls[0]["registrationkey"]=="secret" and "secret" not in one["source_request_identity"]

    provider=frame.iloc[:2].copy(); prior=provider.copy(); prior.loc[0,"value"]=1.0
    extra=prior.iloc[[0]].copy(); extra["date"]=pd.Timestamp("2020-01-31").date(); extra["value"]=9.0
    result=reconcile(pd.concat([prior,extra],ignore_index=True),provider)
    assert len(result)==3 and result.loc[result.date.eq(provider.iloc[0].date),"value"].iloc[0]==provider.iloc[0].value
    assert 9.0 in result.value.values
    print("Smoke 184 passed: governed LAUS adapter is strict and deterministic.")


if __name__=="__main__": main()
