"""Smoke 166: governed FRED monthly runner lifecycle and failure closure."""
from __future__ import annotations
import os, tempfile
from pathlib import Path
import pandas as pd
import jobs.monthly_refresh.fred_macro as runner
from core.source_artifacts import validate_artifact
from sources.fred_macro.artifact import acquire_current

METRIC="fred_cpi_urban_sa_index"
def fact(rows,metric=METRIC):
    return pd.DataFrame([{"geo_id":"united_states__nation","metric_id":metric,"date":date,"property_type_id":"all",
                          "value":value,"source_id":"fred_macro","property_type":"all"} for date,value in rows])
def expect(kind,call):
    try: call()
    except kind: return
    raise AssertionError(f"expected {kind.__name__}")

with tempfile.TemporaryDirectory() as td:
    root=Path(td); base=fact([("2026-06-30",1.0),("2026-07-31",2.0)])
    one=runner.run(target_month="2026-07",output_root=root/"one",acquire=lambda:base,retrieved_at="2026-08-01T00:00:00Z",git_sha="fixture")
    assert one["run_status"]=="refreshed" and one["source_change_detected"] and validate_artifact(root/"one/artifact")["status"]=="passed"
    assert one["target_month_resolution"]=="explicit"
    differing=pd.concat([base,fact([("2026-08-31",3.0)],metric="fred_gs2")],ignore_index=True)
    inferred=runner.run(output_root=root/"inferred",acquire=lambda:differing,retrieved_at="2026-09-01T00:00:00Z",git_sha="fixture")
    assert inferred["target_month"]=="2026-08" and inferred["target_month_resolution"]=="inferred_observation_max"
    per_metric={item["metric_id"]:item["observation_max"] for item in inferred["per_metric"]}
    assert per_metric=={METRIC:"2026-07-31","fred_gs2":"2026-08-31"}
    empty_arg=runner.run(target_month="",output_root=root/"empty-arg",acquire=lambda:base,retrieved_at="2026-08-01T00:00:00Z",git_sha="fixture")
    assert empty_arg["target_month"]=="2026-07" and empty_arg["target_month_resolution"]=="inferred_observation_max"
    expect(ValueError,lambda:runner.run(target_month="2026-7",output_root=root/"bad-month",acquire=lambda:base,git_sha="fixture"))
    expect(ValueError,lambda:runner.run(output_root=root/"empty",acquire=lambda:base.iloc[0:0],git_sha="fixture"))
    two=runner.run(target_month="2026-07",output_root=root/"two",prior_artifact=root/"one/artifact",acquire=lambda:base,retrieved_at="later",git_sha="fixture")
    assert two["run_status"]=="unchanged" and two["resulting_artifact_id"]==one["resulting_artifact_id"] and not two["source_change_detected"]
    revised=base.copy(); revised.loc[0,"value"]=1.5
    three=runner.run(target_month="2026-07",output_root=root/"three",prior_artifact=root/"one/artifact",acquire=lambda:revised,retrieved_at="2026-08-02T00:00:00Z",git_sha="fixture")
    assert three["run_status"]=="refreshed" and three["observation_max"]==one["observation_max"] and three["historical_revision_count"]==1
    extended=pd.concat([base,fact([("2026-08-31",3.0)])],ignore_index=True)
    four=runner.run(target_month="2026-08",output_root=root/"four",prior_artifact=root/"one/artifact",acquire=lambda:extended,retrieved_at="2026-09-01T00:00:00Z",git_sha="fixture")
    assert four["new_key_count"]==1 and four["revision_diagnostics"]["latest_observation_extension"]
    omitted=runner.run(target_month="2026-07",output_root=root/"omitted",prior_artifact=root/"one/artifact",acquire=lambda:base.iloc[[1]],retrieved_at="later",git_sha="fixture")
    assert omitted["run_status"]=="unchanged" and omitted["prior_only_preserved_key_count"]==1
    assert len(pd.read_parquet(root/"omitted/artifact/data.parquet"))==2
    duplicate=pd.concat([base,base.iloc[[0]]],ignore_index=True)
    expect(ValueError,lambda:runner.run(target_month="2026-07",output_root=root/"duplicate",acquire=lambda:duplicate,git_sha="fixture"))
    original=runner.validate_artifact
    def invalid(*args,**kwargs): raise RuntimeError("fixture validator failure")
    runner.validate_artifact=invalid
    expect(RuntimeError,lambda:runner.run(target_month="2026-07",output_root=root/"invalid",acquire=lambda:base,git_sha="fixture"))
    assert '"run_status":"failed"' in (root/"invalid/run_report.json").read_text()
    runner.validate_artifact=original
    old=os.environ.pop("FRED_API_KEY",None)
    expect(RuntimeError,acquire_current)
    if old is not None: os.environ["FRED_API_KEY"]=old
print("FRED monthly artifact runner smoke: ok")
