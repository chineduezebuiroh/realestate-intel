"""Cloud-facing FRED check -> reconcile -> governed artifact boundary."""
from __future__ import annotations
import argparse, json, os, re, shutil, subprocess, time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable
from urllib.error import HTTPError, URLError
import pandas as pd
from core.source_artifacts.artifact import artifact_package_sha256, canonicalize
from core.source_artifacts.hashing import sha256_json, write_canonical_json
from core.source_artifacts.models import CANONICAL_KEY
from core.source_artifacts.validation import validate_artifact
from sources.fred_macro.artifact import acquire_current, governed_config_hashes, produce
from sources.fred_macro.ingest import FRED_SERIES, SPREAD_SERIES_META
SOURCE_ID = "fred_macro"
ACQUISITION_MAX_ATTEMPTS = 3
ACQUISITION_BACKOFF_SECONDS = (2.0, 5.0)


class TransientFREDAcquisitionError(RuntimeError):
    """A bounded FRED provider/transport failure that is safe to retry later."""


def is_transient_acquisition_error(exc: BaseException) -> bool:
    """Classify only provider availability and transport failures as transient."""
    if isinstance(exc, HTTPError):
        return 500 <= exc.code <= 599 or exc.code in {408, 429}
    return isinstance(exc, (URLError, TimeoutError, ConnectionError))


def acquire_with_retry(acquire: Callable[[], pd.DataFrame], *,
                       max_attempts: int = ACQUISITION_MAX_ATTEMPTS,
                       backoff_seconds: tuple[float, ...] = ACQUISITION_BACKOFF_SECONDS,
                       sleep: Callable[[float], None] = time.sleep) -> pd.DataFrame:
    """Acquire current FRED truth with an explicit, bounded transient-only retry."""
    if max_attempts < 1 or len(backoff_seconds) < max_attempts - 1:
        raise ValueError("invalid FRED acquisition retry policy")
    for attempt in range(1, max_attempts + 1):
        try:
            return acquire()
        except Exception as exc:
            if not is_transient_acquisition_error(exc):
                raise
            if attempt == max_attempts:
                raise TransientFREDAcquisitionError(
                    f"FRED acquisition exhausted {max_attempts} transient attempts: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            sleep(backoff_seconds[attempt - 1])
    raise AssertionError("unreachable")

def _content_identity(frame: pd.DataFrame) -> str:
    data=canonicalize(frame); data["date"]=data.date.map(str)
    return sha256_json(data.to_dict(orient="records"))

def source_diagnostics(frame: pd.DataFrame) -> dict:
    data=canonicalize(frame); metrics=[]
    for metric,group in data.groupby("metric_id",sort=True):
        metrics.append({"metric_id":str(metric),"row_count":len(group),"observation_min":str(group.date.min()),
                        "observation_max":str(group.date.max()),"value_min":float(group.value.min()),"value_max":float(group.value.max())})
    return {"row_count":len(data),"geography_count":data.geo_id.nunique(),"metric_count":data.metric_id.nunique(),
            "observation_min":str(data.date.min()),"observation_max":str(data.date.max()),"per_metric":metrics}

def revision_diagnostics(current: pd.DataFrame, prior: pd.DataFrame|None) -> dict:
    current=canonicalize(current)
    if prior is None:
        return {"unchanged_matched_keys":0,"revised_matched_keys":0,"new_keys":len(current),"prior_only_preserved_keys":0,
                "latest_observation_extension":True,"revision_counts_by_metric":[],"revisions_by_metric":[]}
    prior=canonicalize(prior); joined=prior.merge(current,on=CANONICAL_KEY,how="outer",suffixes=("_prior","_current"),indicator=True)
    both=joined._merge.eq("both"); revised=joined.loc[both & joined.value_prior.ne(joined.value_current)].copy(); details=[]
    for metric,group in revised.groupby("metric_id",sort=True):
        details.append({"metric_id":str(metric),"revision_count":len(group),"earliest_revised_observation":str(group.date.min()),
                        "latest_revised_observation":str(group.date.max()),
                        "maximum_absolute_revision":float((group.value_current-group.value_prior).abs().max())})
    return {"unchanged_matched_keys":int((both & joined.value_prior.eq(joined.value_current)).sum()),
            "revised_matched_keys":len(revised),"new_keys":int(joined._merge.eq("right_only").sum()),
            "prior_only_preserved_keys":int(joined._merge.eq("left_only").sum()),
            "latest_observation_extension":current.date.max()>prior.date.max(),
            "revision_counts_by_metric":[{"metric_id":d["metric_id"],"count":d["revision_count"]} for d in details],
            "revisions_by_metric":details}

def _validate_acquired(frame: pd.DataFrame) -> pd.DataFrame:
    data=canonicalize(frame)
    if data.empty: raise ValueError("FRED acquisition returned no governed observations")
    if data[CANONICAL_KEY].duplicated().any(): raise ValueError("duplicate canonical key in acquired FRED state")
    if set(data.source_id)!={SOURCE_ID}: raise ValueError("unexpected FRED source identity")
    unexpected=sorted(set(data.metric_id)-(set(FRED_SERIES)|set(SPREAD_SERIES_META)))
    if unexpected: raise ValueError(f"invalid FRED metric ownership: {unexpected}")
    if set(data.geo_id)!={"united_states__nation"}: raise ValueError("invalid FRED geography membership")
    return data

def _git_sha() -> str:
    result=subprocess.run(["git","rev-parse","HEAD"],capture_output=True,text=True)
    return result.stdout.strip() if result.returncode==0 else "unknown"

def _resolve_target_month(target_month: str|None, current: pd.DataFrame) -> tuple[str,str]:
    if target_month not in (None, ""):
        return target_month,"explicit"
    latest=current["date"].max()
    if pd.isna(latest): raise ValueError("cannot infer target_month from FRED observations")
    return latest.strftime("%Y-%m"),"inferred_observation_max"

def run(*,target_month:str|None=None,output_root:Path,prior_artifact:Path|None=None,acquire:Callable[[],pd.DataFrame]=acquire_current,
        retrieved_at:str|None=None,git_sha:str|None=None,repository_root:Path=Path(".")) -> dict:
    output_root.mkdir(parents=True,exist_ok=True); report_path=output_root/"run_report.json"; resolution=None
    try:
        if target_month not in (None, ""):
            if re.fullmatch(r"\d{4}-\d{2}", target_month) is None: raise ValueError("target_month must use YYYY-MM")
            datetime.strptime(target_month,"%Y-%m")
        current=_validate_acquired(acquire_with_retry(acquire)); target_month,resolution=_resolve_target_month(target_month,current)
        acquired=source_diagnostics(current); prior=None; prior_manifest=None
        if prior_artifact is not None:
            prior_manifest=validate_artifact(prior_artifact,expected_source_id=SOURCE_ID)["manifest"]
            prior=pd.read_parquet(prior_artifact/"data.parquet")
        revisions=revision_diagnostics(current,prior)
        changed=prior is None or revisions["revised_matched_keys"]>0 or revisions["new_keys"]>0
        artifact_dir=output_root/"artifact"
        if artifact_dir.exists(): shutil.rmtree(artifact_dir)
        if not changed:
            shutil.copytree(prior_artifact,artifact_dir); manifest=validate_artifact(artifact_dir,expected_source_id=SOURCE_ID)["manifest"]
            status="unchanged"
        else:
            retrieval=retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00","Z")
            manifest=produce(artifact_dir,current,target_month=target_month,provider_release_id="ordinary-current:"+_content_identity(current),
                             retrieved_at=retrieval,prior_artifact=prior_artifact,git_sha=git_sha or _git_sha(),repository_root=repository_root)
            validate_artifact(artifact_dir,expected_source_id=SOURCE_ID); status="refreshed"
        report={"schema_version":"fred_monthly_source_run_v1","source_id":SOURCE_ID,"run_status":status,
                "git_sha":git_sha or _git_sha(),"target_month":target_month,"target_month_resolution":resolution,**acquired,
                "prior_artifact_id":prior_manifest["artifact_id"] if prior_manifest else None,
                "resulting_artifact_id":manifest["artifact_id"],"resulting_artifact_content_hash":manifest["artifact_content_hash"],
                "data_sha256":manifest["data_sha256"],"validation_status":"passed","source_change_detected":changed,
                "artifact_package_sha256":artifact_package_sha256(artifact_dir),
                "historical_revision_count":revisions["revised_matched_keys"],"new_key_count":revisions["new_keys"],
                "prior_only_preserved_key_count":revisions["prior_only_preserved_keys"],"revision_diagnostics":revisions,
                "artifact_output_path":str(artifact_dir),"governed_config_hashes":governed_config_hashes(repository_root)}
        write_canonical_json(report_path,report); return report
    except Exception as exc:
        write_canonical_json(report_path,{"schema_version":"fred_monthly_source_run_v1","source_id":SOURCE_ID,"run_status":"failed",
            "git_sha":git_sha or _git_sha(),"target_month":target_month,"target_month_resolution":resolution,
            "validation_status":"failed",
            "retryability":"retryable" if isinstance(exc, TransientFREDAcquisitionError) else "terminal",
            "error":f"{type(exc).__name__}: {exc}"})
        raise

def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("--target-month")
    parser.add_argument("--output-root",type=Path,default=Path("artifacts/source_artifacts/fred_macro/current_run")); parser.add_argument("--prior-artifact",type=Path)
    args=parser.parse_args()
    if not os.getenv("FRED_API_KEY","").strip(): parser.error("FRED_API_KEY is required for production FRED artifact acquisition")
    print(json.dumps(run(target_month=args.target_month,output_root=args.output_root,prior_artifact=args.prior_artifact),sort_keys=True,indent=2)); return 0
if __name__=="__main__": raise SystemExit(main())
