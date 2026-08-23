from __future__ import annotations
import hashlib
import os
from pathlib import Path
import pandas as pd
from core.source_artifacts import create_artifact, preserve_prior
from core.source_artifacts.validation import validate_artifact

GOVERNED_CONFIG_PATHS = ("config/geo_manifest.generated.csv", "config/source_metric_registry.csv",
                         "config/source_refresh_revision_policy_v0_2.json")

def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result = {}
    for relative in GOVERNED_CONFIG_PATHS:
        path = repository_root / relative
        if not path.is_file(): raise FileNotFoundError(f"missing governed FRED configuration: {relative}")
        result[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(result.items()))

def acquire_current() -> pd.DataFrame:
    if not os.getenv("FRED_API_KEY","").strip(): raise RuntimeError("FRED_API_KEY is required for production FRED artifact acquisition")
    from fredapi import Fred
    from .ingest import FRED_SERIES, fetch_monthly_as_is, fetch_monthly_avg, load_fred_geo_map, SOURCE_ID
    client=Fred(api_key=os.environ["FRED_API_KEY"].strip()); geo=load_fred_geo_map(); frames=[]
    for metric,meta in FRED_SERIES.items():
        frame=(fetch_monthly_avg if meta.get("agg")=="mean" else fetch_monthly_as_is)(meta["series_id"],client)
        if not frame.empty: frames.append(frame.assign(geo_id=geo[meta.get("geo_code","US")],metric_id=metric))
    if not frames: raise RuntimeError("FRED returned no governed observations")
    base=pd.concat(frames,ignore_index=True); rows=[base]
    definitions={"fred_spread_2y_10y":("fred_gs2","fred_gs10"),"fred_spread_10y_30y":("fred_gs10","fred_gs30"),"fred_spread_2y_30y":("fred_gs2","fred_gs30"),"fred_spread_2y_fedfunds":("fred_gs2","fred_fedfunds"),"fred_spread_10y_fedfunds":("fred_gs10","fred_fedfunds"),"fred_spread_30y_fedfunds":("fred_gs30","fred_fedfunds")}
    for geo_id,group in base.groupby("geo_id"):
        wide=group.pivot(index="date",columns="metric_id",values="value")
        for metric,(left,right) in definitions.items():
            if left in wide and right in wide:
                spread=(wide[left]-wide[right]).dropna().rename("value").reset_index(); rows.append(spread.assign(geo_id=geo_id,metric_id=metric))
    result=pd.concat(rows,ignore_index=True); result["property_type_id"]="all"; result["source_id"]=SOURCE_ID; result["property_type"]="all"
    return result[["geo_id","metric_id","date","property_type_id","value","source_id","property_type"]]

def produce(output: Path, normalized: pd.DataFrame, *, target_month: str, provider_release_id: str,
 retrieved_at: str, prior_artifact: Path|None=None, git_sha: str="unknown", max_single_asset_bytes: int|None=None,
 artifact_created_at: str|None=None, repository_root: Path=Path(".")) -> dict:
    prior=pd.read_parquet(prior_artifact/"data.parquet") if prior_artifact else None
    prior_manifest=validate_artifact(prior_artifact,expected_source_id="fred_macro")["manifest"] if prior_artifact else None
    reconciled=preserve_prior(prior,normalized)
    return create_artifact(output,reconciled,source_id="fred_macro",source_family="Federal Reserve Economic Data macro series",source_type="revisionary_current_truth",provider="Federal Reserve Bank of St. Louis",distribution_channel="FRED API",provider_release_id=provider_release_id,provider_release_timestamp_or_date=None,retrieved_at=retrieved_at,artifact_created_at=artifact_created_at,target_month=target_month,source_request_identity=f"fred-series-spec:{provider_release_id}",source_urls_or_endpoint_identity=["api.stlouisfed.org/fred/series/observations"],prior_artifact_id=prior_manifest["artifact_id"] if prior_manifest else None,prior_artifact_sha256=prior_manifest["artifact_content_hash"] if prior_manifest else None,config_hashes=governed_config_hashes(repository_root),git_sha=git_sha,max_single_asset_bytes=max_single_asset_bytes)
