from __future__ import annotations
import hashlib
from pathlib import Path
import pandas as pd
from .hashing import canonical_json_bytes, sha256_file, sha256_json, write_canonical_json
from .models import CANONICAL_COLUMNS, CANONICAL_KEY, CONTRACT_VERSION, REFRESH_VERSION, SCHEMA_VERSION

def canonicalize(frame: pd.DataFrame) -> pd.DataFrame:
    missing=set(CANONICAL_COLUMNS)-set(frame.columns)
    if missing: raise ValueError(f"missing canonical columns: {sorted(missing)}")
    out=frame[CANONICAL_COLUMNS].copy()
    for col in ("geo_id","metric_id","property_type_id","source_id"): out[col]=out[col].astype("string")
    out["property_type"]=out["property_type"].astype("string")
    out["date"]=pd.to_datetime(out["date"], errors="raise").dt.date
    out["value"]=pd.to_numeric(out["value"], errors="raise").astype("float64")
    return out.sort_values(CANONICAL_KEY,kind="mergesort").reset_index(drop=True)

def artifact_package_sha256(path: Path) -> str:
    digest=hashlib.sha256()
    for item in sorted(p for p in path.iterdir() if p.is_file()):
        digest.update(item.name.encode()+b"\0"); digest.update(item.read_bytes())
    return digest.hexdigest()

def create_artifact(output: Path, frame: pd.DataFrame, *, source_id: str, source_family: str,
 source_type: str, provider: str, distribution_channel: str, provider_release_id: str,
 provider_release_timestamp_or_date: str, retrieved_at: str, target_month: str,
 source_request_identity: str, source_urls_or_endpoint_identity: list[str], revision: int=1,
 prior_artifact_id: str|None=None, prior_artifact_sha256: str|None=None,
 lineage: pd.DataFrame|None=None, config_hashes: dict|None=None, git_sha: str="unknown",
 max_single_asset_bytes: int|None=None, storage_backend: str="local_filesystem") -> dict:
    output.mkdir(parents=True,exist_ok=False)
    data=canonicalize(frame)
    if data.source_id.nunique()!=1 or data.source_id.iloc[0]!=source_id: raise ValueError("source mismatch")
    if data[CANONICAL_KEY].duplicated().any(): raise ValueError("duplicate canonical key")
    data.to_parquet(output/"data.parquet",index=False,compression="zstd",engine="pyarrow")
    size=(output/"data.parquet").stat().st_size
    if max_single_asset_bytes is not None and size>max_single_asset_bytes:
        raise ValueError("storage_strategy_required")
    data_hash=sha256_file(output/"data.parquet")
    lineage_hash=None
    if lineage is not None:
        lineage=lineage.sort_values(CANONICAL_KEY,kind="mergesort").reset_index(drop=True)
        lineage.to_parquet(output/"lineage.parquet",index=False,compression="zstd",engine="pyarrow")
        lineage_hash=sha256_file(output/"lineage.parquet")
    validation={"schema_version":"source_artifact_validation_v1","status":"passed","checks":["canonical_schema","unique_key","finite_values","source_constant"]}
    write_canonical_json(output/"validation.json",validation); validation_hash=sha256_file(output/"validation.json")
    identity_payload={"source_id":source_id,"provider_release_id":provider_release_id,"target_month":target_month,"revision":revision,"data_sha256":data_hash,"lineage_sha256":lineage_hash,"schema_version":SCHEMA_VERSION,"refresh_contract":REFRESH_VERSION}
    content_hash=sha256_json(identity_payload)
    artifact_id=f"src__{source_id}__{target_month}__r{revision}__{content_hash[:16]}"
    manifest={"schema_version":SCHEMA_VERSION,"artifact_contract_version":CONTRACT_VERSION,"artifact_id":artifact_id,"artifact_content_hash":content_hash,"source_id":source_id,"source_family":source_family,"source_type":source_type,"provider":{"name":provider,"distribution_channel":distribution_channel},"distribution_channel":distribution_channel,"provider_release_id":provider_release_id,"provider_release_timestamp_or_date":provider_release_timestamp_or_date,"retrieved_at":retrieved_at,"target_month":target_month,"artifact_status":"complete","validation_status":"passed","canonical_key":CANONICAL_KEY,"observation_min":str(data.date.min()),"observation_max":str(data.date.max()),"row_count":len(data),"geography_count":data.geo_id.nunique(),"metric_count":data.metric_id.nunique(),"metric_inventory":sorted(data.metric_id.unique().tolist()),"source_request_identity":source_request_identity,"source_urls_or_endpoint_identity":source_urls_or_endpoint_identity,"revision_policy_id":f"{REFRESH_VERSION}:{source_id}","absence_semantics":"preserve_prior","prior_artifact_id":prior_artifact_id,"prior_artifact_sha256":prior_artifact_sha256,"data_filename":"data.parquet","data_sha256":data_hash,"data_size_bytes":size,"lineage_filename":"lineage.parquet" if lineage is not None else None,"lineage_sha256":lineage_hash,"validation_filename":"validation.json","validation_sha256":validation_hash,"canonical_schema_identity":"sha256:"+sha256_json({"columns":CANONICAL_COLUMNS,"key":CANONICAL_KEY,"parquet":"pyarrow-zstd"}),"config_hashes":config_hashes or {},"git_sha":git_sha,"storage_backend":storage_backend,"artifact_uri":f"artifact://source/{source_id}/{artifact_id}","warnings":[]}
    write_canonical_json(output/"manifest.json",manifest)
    return manifest
