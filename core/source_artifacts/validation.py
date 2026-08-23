from __future__ import annotations
import json, math, re
from pathlib import Path
import pandas as pd
from .hashing import sha256_file
from .models import CANONICAL_COLUMNS, CANONICAL_KEY, SCHEMA_VERSION

class ArtifactValidationError(RuntimeError): pass

def validate_artifact(path: Path, *, expected_source_id: str|None=None, max_single_asset_bytes: int|None=None) -> dict:
    try: manifest=json.loads((path/"manifest.json").read_text())
    except Exception as exc: raise ArtifactValidationError("invalid manifest") from exc
    required={"artifact_id","source_id","provider_release_id","provider_release_timestamp_or_date","retrieved_at","registered_at","acquisition_time_status","artifact_created_at","target_month","data_sha256","validation_sha256","row_count","artifact_uri"}
    if manifest.get("schema_version")!=SCHEMA_VERSION or required-set(manifest): raise ArtifactValidationError("manifest schema mismatch")
    if expected_source_id and manifest["source_id"]!=expected_source_id: raise ArtifactValidationError("source mismatch")
    status=manifest["acquisition_time_status"]
    allowed={"retrieved_at_recorded","historical_not_recorded","registration_time_only"}
    if status not in allowed: raise ArtifactValidationError("invalid acquisition_time_status")
    if status=="retrieved_at_recorded" and manifest["retrieved_at"] is None: raise ArtifactValidationError("retrieved_at required by acquisition policy")
    if status=="historical_not_recorded" and manifest["retrieved_at"] is not None: raise ArtifactValidationError("historical acquisition status conflicts with retrieved_at")
    if status=="registration_time_only" and (manifest["retrieved_at"] is not None or manifest["registered_at"] is None): raise ArtifactValidationError("registration-only provenance is incomplete")
    if manifest["source_id"]=="redfin":
        required_configs={"config/redfin_baseline_manifest.json","config/redfin_metric_domain_contract.json","config/geo_manifest.generated.csv","config/source_refresh_revision_policy_v0_2.json"}
        if set(manifest.get("config_hashes",{}))!=required_configs: raise ArtifactValidationError("redfin governed config hashes incomplete")
    for config_path,digest in manifest.get("config_hashes",{}).items():
        if Path(config_path).is_absolute() or ".." in Path(config_path).parts: raise ArtifactValidationError("config hash path must be repository-relative")
        if not isinstance(digest,str) or not re.fullmatch(r"[0-9a-f]{64}",digest): raise ArtifactValidationError("invalid governed config SHA-256")
    for name,key in (("data.parquet","data_sha256"),("validation.json","validation_sha256")):
        if not (path/name).is_file() or sha256_file(path/name)!=manifest[key]: raise ArtifactValidationError(f"{name} hash mismatch")
    if manifest.get("lineage_filename") and sha256_file(path/manifest["lineage_filename"])!=manifest["lineage_sha256"]: raise ArtifactValidationError("lineage hash mismatch")
    if max_single_asset_bytes is not None and (path/"data.parquet").stat().st_size>max_single_asset_bytes: raise ArtifactValidationError("storage_strategy_required")
    data=pd.read_parquet(path/"data.parquet")
    if list(data.columns)!=CANONICAL_COLUMNS: raise ArtifactValidationError("canonical column order mismatch")
    if len(data)!=manifest["row_count"] or data[CANONICAL_KEY].duplicated().any(): raise ArtifactValidationError("row count or duplicate canonical key")
    if data[CANONICAL_KEY+["source_id","value"]].isna().any().any(): raise ArtifactValidationError("null critical field")
    if set(data.source_id)!={manifest["source_id"]}: raise ArtifactValidationError("source mismatch")
    if not data.value.map(math.isfinite).all(): raise ArtifactValidationError("non-finite value")
    if str(pd.to_datetime(data.date).dt.date.min())!=manifest["observation_min"] or str(pd.to_datetime(data.date).dt.date.max())!=manifest["observation_max"]: raise ArtifactValidationError("observation bounds mismatch")
    if manifest.get("lineage_filename"):
        lin=pd.read_parquet(path/manifest["lineage_filename"])
        def keys(frame):
            comparable=frame[CANONICAL_KEY].copy(); comparable["date"]=pd.to_datetime(comparable["date"]).dt.strftime("%Y-%m-%d")
            return set(map(tuple,comparable.values))
        if len(lin)!=len(data) or keys(lin)!=keys(data): raise ArtifactValidationError("lineage alignment mismatch")
    return {"status":"passed","artifact_id":manifest["artifact_id"],"rows":len(data),"manifest":manifest}
