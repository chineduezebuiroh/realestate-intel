from __future__ import annotations
import json
from pathlib import Path
from .artifact import artifact_package_sha256
from .hashing import sha256_json, write_canonical_json
from .validation import ArtifactValidationError, validate_artifact
from .storage import ArtifactResolver

VERSION="source_set_manifest_v1"
def create_source_set(output: Path, artifact_dirs: list[Path], *, target_month: str, created_at: str,
 git_sha: str="unknown", config_hashes: dict|None=None, partial_vertical_slice: bool=True) -> dict:
    entries=[]
    for directory in artifact_dirs:
        result=validate_artifact(directory); m=result["manifest"]
        entries.append({"source_id":m["source_id"],"artifact_id":m["artifact_id"],"artifact_sha256":artifact_package_sha256(directory),"artifact_uri":m["artifact_uri"],"provider_release_id":m["provider_release_id"],"observation_max":m["observation_max"],"monthly_status":"refreshed","validation_status":"passed"})
    entries.sort(key=lambda x:x["source_id"]); inventory=[e["source_id"] for e in entries]
    identity={"target_month":target_month,"sources":[(e["source_id"],e["artifact_id"],e["artifact_sha256"]) for e in entries],"contracts":[VERSION,"source_artifact_schema_v1","canonical_market_assembly_v1","source_refresh_revision_v0_2"],"config_hashes":config_hashes or {},"git_sha":git_sha,"family_resolution":"none"}
    payload={"schema_version":VERSION,"source_set_id":f"source_set__{target_month}__v1__{sha256_json(identity)[:16]}","target_month":target_month,"created_at":created_at,"git_sha":git_sha,"required_source_inventory":inventory,"included_source_inventory":inventory,"contract_versions":identity["contracts"],"config_hashes":config_hashes or {},"sources":entries,"family_resolution":"none","partial_vertical_slice":partial_vertical_slice}
    write_canonical_json(output,payload); return payload

def validate_source_set(path: Path, resolver: ArtifactResolver) -> dict:
    try: payload=json.loads(path.read_text())
    except Exception as exc: raise ArtifactValidationError("invalid source-set JSON") from exc
    if payload.get("schema_version")!=VERSION or payload.get("partial_vertical_slice") is not True: raise ArtifactValidationError("unsupported or falsely complete source set")
    required=payload.get("required_source_inventory"); included=payload.get("included_source_inventory")
    if required!=sorted(required) or required!=included or len(required)!=len(set(required)): raise ArtifactValidationError("source inventory mismatch")
    entries=payload.get("sources",[])
    if [e.get("source_id") for e in entries]!=required: raise ArtifactValidationError("missing or unordered source")
    resolved={}
    for entry in entries:
        directory=resolver.resolve(entry["artifact_uri"])
        result=validate_artifact(directory,expected_source_id=entry["source_id"])
        m=result["manifest"]
        if m["artifact_id"]!=entry["artifact_id"] or artifact_package_sha256(directory)!=entry["artifact_sha256"] or m["provider_release_id"]!=entry["provider_release_id"]: raise ArtifactValidationError("source-set artifact identity mismatch")
        resolved[entry["source_id"]]=directory
    return {"status":"passed","source_set":payload,"resolved":resolved}
