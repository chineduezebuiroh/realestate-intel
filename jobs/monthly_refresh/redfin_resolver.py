"""Resolve an exact hosted Redfin candidate; never ingest provider CSV files."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from core.source_artifacts.github_release import GitHubAPI, GitHubReleaseArtifactResolver
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.production import RESULT_VERSION, validate_source_result
from jobs.monthly_refresh.readiness import eligible_record

def resolve(*, cycle: str, expected_artifact_id: str, catalog: dict, readiness: dict,
            policy: Path, api: GitHubAPI, workspace: Path) -> dict:
    record = eligible_record(readiness, catalog=catalog, policy_path=policy, requested_cycle_id=cycle)
    if record is None: raise ValueError("exact Redfin readiness did not resolve")
    if record["candidate_artifact_id"] != expected_artifact_id:
        raise ValueError("supplied Redfin candidate pin drift")
    item = next(r for r in catalog["immutable_records"] if r["object_id"] == record["candidate_artifact_id"])
    artifact = GitHubReleaseArtifactResolver(catalog, api, workspace).resolve(item["logical_artifact_uri"])
    manifest = validate_artifact(artifact, expected_source_id="redfin")["manifest"]
    if manifest["artifact_id"] != record["candidate_artifact_id"] or manifest["artifact_content_hash"] != record["artifact_content_hash"]:
        raise ValueError("resolved Redfin package drift")
    accepted = catalog["accepted"]["source"].get("redfin")
    prior = next(r for r in catalog["immutable_records"] if r["object_id"] == accepted)
    result = {"schema_version":RESULT_VERSION,"source_id":"redfin","cycle_id":cycle,"status":"succeeded",
        "candidate_artifact_id":item["object_id"],"artifact_content_hash":item["artifact_content_hash"],
        "package_sha256":item["package_sha256"],"publication_state":"published_verified",
        "validation_status":"passed","provider_release_id":item["metadata"]["provider_release_id"],
        "observation_max":item["metadata"]["observation_max"],"prior_artifact_id":accepted,
        "source_change_detected":item["metadata"]["data_sha256"] != prior["metadata"]["data_sha256"],
        "retryability":"not_applicable","accepted_pointer_changed":False,"evidence_uri":item["logical_artifact_uri"]}
    return validate_source_result(result, expected_cycle_id=cycle)

def main() -> int:
    p=argparse.ArgumentParser(); p.add_argument("--cycle-id",required=True)
    p.add_argument("--candidate-artifact-id", required=True); p.add_argument("--output",type=Path,required=True)
    p.add_argument("--catalog",type=Path,default=Path("config/artifact_catalog.json")); p.add_argument("--readiness",type=Path,default=Path("config/monthly_refresh_readiness.json")); a=p.parse_args()
    value=resolve(cycle=a.cycle_id,expected_artifact_id=a.candidate_artifact_id,
        catalog=json.loads(a.catalog.read_text()),readiness=json.loads(a.readiness.read_text()),
        policy=Path("config/monthly_refresh_policy.json"),api=GitHubAPI("chineduezebuiroh/realestate-intel",os.environ["GITHUB_TOKEN"]),workspace=a.output.parent/"redfin")
    a.output.write_text(json.dumps(value,sort_keys=True,separators=(",",":"))+"\n"); print(json.dumps(value,sort_keys=True)); return 0
if __name__ == "__main__": raise SystemExit(main())
