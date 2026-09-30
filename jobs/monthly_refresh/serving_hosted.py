"""Hosted, recoverable serving transaction over the exact accepted canonical object."""
from __future__ import annotations

import argparse
import json
import os
import tarfile
from pathlib import Path
from typing import Any

from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.object_package import build_object_package, validate_object_package
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.serving_market import create_serving_manifest, promote_serving
from jobs.monthly_refresh.cohort_promotion_hosted import (GitHubJSONCAS, _publish_object,
    _verify_object_record)
from scripts.build_serving_snapshot import build_candidate
from scripts.validate_serving_snapshot import validate_snapshot_path

RECORD_ROOT="config/serving_promotion_records"


def _extract_exact(api: GitHubAPI, record: dict[str, Any], workspace: Path) -> Path:
    _verify_object_record(api,record,workspace/"verification")
    package=workspace/record["asset_filename"]; api.download_asset(record["asset_id"],package)
    if sha256_file(package)!=record["package_sha256"]: raise PublicationError("accepted canonical package hash mismatch")
    with tarfile.open(package,"r:") as archive:
        hashes={}
        for member in archive.getmembers():
            stream=archive.extractfile(member)
            if stream is None: raise PublicationError("accepted canonical package member unreadable")
            import hashlib
            hashes[member.name]=hashlib.sha256(stream.read()).hexdigest()
    extracted=validate_object_package(package,workspace/"canonical",object_type="canonical_market",
        expected={"object_id":record["object_id"],"artifact_content_hash":record["artifact_content_hash"],"member_hashes":hashes})
    return extracted


def _post_validate_serving(api: GitHubAPI, record: dict[str, Any], workspace: Path) -> dict[str, object]:
    _verify_object_record(api,record,workspace/"verification")
    package=workspace/record["asset_filename"]; api.download_asset(record["asset_id"],package)
    if sha256_file(package)!=record["package_sha256"]: raise PublicationError("serving package hash mismatch")
    import hashlib
    with tarfile.open(package,"r:") as archive:
        hashes={m.name:hashlib.sha256(archive.extractfile(m).read()).hexdigest() for m in archive.getmembers()}
    extracted=validate_object_package(package,workspace/"serving",object_type="serving_market",
        expected={"object_id":record["object_id"],"artifact_content_hash":record["artifact_content_hash"],"member_hashes":hashes})
    return validate_snapshot_path(extracted/"market_serving.duckdb")


def create_serving_plan(*, canonical_id: str, canonical_hash: str,
                        expected_serving: str | None, serving_id: str,
                        serving_hash: str) -> dict[str, Any]:
    semantic={"schema_version":"serving_promotion_record_v1","canonical_artifact_id":canonical_id,
        "canonical_artifact_hash":canonical_hash,"expected_serving":expected_serving,
        "serving_artifact_id":serving_id,"serving_artifact_hash":serving_hash}
    return validate_serving_plan({**semantic,"promotion_id":"serving_promotion__"+sha256_json(semantic)[:24]})


def validate_serving_plan(plan: dict[str, Any]) -> dict[str, Any]:
    required={"schema_version","canonical_artifact_id","canonical_artifact_hash",
        "expected_serving","serving_artifact_id","serving_artifact_hash","promotion_id"}
    if set(plan)!=required or plan.get("schema_version")!="serving_promotion_record_v1":
        raise PublicationError("serving promotion record schema mismatch")
    semantic={key:plan[key] for key in required if key!="promotion_id"}
    if plan["promotion_id"]!="serving_promotion__"+sha256_json(semantic)[:24]:
        raise PublicationError("serving promotion record identity mismatch")
    return plan


def authorization_token(plan: dict[str, Any]) -> str:
    validate_serving_plan(plan)
    return "AUTHORIZE_SERVING_PLAN__"+sha256_json(plan)


def _persist(store: GitHubJSONCAS, plan: dict[str, Any]) -> tuple[dict[str, Any],bool]:
    validate_serving_plan(plan)
    existing,oid=store.read()
    if existing is not None:
        validate_serving_plan(existing)
        if existing!=plan: raise IdentityCollisionError("contradictory serving promotion plan")
    changed=existing is None
    if changed: store.write(plan,oid,f"Prepare serving promotion {plan['promotion_id']}")
    durable,_=store.read()
    if durable!=plan: raise PublicationError("serving promotion durable reread contradiction")
    return plan,changed


def run(*, api: GitHubAPI, branch: str, workspace: Path, git_sha: str,
        mutate: bool, supplied_authorization: str="") -> dict[str, Any]:
    if mutate and not supplied_authorization: raise PublicationError("serving live promotion requires exact preflight authorization")
    workspace.mkdir(parents=True,exist_ok=True)
    cas=GitHubCatalogCAS(api,"config/artifact_catalog.json",branch); catalog,catalog_oid=cas.read()
    canonical_id=catalog["accepted"].get("canonical_market")
    if mutate:
        plan_store=GitHubJSONCAS(api,f"{RECORD_ROOT}/{canonical_id}.json",branch)
        plan,_=plan_store.read()
        if plan is None: raise PublicationError("durable serving promotion plan is absent")
        validate_serving_plan(plan)
        canonical_records=[r for r in catalog["immutable_records"] if r["object_type"]=="canonical_market" and r["object_id"]==canonical_id]
        if (plan.get("canonical_artifact_id")!=canonical_id or len(canonical_records)!=1 or
                plan.get("canonical_artifact_hash")!=canonical_records[0]["artifact_content_hash"]):
            raise PublicationError("durable serving plan canonical identity mismatch")
        if supplied_authorization!=authorization_token(plan): raise PublicationError("serving authorization is not bound to exact durable plan")
        records=[r for r in catalog["immutable_records"] if r["object_type"]=="serving_market" and r["object_id"]==plan["serving_artifact_id"]]
        if len(records)!=1 or records[0]["artifact_content_hash"]!=plan["serving_artifact_hash"]:
            raise PublicationError("durable serving target publication mismatch")
        updated,changed=promote_serving(catalog,expected_canonical=canonical_id,
            expected_serving=plan["expected_serving"],serving_artifact_id=plan["serving_artifact_id"])
        if changed: cas._write(updated,catalog_oid,f"Advance serving promotion {plan['promotion_id']}")
        durable,_=cas.read()
        if durable["accepted"].get("serving_market")!=plan["serving_artifact_id"]: raise PublicationError("serving accepted transition did not persist")
        _post_validate_serving(api,records[0],workspace/"post-transition-proof")
        return {"canonical_artifact_id":canonical_id,"serving_artifact_id":plan["serving_artifact_id"],
            "promotion_id":plan["promotion_id"],"live_promotion_performed":True,
            "accepted_transition_changed":changed,"post_transition_validation":"passed",
            "provider_discovery_performed":False,"acquisition_performed":False}
    matches=[r for r in catalog["immutable_records"] if r["object_type"]=="canonical_market" and r["object_id"]==canonical_id]
    if len(matches)!=1: raise PublicationError("accepted canonical artifact does not resolve once")
    canonical_record=matches[0]; extracted=_extract_exact(api,canonical_record,workspace)
    manifest=json.loads((extracted/"canonical-market.json").read_text())
    if manifest["market_artifact_id"]!=canonical_id or sha256_file(extracted/"market.duckdb")!=manifest["database_sha256"]:
        raise PublicationError("accepted canonical materialization identity mismatch")
    serving_db=build_candidate(extracted/"market.duckdb",workspace/"market_serving.duckdb")
    validation=validate_snapshot_path(serving_db)
    serving=create_serving_manifest(workspace/"serving-market.json",database_path=serving_db,
        canonical_market_artifact_id=canonical_id,canonical_database_sha256=manifest["database_sha256"],
        validation=validation,built_at=manifest["built_at"],builder_git_sha=git_sha)
    package=build_object_package({"serving-market.json":workspace/"serving-market.json",
        "market_serving.duckdb":serving_db},workspace/"serving-market.tar")
    record=_publish_object(api=api,cas=cas,package=workspace/"serving-market.tar",
        object_id=serving["serving_artifact_id"],object_type="serving_market",content_hash=sha256_json(serving),
        metadata={"canonical_market_artifact_id":canonical_id,"database_sha256":serving["database_sha256"]},
        members=package["member_hashes"],git_sha=git_sha)
    current,_=cas.read()
    if current["accepted"].get("canonical_market")!=canonical_id:
        raise IdentityCollisionError("accepted canonical changed during serving preparation")
    plan=create_serving_plan(canonical_id=canonical_id,canonical_hash=canonical_record["artifact_content_hash"],
        expected_serving=catalog["accepted"].get("serving_market"),serving_id=record["object_id"],
        serving_hash=record["artifact_content_hash"])
    plan,created=_persist(GitHubJSONCAS(api,f"{RECORD_ROOT}/{canonical_id}.json",branch),plan)
    report={"canonical_artifact_id":canonical_id,"serving_artifact_id":record["object_id"],
        "promotion_id":plan["promotion_id"],"authorization_token":authorization_token(plan),
        "prepared_record_created":created,"live_promotion_performed":False,
        "provider_discovery_performed":False,"acquisition_performed":False}
    return report


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--repository",required=True); parser.add_argument("--branch",required=True)
    parser.add_argument("--workspace",type=Path,required=True); parser.add_argument("--git-sha",required=True)
    parser.add_argument("--live",action="store_true"); parser.add_argument("--authorization-token",default="")
    parser.add_argument("--output",type=Path,required=True); args=parser.parse_args()
    report=run(api=GitHubAPI(args.repository,os.environ.get("GITHUB_TOKEN","")),branch=args.branch,
        workspace=args.workspace,git_sha=args.git_sha,mutate=args.live,supplied_authorization=args.authorization_token)
    write_canonical_json(args.output,report); print(json.dumps(report,sort_keys=True)); return 0


if __name__=="__main__": raise SystemExit(main())
