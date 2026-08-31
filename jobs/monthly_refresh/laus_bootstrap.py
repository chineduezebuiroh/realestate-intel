"""Controlled, recoverable LAUS bootstrap and read-only legacy equivalence tooling."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pandas as pd

from core.source_artifacts import create_artifact
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.validation import validate_artifact
from sources.bls_laus.artifact import (
    BLS_API_ENDPOINT, CANONICAL_COLUMNS, DIAGNOSTIC_METRICS, KEY, REQUIRED_METRICS,
    SOURCE_ID, acquire, build_request_plan, canonicalize, load_registry,
)

CATALOG_PATH = "config/artifact_catalog.json"
BRANCH = "monthly-refresh-orchestration"
TECHNICAL_TOLERANCE = 1e-12
CATEGORIES = ("EXACT_MATCH", "PROVIDER_REVISION", "PROVIDER_NEWER", "PROVIDER_HISTORICAL_ONLY",
              "LEGACY_PRIOR_ONLY", "IDENTITY_MISMATCH", "UNEXPLAINED_NUMERIC_MISMATCH", "UNIT_SCALE_MISMATCH")
REQUIRED_EVIDENCE = ("preflight.json", "request_plan.json", "acquired.json", "provider_observations.json", "canonical.parquet",
                     "completeness.json", "acquisition.json")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def git_sha() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def inspect_catalog(catalog: Mapping[str, Any]) -> dict[str, Any]:
    records = [record for record in catalog.get("immutable_records", [])
               if record.get("object_type") == "source" and record.get("metadata", {}).get("source_id") == SOURCE_ID]
    return {"accepted_artifact_id": catalog.get("accepted", {}).get("source", {}).get(SOURCE_ID),
            "immutable_record_count": len(records), "immutable_artifact_ids": sorted(r["object_id"] for r in records)}


def preflight(catalog: Mapping[str, Any]) -> dict[str, Any]:
    rows = load_registry(); state = inspect_catalog(catalog)
    result = {**state, "series_count": len(rows),
              "required_series_count": sum(row["classification"] == "GOVERNED_REQUIRED" for row in rows),
              "diagnostic_series_count": sum(row["classification"] == "GOVERNED_DIAGNOSTIC" for row in rows),
              "geography_count": len({row["geo_id"] for row in rows}),
              "metric_count": len({row["metric_id"] for row in rows}),
              "seasonal_adjustments": sorted({row["seasonal_adjustment"] for row in rows}),
              "units_complete": all(row["unit"] for row in rows),
              "transforms_complete": all(row["scale_transform"] for row in rows)}
    expected = (820, 615, 205, 205, 4, ["NSA"], True, True)
    actual = tuple(result[key] for key in ("series_count", "required_series_count", "diagnostic_series_count",
                                           "geography_count", "metric_count", "seasonal_adjustments",
                                           "units_complete", "transforms_complete"))
    if actual != expected:
        raise RuntimeError(f"LAUS governed registry preflight failed: {result}")
    if state["accepted_artifact_id"] is not None or state["immutable_record_count"]:
        raise RuntimeError(f"LAUS durable state already exists and requires review: {state}")
    return result


def read_legacy(path: Path, expected_pairs: set[tuple[str, str]]) -> pd.DataFrame:
    columns = KEY + ["value", "source_id", "identity_configured"]
    if not path.is_file(): return pd.DataFrame(columns=columns)
    connection = duckdb.connect(str(path), read_only=True)
    try:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "fact_timeseries" not in tables: raise RuntimeError(f"legacy DB lacks fact_timeseries: {path}")
        metrics = sorted(REQUIRED_METRICS | DIAGNOSTIC_METRICS)
        frame = connection.execute("""SELECT geo_id, metric_id, date, property_type_id, value, source_id
          FROM fact_timeseries WHERE metric_id IN (?, ?, ?, ?)""", metrics).fetchdf()
    finally: connection.close()
    frame["date"] = pd.to_datetime(frame.date).dt.date
    frame["property_type_id"] = frame.property_type_id.fillna("all").astype(str)
    frame["identity_configured"] = [(str(g), str(m)) in expected_pairs for g, m in zip(frame.geo_id, frame.metric_id)]
    return frame


def _scale_diagnostic(overlap: pd.DataFrame) -> dict[str, Any]:
    if overlap.empty: return {"unit_scale_mismatch": False, "overlap_count": 0}
    p = overlap.value_provider.astype(float); l = overlap.value_legacy.astype(float)
    legacy_1000 = float(((p * 1000 - l).abs() <= TECHNICAL_TOLERANCE).mean())
    provider_1000 = float(((p - l * 1000).abs() <= TECHNICAL_TOLERANCE).mean())
    mismatch = max(legacy_1000, provider_1000) >= .8
    return {"unit_scale_mismatch": mismatch, "overlap_count": len(overlap),
            "legacy_is_1000x_fraction": legacy_1000, "provider_is_1000x_fraction": provider_1000,
            "technical_tolerance": TECHNICAL_TOLERANCE}


def equivalence_audit(provider: pd.DataFrame, legacy: pd.DataFrame, plan: Mapping[str, Any], *,
                      allow_provider_revisions: bool = True) -> tuple[pd.DataFrame, dict[str, Any]]:
    pair_to_series = {(item["geo_id"], item["metric_id"]): item["series_id"] for item in plan["series"]}
    p = provider.copy(); p["date"] = pd.to_datetime(p.date).dt.date
    l = legacy.copy(); l["date"] = pd.to_datetime(l.date).dt.date
    if "identity_configured" not in l:
        l["identity_configured"] = [(g, m) in pair_to_series for g, m in zip(l.geo_id, l.metric_id)]
    merged = p[KEY + ["value"]].merge(l[KEY + ["value", "identity_configured"]], on=KEY, how="outer",
                                      suffixes=("_provider", "_legacy"), indicator=True)
    legacy_max = l[l["identity_configured"].fillna(False).astype(bool)].groupby(["geo_id", "metric_id"])["date"].max().to_dict()
    scale = _scale_diagnostic(merged[merged._merge.eq("both")]); categories = []
    for _, row in merged.iterrows():
        configured = bool(row.get("identity_configured", True))
        pair = (row.geo_id, row.metric_id)
        if not configured or pair not in pair_to_series: category = "IDENTITY_MISMATCH"
        elif row["_merge"] == "left_only":
            maximum = legacy_max.get(pair); category = "PROVIDER_NEWER" if maximum and row.date > maximum else "PROVIDER_HISTORICAL_ONLY"
        elif row["_merge"] == "right_only": category = "LEGACY_PRIOR_ONLY"
        elif abs(float(row.value_provider)-float(row.value_legacy)) <= TECHNICAL_TOLERANCE: category = "EXACT_MATCH"
        elif scale["unit_scale_mismatch"]: category = "UNIT_SCALE_MISMATCH"
        elif allow_provider_revisions: category = "PROVIDER_REVISION"
        else: category = "UNEXPLAINED_NUMERIC_MISMATCH"
        categories.append(category)
    merged["comparison_category"] = categories
    merged["series_id"] = [pair_to_series.get((g, m)) for g, m in zip(merged.geo_id, merged.metric_id)]
    merged["absolute_difference"] = (merged.value_provider-merged.value_legacy).abs()
    counts = Counter(categories)
    summary = {"schema_version": "laus_bootstrap_equivalence_v1", "technical_tolerance": TECHNICAL_TOLERANCE,
               "unit_scale": scale, **{category.lower()+"_count": int(counts[category]) for category in CATEGORIES},
               "row_count": len(merged)}
    return merged.sort_values(KEY, kind="mergesort").reset_index(drop=True), summary


def acceptance_gates(frame: pd.DataFrame, diagnostics: Mapping[str, Any], equivalence: Mapping[str, Any]) -> dict[str, Any]:
    checks = {"exact_metric_scope": set(frame.metric_id.unique()) == REQUIRED_METRICS | DIAGNOSTIC_METRICS,
              "required_complete": diagnostics.get("required_series_count") == 615 and not diagnostics.get("missing_series"),
              "target_resolved": bool(diagnostics.get("target_month")), "canonical_unique": not frame.duplicated(KEY).any(),
              "canonical_finite": pd.to_numeric(frame.value).map(lambda x: pd.notna(x) and abs(x) != float("inf")).all(),
              "no_identity_mismatch": equivalence.get("identity_mismatch_count", 0) == 0,
              "no_unit_scale_mismatch": not equivalence.get("unit_scale", {}).get("unit_scale_mismatch", True),
              "no_unexplained_numeric_mismatch": equivalence.get("unexplained_numeric_mismatch_count", 0) == 0}
    checks = {key: bool(value) for key, value in checks.items()}
    return {"schema_version": "laus_bootstrap_acceptance_v1", "status": "passed" if all(checks.values()) else "failed",
            "checks": checks, "failed_checks": sorted(key for key, value in checks.items() if not value)}


def create_bootstrap_artifact(output: Path, frame: pd.DataFrame, plan: Mapping[str, Any], diagnostics: Mapping[str, Any], *,
                              retrieved_at: str, artifact_created_at: str | None = None) -> dict[str, Any]:
    manifest = create_artifact(output, frame, source_id=SOURCE_ID,
        source_family="BLS Local Area Unemployment Statistics", source_type="revisionary_current_truth",
        provider="U.S. Bureau of Labor Statistics", distribution_channel="BLS Public Data API v2",
        provider_release_id=diagnostics["provider_release_id"], provider_release_timestamp_or_date=None,
        retrieved_at=retrieved_at, artifact_created_at=artifact_created_at, target_month=diagnostics["target_month"],
        source_request_identity=plan["source_request_identity"], source_urls_or_endpoint_identity=[BLS_API_ENDPOINT],
        config_hashes=plan["config_hashes"], git_sha=git_sha(), raw_source_lineage={"laus_contract": "laus_governed_source_v1",
        "provider_observation_identity": diagnostics["provider_release_id"].split(":", 1)[1]})
    return validate_artifact(output, expected_source_id=SOURCE_ID)["manifest"]


def _load_json(path: Path) -> Any: return json.loads(path.read_text())


def validate_workspace(root: Path, *, expected_end_year: int | None = None) -> tuple[dict[str, Any], pd.DataFrame, dict[str, Any]]:
    missing = [name for name in REQUIRED_EVIDENCE if not (root/name).is_file()]
    if missing: raise RuntimeError(f"LAUS recovery evidence incomplete: {missing}")
    plan = _load_json(root/"request_plan.json"); acquisition = _load_json(root/"acquisition.json")
    if plan.get("acquisition_mode") != "bootstrap" or (expected_end_year is not None and plan.get("end_year") != expected_end_year):
        raise RuntimeError("LAUS recovery request identity/end_year contradiction")
    hashes = acquisition.get("evidence_sha256", {})
    for name in ("acquired.json", "provider_observations.json", "canonical.parquet", "completeness.json"):
        if hashes.get(name) != sha256_file(root/name): raise RuntimeError(f"corrupt LAUS recovery evidence: {name}")
    diagnostics = _load_json(root/"completeness.json"); frame = pd.read_parquet(root/"canonical.parquet")
    observations = _load_json(root/"provider_observations.json")
    if (acquisition.get("source_request_identity") != plan.get("source_request_identity")
            or acquisition.get("provider_release_id") != diagnostics.get("provider_release_id")
            or acquisition.get("row_count") != len(frame) or not observations):
        raise RuntimeError("LAUS recovery evidence internally inconsistent")
    return plan, frame, diagnostics


def persist_acquisition(root: Path, plan: Mapping[str, Any], frame: pd.DataFrame,
                        diagnostics: Mapping[str, Any], observations: list[dict[str, str]]) -> None:
    frame.to_parquet(root/"canonical.parquet", index=False)
    write_canonical_json(root/"provider_observations.json", observations)
    write_canonical_json(root/"completeness.json", diagnostics)
    hashes = {name: sha256_file(root/name) for name in ("acquired.json", "provider_observations.json", "canonical.parquet", "completeness.json")}
    write_canonical_json(root/"acquisition.json", {"source_request_identity": plan["source_request_identity"],
        "provider_release_id": diagnostics["provider_release_id"], "request_count": len(plan["requests"]),
        "series_count": len(plan["series"]), "row_count": len(frame), "target_month": diagnostics["target_month"],
        "evidence_sha256": hashes})



def build_artifact_recoverable(root: Path, frame: pd.DataFrame, plan: Mapping[str, Any],
                               diagnostics: Mapping[str, Any], *, retrieved_at: str) -> tuple[dict[str, Any], bool]:
    """Construct through preserved deterministic stages; never overwrite failed evidence."""
    artifact = root / "artifact"
    if artifact.exists():
        return validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"], True
    index = 1
    while (root / f"artifact-build-{index}").exists():
        index += 1
    stage = root / f"artifact-build-{index}"
    manifest = create_bootstrap_artifact(stage, frame, plan, diagnostics, retrieved_at=retrieved_at)
    stage.rename(artifact)
    return manifest, False

def complete_post_acquisition(root: Path, plan: Mapping[str, Any], frame: pd.DataFrame, diagnostics: Mapping[str, Any],
                              legacy_serving: Path, legacy_secondary: list[Path], *, retrieved_at: str) -> dict[str, Any]:
    pairs = {(item["geo_id"], item["metric_id"]) for item in plan["series"]}
    legacy = read_legacy(legacy_serving, pairs); detail, summary = equivalence_audit(frame, legacy, plan)
    detail.to_parquet(root/"equivalence_detail.parquet", index=False); write_canonical_json(root/"equivalence.json", summary)
    secondary = {}
    for path in legacy_secondary:
        _, secondary[str(path)] = equivalence_audit(frame, read_legacy(path, pairs), plan)
    write_canonical_json(root/"secondary_equivalence.json", secondary)
    gates = acceptance_gates(frame, diagnostics, summary); write_canonical_json(root/"acceptance.json", gates)
    if gates["status"] != "passed": raise RuntimeError(f"LAUS equivalence gates failed: {gates['failed_checks']}")
    manifest, reused = build_artifact_recoverable(root, frame, plan, diagnostics, retrieved_at=retrieved_at)
    write_canonical_json(root/"artifact_validation.json", {"status": "passed", "artifact_id": manifest["artifact_id"]})
    return {"status": "audit_passed", "artifact_id": manifest["artifact_id"], "target_month": diagnostics["target_month"],
            "provider_release_id": diagnostics["provider_release_id"], "reused": reused}


def audit(args: argparse.Namespace) -> dict[str, Any]:
    root=args.output_root; root.mkdir(parents=True, exist_ok=True)
    if (root/"acquisition.json").exists():
        plan, frame, diagnostics = validate_workspace(root, expected_end_year=args.end_year)
        return complete_post_acquisition(root, plan, frame, diagnostics, args.legacy_serving, args.legacy_secondary,
                                         retrieved_at=args.retrieved_at or utc_now())
    allowed = {"preflight.json", "request_plan.json", "acquired.json", "canonical.parquet",
               "provider_observations.json", "completeness.json"}
    unexpected = sorted(path.name for path in root.iterdir() if path.name not in allowed)
    if unexpected: raise RuntimeError(f"contradictory LAUS bootstrap workspace files: {unexpected}")
    catalog=_load_json(args.catalog)
    current_preflight=preflight(catalog)
    if (root/"preflight.json").exists() and _load_json(root/"preflight.json") != current_preflight:
        raise RuntimeError("LAUS bootstrap preflight state changed during recovery")
    write_canonical_json(root/"preflight.json", current_preflight)
    expected_plan=build_request_plan(acquisition_mode="bootstrap", end_year=args.end_year)
    if (root/"request_plan.json").exists() and _load_json(root/"request_plan.json") != expected_plan:
        raise RuntimeError("LAUS bootstrap request plan changed during recovery")
    write_canonical_json(root/"request_plan.json", expected_plan); plan=expected_plan
    if (root/"acquired.json").exists():
        acquired=_load_json(root/"acquired.json")
    else:
        acquired=acquire(plan, api_key=os.environ.get("BLS_API_KEY", ""))
        # This all-or-nothing evidence is written before transform/report/artifact work,
        # so any later local failure can replay without another provider request.
        write_canonical_json(root/"acquired.json", acquired)
    frame, diagnostics, observations=canonicalize(plan, acquired)
    persist_acquisition(root, plan, frame, diagnostics, observations)
    return complete_post_acquisition(root, plan, frame, diagnostics, args.legacy_serving, args.legacy_secondary,
                                     retrieved_at=args.retrieved_at or utc_now())


def recover(args: argparse.Namespace) -> dict[str, Any]:
    plan, frame, diagnostics = validate_workspace(args.output_root, expected_end_year=args.end_year)
    return complete_post_acquisition(args.output_root, plan, frame, diagnostics, args.legacy_serving,
                                     args.legacy_secondary, retrieved_at=args.retrieved_at or utc_now())


def publication_preconditions(root: Path, *, remote_inventory_complete: bool) -> dict[str, bool]:
    acceptance = _load_json(root/"acceptance.json") if (root/"acceptance.json").is_file() else {}
    artifact_ok = False
    if (root/"artifact").is_dir():
        try: validate_artifact(root/"artifact", expected_source_id=SOURCE_ID); artifact_ok=True
        except Exception: artifact_ok=False
    checks={"audit_passed": acceptance.get("status")=="passed", "artifact_valid": artifact_ok,
            "remote_inventory_complete": remote_inventory_complete}
    if not all(checks.values()): raise RuntimeError(f"LAUS publication preconditions failed: {checks}")
    return checks


def publication_metadata(artifact: Path, package: Mapping[str, Any]) -> dict[str, Any]:
    manifest=validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    return {"logical_artifact_uri":manifest["artifact_uri"],"object_id":manifest["artifact_id"],"object_type":"source",
      "object_metadata":{"source_id":SOURCE_ID},"artifact_content_hash":manifest["artifact_content_hash"],
      "member_hashes":{item["path"]:item["sha256"] for item in package["members"]},"publisher_git_sha":git_sha(),
      "contract_versions":[manifest["artifact_contract_version"],package["package_contract_version"]]}


def catalog_record(manifest: Mapping[str, Any], receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {"object_type":"source","object_id":manifest["artifact_id"],"logical_artifact_uri":manifest["artifact_uri"],
      "remote_repository":receipt["remote_repository"],"release_tag":receipt["release_tag"],"release_id":receipt["release_id"],
      "asset_id":receipt["asset_id"],"asset_filename":receipt["asset_filename"],"package_sha256":receipt["package_sha256"],
      "artifact_content_hash":manifest["artifact_content_hash"],"publication_receipt_id":receipt["receipt_id"],
      "publication_state":receipt["publication_state"],"metadata":{"source_id":SOURCE_ID,"data_sha256":manifest["data_sha256"],
      "provider_release_id":manifest["provider_release_id"],"observation_max":manifest["observation_max"]}}


def _api(args: argparse.Namespace):
    api=GitHubAPI(args.repository,os.environ.get("GITHUB_TOKEN","")); return api,GitHubCatalogCAS(api,CATALOG_PATH,args.branch,fixture=False)


def publish(args: argparse.Namespace) -> dict[str, Any]:
    publication_preconditions(args.output_root,remote_inventory_complete=args.remote_inventory_complete)
    artifact=args.output_root/"artifact"; manifest=validate_artifact(artifact,expected_source_id=SOURCE_ID)["manifest"]
    api,cas=_api(args); catalog,_=cas.read()
    if catalog["accepted"]["source"].get(SOURCE_ID) is not None: raise RuntimeError("LAUS pointer exists before publication")
    existing=[r for r in catalog["immutable_records"] if r["object_id"]==manifest["artifact_id"]]
    if existing: raise RuntimeError("existing LAUS durable evidence requires explicit classification")
    package=build_publication_package(artifact,args.output_root/f"{manifest['artifact_id']}.tar")
    publisher=GitHubReleaseArtifactPublisher(api); uri=manifest["artifact_uri"]
    publisher.prepare(uri,(args.output_root/f"{manifest['artifact_id']}.tar").read_bytes(),publication_metadata(artifact,package))
    publisher.upload(uri); publisher.verify(uri); receipt=publisher.finalize(uri); catalog,changed=cas.add(catalog_record(manifest,receipt),receipt)
    result={"status":"published_verified","artifact_id":manifest["artifact_id"],"catalog_changed":changed,
            "accepted_pointer_changed":False,"package_sha256":package["package_sha256"]}
    write_canonical_json(args.output_root/"publication.json",result); return result


def activate_catalog(catalog: Mapping[str, Any], artifact_id: str) -> dict[str, Any]:
    value=json.loads(json.dumps(catalog)); before=dict(value["accepted"]["source"])
    records=[r for r in value["immutable_records"] if r["object_id"]==artifact_id and r.get("metadata",{}).get("source_id")==SOURCE_ID]
    if len(records)!=1: raise RuntimeError("LAUS activation identity does not resolve once")
    if before.get(SOURCE_ID) not in {None,artifact_id}: raise RuntimeError("different LAUS pointer already accepted")
    value["accepted"]["source"][SOURCE_ID]=artifact_id
    for source in ("redfin","fred_macro","ces"):
        if value["accepted"]["source"].get(source)!=before.get(source): raise RuntimeError("unrelated pointer changed")
    return value


def activate(args: argparse.Namespace) -> dict[str, Any]:
    publication=_load_json(args.output_root/"publication.json")
    if publication.get("status")!="published_verified": raise RuntimeError("LAUS activation requires publication")
    api,cas=_api(args); catalog,_=cas.read(); before=dict(catalog["accepted"]["source"])
    intended=activate_catalog(catalog,publication["artifact_id"]); catalog,changed=cas.activate_source(SOURCE_ID,publication["artifact_id"])
    if catalog["accepted"]["source"]!=intended["accepted"]["source"]: raise RuntimeError("LAUS activation CAS drift")
    result={"accepted_pointer_changed":changed,"prior":before.get(SOURCE_ID),"accepted":publication["artifact_id"]}
    write_canonical_json(args.output_root/"activation.json",result); return result


def verify(args: argparse.Namespace) -> dict[str, Any]:
    api,cas=_api(args); catalog,_=cas.read(); accepted=catalog["accepted"]["source"].get(SOURCE_ID)
    records=[r for r in catalog["immutable_records"] if r["object_id"]==accepted and r.get("metadata",{}).get("source_id")==SOURCE_ID]
    if len(records)!=1: raise RuntimeError("accepted LAUS artifact does not resolve once")
    resolved=GitHubReleaseArtifactResolver(catalog,api,args.output_root/"fresh-accepted-proof").resolve(records[0]["logical_artifact_uri"])
    manifest=validate_artifact(resolved,expected_source_id=SOURCE_ID)["manifest"]
    result={"durable_resolution_passed":True,"resolved_artifact_id":manifest["artifact_id"]}
    write_canonical_json(args.output_root/"fresh_durable_verification.json",result); return result


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__); sub=parser.add_subparsers(dest="command",required=True)
    common=argparse.ArgumentParser(add_help=False); common.add_argument("--output-root",type=Path,required=True)
    common.add_argument("--repository",default=os.environ.get("GITHUB_REPOSITORY","")); common.add_argument("--branch",default=BRANCH)
    for command in ("audit","recover"):
        p=sub.add_parser(command,parents=[common]); p.add_argument("--end-year",type=int,required=True); p.add_argument("--retrieved-at")
        p.add_argument("--legacy-serving",type=Path,default=Path("data/market_serving.duckdb")); p.add_argument("--legacy-secondary",action="append",type=Path,default=[Path("data/market_public.duckdb")])
        if command=="audit": p.add_argument("--catalog",type=Path,default=Path(CATALOG_PATH))
    publish_parser=sub.add_parser("publish",parents=[common]); publish_parser.add_argument("--remote-inventory-complete",action="store_true")
    sub.add_parser("activate",parents=[common]); sub.add_parser("verify",parents=[common])
    args=parser.parse_args(); result={"audit":audit,"recover":recover,"publish":publish,"activate":activate,"verify":verify}[args.command](args)
    print(json.dumps(result,sort_keys=True)); return 0


if __name__=="__main__": raise SystemExit(main())
