"""Controlled, phase-separated bootstrap for the first governed CES artifact."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import duckdb
import pandas as pd

from core.source_artifacts import create_artifact
from core.source_artifacts.github_release import (
    GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactPublisher,
    GitHubReleaseArtifactResolver,
)
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.validation import validate_artifact
from sources.bls_ces.artifact import (
    BLS_API_ENDPOINT, GOVERNED_METRICS, MANDATORY_TARGET_METRIC, SCALE_TRANSFORM,
    SOURCE_ID, UNIT, acquire, build_request_plan, canonicalize,
    governed_config_hashes, load_series_spec,
)

CATALOG_PATH = "config/artifact_catalog.json"
BRANCH = "monthly-refresh-orchestration"
TECHNICAL_TOLERANCE = 1e-12
CATEGORIES = (
    "EXACT_MATCH", "PROVIDER_REVISION", "PROVIDER_NEWER_OBSERVATION",
    "LEGACY_PRIOR_ONLY", "PROVIDER_HISTORICAL_ONLY", "IDENTITY_MISMATCH",
    "UNIT_SCALE_MISMATCH", "UNEXPLAINED_NUMERIC_MISMATCH",
)
KEY = ["geo_id", "metric_id", "date", "property_type_id"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def git_sha() -> str:
    result = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def preflight(catalog: Mapping[str, Any], rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    ces_records = [record for record in catalog["immutable_records"]
                   if record["object_type"] == "source"
                   and record["metadata"].get("source_id") == SOURCE_ID]
    accepted = catalog["accepted"]["source"].get(SOURCE_ID)
    plan = build_request_plan(rows, start_year=1960, end_year=2026,
                              acquisition_mode="deep_reconciliation", config_hashes={"preflight": "0" * 64})
    metrics = sorted({item["metric_id"] for item in plan["series"]})
    result = {"accepted_artifact_id": accepted, "existing_record_count": len(ces_records),
              "governed_metrics": metrics, "series_count": len(plan["series"]),
              "mandatory_series_count": sum(item["mandatory_for_target"] for item in plan["series"]),
              "unit": UNIT, "scale_transform": SCALE_TRANSFORM}
    if metrics != sorted(GOVERNED_METRICS) or result["series_count"] != 59 \
            or result["mandatory_series_count"] != 50:
        raise RuntimeError("CES-A governed scope preflight failed")
    if accepted is not None or ces_records:
        raise RuntimeError(f"CES bootstrap state already exists and requires inspection: {result}")
    return result


def read_legacy(path: Path, expected_pairs: set[tuple[str, str]]) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame(columns=KEY + ["value", "source_id"])
    connection = duckdb.connect(str(path), read_only=True)
    try:
        tables = {row[0] for row in connection.execute("SHOW TABLES").fetchall()}
        if "fact_timeseries" not in tables:
            raise RuntimeError(f"legacy comparison database lacks fact_timeseries: {path}")
        frame = connection.execute("""
            SELECT geo_id, metric_id, date, property_type_id, value, source_id
            FROM fact_timeseries WHERE metric_id IN (?, ?, ?)
        """, list(GOVERNED_METRICS)).fetchdf()
    finally:
        connection.close()
    frame["date"] = pd.to_datetime(frame["date"]).dt.date
    frame["property_type_id"] = frame["property_type_id"].fillna("all").astype(str)
    frame["identity_configured"] = [
        (str(geo), str(metric)) in expected_pairs
        for geo, metric in zip(frame["geo_id"], frame["metric_id"])
    ]
    return frame


def _scale_diagnostic(overlap: pd.DataFrame) -> dict[str, Any]:
    if overlap.empty:
        return {"status": "insufficient_overlap", "unit_scale_mismatch": True,
                "overlap_count": 0, "same_scale_fraction": 0.0,
                "legacy_is_1000x_fraction": 0.0, "provider_is_1000x_fraction": 0.0}
    provider = overlap["value_provider"].astype(float)
    legacy = overlap["value_legacy"].astype(float)
    scale = pd.concat({
        "same": (provider - legacy).abs() <= TECHNICAL_TOLERANCE,
        "legacy_1000x": (provider * 1000 - legacy).abs() <= TECHNICAL_TOLERANCE,
        "provider_1000x": (provider - legacy * 1000).abs() <= TECHNICAL_TOLERANCE,
    }, axis=1).mean()
    systematic = max(float(scale.legacy_1000x), float(scale.provider_1000x)) >= .8
    return {"status": "systematic_scale_mismatch" if systematic else "provider_scale_supported",
            "unit_scale_mismatch": systematic, "overlap_count": len(overlap),
            "same_scale_fraction": float(scale.same),
            "legacy_is_1000x_fraction": float(scale.legacy_1000x),
            "provider_is_1000x_fraction": float(scale.provider_1000x),
            "technical_tolerance": TECHNICAL_TOLERANCE}


def equivalence_audit(provider: pd.DataFrame, legacy: pd.DataFrame, plan: Mapping[str, Any], *,
                      revision_policy: str = "deep_bootstrap",
                      revision_keys: set[tuple[Any, ...]] | None = None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Classify provider/legacy facts without incorporating mutable legacy residue."""
    pair_to_series = {(item["geo_id"], item["metric_id"]): item["series_id"] for item in plan["series"]}
    p = provider.copy(); p["date"] = pd.to_datetime(p["date"]).dt.date
    p["series_id"] = [pair_to_series.get((g, m)) for g, m in zip(p.geo_id, p.metric_id)]
    l = legacy.copy(); l["date"] = pd.to_datetime(l["date"]).dt.date
    if "identity_configured" not in l:
        l["identity_configured"] = [(g, m) in pair_to_series for g, m in zip(l.geo_id, l.metric_id)]
    merged = p[KEY + ["value", "series_id"]].merge(
        l[KEY + ["value", "identity_configured"]], on=KEY, how="outer",
        suffixes=("_provider", "_legacy"), indicator=True,
    )
    legacy_max = l[l.identity_configured].groupby(["geo_id", "metric_id"])["date"].max().to_dict()
    scale = _scale_diagnostic(merged[merged._merge.eq("both")])
    categories = []
    for _, row in merged.iterrows():
        key = tuple(row[column] for column in KEY)
        configured = bool(row.get("identity_configured", True))
        if not configured or (row.geo_id, row.metric_id) not in pair_to_series:
            category = "IDENTITY_MISMATCH"
        elif row["_merge"] == "left_only":
            maximum = legacy_max.get((row.geo_id, row.metric_id))
            category = "PROVIDER_NEWER_OBSERVATION" if maximum is not None and row.date > maximum else "PROVIDER_HISTORICAL_ONLY"
        elif row["_merge"] == "right_only":
            category = "LEGACY_PRIOR_ONLY"
        elif abs(float(row.value_provider) - float(row.value_legacy)) <= TECHNICAL_TOLERANCE:
            category = "EXACT_MATCH"
        elif scale["unit_scale_mismatch"]:
            category = "UNIT_SCALE_MISMATCH"
        elif revision_policy == "deep_bootstrap" or key in (revision_keys or set()):
            category = "PROVIDER_REVISION"
        else:
            category = "UNEXPLAINED_NUMERIC_MISMATCH"
        categories.append(category)
    merged["comparison_category"] = categories
    merged["absolute_difference"] = (merged["value_provider"] - merged["value_legacy"]).abs()
    denominator = merged["value_legacy"].abs().where(merged["value_legacy"].ne(0))
    merged["relative_difference"] = merged["absolute_difference"] / denominator
    merged["series_id"] = [pair_to_series.get((g, m)) for g, m in zip(merged.geo_id, merged.metric_id)]
    merged["legacy_prior_only_reason"] = None
    prior_only = merged.comparison_category.eq("LEGACY_PRIOR_ONLY")
    start = int(plan["start_year"])
    merged.loc[prior_only & pd.to_datetime(merged.date).dt.year.lt(start), "legacy_prior_only_reason"] = "OUTSIDE_REQUEST_BOUNDS"
    merged.loc[prior_only & pd.to_datetime(merged.date).dt.year.ge(start), "legacy_prior_only_reason"] = "CURRENT_IDENTITY_PROVIDER_OMISSION"
    counts = Counter(merged["comparison_category"])
    revisions = merged[merged.comparison_category.eq("PROVIDER_REVISION")]
    summary = {"schema_version": "ces_bootstrap_equivalence_v1",
        **{category.lower() + "_count": int(counts.get(category, 0)) for category in CATEGORIES},
        "exact_match_count": int(counts.get("EXACT_MATCH", 0)),
        "provider_revision_count": int(counts.get("PROVIDER_REVISION", 0)),
        "provider_newer_count": int(counts.get("PROVIDER_NEWER_OBSERVATION", 0)),
        "legacy_prior_only_count": int(counts.get("LEGACY_PRIOR_ONLY", 0)),
        "provider_historical_only_count": int(counts.get("PROVIDER_HISTORICAL_ONLY", 0)),
        "identity_mismatch_count": int(counts.get("IDENTITY_MISMATCH", 0)),
        "unexplained_numeric_mismatch_count": int(counts.get("UNEXPLAINED_NUMERIC_MISMATCH", 0)),
        "unexplained_legacy_prior_only_count": int((merged.legacy_prior_only_reason == "CURRENT_IDENTITY_PROVIDER_OMISSION").sum()),
        "unit_scale": scale,
        "earliest_revision_date": str(revisions.date.min()) if not revisions.empty else None,
        "latest_revision_date": str(revisions.date.max()) if not revisions.empty else None,
        "revision_count_by_year": {str(k): int(v) for k, v in sorted(Counter(pd.to_datetime(revisions.date).dt.year).items())},
        "revision_count_by_metric": dict(sorted(Counter(revisions.metric_id).items())),
        "revision_count_by_geo": dict(sorted(Counter(revisions.geo_id).items())),
        "category_count_by_metric": _group_counts(merged, "metric_id"),
        "category_count_by_geography": _group_counts(merged, "geo_id"),
        "category_count_by_series": _group_counts(merged, "series_id"),
        "category_count_by_date": _group_counts(merged.assign(date=merged.date.map(str)), "date")}
    return merged.sort_values(KEY, kind="mergesort").reset_index(drop=True), summary


def _group_counts(frame: pd.DataFrame, field: str) -> dict[str, dict[str, int]]:
    result: dict[str, dict[str, int]] = {}
    for value, group in frame.groupby(field, dropna=False, sort=True):
        result[str(value)] = dict(sorted((str(k), int(v)) for k, v in Counter(group.comparison_category).items()))
    return result


def acceptance_gates(frame: pd.DataFrame, diagnostics: Mapping[str, Any],
                     equivalence: Mapping[str, Any]) -> dict[str, Any]:
    checks = {
        "exact_metric_scope": bool(sorted(frame.metric_id.unique()) == sorted(GOVERNED_METRICS)),
        "all_mandatory_series_present": bool(
            not diagnostics["missing_mandatory_series"]
            and len(diagnostics["mandatory_series"]) == 50
        ),
        "mandatory_target_resolved": bool(diagnostics["target_month"] is not None),
        "no_provider_membership_omission": bool(not diagnostics["missing_request_memberships"]),
        "canonical_unique": bool(not frame.duplicated(KEY).any()),
        "canonical_finite": bool(
            pd.to_numeric(frame.value)
            .map(lambda value: pd.notna(value) and abs(value) != float("inf"))
            .all()
        ),
        "unit_contract": bool(
            diagnostics["unit"] == UNIT
            and diagnostics["scale_transform"] == SCALE_TRANSFORM
        ),
        "unit_scale_supported": bool(not equivalence["unit_scale"]["unit_scale_mismatch"]),
        "no_identity_mismatch": bool(equivalence["identity_mismatch_count"] == 0),
        "no_unexplained_numeric_mismatch": bool(
            equivalence["unexplained_numeric_mismatch_count"] == 0
        ),
        "no_unexplained_mandatory_history_truncation": bool(
            equivalence["unexplained_legacy_prior_only_count"] == 0
        ),
    }
    return {"schema_version": "ces_bootstrap_acceptance_v1", "status": "passed" if all(checks.values()) else "failed",
            "checks": checks, "failed_checks": sorted(key for key, value in checks.items() if not value)}


def provider_release_id(plan: Mapping[str, Any], frame: pd.DataFrame) -> str:
    values = frame.copy(); values["date"] = values.date.map(str)
    return "ordinary-current:" + sha256_json({"source_request_identity": plan["source_request_identity"],
                                               "canonical_rows": values.to_dict(orient="records")})


def create_bootstrap_artifact(output: Path, frame: pd.DataFrame, plan: Mapping[str, Any],
                              diagnostics: Mapping[str, Any], *, retrieved_at: str,
                              artifact_created_at: str | None = None, lineage_contract: str = "ces_bootstrap_v1") -> dict[str, Any]:
    release_id = provider_release_id(plan, frame)
    manifest = create_artifact(output, frame, source_id=SOURCE_ID,
        source_family="BLS Current Employment Statistics — State and Metro Area",
        source_type="revisionary_current_truth", provider="U.S. Bureau of Labor Statistics",
        distribution_channel="BLS Public Data API v2", provider_release_id=release_id,
        provider_release_timestamp_or_date=None, retrieved_at=retrieved_at,
        artifact_created_at=artifact_created_at, target_month=diagnostics["target_month"],
        source_request_identity=plan["source_request_identity"],
        source_urls_or_endpoint_identity=[BLS_API_ENDPOINT], config_hashes=plan["config_hashes"],
        git_sha=git_sha(), raw_source_lineage={"ces_contract": lineage_contract,
            "provider_response_identity": release_id.split(":", 1)[1]})
    return validate_artifact(output, expected_source_id=SOURCE_ID)["manifest"]


def publication_metadata(artifact: Path, package: Mapping[str, Any]) -> dict[str, Any]:
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    return {"logical_artifact_uri": manifest["artifact_uri"], "object_id": manifest["artifact_id"],
        "object_type": "source", "object_metadata": {"source_id": SOURCE_ID},
        "artifact_content_hash": manifest["artifact_content_hash"],
        "member_hashes": {item["path"]: item["sha256"] for item in package["members"]},
        "publisher_git_sha": git_sha(),
        "contract_versions": [manifest["artifact_contract_version"], package["package_contract_version"]]}


def catalog_record(manifest: Mapping[str, Any], receipt: Mapping[str, Any]) -> dict[str, Any]:
    return {"object_type": "source", "object_id": manifest["artifact_id"],
        "logical_artifact_uri": manifest["artifact_uri"], "remote_repository": receipt["remote_repository"],
        "release_tag": receipt["release_tag"], "release_id": receipt["release_id"], "asset_id": receipt["asset_id"],
        "asset_filename": receipt["asset_filename"], "package_sha256": receipt["package_sha256"],
        "artifact_content_hash": manifest["artifact_content_hash"],
        "publication_receipt_id": receipt["receipt_id"], "publication_state": receipt["publication_state"],
        "metadata": {"source_id": SOURCE_ID, "data_sha256": manifest["data_sha256"],
            "provider_release_id": manifest["provider_release_id"], "observation_max": manifest["observation_max"]}}


def activation_summary(catalog: Mapping[str, Any], artifact_id: str, artifact: Path) -> dict[str, Any]:
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    records = [record for record in catalog["immutable_records"] if record["object_id"] == artifact_id
               and record["metadata"].get("source_id") == SOURCE_ID]
    if len(records) != 1 or manifest["artifact_id"] != artifact_id:
        raise RuntimeError("CES activation identity does not resolve exactly once")
    record = records[0]
    return {"artifact_id": artifact_id, "content_hash": manifest["artifact_content_hash"],
        "data_sha256": manifest["data_sha256"], "package_sha256": record["package_sha256"],
        "target_month": manifest["target_month"], "observation_min": manifest["observation_min"],
        "observation_max": manifest["observation_max"], "row_count": manifest["row_count"],
        "metric_count": manifest["metric_count"], "geo_count": manifest["geography_count"],
        "provider_release_id": manifest["provider_release_id"], "release_id": record["release_id"],
        "asset_id": record["asset_id"]}


def _api(args: argparse.Namespace) -> tuple[GitHubAPI, GitHubCatalogCAS]:
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""))
    return api, GitHubCatalogCAS(api, CATALOG_PATH, args.branch, fixture=False)


def audit(args: argparse.Namespace) -> dict[str, Any]:
    root = args.output_root; root.mkdir(parents=True, exist_ok=False)
    catalog = json.loads(args.catalog.read_text()); rows = load_series_spec()
    pre = preflight(catalog, rows); hashes = governed_config_hashes()
    write_canonical_json(root / "preflight.json", pre)
    plan = build_request_plan(rows, start_year=args.start_year, end_year=args.end_year,
                              acquisition_mode="deep_reconciliation", config_hashes=hashes)
    write_canonical_json(root / "request_plan.json", plan)
    acquired = acquire(plan, api_key=os.environ.get("BLS_API_KEY", ""))
    frame, diagnostics = canonicalize(plan, acquired)
    frame.to_parquet(root / "canonical.parquet", index=False)
    write_canonical_json(root / "completeness.json", diagnostics)
    response_hashes = [sha256_json(item["response"]) for item in acquired]
    write_canonical_json(root / "acquisition.json", {"request_count": len(acquired),
        "series_count": len(plan["series"]), "response_sha256": response_hashes,
        "source_request_identity": plan["source_request_identity"], "row_count": len(frame),
        "metric_count": int(frame.metric_id.nunique()), "geo_count": int(frame.geo_id.nunique()),
        "observation_min": diagnostics["observation_min"], "observation_max": diagnostics["observation_max"],
        "target_month": diagnostics["target_month"]})
    pairs = {(item["geo_id"], item["metric_id"]) for item in plan["series"]}
    serving = read_legacy(args.legacy_serving, pairs)
    detail, equivalence = equivalence_audit(frame, serving, plan)
    detail.to_parquet(root / "equivalence_detail.parquet", index=False)
    write_canonical_json(root / "equivalence.json", equivalence)
    secondary = {}
    for path in args.legacy_secondary:
        legacy = read_legacy(path, pairs)
        _, summary = equivalence_audit(frame, legacy, plan)
        secondary[str(path)] = summary
    write_canonical_json(root / "secondary_equivalence.json", secondary)
    gates = acceptance_gates(frame, diagnostics, equivalence)
    write_canonical_json(root / "acceptance.json", gates)
    if gates["status"] != "passed":
        raise RuntimeError(f"CES bootstrap acceptance failed: {gates['failed_checks']}")
    retrieved_at = utc_now()
    manifest = create_bootstrap_artifact(root / "artifact", frame, plan, diagnostics, retrieved_at=retrieved_at)
    write_canonical_json(root / "artifact_validation.json", {"status": "passed", "artifact_id": manifest["artifact_id"],
                                                               "data_sha256": manifest["data_sha256"]})
    return {"status": "audit_passed", "preflight": pre, "artifact_id": manifest["artifact_id"],
            "target_month": diagnostics["target_month"], "provider_release_id": manifest["provider_release_id"]}


def recover(args: argparse.Namespace) -> dict[str, Any]:
    """Resume only the post-acquisition bootstrap boundary from immutable evidence."""
    root = args.output_root
    required = ("preflight.json", "request_plan.json", "acquisition.json", "canonical.parquet",
                "completeness.json", "equivalence.json", "equivalence_detail.parquet",
                "secondary_equivalence.json")
    missing = [name for name in required if not (root / name).is_file()]
    if missing:
        raise RuntimeError(f"CES recovery evidence incomplete: {missing}")
    if (root / "artifact").exists() or (root / "acceptance.json").exists():
        raise RuntimeError("CES recovery refuses to overwrite post-audit outputs")
    pre = json.loads((root / "preflight.json").read_text())
    plan = json.loads((root / "request_plan.json").read_text())
    acquisition = json.loads((root / "acquisition.json").read_text())
    diagnostics = json.loads((root / "completeness.json").read_text())
    equivalence = json.loads((root / "equivalence.json").read_text())
    secondary = json.loads((root / "secondary_equivalence.json").read_text())
    frame = pd.read_parquet(root / "canonical.parquet")
    detail = pd.read_parquet(root / "equivalence_detail.parquet")
    if not isinstance(secondary, dict) or plan.get("acquisition_mode") != "deep_reconciliation":
        raise RuntimeError("CES recovery evidence contract mismatch")
    if (pre.get("series_count") != 59 or pre.get("mandatory_series_count") != 50
            or len(plan.get("series", [])) != 59
            or sum(bool(item.get("mandatory_for_target")) for item in plan.get("series", [])) != 50
            or acquisition.get("source_request_identity") != plan.get("source_request_identity")
            or acquisition.get("row_count") != len(frame)
            or acquisition.get("target_month") != diagnostics.get("target_month")):
        raise RuntimeError("CES recovery evidence is internally inconsistent")
    counts = Counter(detail["comparison_category"])
    for category in CATEGORIES:
        if int(counts.get(category, 0)) != int(equivalence.get(category.lower() + "_count", 0)):
            raise RuntimeError("CES recovery equivalence detail disagrees with summary")
    gates = acceptance_gates(frame, diagnostics, equivalence)
    write_canonical_json(root / "acceptance.json", gates)
    if gates["status"] != "passed":
        raise RuntimeError(f"CES bootstrap recovery acceptance failed: {gates['failed_checks']}")
    manifest = create_bootstrap_artifact(root / "artifact", frame, plan, diagnostics,
        retrieved_at=args.retrieved_at or utc_now())
    write_canonical_json(root / "artifact_validation.json", {"status":"passed",
        "artifact_id":manifest["artifact_id"], "data_sha256":manifest["data_sha256"]})
    return {"status":"recovery_passed", "artifact_id":manifest["artifact_id"],
            "target_month":diagnostics["target_month"], "provider_release_id":manifest["provider_release_id"]}


def publish(args: argparse.Namespace) -> dict[str, Any]:
    root = args.output_root; acceptance = json.loads((root / "acceptance.json").read_text())
    if acceptance.get("status") != "passed": raise RuntimeError("CES bootstrap audit has not passed")
    artifact = root / "artifact"; manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    api, cas = _api(args); catalog, _ = cas.read()
    if catalog["accepted"]["source"].get(SOURCE_ID) is not None:
        raise RuntimeError("CES accepted pointer appeared before bootstrap publication")
    existing = [r for r in catalog["immutable_records"] if r["object_id"] == manifest["artifact_id"]]
    if existing:
        if len(existing) != 1 or existing[0]["artifact_content_hash"] != manifest["artifact_content_hash"]:
            raise RuntimeError("conflicting CES catalog identity")
        resolved = GitHubReleaseArtifactResolver(catalog, api, root / "published-existing").resolve(existing[0]["logical_artifact_uri"])
        remote = validate_artifact(resolved, expected_source_id=SOURCE_ID)["manifest"]
        result = {"status": "published_verified", "catalog_changed": False, "existing_immutable_asset_reused": True,
                "artifact_id": remote["artifact_id"], "package_sha256": existing[0]["package_sha256"],
                "release_id": existing[0]["release_id"], "asset_id": existing[0]["asset_id"],
                "accepted_pointer_changed": False}
        write_canonical_json(root / "publication.json", result); return result
    package_path = root / f"{manifest['artifact_id']}.tar"; package = build_publication_package(artifact, package_path)
    publisher = GitHubReleaseArtifactPublisher(api); metadata = publication_metadata(artifact, package)
    publisher.prepare(manifest["artifact_uri"], package_path.read_bytes(), metadata)
    publisher.upload(manifest["artifact_uri"]); publisher.verify(manifest["artifact_uri"])
    receipt = publisher.finalize(manifest["artifact_uri"])
    catalog, changed = cas.add(catalog_record(manifest, receipt), receipt)
    resolved = GitHubReleaseArtifactResolver(catalog, api, root / "publication-proof").resolve(manifest["artifact_uri"])
    remote = validate_artifact(resolved, expected_source_id=SOURCE_ID)["manifest"]
    if remote["data_sha256"] != manifest["data_sha256"]: raise RuntimeError("CES durable publication data drift")
    result = {"status": "published_verified", "artifact_id": manifest["artifact_id"],
        "package_sha256": package["package_sha256"], "release_id": receipt["release_id"],
        "asset_id": receipt["asset_id"], "publication_state": receipt["publication_state"],
        "catalog_changed": changed, "accepted_pointer_changed": False, "durable_resolution_passed": True}
    write_canonical_json(root / "publication.json", result); return result


def activate(args: argparse.Namespace) -> dict[str, Any]:
    root = args.output_root; artifact = root / "artifact"
    acceptance = json.loads((root / "acceptance.json").read_text())
    publication = json.loads((root / "publication.json").read_text())
    if acceptance.get("status") != "passed" or publication.get("status") != "published_verified":
        raise RuntimeError("CES activation requires passed audit and verified publication evidence")
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    api, cas = _api(args); catalog, _ = cas.read()
    before = dict(catalog["accepted"]["source"]); summary = activation_summary(catalog, manifest["artifact_id"], artifact)
    if before.get(SOURCE_ID) not in {None, manifest["artifact_id"]}:
        raise RuntimeError("a different CES accepted pointer appeared concurrently")
    write_canonical_json(root / "activation_intent.json", summary)
    catalog, changed = cas.activate_source(SOURCE_ID, manifest["artifact_id"])
    after = catalog["accepted"]["source"]
    if before.get("redfin") != after.get("redfin") or before.get("fred_macro") != after.get("fred_macro"):
        raise RuntimeError("unrelated accepted pointer changed during CES activation")
    result = {**summary, "prior_accepted_artifact_id": before.get(SOURCE_ID),
              "new_accepted_artifact_id": after.get(SOURCE_ID), "accepted_pointer_changed": changed,
              "redfin_pointer_changed": before.get("redfin") != after.get("redfin"),
              "fred_pointer_changed": before.get("fred_macro") != after.get("fred_macro")}
    write_canonical_json(root / "activation.json", result); return result


def verify(args: argparse.Namespace) -> dict[str, Any]:
    root = args.output_root; api, cas = _api(args); catalog, _ = cas.read()
    accepted = catalog["accepted"]["source"].get(SOURCE_ID)
    records = [record for record in catalog["immutable_records"] if record["object_id"] == accepted
               and record["metadata"].get("source_id") == SOURCE_ID]
    if len(records) != 1: raise RuntimeError("accepted CES artifact does not resolve exactly once")
    record = records[0]
    resolved = GitHubReleaseArtifactResolver(catalog, api, root / "fresh-accepted-proof").resolve(record["logical_artifact_uri"])
    manifest = validate_artifact(resolved, expected_source_id=SOURCE_ID)["manifest"]
    result = {"resolved_artifact_id": manifest["artifact_id"],
        "resolved_content_hash": manifest["artifact_content_hash"],
        "resolved_package_sha256": record["package_sha256"], "resolved_release_id": record["release_id"],
        "resolved_asset_id": record["asset_id"], "durable_resolution_passed": True}
    write_canonical_json(root / "fresh_durable_verification.json", result); return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); sub = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--output-root", type=Path, required=True)
    common.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    common.add_argument("--branch", default=BRANCH)
    audit_parser = sub.add_parser("audit", parents=[common]); audit_parser.add_argument("--start-year", type=int, default=1960)
    audit_parser.add_argument("--end-year", type=int, required=True); audit_parser.add_argument("--catalog", type=Path, default=Path(CATALOG_PATH))
    audit_parser.add_argument("--legacy-serving", type=Path, default=Path("data/market_serving.duckdb"))
    audit_parser.add_argument("--legacy-secondary", action="append", type=Path,
                              default=[Path("data/market_public.duckdb"), Path("data/market.duckdb")])
    recovery = sub.add_parser("recover", parents=[common]); recovery.add_argument("--retrieved-at")
    sub.add_parser("publish", parents=[common]); sub.add_parser("activate", parents=[common]); sub.add_parser("verify", parents=[common])
    args = parser.parse_args(); result = {"audit": audit, "recover": recover, "publish": publish, "activate": activate, "verify": verify}[args.command](args)
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
