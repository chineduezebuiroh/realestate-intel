"""Resolve exact immutable ACS1/ACS5 parents into the governed ACS family.

Parent selection is deliberately external and explicit: callers supply two
artifact IDs from the durable catalog.  This module never discovers provider
data and never mutates accepted, Source Set, or database state.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import urllib.parse
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactResolver
from core.source_artifacts.hashing import canonical_json_bytes, sha256_json, write_canonical_json
from core.source_artifacts.models import CANONICAL_COLUMNS, CANONICAL_KEY
from core.source_artifacts.publication import IdentityCollisionError, TransientPublicationError
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.bps_hosted import publish_candidate
from sources.census_acs.artifact import CONTRACT_VERSION, governed_config_hashes, geography_plan

RESOLVER_VERSION = "acs_family_resolver_v1"
POLICY_VERSION = "acs1_preferred_observation_key_v1"
RECORD_VERSION = "acs_family_resolution_record_v1"
FAMILY_SOURCE_ID = "acs"
PHYSICAL_TO_LOGICAL = {
    "census_acs1_pop_total": "census_acs_pop_total",
    "census_acs1_median_household_income": "census_acs_median_household_income",
    "census_acs5_pop_total": "census_acs_pop_total",
    "census_acs5_median_household_income": "census_acs_median_household_income",
}
LOGICAL_KEY = ["geo_id", "metric_id", "date", "property_type_id"]


def _identity(record: Mapping[str, Any], source_id: str) -> dict[str, Any]:
    required = ("object_id", "artifact_content_hash", "package_sha256")
    if (record.get("object_type") != "source" or
            record.get("publication_state") != "published_immutable_verified" or
            record.get("metadata", {}).get("source_id") != source_id or
            any(not isinstance(record.get(k), str) or not record[k] for k in required)):
        raise ValueError(f"{source_id} malformed immutable parent identity")
    return {"source_id": source_id, "artifact_id": record["object_id"],
            "artifact_content_hash": record["artifact_content_hash"],
            "package_sha256": record["package_sha256"]}


def _parent(record: Mapping[str, Any], artifact: Path, source_id: str) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any]]:
    identity = _identity(record, source_id)
    manifest = validate_artifact(artifact, expected_source_id=source_id)["manifest"]
    metadata = record.get("metadata", {})
    if (manifest.get("artifact_id") != identity["artifact_id"] or
            manifest.get("artifact_content_hash") != identity["artifact_content_hash"] or
            manifest.get("data_sha256") != metadata.get("data_sha256") or
            manifest.get("source_contract_version", CONTRACT_VERSION) != CONTRACT_VERSION):
        raise ValueError(f"{source_id} artifact/catalog identity mismatch")
    frame = pd.read_parquet(artifact / "data.parquet")
    return frame, manifest, identity


def resolve_frames(acs1: pd.DataFrame, acs5: pd.DataFrame, *,
                   acs1_parent: Mapping[str, Any], acs5_parent: Mapping[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Choose ACS1, otherwise ACS5, independently for every logical key."""
    prepared: dict[str, pd.DataFrame] = {}
    for role, source_id, frame in (("acs1", "census_acs1", acs1), ("acs5", "census_acs5", acs5)):
        missing = set(CANONICAL_COLUMNS) - set(frame)
        if missing:
            raise ValueError(f"{role} missing canonical columns: {sorted(missing)}")
        if not frame["source_id"].eq(source_id).all():
            raise ValueError(f"{role} contains contradictory physical source lineage")
        if frame.duplicated(CANONICAL_KEY, keep=False).any():
            raise ValueError(f"duplicate physical canonical key within {role} parent")
        unknown = sorted(set(frame.metric_id) - set(PHYSICAL_TO_LOGICAL))
        if unknown:
            raise ValueError(f"unknown ACS physical metric mapping: {unknown}")
        mapped = frame.copy()
        mapped["physical_metric_id"] = mapped.metric_id
        mapped["metric_id"] = mapped.metric_id.map(PHYSICAL_TO_LOGICAL)
        if mapped.duplicated(LOGICAL_KEY, keep=False).any():
            raise ValueError(f"duplicate logical key within {role} parent")
        prepared[role] = mapped.set_index(LOGICAL_KEY, drop=False)

    one, five = prepared["acs1"], prepared["acs5"]
    overlap = one.index.intersection(five.index)
    equal = one.loc[overlap, "value"].eq(five.loc[overlap, "value"])
    selected = pd.concat([one.reset_index(drop=True), five.loc[~five.index.isin(one.index)].reset_index(drop=True)], ignore_index=True)
    selected["source_id"] = FAMILY_SOURCE_ID
    output = selected[list(CANONICAL_COLUMNS)].sort_values(LOGICAL_KEY, kind="mergesort").reset_index(drop=True)
    if output.duplicated(LOGICAL_KEY).any():
        raise AssertionError("ACS family resolver produced duplicate logical keys")

    lineage = []
    for row in output.itertuples(index=False):
        key = tuple(getattr(row, k) for k in LOGICAL_KEY)
        has_one, has_five = key in one.index, key in five.index
        winner, parent = (one, acs1_parent) if has_one else (five, acs5_parent)
        physical_source = "census_acs1" if has_one else "census_acs5"
        lineage.append({**{k: getattr(row, k) for k in LOGICAL_KEY},
            "winning_physical_source_id": physical_source,
            "winning_physical_metric_id": winner.loc[key, "physical_metric_id"],
            "winning_parent_artifact_id": parent["artifact_id"],
            "winning_parent_artifact_content_hash": parent["artifact_content_hash"],
            "acs1_present": has_one, "acs5_present": has_five,
            "acs1_value": float(one.loc[key, "value"]) if has_one else None,
            "acs5_value": float(five.loc[key, "value"]) if has_five else None,
            "resolution_reason": "ACS1_PREFERRED_AVAILABLE" if has_one else "ACS5_FALLBACK_ACS1_UNAVAILABLE"})
    lineage_frame = pd.DataFrame(lineage)
    diagnostics = {"acs1_input_row_count": len(acs1), "acs5_input_row_count": len(acs5),
        "logical_output_row_count": len(output), "acs1_only_logical_key_count": len(one.index.difference(five.index)),
        "acs5_only_logical_key_count": len(five.index.difference(one.index)), "overlap_logical_key_count": len(overlap),
        "acs1_wins_count": int(lineage_frame.acs1_present.sum()),
        "acs5_fallback_count": int((~lineage_frame.acs1_present).sum()),
        "overlap_equal_value_count": int(equal.sum()), "overlap_differing_value_count": int((~equal).sum()),
        "output_duplicate_key_count": 0, "geography_union_count": int(output.geo_id.nunique()),
        "physical_to_logical_metric_mapping": dict(sorted(PHYSICAL_TO_LOGICAL.items())),
        "acs1_parent_artifact_id": acs1_parent["artifact_id"], "acs1_parent_content_hash": acs1_parent["artifact_content_hash"],
        "acs5_parent_artifact_id": acs5_parent["artifact_id"], "acs5_parent_content_hash": acs5_parent["artifact_content_hash"]}
    return output, lineage_frame, diagnostics


def build_family_artifact(*, acs1_artifact: Path, acs5_artifact: Path,
                          acs1_record: Mapping[str, Any], acs5_record: Mapping[str, Any],
                          output: Path, repository_root: Path = Path("."), git_sha: str = "unknown") -> dict[str, Any]:
    one, one_manifest, one_id = _parent(acs1_record, acs1_artifact, "census_acs1")
    five, five_manifest, five_id = _parent(acs5_record, acs5_artifact, "census_acs5")
    governed, excluded = geography_plan(repository_root / "config/geo_manifest.generated.csv")
    allowed = {x["geo_id"] for x in governed}
    outside = sorted((set(one.geo_id) | set(five.geo_id)) - allowed)
    if outside:
        raise ValueError(f"logical output outside governed ACS geography contract: {outside}")
    data, lineage, diagnostics = resolve_frames(one, five, acs1_parent=one_id, acs5_parent=five_id)
    raw_one = one_manifest.get("raw_source_lineage", {}).get("coverage", {})
    raw_five = five_manifest.get("raw_source_lineage", {}).get("coverage", {})
    diagnostics.update({"geography_count_by_governed_type": {
        level: int(data.geo_id.isin({x["geo_id"] for x in governed if x["level"] == level}).sum() > 0 and
                   data.loc[data.geo_id.isin({x["geo_id"] for x in governed if x["level"] == level}), "geo_id"].nunique())
        for level in ("nation", "state", "county", "cbsa_metro")},
        "excluded_geography_count": len(excluded), "excluded_geographies": excluded,
        "provider_ineligible": {"census_acs1": raw_one.get("provider_ineligible", []),
                                "census_acs5": raw_five.get("provider_ineligible", [])}})
    parents = [{"role": "acs1", **one_id, "data_sha256": one_manifest["data_sha256"]},
               {"role": "acs5", **five_id, "data_sha256": five_manifest["data_sha256"]}]
    config_hashes = governed_config_hashes(repository_root)
    identity_context = {"resolver_version": RESOLVER_VERSION, "resolution_policy_version": POLICY_VERSION,
        "source_contract_version": CONTRACT_VERSION, "physical_to_logical_metric_mapping": dict(sorted(PHYSICAL_TO_LOGICAL.items())),
        "parents": parents, "config_hashes": config_hashes}
    target_month = max(one_manifest["target_month"], five_manifest["target_month"])
    manifest = create_artifact(output, data, source_id=FAMILY_SOURCE_ID, source_family="census_acs",
        source_type="logical_governed_family", provider="resolved immutable Census ACS parents",
        distribution_channel="governed_family_resolution", provider_release_id="acs-family:" + sha256_json(identity_context)[:24],
        provider_release_timestamp_or_date=None, retrieved_at=None, target_month=target_month,
        source_request_identity="acs-family-resolution:" + sha256_json(identity_context),
        source_urls_or_endpoint_identity=[f"artifact://source/{x['source_id']}/{x['artifact_id']}" for x in parents],
        lineage=lineage, config_hashes=config_hashes, git_sha=git_sha,
        acquisition_time_status="historical_not_recorded", source_contract_version=CONTRACT_VERSION,
        identity_context=identity_context, manifest_extensions={"family_resolution": {"diagnostics": diagnostics, "parents": parents}})
    return {"manifest": manifest, "diagnostics": diagnostics, "parents": parents}


def resolution_record(*, manifest: Mapping[str, Any], catalog_record: Mapping[str, Any]) -> dict[str, Any]:
    family = manifest.get("family_resolution", {})
    if (manifest.get("source_id") != FAMILY_SOURCE_ID or catalog_record.get("object_id") != manifest.get("artifact_id") or
            catalog_record.get("artifact_content_hash") != manifest.get("artifact_content_hash") or
            catalog_record.get("publication_state") != "published_immutable_verified"):
        raise ValueError("ACS family artifact publication identity mismatch")
    semantic = {"resolver_version": RESOLVER_VERSION, "resolution_policy_version": POLICY_VERSION,
        "source_contract_version": CONTRACT_VERSION, "parents": family["parents"],
        "config_hashes": manifest["config_hashes"], "physical_to_logical_metric_mapping": dict(sorted(PHYSICAL_TO_LOGICAL.items())),
        "output_artifact_id": manifest["artifact_id"], "output_content_hash": manifest["artifact_content_hash"]}
    return {"schema_version": RECORD_VERSION, "resolution_id": "acs_family_resolution__" + sha256_json(semantic)[:24],
        **semantic, "output_package_sha256": catalog_record["package_sha256"], "diagnostics": family["diagnostics"],
        "accepted_pointer_changed": False, "source_set_created": False, "duckdb_mutated": False,
        "serving_db_mutated": False, "provider_discovery_performed": False}


def add_record(existing: Mapping[str, Any] | None, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    flags = ("accepted_pointer_changed", "source_set_created", "duckdb_mutated", "serving_db_mutated", "provider_discovery_performed")
    if proposed.get("schema_version") != RECORD_VERSION or any(proposed.get(k) is not False for k in flags):
        raise ValueError("invalid ACS family resolution record")
    if existing is None:
        return dict(proposed), True
    if dict(existing) == dict(proposed):
        return dict(existing), False
    raise IdentityCollisionError("contradictory ACS family resolution record")


class GitHubFamilyResolutionStore:
    def __init__(self, api: GitHubAPI, branch: str, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def put(self, record: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        path = f"config/acs_family_resolutions/{record['resolution_id']}.json"
        encoded = urllib.parse.quote(path, safe="/")
        for attempt in range(self.attempts):
            item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}", expected=(200, 404))
            existing = json.loads(base64.b64decode(item["content"])) if item else None
            value, changed = add_record(existing, record)
            if not changed:
                return value, False
            payload = {"message": f"Record {record['resolution_id']}",
                "content": base64.b64encode(canonical_json_bytes(value)).decode(), "branch": self.branch}
            if item:
                payload["sha"] = item["sha"]
            try:
                self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts:
                    raise
        raise AssertionError("unreachable")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", required=True)
    parser.add_argument("--acs1-artifact-id", required=True); parser.add_argument("--acs5-artifact-id", required=True)
    parser.add_argument("--workspace", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", ""))
    cas = GitHubCatalogCAS(api, "config/artifact_catalog.json", args.branch)
    catalog, _ = cas.read(); records = {r["object_id"]: r for r in catalog["immutable_records"]}
    requested = {"acs1": ("census_acs1", args.acs1_artifact_id), "acs5": ("census_acs5", args.acs5_artifact_id)}
    if any(artifact_id not in records for _, artifact_id in requested.values()):
        raise ValueError("requested ACS parent is absent from durable artifact catalog")
    resolver = GitHubReleaseArtifactResolver(catalog, api, args.workspace / "parents")
    paths = {role: resolver.resolve(f"artifact://source/{source}/{artifact_id}") for role, (source, artifact_id) in requested.items()}
    built = build_family_artifact(acs1_artifact=paths["acs1"], acs5_artifact=paths["acs5"],
        acs1_record=records[args.acs1_artifact_id], acs5_record=records[args.acs5_artifact_id],
        output=args.workspace / "family-artifact", git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    publication = publish_candidate(artifact=args.workspace / "family-artifact", source_id=FAMILY_SOURCE_ID,
        api=api, cas=cas, workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    record = resolution_record(manifest=built["manifest"], catalog_record=publication["record"])
    durable, changed = GitHubFamilyResolutionStore(api, args.branch).put(record)
    result = {"record": durable, "record_changed": changed, "candidate_reused": publication["reused"]}
    write_canonical_json(args.output, result); print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
