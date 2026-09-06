"""Deterministic resolution of exact immutable BPS physical parents.

This module deliberately has no provider acquisition, acceptance, Source Set,
readiness, or database dependency.  Its only inputs are validated source
artifact directories and their exact catalog records.
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
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.hashing import canonical_json_bytes, write_canonical_json
from core.source_artifacts.github_release import GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactResolver
from core.source_artifacts.models import CANONICAL_COLUMNS, CANONICAL_KEY
from core.source_artifacts.publication import IdentityCollisionError
from core.source_artifacts.publication import TransientPublicationError
from core.source_artifacts.validation import validate_artifact
from sources.census_bps.artifact import ADAPTER_CONTRACT_VERSION, governed_config_hashes
from jobs.monthly_refresh.bps_hosted import publish_candidate

RESOLVER_VERSION = "bps_family_resolver_v1"
RECORD_VERSION = "bps_family_resolution_record_v1"
FAMILY_SOURCE_ID = "bps"
CYCLE_ID = "monthly_cycle__2026-07__7cab1c5df177a1e4"
EXPECTED_PARENTS = {
    "compiled": {
        "source_id": "census_bps",
        "artifact_id": "src__census_bps__2026-04__r2__993afaddb934ce4f",
        "artifact_content_hash": "993afaddb934ce4f8ea40e14a8e29ce63ddb6c1c743ba1e976b796b185dced4e",
        "package_sha256": "2c64d65d784dd0447cd10273631b9f3f7c1cfa52d159031eab5cbdd8a4e41620",
    },
    "provisional": {
        "source_id": "census_bps_provisional",
        "artifact_id": "src__census_bps_provisional__2026-07__r2__61c56540953237cb",
        "artifact_content_hash": "61c56540953237cb72cc2fec062e9aeb092de411153cd78a250994254004f7ab",
        "package_sha256": "7376bc3fb41ec7a8e20a976ca5e235de285e3725ad63d98084af5cd42b3bfb88",
    },
}
ABSENT_CBSA_CODES = {"15680", "31460"}


def _parent(record: Mapping[str, Any], artifact: Path, role: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    expected = EXPECTED_PARENTS[role]
    required = {"object_type": "source", "object_id": expected["artifact_id"],
                "artifact_content_hash": expected["artifact_content_hash"],
                "package_sha256": expected["package_sha256"],
                "publication_state": "published_immutable_verified"}
    if any(record.get(k) != v for k, v in required.items()) or record.get("metadata", {}).get("source_id") != expected["source_id"]:
        raise ValueError(f"{role} immutable parent identity mismatch")
    manifest = validate_artifact(artifact, expected_source_id=expected["source_id"])["manifest"]
    if (manifest["artifact_id"] != expected["artifact_id"] or
            manifest["artifact_content_hash"] != expected["artifact_content_hash"] or
            manifest["data_sha256"] != record.get("metadata", {}).get("data_sha256")):
        raise ValueError(f"{role} artifact/catalog identity mismatch")
    frame = pd.read_parquet(artifact / "data.parquet")
    if frame.duplicated(CANONICAL_KEY, keep=False).any():
        raise ValueError(f"duplicate canonical key within {role} parent")
    return frame, manifest


def resolve_frames(compiled: pd.DataFrame, provisional: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Return the full-key union, choosing compiled only on identical keys."""
    for role, frame in (("compiled", compiled), ("provisional", provisional)):
        missing = set(CANONICAL_COLUMNS) - set(frame)
        if missing: raise ValueError(f"{role} missing canonical columns: {sorted(missing)}")
        if frame.duplicated(CANONICAL_KEY, keep=False).any():
            raise ValueError(f"duplicate canonical key within {role} parent")
    c = compiled.set_index(CANONICAL_KEY, drop=False)
    p = provisional.set_index(CANONICAL_KEY, drop=False)
    overlap = c.index.intersection(p.index)
    same = c.loc[overlap, "value"].eq(p.loc[overlap, "value"])
    merged = pd.concat([compiled, provisional.loc[~p.index.isin(c.index)]], ignore_index=True)
    merged["source_id"] = FAMILY_SOURCE_ID
    merged = merged[CANONICAL_COLUMNS].sort_values(CANONICAL_KEY, kind="mergesort").reset_index(drop=True)
    if merged.duplicated(CANONICAL_KEY).any(): raise AssertionError("family resolver produced duplicate keys")

    rows = []
    for row in merged.itertuples(index=False):
        key = tuple(getattr(row, k) for k in CANONICAL_KEY)
        ci, pi = key in c.index, key in p.index
        rows.append({**{k: getattr(row, k) for k in CANONICAL_KEY},
                     "winning_parent": "compiled" if ci else "provisional",
                     "compiled_present": ci, "provisional_present": pi,
                     "compiled_artifact_id": EXPECTED_PARENTS["compiled"]["artifact_id"] if ci else None,
                     "compiled_content_hash": EXPECTED_PARENTS["compiled"]["artifact_content_hash"] if ci else None,
                     "provisional_artifact_id": EXPECTED_PARENTS["provisional"]["artifact_id"] if pi else None,
                     "provisional_content_hash": EXPECTED_PARENTS["provisional"]["artifact_content_hash"] if pi else None,
                     "compiled_value": float(c.loc[key, "value"]) if ci else None,
                     "provisional_value": float(p.loc[key, "value"]) if pi else None})
    lineage = pd.DataFrame(rows)
    diagnostics = {
        "compiled_input_row_count": len(compiled), "provisional_input_row_count": len(provisional),
        "output_row_count": len(merged), "compiled_only_key_count": len(c.index.difference(p.index)),
        "provisional_only_key_count": len(p.index.difference(c.index)), "overlap_key_count": len(overlap),
        "overlap_same_value_count": int(same.sum()), "overlap_differing_value_count": int((~same).sum()),
        "compiled_wins_count": len(overlap), "compiled_geography_count": int(compiled.geo_id.nunique()),
        "provisional_geography_count": int(provisional.geo_id.nunique()),
        "family_geography_union_count": int(merged.geo_id.nunique()),
    }
    return merged, lineage, diagnostics


def _cbsa_diagnostics(compiled: pd.DataFrame, provisional: pd.DataFrame, concepts_path: Path) -> dict[str, Any]:
    concepts = pd.read_csv(concepts_path, dtype=str)
    governed = set(concepts.loc[concepts.bps_compatibility.eq("compatible"), "census_code"])
    divisions = set(concepts.loc[concepts.canonical_concept.eq("metropolitan_division"), "census_code"])
    code_by_geo = dict(zip(concepts.canonical_geo_id, concepts.census_code))
    codes = lambda f: {code_by_geo[g] for g in set(f.geo_id) if g in code_by_geo}
    c, p = codes(compiled), codes(provisional); union = c | p
    result = {"governed_compatible_count": len(governed), "compiled_physical_count": len(c),
              "provisional_physical_count": len(p), "shared_count": len(c & p),
              "compiled_only_count": len(c - p), "provisional_only_count": len(p - c),
              "union_count": len(union), "absent_from_both_count": len(governed - union),
              "absent_from_both_codes": sorted(governed - union)}
    expected = (53, 42, 50, 41, 1, 9, 51, 2)
    actual = tuple(result[k] for k in ("governed_compatible_count", "compiled_physical_count",
        "provisional_physical_count", "shared_count", "compiled_only_count",
        "provisional_only_count", "union_count", "absent_from_both_count"))
    if actual != expected or set(result["absent_from_both_codes"]) != ABSENT_CBSA_CODES:
        raise ValueError("BPS family CBSA reconciliation contradicts Smoke 200")
    if "09999" in union or divisions & union: raise ValueError("unsupported BPS geography entered family")
    return result


def build_family_artifact(*, compiled_artifact: Path, provisional_artifact: Path,
                          compiled_record: Mapping[str, Any], provisional_record: Mapping[str, Any],
                          output: Path, repository_root: Path = Path("."), git_sha: str = "unknown") -> dict[str, Any]:
    compiled, cm = _parent(compiled_record, compiled_artifact, "compiled")
    provisional, pm = _parent(provisional_record, provisional_artifact, "provisional")
    if provisional.geo_id.astype(str).str.endswith("__nation").any():
        raise ValueError("provisional BPS parent contains a forbidden nation row")
    data, lineage, diagnostics = resolve_frames(compiled, provisional)
    diagnostics.update({
        "compiled_parent_artifact_id": EXPECTED_PARENTS["compiled"]["artifact_id"],
        "compiled_parent_content_hash": EXPECTED_PARENTS["compiled"]["artifact_content_hash"],
        "provisional_parent_artifact_id": EXPECTED_PARENTS["provisional"]["artifact_id"],
        "provisional_parent_content_hash": EXPECTED_PARENTS["provisional"]["artifact_content_hash"],
    })
    diagnostics["geography_count_by_type"] = {
        level: int(data.geo_id.astype(str).str.endswith(f"__{level}").groupby(data.geo_id).max().sum())
        for level in ("nation", "state", "county", "cbsa_metro")
    }
    diagnostics["cbsa"] = _cbsa_diagnostics(compiled, provisional, repository_root / "config/bps_cbsa_canonical_concepts_v1.csv")
    parents = [{"role": role, **EXPECTED_PARENTS[role], "data_sha256": m["data_sha256"]}
               for role, m in (("compiled", cm), ("provisional", pm))]
    identity_context = {"resolver_version": RESOLVER_VERSION, "merge_policy": "full_key_union_compiled_precedence_v1",
                        "source_contract_version": ADAPTER_CONTRACT_VERSION, "parents": parents}
    manifest = create_artifact(output, data, source_id=FAMILY_SOURCE_ID, source_family="bps",
        source_type="logical_governed_family", provider="resolved immutable BPS parents",
        distribution_channel="governed_family_resolution", provider_release_id="bps-family:2026-07",
        provider_release_timestamp_or_date=None, retrieved_at=None, target_month="2026-07",
        source_request_identity="bps-family-resolution:" + sha256_json(identity_context),
        source_urls_or_endpoint_identity=[f"artifact://source/{x['source_id']}/{x['artifact_id']}" for x in parents],
        revision=1, lineage=lineage, config_hashes=governed_config_hashes(repository_root), git_sha=git_sha,
        acquisition_time_status="historical_not_recorded", source_contract_version=ADAPTER_CONTRACT_VERSION,
        identity_context=identity_context, manifest_extensions={"family_resolution": {"diagnostics": diagnostics, "parents": parents}})
    return {"manifest": manifest, "diagnostics": diagnostics, "parents": parents}


def resolution_record(*, manifest: Mapping[str, Any], catalog_record: Mapping[str, Any]) -> dict[str, Any]:
    family = manifest.get("family_resolution", {})
    if (manifest.get("source_id") != FAMILY_SOURCE_ID or catalog_record.get("object_id") != manifest.get("artifact_id") or
            catalog_record.get("artifact_content_hash") != manifest.get("artifact_content_hash") or
            catalog_record.get("publication_state") != "published_immutable_verified"):
        raise ValueError("family artifact publication identity mismatch")
    semantic = {"cycle_id": CYCLE_ID, "resolver_version": RESOLVER_VERSION,
                "source_contract_version": ADAPTER_CONTRACT_VERSION, "parents": family["parents"],
                "config_hashes": manifest["config_hashes"], "output_artifact_id": manifest["artifact_id"],
                "output_content_hash": manifest["artifact_content_hash"]}
    return {"schema_version": RECORD_VERSION, "resolution_id": "bps_family_resolution__" + sha256_json(semantic)[:24],
            **semantic, "output_package_sha256": catalog_record["package_sha256"],
            "diagnostics": family["diagnostics"], "accepted_pointer_changed": False,
            "source_set_created": False, "duckdb_mutated": False, "redfin_consumed": False,
            "provider_discovery_performed": False}


def add_record(existing: Mapping[str, Any] | None, proposed: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    if proposed.get("schema_version") != RECORD_VERSION or any(proposed.get(k) is not False for k in
            ("accepted_pointer_changed", "source_set_created", "duckdb_mutated", "redfin_consumed", "provider_discovery_performed")):
        raise ValueError("invalid BPS family resolution record")
    if existing is None: return dict(proposed), True
    if dict(existing) == dict(proposed): return dict(existing), False
    raise IdentityCollisionError("contradictory BPS family resolution record")


class GitHubFamilyResolutionStore:
    """Create-once durable record, isolated from physical monthly results."""
    def __init__(self, api: GitHubAPI, branch: str, attempts: int = 4):
        self.api, self.branch, self.attempts = api, branch, attempts

    def put(self, record: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
        path = f"config/bps_family_resolutions/{record['resolution_id']}.json"
        encoded = urllib.parse.quote(path, safe="/")
        for attempt in range(self.attempts):
            item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}", expected=(200, 404))
            existing = json.loads(base64.b64decode(item["content"])) if item else None
            value, changed = add_record(existing, record)
            if not changed: return value, False
            payload = {"message": f"Record {record['resolution_id']}",
                       "content": base64.b64encode(canonical_json_bytes(value)).decode(), "branch": self.branch}
            if item: payload["sha"] = item["sha"]
            try:
                self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))
                return value, True
            except TransientPublicationError:
                if attempt + 1 == self.attempts: raise
        raise AssertionError("unreachable")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", required=True); parser.add_argument("--branch", required=True)
    parser.add_argument("--workspace", type=Path, required=True); parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    api = GitHubAPI(args.repository, os.environ.get("GITHUB_TOKEN", "")); cas = GitHubCatalogCAS(api, "config/artifact_catalog.json", args.branch)
    catalog, _ = cas.read(); records = {r["object_id"]: r for r in catalog["immutable_records"]}
    resolver = GitHubReleaseArtifactResolver(catalog, api, args.workspace / "parents")
    parent_paths = {role: resolver.resolve(f"artifact://source/{value['source_id']}/{value['artifact_id']}")
                    for role, value in EXPECTED_PARENTS.items()}
    artifact = args.workspace / "family-artifact"
    built = build_family_artifact(compiled_artifact=parent_paths["compiled"], provisional_artifact=parent_paths["provisional"],
        compiled_record=records[EXPECTED_PARENTS["compiled"]["artifact_id"]],
        provisional_record=records[EXPECTED_PARENTS["provisional"]["artifact_id"]], output=artifact,
        git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    publication = publish_candidate(artifact=artifact, source_id=FAMILY_SOURCE_ID, api=api, cas=cas,
        workspace=args.workspace / "publication", git_sha=os.environ.get("GITHUB_SHA", "unknown"))
    record = resolution_record(manifest=built["manifest"], catalog_record=publication["record"])
    durable, changed = GitHubFamilyResolutionStore(api, args.branch).put(record)
    result = {"record": durable, "record_changed": changed, "candidate_reused": publication["reused"]}
    write_canonical_json(args.output, result); print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
