from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .hashing import sha256_file, sha256_json, write_canonical_json
from .publication import PublicationError

VERSION = "source_set_manifest_v2"
STATUSES = {"refreshed", "unchanged", "provider_still_stale", "failed"}
REQUIRED_CONFIGS = {
    "config/monthly_refresh_policy.json", "config/source_refresh_revision_policy_v0_2.json",
    "config/source_metric_registry.csv", "config/geo_manifest.generated.csv",
}
SHA = re.compile(r"[0-9a-f]{64}")
FAMILY_MAP_VERSION = "source_family_resolution_map_v1"


def governed_config_hashes(repository_root: Path = Path(".")) -> dict[str, str]:
    result = {}
    for relative in sorted(REQUIRED_CONFIGS):
        path = repository_root / relative
        if not path.is_file(): raise PublicationError(f"required governed config missing: {relative}")
        result[relative] = sha256_file(path)
    return result


def _semantic(payload: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in payload.items() if k not in {"source_set_id", "created_at", "builder_git_sha"}}


def source_set_semantic_sha256(payload: dict[str, Any]) -> str:
    return sha256_json(_semantic(payload))


def validate_source_set_v2(payload: dict[str, Any]) -> dict[str, Any]:
    required = {"schema_version", "source_set_id", "target_month", "created_at", "builder_git_sha",
                "required_source_inventory", "included_source_inventory", "contract_versions",
                "config_hashes", "sources", "family_resolution", "complete"}
    if set(payload) != required or payload["schema_version"] != VERSION or payload["complete"] is not True:
        raise PublicationError("source-set v2 schema mismatch")
    configs = payload["config_hashes"]
    if set(configs) != REQUIRED_CONFIGS or any(not SHA.fullmatch(v) for v in configs.values()):
        raise PublicationError("source-set v2 governed config hashes incomplete")
    required_inventory, included = payload["required_source_inventory"], payload["included_source_inventory"]
    if required_inventory != sorted(set(required_inventory)) or included != sorted(set(included)) or required_inventory != included:
        raise PublicationError("complete source-set inventory mismatch")
    if [e.get("source_id") for e in payload["sources"]] != included: raise PublicationError("source-set entries unordered or missing")
    family = payload["family_resolution"]
    if family:
        fields = {"schema_version", "cycle_id", "physical_source_inventory",
                  "logical_source_inventory", "families"}
        if set(family) != fields or family["schema_version"] != FAMILY_MAP_VERSION \
                or not str(family["cycle_id"]).startswith("monthly_cycle__"):
            raise PublicationError("source family-resolution map schema mismatch")
        physical = family["physical_source_inventory"]
        logical = family["logical_source_inventory"]
        if physical != sorted(set(physical)) or logical != included:
            raise PublicationError("physical/logical source inventories are not canonical")
        resolved_logical, resolved_physical = set(), set()
        family_fields = {"logical_source_id", "resolution_id", "output_artifact_id",
                         "output_content_hash", "output_package_sha256", "physical_sources"}
        physical_fields = {"source_id", "artifact_id", "artifact_content_hash", "package_sha256"}
        for item in family["families"]:
            if set(item) != family_fields or item["logical_source_id"] not in logical:
                raise PublicationError("logical family declaration invalid")
            if item["logical_source_id"] in resolved_logical:
                raise PublicationError("duplicate logical family")
            if not item["resolution_id"] or not all(SHA.fullmatch(item[k]) for k in
                    ("output_content_hash", "output_package_sha256")):
                raise PublicationError("family output identity invalid")
            members = item["physical_sources"]
            if not members or [m.get("source_id") for m in members] != sorted(m["source_id"] for m in members):
                raise PublicationError("family physical members are not canonical")
            for member in members:
                if set(member) != physical_fields or member["source_id"] not in physical \
                        or member["source_id"] in logical or member["source_id"] in resolved_physical \
                        or not SHA.fullmatch(member["artifact_content_hash"]) \
                        or not SHA.fullmatch(member["package_sha256"]):
                    raise PublicationError("family physical member identity invalid")
                resolved_physical.add(member["source_id"])
            entry = next(e for e in payload["sources"] if e["source_id"] == item["logical_source_id"])
            if (entry["artifact_id"] != item["output_artifact_id"] or
                    entry["artifact_content_hash"] != item["output_content_hash"] or
                    entry["package_sha256"] != item["output_package_sha256"]):
                raise PublicationError("family output/source-set entry mismatch")
            resolved_logical.add(item["logical_source_id"])
        if set(physical) != (set(logical) - resolved_logical) | resolved_physical:
            raise PublicationError("physical completion inventory is not completely mapped")
    entry_fields = {"source_id", "artifact_id", "logical_artifact_uri", "package_sha256", "artifact_content_hash",
                    "provider_release_id", "observation_max", "validation_status", "monthly_status", "release_tag",
                    "asset_id", "publication_receipt_id", "cycle_check_succeeded", "carried_forward", "carry_forward_policy_allowed"}
    for entry in payload["sources"]:
        if set(entry) != entry_fields or entry["monthly_status"] not in STATUSES: raise PublicationError("source-set source entry schema mismatch")
        if entry["monthly_status"] == "failed": raise PublicationError("failed source is not eligible")
        if entry["validation_status"] != "passed" or not entry["cycle_check_succeeded"]: raise PublicationError("source did not pass current cycle check")
        if not entry["logical_artifact_uri"].endswith("/" + entry["artifact_id"]) or "latest" in entry["logical_artifact_uri"]:
            raise PublicationError("source-set requires exact immutable artifact URI")
        if not SHA.fullmatch(entry["package_sha256"]) or not SHA.fullmatch(entry["artifact_content_hash"]): raise PublicationError("source-set SHA invalid")
        stale = entry["monthly_status"] == "provider_still_stale"
        if stale and (not entry["carried_forward"] or not entry["carry_forward_policy_allowed"]):
            raise PublicationError("provider-stale carry-forward evidence invalid")
        if not stale and entry["carry_forward_policy_allowed"]:
            raise PublicationError("carry-forward policy flag is only valid for provider staleness")
        if entry["monthly_status"] == "unchanged" and not entry["carried_forward"]:
            raise PublicationError("unchanged status must retain the exact prior artifact")
        if entry["monthly_status"] == "refreshed" and entry["carried_forward"]:
            raise PublicationError("refreshed source cannot be carried forward")
        if entry["source_id"] == "redfin" and (entry["monthly_status"] != "refreshed" or not entry["artifact_id"].startswith(f"src__redfin__{payload['target_month']}__")):
            raise PublicationError("Redfin requires exact target-month refreshed artifact")
    expected = f"source_set__{payload['target_month']}__v2__{source_set_semantic_sha256(payload)[:16]}"
    if payload["source_set_id"] != expected: raise PublicationError("source-set semantic identity mismatch")
    return payload


def create_source_set_v2(output: Path, *, target_month: str, created_at: str, builder_git_sha: str,
                         entries: list[dict[str, Any]], config_hashes: dict[str, str],
                         contract_versions: list[str] | None = None, family_resolution: dict | None = None) -> dict[str, Any]:
    sources = sorted(entries, key=lambda e: e["source_id"]); inventory = [e["source_id"] for e in sources]
    payload = {"schema_version": VERSION, "source_set_id": "", "target_month": target_month, "created_at": created_at,
               "builder_git_sha": builder_git_sha, "required_source_inventory": inventory,
               "included_source_inventory": inventory, "contract_versions": contract_versions or
               [VERSION, "source_artifact_schema_v1", "canonical_market_assembly_v1", "source_refresh_revision_v0_2"],
               "config_hashes": dict(sorted(config_hashes.items())), "sources": sources,
               "family_resolution": family_resolution or {}, "complete": True}
    payload["source_set_id"] = f"source_set__{target_month}__v2__{source_set_semantic_sha256(payload)[:16]}"
    validate_source_set_v2(payload); write_canonical_json(output, payload); return payload
