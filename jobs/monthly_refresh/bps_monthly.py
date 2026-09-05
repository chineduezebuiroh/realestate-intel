"""BPS adapters for the common governed monthly input-pin/candidate lifecycle.

This module has no cohort barrier, pointer, Source Set, or database-writing API.
It produces the two physical BPS candidates independently from exact pins.
"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping
from urllib.parse import unquote, urlparse

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file
from jobs.monthly_refresh.bps_bootstrap import acquire as acquire_compiled
from jobs.monthly_refresh.bps_bootstrap import inspect_zip, verify as verify_compiled
from jobs.monthly_refresh.bps_provisional_verification import (
    LEVELS, acquire as acquire_provisional, read_member, resolve_inputs, verify as verify_provisional,
)
from jobs.monthly_refresh.source_inputs import provider_pin, verify_member_bytes
from sources.census_bps.artifact import governed_config_hashes
from sources.census_bps.ingest import discover_latest_compiled_zip_url
from sources.census_bps_provisional.ingest import discover_latest_provisional_urls

LOGICAL_SOURCE_ID = "bps"
COMPILED_SOURCE_ID = "census_bps"
PROVISIONAL_SOURCE_ID = "census_bps_provisional"


def validate_compiled_coverage(coverage: Any) -> None:
    """Require stable compiled levels while allowing provider-variable CBSA history."""
    missing = coverage.loc[~coverage.present_in_release]
    required_missing = missing[~missing.provider_geography_type.eq("Metro")]
    if not required_missing.empty:
        identities = required_missing.geo_id.sort_values().tolist()
        raise ValueError(f"compiled snapshot lacks required nation/state/county geographies: {identities}")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def compiled_release(url: str) -> str:
    match = re.search(r"Compiled(?:%20|[ _])(?:File(?:%20|[ _]))?(\d{6})\.zip", url, re.I)
    if not match:
        raise ValueError("compiled discovery returned an unrecognized release URL")
    return match.group(1)


def discover_compiled_pin(*, cycle_id: str, workspace: Path,
                          discover: Callable[[], str] = discover_latest_compiled_zip_url,
                          retrieve: Callable[..., Mapping[str, Any]] = acquire_compiled,
                          retrieved_at: str | None = None) -> tuple[dict[str, Any], dict[str, Path]]:
    url = discover()  # Called only by the common normal-mode pin planner.
    release_id = compiled_release(url)
    path = workspace / Path(unquote(urlparse(url).path)).name
    http = dict(retrieve(url, path))
    stamp = retrieved_at or _now()
    member = {"url": url, "retrieved_at": stamp,
              "sha256": sha256_file(path), "http": http,
              "size_bytes": path.stat().st_size}
    return provider_pin(cycle_id=cycle_id, source_id=COMPILED_SOURCE_ID,
                        provider_release_id=release_id, members={"compiled_zip": member}), {"compiled_zip": path}


def discover_provisional_pin(*, cycle_id: str, workspace: Path,
                             discover: Callable[[], Mapping[str, str]] = discover_latest_provisional_urls,
                             retrieve: Callable[..., Mapping[str, Any]] = acquire_provisional,
                             retrieved_at: str | None = None) -> tuple[dict[str, Any], dict[str, Path]]:
    release_id, urls = resolve_inputs(discover())
    members, paths = {}, {}
    for level in LEVELS:
        path = workspace / Path(unquote(urlparse(urls[level]).path)).name
        http = dict(retrieve(urls[level], path))
        paths[level] = path
        members[level] = {"url": urls[level], "retrieved_at": retrieved_at or _now(),
                          "sha256": sha256_file(path), "http": http,
                          "size_bytes": path.stat().st_size}
    return provider_pin(cycle_id=cycle_id, source_id=PROVISIONAL_SOURCE_ID,
                        provider_release_id=release_id, members=members), paths


def compiled_candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
                       cycle_id: str, git_sha: str = "unknown", repository_root: Path = Path("."),
                       revision: int = 1, prior_artifact_id: str | None = None,
                       prior_artifact_sha256: str | None = None,
                       republication_id: str | None = None,
                       source_contract_version: str | None = None) -> dict[str, Any]:
    def stage(name: str, **measurements: Any) -> None:
        print(json.dumps({"bps_compiled_stage": name, **measurements}, sort_keys=True), flush=True)

    verify_member_bytes(pin, paths)
    stage("PIN_HASH_VERIFIED", input_bytes=paths["compiled_zip"].stat().st_size)
    release_id = str(pin["provider_release_id"])
    started = time.monotonic(); stage("COMPILED_ZIP_INSPECTION_START")
    frame, zip_evidence = inspect_zip(paths["compiled_zip"])
    inspection = frame.attrs["compiled_inspection"]
    stage("COMPILED_ZIP_INSPECTION_COMPLETE", raw_rows=zip_evidence["raw_row_count"],
          chunk_count=inspection["chunk_count"], governed_retained_rows=len(frame),
          elapsed_seconds=round(time.monotonic() - started, 3))
    started = time.monotonic(); stage("COMPILED_VERIFY_START")
    canonical, coverage, diagnostics, examples = verify_compiled(
        frame, release_month=f"{release_id[:4]}-{release_id[4:]}")
    stage("COMPILED_VERIFY_COMPLETE", canonical_rows=len(canonical),
          governed_retained_rows=diagnostics["governed_raw_row_count"],
          elapsed_seconds=round(time.monotonic() - started, 3))
    if diagnostics["authoritative_total_field"] != "total_units" or examples:
        raise ValueError("compiled authoritative TOTAL_UNITS contains unsafe values")
    validate_compiled_coverage(coverage)
    target = str(diagnostics["observation_max"])[:7]
    evidence = {"schema_version": "bps_compiled_candidate_evidence_v1",
                "physical_source_id": COMPILED_SOURCE_ID, "logical_source_id": LOGICAL_SOURCE_ID,
                "cycle_id": cycle_id, "provider_pin": dict(pin), "zip": zip_evidence,
                "coverage": {"applicable": diagnostics["configured_geography_count"],
                             "present": diagnostics["present_geography_count"],
                             "configured": diagnostics["configured_geography_count"],
                             "provider_snapshot_present": diagnostics["present_geography_count"],
                             "missing_configured": diagnostics["missing_configured_geographies"]},
                "duplicate_diagnostics": {k: diagnostics[k] for k in diagnostics if "duplicate" in k},
                "provider_diagnostics": diagnostics}
    # The governed metric belongs to logical family ``bps``, while immutable
    # publication and cycle-result identities belong to this physical member.
    canonical = canonical.assign(source_id=COMPILED_SOURCE_ID)
    stage("ARTIFACT_CREATE_START")
    manifest = create_artifact(output, canonical, source_id=COMPILED_SOURCE_ID,
        source_family="census_bps", source_type="government_survey", provider="U.S. Census Bureau",
        distribution_channel="compiled_master_zip", provider_release_id=f"bps-compiled:{release_id}",
        provider_release_timestamp_or_date=f"{release_id[:4]}-{release_id[4:]}",
        retrieved_at=pin["members"]["compiled_zip"]["retrieved_at"], target_month=target,
        source_request_identity=pin["pin_id"],
        source_urls_or_endpoint_identity=[pin["members"]["compiled_zip"]["url"]],
        raw_source_lineage=evidence, config_hashes=governed_config_hashes(repository_root), git_sha=git_sha,
        revision=revision, prior_artifact_id=prior_artifact_id,
        prior_artifact_sha256=prior_artifact_sha256,
        supersedes_artifact_id=prior_artifact_id if revision > 1 else None,
        republication_id=republication_id, source_contract_version=source_contract_version)
    stage("ARTIFACT_CREATE_COMPLETE", artifact_bytes=sum(p.stat().st_size for p in output.iterdir() if p.is_file()))
    return {"manifest": manifest, "evidence": evidence}


def provisional_candidate(*, pin: Mapping[str, Any], paths: Mapping[str, Path], output: Path,
                          cycle_id: str, git_sha: str = "unknown", repository_root: Path = Path("."),
                          revision: int = 1, prior_artifact_id: str | None = None,
                          prior_artifact_sha256: str | None = None,
                          republication_id: str | None = None,
                          source_contract_version: str | None = None) -> dict[str, Any]:
    verify_member_bytes(pin, paths)
    frames = {level: read_member(paths[level], level)[0] for level in LEVELS}
    canonical, coverage, outside, diagnostics, examples = verify_provisional(
        frames, release_id=str(pin["provider_release_id"]))
    if examples or diagnostics["nonnumeric_or_unavailable_token_counts"]:
        raise ValueError("provisional required unit component contains an unsafe token")
    if diagnostics["present_provisional_applicable_geography_count"] != 220 or len(canonical) != 220:
        raise ValueError("provisional release does not cover exactly 220 applicable geographies")
    if "united_states__nation" in set(canonical.geo_id):
        raise ValueError("provisional candidate must not synthesize a national observation")
    target = str(diagnostics["observation_max"])[:7]
    evidence = {"schema_version": "bps_provisional_candidate_evidence_v1",
                "physical_source_id": PROVISIONAL_SOURCE_ID, "logical_source_id": LOGICAL_SOURCE_ID,
                "cycle_id": cycle_id, "provider_pin": dict(pin),
                "coverage": {"applicable": 220, "present": 220},
                "out_of_governance": {"classification": "OUT_OF_GOVERNANCE",
                                      "count": len(outside),
                                      "inventory": outside.to_dict(orient="records")},
                "token_diagnostics": diagnostics["nonnumeric_or_unavailable_token_counts"],
                "duplicate_diagnostics": {k: diagnostics[k] for k in diagnostics if "duplicate" in k},
                "provider_diagnostics": diagnostics}
    canonical = canonical.assign(source_id=PROVISIONAL_SOURCE_ID)
    manifest = create_artifact(output, canonical, source_id=PROVISIONAL_SOURCE_ID,
        source_family="census_bps_provisional", source_type="government_survey",
        provider="U.S. Census Bureau", distribution_channel="current_provisional_files",
        provider_release_id=f"bps-provisional:{pin['provider_release_id']}",
        provider_release_timestamp_or_date=target,
        retrieved_at=max(item["retrieved_at"] for item in pin["members"].values()), target_month=target,
        source_request_identity=pin["pin_id"],
        source_urls_or_endpoint_identity=[pin["members"][level]["url"] for level in LEVELS],
        raw_source_lineage=evidence, config_hashes=governed_config_hashes(repository_root), git_sha=git_sha,
        revision=revision, prior_artifact_id=prior_artifact_id,
        prior_artifact_sha256=prior_artifact_sha256,
        supersedes_artifact_id=prior_artifact_id if revision > 1 else None,
        republication_id=republication_id, source_contract_version=source_contract_version)
    return {"manifest": manifest, "evidence": evidence}
