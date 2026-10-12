"""Complete ACS physical history from accepted immutable single-vintage parents.

The prior state is only a reconciliation baseline; it is never a data parent.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from core.source_artifacts.artifact import canonicalize, create_artifact
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.models import CANONICAL_KEY
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.acs_family_resolution import _parent
from jobs.monthly_refresh.acs_scope import ACSGovernance, check_identity, identify
from sources.census_acs.artifact import PRODUCTS, VARIABLES, geography_plan

INVENTORY_VERSION = "acs_historical_inventory_v2"
STATE_VERSION = "acs_physical_historical_state_v2"


def frame_identity(frame: pd.DataFrame) -> str:
    """Semantic rows, independent of Parquet encoding and pandas dtypes."""
    data = frame.sort_values(CANONICAL_KEY, kind="mergesort").reset_index(drop=True).copy()
    data["date"] = pd.to_datetime(data.date).dt.strftime("%Y-%m-%d")
    # Python float JSON retains exact round-trip precision (no decimal rounding).
    rows = data.astype(object).where(pd.notna(data), None).to_dict(orient="records")
    return sha256_json(rows)


def inventory(*, authority: dict, parents: list[dict]) -> dict:
    return identify(INVENTORY_VERSION, {"schema_version": INVENTORY_VERSION,
        "source_id": authority["source_id"], "authority_id": authority["authority_id"],
        "namespace": authority["namespace"],
        "parents": sorted(parents, key=lambda p: p["vintage"])}, "inventory_id")


def verify_inventory(value: dict, governance: ACSGovernance, root: Path) -> dict:
    check_identity(INVENTORY_VERSION, value, "inventory_id")
    authority = governance.scope(value["source_id"], value["authority_id"], root)
    parents = value["parents"]
    if (value.get("schema_version") != INVENTORY_VERSION or value.get("namespace") != governance.namespace
            or [p["vintage"] for p in parents] != authority["expected_available_vintages"]):
        raise ValueError("inventory must exactly match independently approved scope")
    if any(p.get("source_id") != value["source_id"] for p in parents):
        raise ValueError("inventory physical product mismatch")
    return authority


def _accepted_parent(parent: dict, *, paths: Mapping[str, Path], records: Mapping[str, dict],
                     allowed: set[str], source: str) -> pd.DataFrame:
    artifact_id = parent["artifact_id"]
    frame, manifest, identity = _parent(records[artifact_id], paths[artifact_id], source)
    if (manifest.get("artifact_status") != "complete" or manifest.get("validation_status") != "passed"
            or json.loads((paths[artifact_id] / "validation.json").read_text()).get("status") != "passed"):
        raise ValueError("ACS acquisition parent must be complete and passed")
    if any(parent.get(k) != v for k, v in identity.items()) or parent.get("data_sha256") != manifest["data_sha256"]:
        raise ValueError("immutable acquisition parent identity mismatch")
    year = parent["vintage"]
    if (manifest["provider_release_id"] != f"{PRODUCTS[source]}:{year}"
            or manifest["target_month"] != f"{year}-12"
            or not pd.to_datetime(frame.date).eq(pd.Timestamp(year, 12, 31)).all()
            or not frame.property_type_id.eq("all").all() or not frame.property_type.eq("all").all()):
        raise ValueError("ACS acquisition observation/vintage semantics mismatch")
    if set(frame.geo_id) - allowed or set(frame.metric_id) - {f"{source}_{s}" for s in VARIABLES.values()}:
        raise ValueError("ACS acquisition geography/metric scope mismatch")
    # Verify the package hash too, even for a local resolver; _parent verifies
    # catalog/content/data identities but relies on transport for package bytes.
    import tempfile
    with tempfile.TemporaryDirectory() as directory:
        package = build_publication_package(paths[artifact_id], Path(directory) / "parent.tar")
    if package["package_sha256"] != parent["package_sha256"]:
        raise ValueError("ACS acquisition package identity mismatch")
    return canonicalize(frame)


def reconstruct(value: dict, *, governance: ACSGovernance, paths: Mapping[str, Path],
                records: Mapping[str, dict], repository_root: Path = Path(".")) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    authority = verify_inventory(value, governance, repository_root)
    governed, _ = geography_plan(repository_root / "config/geo_manifest.generated.csv")
    allowed = {g["geo_id"] for g in governed}
    frames, lineages = [], []
    for parent in value["parents"]:
        frame = _accepted_parent(parent, paths=paths, records=records, allowed=allowed, source=value["source_id"])
        frames.append(frame)
        lineage = frame[CANONICAL_KEY].copy()
        for key, item in {"inventory_id": value["inventory_id"], "authority_id": authority["authority_id"],
                "provider_vintage": parent["vintage"], "acquisition_parent_artifact_id": parent["artifact_id"],
                "acquisition_parent_content_hash": parent["artifact_content_hash"],
                "acquisition_parent_package_sha256": parent["package_sha256"]}.items():
            lineage[key] = item
        lineages.append(lineage)
    data = canonicalize(pd.concat(frames, ignore_index=True))
    if data.duplicated(CANONICAL_KEY).any() or not data.value.map(math.isfinite).all():
        raise ValueError("duplicate/conflicting physical historical key or invalid value")
    lineage = pd.concat(lineages, ignore_index=True).sort_values(CANONICAL_KEY).reset_index(drop=True)
    state = identify(STATE_VERSION, {"schema_version": STATE_VERSION, "inventory": value,
        "semantic_data_sha256": frame_identity(data), "semantic_lineage_sha256": frame_identity(lineage)}, "state_id")
    return data, lineage, state


def materialize(*, mode: str, value: dict, governance: ACSGovernance, prior: dict | None,
                paths: Mapping[str, Path], records: Mapping[str, dict], output: Path,
                repository_root: Path = Path("."), artifact_created_at: str | None = None) -> dict:
    if governance.namespace == "production":
        governance.require_production()
    verify_inventory(value, governance, repository_root)
    current = governance.current_state(value["source_id"])
    if mode == "BOOTSTRAP":
        if current is not None or prior is not None:
            raise ValueError("BOOTSTRAP requires no existing governed state")
    elif mode in {"UPDATE", "REPLAY"}:
        if current is None or prior is None or prior != current:
            raise ValueError(f"{mode} requires exact current governed prior state; missing, stale or wrong prior")
        old = current["inventory"]
        if mode == "REPLAY" and value != old:
            raise ValueError("REPLAY requires identical governed inventory")
        old_parents = {p["vintage"]: p for p in old["parents"]}
        new_parents = {p["vintage"]: p for p in value["parents"]}
        if any(new_parents.get(year) != parent for year, parent in old_parents.items()):
            raise ValueError("historical retention or immutable parent replacement violation; separate correction governance required")
    else:
        raise ValueError("invalid ACS historical lifecycle mode")
    data, lineage, state = reconstruct(value, governance=governance, paths=paths, records=records,
                                      repository_root=repository_root)
    if mode == "REPLAY" and state != current:
        raise ValueError("REPLAY historical content contradiction")
    manifest = create_artifact(output, data, source_id=value["source_id"], source_family="census_acs",
        source_type=STATE_VERSION, provider="U.S. Census Bureau", distribution_channel="governed_historical_materialization",
        provider_release_id=state["state_id"], provider_release_timestamp_or_date=None, retrieved_at=None,
        target_month=f"{max(p['vintage'] for p in value['parents'])}-12", source_request_identity=state["state_id"],
        source_urls_or_endpoint_identity=[f"artifact://source/{p['source_id']}/{p['artifact_id']}" for p in value["parents"]],
        lineage=lineage, config_hashes=governance.scope(value["source_id"], value["authority_id"], repository_root)["config_hashes"],
        source_contract_version=STATE_VERSION, identity_context=state,
        artifact_created_at=artifact_created_at, manifest_extensions={"acs_history": state})
    validate_artifact(output)
    return {"manifest": manifest, "state": state, "mode": mode,
            "reconciliation_prior_state_id": current["state_id"] if current else None}


def verify_state(path: Path, *, governance: ACSGovernance, paths: Mapping[str, Path],
                 records: Mapping[str, dict], repository_root: Path = Path(".")) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    manifest = validate_artifact(path)["manifest"]
    claimed = manifest.get("acs_history", {})
    data, lineage, state = reconstruct(claimed["inventory"], governance=governance, paths=paths, records=records,
                                      repository_root=repository_root)
    if (claimed != state or manifest.get("identity_context") != state or manifest.get("source_type") != STATE_VERSION
            or manifest.get("source_contract_version") != STATE_VERSION or manifest.get("source_id") != state["inventory"]["source_id"]
            or manifest.get("config_hashes") != governance.scope(state["inventory"]["source_id"], state["inventory"]["authority_id"], repository_root)["config_hashes"]
            or manifest.get("source_request_identity") != state["state_id"]
            or manifest.get("provider_release_id") != state["state_id"]
            or manifest.get("target_month") != str(max(data.date))[:7]
            or manifest.get("artifact_status") != "complete" or manifest.get("validation_status") != "passed"
            or frame_identity(pd.read_parquet(path / "data.parquet")) != frame_identity(data)
            or frame_identity(pd.read_parquet(path / "lineage.parquet")) != frame_identity(lineage)):
        raise ValueError("physical historical state differs from independent reconstruction")
    return data, lineage, state


def materialize_current(*, mode: str, governance: ACSGovernance, value: dict, **kwargs: Any) -> dict:
    """Steady-state adapter: resolve the baseline from governance, not human IDs.

    Publication and atomic governance advancement remain separate operations.
    The future scheduler must reread main and CAS its governance snapshot after
    publishing, rejecting a concurrent state/scope change.
    """
    return materialize(mode=mode, value=value, governance=governance,
                       prior=governance.current_state(value["source_id"]), **kwargs)
