"""Independent reconstruction at the ACS historical logical-source boundary."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.acs_family_resolution import POLICY_VERSION, PHYSICAL_TO_LOGICAL, resolve_frames
from jobs.monthly_refresh.acs_history import frame_identity, verify_state
from jobs.monthly_refresh.acs_scope import ACSGovernance
from sources.census_acs.artifact import governed_config_hashes

LOGICAL_VERSION = "acs_logical_historical_source_v2"
RECORD_VERSION = "acs_family_resolution_record_v3"
RESOLVER_VERSION = "acs_historical_family_resolver_v2"
FLAGS = ("accepted_pointer_changed", "source_set_created", "duckdb_mutated", "serving_db_mutated", "provider_discovery_performed")


def reconstruct_family(*, parents: list[dict], paths: Mapping[str, Path], records: Mapping[str, dict],
                       governance: ACSGovernance, repository_root: Path = Path(".")) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if len(parents) != 2 or {p["source_id"] for p in parents} != {"census_acs1", "census_acs5"}:
        raise ValueError("exact two historical ACS physical parents required")
    verified = {}
    for parent in parents:
        source = parent["source_id"]
        record = records[parent["artifact_id"]]
        if (record.get("publication_state") != "published_immutable_verified" or record.get("object_type") != "source"
                or record.get("metadata", {}).get("source_id") != source
                or any(record.get(k) != parent[v] for k, v in (("object_id", "artifact_id"),
                    ("artifact_content_hash", "artifact_content_hash"), ("package_sha256", "package_sha256")))):
            raise ValueError("historical physical parent catalog identity mismatch")
        path = paths[parent["artifact_id"]]
        manifest = validate_artifact(path, expected_source_id=source)["manifest"]
        if (manifest["artifact_id"] != parent["artifact_id"] or manifest["artifact_content_hash"] != parent["artifact_content_hash"]
                or manifest["data_sha256"] != parent["data_sha256"]):
            raise ValueError("historical physical parent manifest identity mismatch")
        # Local and hosted resolvers get the same package-byte verification.
        import tempfile
        from core.source_artifacts.package import build_publication_package
        with tempfile.TemporaryDirectory() as directory:
            package = build_publication_package(path, Path(directory) / "parent.tar")
        if package["package_sha256"] != parent["package_sha256"]:
            raise ValueError("historical physical parent package identity mismatch")
        data, lineage, state = verify_state(path, governance=governance, paths=paths, records=records,
                                           repository_root=repository_root)
        if governance.current_state(source) != state:
            raise ValueError("family parent must be exact current governed historical state")
        verified[source] = (data, lineage, state, parent)
    one, five = verified["census_acs1"], verified["census_acs5"]
    data, lineage, diagnostics = resolve_frames(one[0], five[0], acs1_parent=one[3], acs5_parent=five[3])
    by_key = {source: item[1].set_index(list(item[1].columns[:4])) for source, item in verified.items()}
    extra = []
    for row in lineage.itertuples(index=False):
        source = row.winning_physical_source_id
        key = (row.geo_id, row.winning_physical_metric_id, row.date, row.property_type_id)
        physical = by_key[source].loc[key]
        extra.append({"winning_physical_historical_state_id": verified[source][2]["state_id"],
            **{name: physical[name] for name in ("inventory_id", "authority_id", "provider_vintage",
                "acquisition_parent_artifact_id", "acquisition_parent_content_hash", "acquisition_parent_package_sha256")}})
    lineage = pd.concat([lineage, pd.DataFrame(extra)], axis=1)
    context = {"schema_version": LOGICAL_VERSION, "resolver_version": RESOLVER_VERSION,
        "resolution_policy_version": POLICY_VERSION, "namespace": governance.namespace,
        "parents": sorted(parents, key=lambda p: p["source_id"]),
        "historical_states": {s: item[2] for s, item in sorted(verified.items())},
        "config_hashes": governed_config_hashes(repository_root),
        "physical_to_logical_metric_mapping": dict(sorted(PHYSICAL_TO_LOGICAL.items())),
        "semantic_data_sha256": frame_identity(data), "semantic_lineage_sha256": frame_identity(lineage),
        "diagnostics": diagnostics}
    return data, lineage, context


def build_family(*, output: Path, artifact_created_at: str | None = None, **inputs: Any) -> dict:
    data, lineage, context = reconstruct_family(**inputs)
    semantic = sha256_json(context)
    manifest = create_artifact(output, data, source_id="acs", source_family="census_acs",
        source_type=LOGICAL_VERSION, provider="resolved immutable Census ACS historical parents",
        distribution_channel="governed_family_resolution", provider_release_id=LOGICAL_VERSION + "__" + semantic,
        provider_release_timestamp_or_date=None, retrieved_at=None, target_month=str(max(data.date))[:7],
        source_request_identity=semantic, source_urls_or_endpoint_identity=[
            f"artifact://source/{p['source_id']}/{p['artifact_id']}" for p in context["parents"]],
        lineage=lineage, config_hashes=context["config_hashes"], source_contract_version=LOGICAL_VERSION,
        artifact_created_at=artifact_created_at, identity_context=context,
        manifest_extensions={"acs_historical_family": context})
    validate_family(output, **inputs)
    return manifest


def validate_family(path: Path, **inputs: Any) -> dict:
    manifest = validate_artifact(path, expected_source_id="acs")["manifest"]
    data, lineage, context = reconstruct_family(**inputs)
    if (manifest.get("acs_historical_family") != context or manifest.get("identity_context") != context
            or manifest.get("source_contract_version") != LOGICAL_VERSION or manifest.get("source_type") != LOGICAL_VERSION
            or manifest.get("config_hashes") != context["config_hashes"]
            or manifest.get("source_request_identity") != sha256_json(context)
            or manifest.get("provider_release_id") != LOGICAL_VERSION + "__" + sha256_json(context)
            or manifest.get("target_month") != str(max(data.date))[:7]
            or manifest.get("artifact_status") != "complete" or manifest.get("validation_status") != "passed"
            or frame_identity(pd.read_parquet(path / "data.parquet")) != frame_identity(data)
            or frame_identity(pd.read_parquet(path / "lineage.parquet")) != frame_identity(lineage)):
        raise ValueError("logical ACS package differs from independent complete reconstruction")
    return manifest


def resolution_record(*, artifact: Path, catalog_record: dict, **inputs: Any) -> dict:
    # Record construction repeats validation; a manifest diagnostic is not proof.
    manifest = validate_family(artifact, **inputs)
    if (catalog_record.get("object_id") != manifest["artifact_id"]
            or catalog_record.get("artifact_content_hash") != manifest["artifact_content_hash"]
            or catalog_record.get("publication_state") != "published_immutable_verified"):
        raise ValueError("logical ACS publication identity mismatch")
    import tempfile
    from core.source_artifacts.package import build_publication_package
    with tempfile.TemporaryDirectory() as directory:
        package = build_publication_package(artifact, Path(directory) / "logical.tar")
    if package["package_sha256"] != catalog_record.get("package_sha256"):
        raise ValueError("logical ACS publication package mismatch")
    context = manifest["acs_historical_family"]
    semantic = {"schema_version": RECORD_VERSION, "context": context,
        "parents": context["parents"], "config_hashes": context["config_hashes"],
        "output_artifact_id": manifest["artifact_id"], "output_content_hash": manifest["artifact_content_hash"],
        "output_package_sha256": catalog_record["package_sha256"]}
    return {**semantic, "resolution_id": "acs_family_resolution__" + sha256_json(semantic),
            **{flag: False for flag in FLAGS}}


def validate_record(record: dict, *, artifact: Path, catalog_record: dict, **inputs: Any) -> None:
    if record != resolution_record(artifact=artifact, catalog_record=catalog_record, **inputs):
        raise ValueError("durable ACS resolution record disagrees with validated package")


def resolve_inputs(*, parents: list[dict], resolver: Any, catalog: dict, governance: ACSGovernance,
                   repository_root: Path = Path(".")) -> dict:
    """Resolve exact state and acquisition objects; never provider discovery."""
    records = {r["object_id"]: r for r in catalog["immutable_records"]}
    paths = {}
    for parent in parents:
        source = parent["source_id"]
        paths[parent["artifact_id"]] = resolver.resolve(f"artifact://source/{source}/{parent['artifact_id']}")
        # Use independently governed inventory, not a candidate manifest's list.
        state = governance.current_state(source)
        if state is None:
            raise ValueError("governed ACS historical state is absent")
        for acquisition in state["inventory"]["parents"]:
            aid = acquisition["artifact_id"]
            paths[aid] = resolver.resolve(f"artifact://source/{source}/{aid}")
    return dict(parents=parents, paths=paths, records=records, governance=governance, repository_root=repository_root)


def validate_cohort(record: dict, *, catalog: dict, resolver: Any, governance: ACSGovernance,
                    repository_root: Path = Path("."), _recovery: bool = False) -> None:
    governance.require_production(_recovery=_recovery)
    inputs = resolve_inputs(parents=record["parents"], resolver=resolver, catalog=catalog,
                            governance=governance, repository_root=repository_root)
    artifact = resolver.resolve(f"artifact://source/acs/{record['output_artifact_id']}")
    validate_record(record, artifact=artifact, catalog_record=inputs["records"][record["output_artifact_id"]], **inputs)


def publish_family(*, artifact: Path, api: Any, cas: Any, workspace: Path, git_sha: str, **inputs: Any) -> dict:
    """ACS-specific guard before invoking the shared immutable publisher."""
    from jobs.monthly_refresh.bps_hosted import publish_candidate
    from jobs.monthly_refresh.acs_authoritative_write import CurrentCatalog
    inputs["governance"].require_production()
    validate_family(artifact, **inputs)
    inputs["governance"].require_destination(api.repository)
    inputs["governance"].current_commit(api.repository)
    cas = CurrentCatalog(cas, inputs["governance"])
    publication = publish_candidate(artifact=artifact, source_id="acs", api=api, cas=cas,
                                    workspace=workspace, git_sha=git_sha, logical_source_id="acs")
    # On semantic reuse the durable package may carry a different operational
    # creation clock. Construct records from the verified published bytes.
    from core.source_artifacts.github_release import GitHubReleaseArtifactResolver
    published = GitHubReleaseArtifactResolver(publication["catalog"], api, workspace / "validated-published").resolve(
        publication["record"]["logical_artifact_uri"])
    validate_family(published, **inputs)
    return {**publication, "artifact": published}
