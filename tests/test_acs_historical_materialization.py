"""Adversarial scope, lifecycle, acquisition and independent logical validation."""
from copy import deepcopy
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file, write_canonical_json
from core.source_artifacts.package import build_publication_package
from jobs.monthly_refresh.acs_scope import ACSGovernance, SCOPE_VERSION, GAP_VERSION, identify
from jobs.monthly_refresh.acs_history import inventory, materialize, frame_identity
from jobs.monthly_refresh.acs_history_family import (build_family, validate_family, resolution_record,
    validate_record, validate_cohort, publish_family)
from sources.census_acs.artifact import PRODUCTS, governed_config_hashes

ROOT = Path(".")


def registry():
    return {"schema_version": "acs_historical_governance_registry_v1", "namespace": "test_only",
            "current_scope": {}, "current_state": {}, "scopes": {}, "states": {}, "gap_evidence": {}}


def scope(reg, source, start=2022, end=2024, gaps=None):
    gaps = gaps or []
    authority = identify(SCOPE_VERSION, {"schema_version": SCOPE_VERSION, "namespace": reg["namespace"],
        "source_id": source, "product": PRODUCTS[source], "historical_range": [start, end],
        "expected_available_vintages": [y for y in range(start, end + 1) if y not in {g["vintage"] for g in gaps}],
        "governed_gaps": gaps, "config_hashes": governed_config_hashes(ROOT),
        "approval_provenance": {"review": "test approval"}}, "authority_id")
    reg["scopes"][authority["authority_id"]] = authority
    reg["current_scope"][source] = authority["authority_id"]
    return authority


def record(path, manifest, tmp):
    package = build_publication_package(path, tmp / (manifest["artifact_id"] + ".tar"))
    return {"object_type": "source", "object_id": manifest["artifact_id"],
        "artifact_content_hash": manifest["artifact_content_hash"], "package_sha256": package["package_sha256"],
        "publication_state": "published_immutable_verified", "metadata": {"source_id": manifest["source_id"],
            "data_sha256": manifest["data_sha256"]}}


def parent(tmp, source, year, paths, records, *, value=None, observation=None, suffix=""):
    path = tmp / f"{source}-{year}{suffix}"
    frame = pd.DataFrame([{"geo_id": "alameda_county_ca__county", "metric_id": f"{source}_pop_total",
        "date": observation or date(year, 12, 31), "property_type_id": "all", "value": value or float(year),
        "source_id": source, "property_type": "all"}])
    manifest = create_artifact(path, frame, source_id=source, source_family=source,
        source_type="government_survey", provider="Census", distribution_channel="api",
        provider_release_id=f"{PRODUCTS[source]}:{year}", provider_release_timestamp_or_date=str(year),
        retrieved_at=None, target_month=f"{year}-12", source_request_identity=source + str(year) + suffix,
        source_urls_or_endpoint_identity=["fixture://acs"], artifact_created_at="fixed")
    rec = record(path, manifest, tmp)
    paths[rec["object_id"]] = path
    records[rec["object_id"]] = rec
    return {"source_id": source, "vintage": year, "artifact_id": rec["object_id"],
            "artifact_content_hash": rec["artifact_content_hash"], "package_sha256": rec["package_sha256"],
            "data_sha256": manifest["data_sha256"]}


def accept(reg, state):
    reg["states"][state["state_id"]] = state
    reg["current_state"][state["inventory"]["source_id"]] = state["state_id"]


def build(tmp, reg, inv, paths, records, *, mode="BOOTSTRAP", prior=None, name="state"):
    return materialize(mode=mode, value=inv, governance=(ACSGovernance.from_fixture(reg) if reg["namespace"] == "test_only" else main_governance(reg)), prior=prior,
                       paths=paths, records=records, output=tmp / name, artifact_created_at="2026-10-12T00:00:00Z")


@pytest.fixture
def physical(tmp_path):
    reg = registry(); paths = {}; records = {}; source = "census_acs1"
    authority = scope(reg, source)
    parents = [parent(tmp_path, source, y, paths, records) for y in (2022, 2023, 2024)]
    return reg, paths, records, authority, parents


def test_latest_only_cannot_claim_broad_scope(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents[-1:])
    with pytest.raises(ValueError, match="exactly match"):
        build(tmp_path, reg, inv, paths, records)
    assert len(authority["authority_id"].split("__")[-1]) == 64


@pytest.mark.parametrize("approved", [False, True])
def test_gap_evidence_requires_independent_registry_membership(tmp_path, physical, approved):
    reg, paths, records, _, parents = physical
    evidence = identify(GAP_VERSION, {"schema_version": GAP_VERSION, "namespace": "test_only",
        "source_id": "census_acs1", "vintage": 2023, "provider_evidence": "test unavailable",
        "approval_provenance": "test reviewer"}, "evidence_id")
    authority = scope(reg, "census_acs1", gaps=[{"vintage": 2023, "evidence_id": evidence["evidence_id"]}])
    inv = inventory(authority=authority, parents=[parents[0], parents[2]])
    if approved:
        reg["gap_evidence"][evidence["evidence_id"]] = evidence
        assert build(tmp_path, reg, inv, paths, records)["state"]["inventory"] == inv
    else:
        with pytest.raises(ValueError, match="identity mismatch"):
            build(tmp_path, reg, inv, paths, records)


def test_raw_production_dictionaries_and_fixture_forgery_rejected():
    reg = registry(); reg["namespace"] = "production"
    with pytest.raises(ValueError, match="test_only"):
        ACSGovernance.from_fixture(reg)
    with pytest.raises(ValueError, match="raw dictionaries"):
        ACSGovernance(reg, provenance="main:" + "a" * 40, token=None)
    with pytest.raises(ValueError, match="test_only"):
        ACSGovernance.from_fixture(registry()).require_production()


def test_incremental_complete_reconstruction_and_replay(tmp_path, physical):
    reg, paths, records, _, parents = physical
    prior = None
    for year in (2022, 2023, 2024):
        authority = scope(reg, "census_acs1", end=year)
        inv = inventory(authority=authority, parents=parents[:year - 2021])
        built = build(tmp_path, reg, inv, paths, records, mode="BOOTSTRAP" if prior is None else "UPDATE",
                      prior=prior, name=str(year))
        frame = pd.read_parquet(tmp_path / str(year) / "data.parquet")
        assert len(frame) == year - 2021
        assert built["manifest"]["prior_artifact_id"] is None
        accept(reg, built["state"]); prior = built["state"]
    replay = build(tmp_path, reg, inv, paths, records, mode="REPLAY", prior=prior, name="replay")
    assert replay["state"] == prior
    assert replay["manifest"] == built["manifest"]
    # Replay uses acquisition files alone; no prior state directory is supplied.
    assert frame_identity(pd.read_parquet(tmp_path / "replay/data.parquet")) == frame_identity(frame)


@pytest.mark.parametrize("prior_kind", ["missing", "stale", "wrong"])
def test_update_requires_exact_current_prior(tmp_path, physical, prior_kind):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    first = build(tmp_path, reg, inv, paths, records)
    accept(reg, first["state"])
    prior = None if prior_kind == "missing" else {**first["state"], "state_id": prior_kind}
    with pytest.raises(ValueError, match="exact current"):
        build(tmp_path, reg, inv, paths, records, mode="UPDATE", prior=prior, name="update")


def test_update_missing_governed_state_and_rebootstrap(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    with pytest.raises(ValueError, match="exact current"):
        build(tmp_path, reg, inv, paths, records, mode="UPDATE")
    first = build(tmp_path, reg, inv, paths, records)
    accept(reg, first["state"])
    with pytest.raises(ValueError, match="no existing"):
        build(tmp_path, reg, inv, paths, records, prior=None, name="again")


def test_superseded_scope(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    scope(reg, "census_acs1", end=2023)
    with pytest.raises(ValueError, match="outdated"):
        build(tmp_path, reg, inv, paths, records)


@pytest.mark.parametrize("end", [2023, 2025])
def test_replay_cannot_narrow_or_extend(tmp_path, physical, end):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    state = build(tmp_path, reg, inv, paths, records)["state"]; accept(reg, state)
    # Even a newly approved scope cannot change inventory under REPLAY.
    authority = scope(reg, "census_acs1", end=end)
    if end == 2025:
        parents.append(parent(tmp_path, "census_acs1", 2025, paths, records))
    inv = inventory(authority=authority, parents=[p for p in parents if p["vintage"] <= end])
    with pytest.raises(ValueError, match="identical governed inventory"):
        build(tmp_path, reg, inv, paths, records, mode="REPLAY", prior=state, name="replay")


def test_update_retention_and_surrogate_parent_rejected(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    state = build(tmp_path, reg, inv, paths, records)["state"]; accept(reg, state)
    replacement = parent(tmp_path, "census_acs1", 2023, paths, records, suffix="surrogate")
    inv = inventory(authority=authority, parents=[parents[0], replacement, parents[2]])
    with pytest.raises(ValueError, match="replacement violation"):
        build(tmp_path, reg, inv, paths, records, mode="UPDATE", prior=state, name="update")
    authority = scope(reg, "census_acs1", start=2023)
    with pytest.raises(ValueError, match="retention"):
        build(tmp_path, reg, inventory(authority=authority, parents=parents[1:]), paths, records,
              mode="UPDATE", prior=state, name="narrow")


@pytest.mark.parametrize("field,bad", [("artifact_status", "incomplete"), ("validation_status", "failed")])
def test_failed_incomplete_acquisition_rejected(tmp_path, physical, field, bad):
    reg, paths, records, authority, parents = physical
    path = paths[parents[0]["artifact_id"]]
    import json
    manifest = json.loads((path / "manifest.json").read_text()); manifest[field] = bad
    write_canonical_json(path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="complete and passed"):
        build(tmp_path, reg, inventory(authority=authority, parents=parents), paths, records)


def test_observation_vintage_mismatch(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    parents[0] = parent(tmp_path, "census_acs1", 2022, paths, records, observation=date(2024, 12, 31), suffix="bad")
    with pytest.raises(ValueError, match="observation/vintage"):
        build(tmp_path, reg, inventory(authority=authority, parents=parents), paths, records)


def make_family(tmp_path, namespace="test_only"):
    reg = registry(); reg["namespace"] = namespace
    paths = {}; records = {}; physical_parents = []
    for source in PRODUCTS:
        authority = scope(reg, source)
        acquisitions = [parent(tmp_path, source, y, paths, records, value=float(y) if source.endswith("1") else 5.)
                        for y in (2022, 2023, 2024)]
        inv = inventory(authority=authority, parents=acquisitions)
        built = build(tmp_path, reg, inv, paths, records, name=source)
        accept(reg, built["state"])
        rec = record(tmp_path / source, built["manifest"], tmp_path)
        paths[rec["object_id"]] = tmp_path / source; records[rec["object_id"]] = rec
        physical_parents.append({"source_id": source, "artifact_id": rec["object_id"],
            "artifact_content_hash": rec["artifact_content_hash"], "package_sha256": rec["package_sha256"],
            "data_sha256": rec["metadata"]["data_sha256"]})
    inputs = dict(parents=physical_parents, paths=paths, records=records, governance=(ACSGovernance.from_fixture(reg) if namespace == "test_only" else main_governance(reg)))
    manifest = build_family(output=tmp_path / "logical", **inputs)
    return inputs, manifest


@pytest.fixture
def family(tmp_path):
    return make_family(tmp_path)


def test_logical_complete_values_lineage_and_durable_record(tmp_path, family):
    inputs, manifest = family
    data = pd.read_parquet(tmp_path / "logical/data.parquet")
    lineage = pd.read_parquet(tmp_path / "logical/lineage.parquet")
    assert len(data) == 3 and list(data.value) == [2022., 2023., 2024.]
    assert lineage.winning_physical_source_id.eq("census_acs1").all()
    assert lineage.acquisition_parent_artifact_id.nunique() == 3
    assert lineage.winning_physical_historical_state_id.nunique() == 1
    rec = record(tmp_path / "logical", manifest, tmp_path)
    resolution = resolution_record(artifact=tmp_path / "logical", catalog_record=rec, **inputs)
    assert resolution["schema_version"] == "acs_family_resolution_record_v3"
    validate_record(resolution, artifact=tmp_path / "logical", catalog_record=rec, **inputs)
    bad = deepcopy(resolution); bad["context"]["diagnostics"]["logical_output_row_count"] = 1
    with pytest.raises(ValueError, match="disagrees"):
        validate_record(bad, artifact=tmp_path / "logical", catalog_record=rec, **inputs)


@pytest.mark.parametrize("corruption", ["truncated", "value", "lineage", "metadata"])
def test_independent_logical_validation_rejects_self_consistent_corruption(tmp_path, family, corruption):
    inputs, manifest = family
    path = tmp_path / "logical"
    data = pd.read_parquet(path / "data.parquet"); lineage = pd.read_parquet(path / "lineage.parquet")
    if corruption == "truncated":
        data = data.iloc[-1:]; lineage = lineage.iloc[-1:]
    elif corruption == "value":
        data.loc[0, "value"] = 12345.
    elif corruption == "lineage":
        lineage.loc[0, "acquisition_parent_artifact_id"] = "invented"
    else:
        manifest["acs_historical_family"]["diagnostics"]["logical_output_row_count"] = 1
    # Repair ordinary integrity checks so only independent reconstruction can
    # detect the lie. A checksum/manifest-only validator would accept this.
    data.to_parquet(path / "data.parquet", index=False); lineage.to_parquet(path / "lineage.parquet", index=False)
    manifest.update(data_sha256=sha256_file(path / "data.parquet"), lineage_sha256=sha256_file(path / "lineage.parquet"),
                    row_count=len(data), observation_min=str(min(data.date)), observation_max=str(max(data.date)))
    write_canonical_json(path / "manifest.json", manifest)
    with pytest.raises(ValueError, match="independent complete reconstruction"):
        validate_family(path, **inputs)
    rec = record(path, manifest, tmp_path)
    with pytest.raises(ValueError, match="independent complete reconstruction"):
        resolution_record(artifact=path, catalog_record=rec, **inputs)


def test_fixture_cannot_publish_or_prepare_production(tmp_path, family):
    inputs, manifest = family
    with pytest.raises(ValueError, match="test_only"):
        publish_family(artifact=tmp_path / "logical", api=None, cas=None, workspace=tmp_path, git_sha="test", **inputs)
    with pytest.raises(ValueError, match="test_only"):
        validate_cohort({}, catalog={}, resolver=None, governance=inputs["governance"])


def main_governance(reg):
    """Intercept HTTP below the constrained loader; no injectable factory API."""
    import base64
    import json
    from unittest.mock import patch
    class Response:
        headers = {}
        def __init__(self, value): self.value = value
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def getcode(self): return 200
        def read(self): return json.dumps(self.value).encode()
    class Opener:
        def __init__(self): self.registry = reg; self.tip = "a" * 40
        def open(self, request):
            url = request.full_url
            assert request.method == "GET"
            if url.endswith("/commits/main"):
                return Response({"sha": self.tip})
            if "/compare/" in url:
                return Response({"merge_base_commit": {"sha": "a" * 40}})
            assert url.endswith("?ref=" + self.tip)
            return Response({"content": base64.b64encode(json.dumps(self.registry).encode()).decode()})
    with patch("urllib.request.build_opener", return_value=Opener()):
        return ACSGovernance.load_production("fixture/repo", "test-token")


def test_production_latest_only_and_fabricated_gap(tmp_path):
    reg = registry(); reg["namespace"] = "production"
    authority = scope(reg, "census_acs1", start=2005)
    paths = {}; records = {}
    current = parent(tmp_path, "census_acs1", 2024, paths, records)
    from jobs.monthly_refresh.acs_history import verify_inventory
    with pytest.raises(ValueError, match="exactly match"):
        verify_inventory(inventory(authority=authority, parents=[current]), main_governance(reg), ROOT)
    evidence = identify(GAP_VERSION, {"schema_version": GAP_VERSION, "namespace": "production",
        "source_id": "census_acs1", "vintage": 2023, "provider_evidence": "caller claim",
        "approval_provenance": "caller invented approval"}, "evidence_id")
    authority = scope(reg, "census_acs1", gaps=[{"vintage": 2023, "evidence_id": evidence["evidence_id"]}])
    with pytest.raises(ValueError, match="identity mismatch"):
        verify_inventory(inventory(authority=authority, parents=[
            parent(tmp_path, "census_acs1", 2022, paths, records), current]), main_governance(reg), ROOT)


def test_float_semantics_do_not_round_distinct_values():
    frame = pd.DataFrame([{"geo_id": "x", "metric_id": "x", "date": date(2024, 12, 31),
                           "property_type_id": "all", "value": 1.0000000000000002}])
    other = frame.copy(); other["value"] = 1.
    assert frame_identity(frame) != frame_identity(other)


def test_duplicate_and_conflicting_physical_keys_fail_closed(tmp_path, physical):
    import json
    reg, paths, records, authority, parents = physical
    path = paths[parents[0]["artifact_id"]]
    data = pd.read_parquet(path / "data.parquet")
    conflicting = data.copy(); conflicting["value"] = 99.
    pd.concat([data, conflicting]).to_parquet(path / "data.parquet", index=False)
    manifest = json.loads((path / "manifest.json").read_text())
    manifest.update(data_sha256=sha256_file(path / "data.parquet"), row_count=2)
    write_canonical_json(path / "manifest.json", manifest)
    records[parents[0]["artifact_id"]]["metadata"]["data_sha256"] = manifest["data_sha256"]
    parents[0]["data_sha256"] = manifest["data_sha256"]
    from core.source_artifacts.validation import ArtifactValidationError
    with pytest.raises(ArtifactValidationError, match="duplicate canonical key"):
        build(tmp_path, reg, inventory(authority=authority, parents=parents), paths, records)


def test_steady_state_resolves_current_without_human_id(tmp_path, physical):
    reg, paths, records, authority, parents = physical
    inv = inventory(authority=authority, parents=parents)
    state = build(tmp_path, reg, inv, paths, records)["state"]; accept(reg, state)
    from jobs.monthly_refresh.acs_history import materialize_current
    result = materialize_current(mode="UPDATE", governance=ACSGovernance.from_fixture(reg), value=inv,
        paths=paths, records=records, output=tmp_path / "automatic")
    assert result["state"] == state
    assert result["reconciliation_prior_state_id"] == state["state_id"]


def test_current_state_is_independent_of_caller_mutations(physical):
    reg, _, _, _, _ = physical
    gov = ACSGovernance.from_fixture(reg)
    reg["namespace"] = "production"
    assert gov.namespace == "test_only"


def test_production_package_gates_and_record_store_reconstruct_before_io(tmp_path):
    inputs, manifest = make_family(tmp_path, namespace="production")
    path = tmp_path / "logical"
    rec = record(path, manifest, tmp_path)
    inputs["records"][rec["object_id"]] = rec
    inputs["paths"][rec["object_id"]] = path
    resolution = resolution_record(artifact=path, catalog_record=rec, **inputs)
    from core.source_artifacts.storage import LocalArtifactResolver
    resolver = LocalArtifactResolver({f"artifact://source/{r['metadata']['source_id']}/{aid}": inputs["paths"][aid]
                                     for aid, r in inputs["records"].items()})
    catalog = {"immutable_records": list(inputs["records"].values())}
    validate_cohort(resolution, catalog=catalog, resolver=resolver, governance=inputs["governance"])
    data = pd.read_parquet(path / "data.parquet"); data.loc[0, "value"] += 1
    data.to_parquet(path / "data.parquet", index=False)
    manifest["data_sha256"] = sha256_file(path / "data.parquet")
    write_canonical_json(path / "manifest.json", manifest)
    # Candidate and catalog remain self-consistent, but its logical value lies.
    rec.update(record(path, manifest, tmp_path))
    with pytest.raises(ValueError, match="independent complete reconstruction"):
        validate_cohort(resolution, catalog=catalog, resolver=resolver, governance=inputs["governance"])
    with pytest.raises(ValueError, match="independent complete reconstruction"):
        publish_family(artifact=path, api=None, cas=None, workspace=tmp_path, git_sha="test", **inputs)
    from jobs.monthly_refresh.acs_family_resolution import GitHubFamilyResolutionStore
    with pytest.raises(ValueError, match="independent complete reconstruction"):
        GitHubFamilyResolutionStore(None, "main").put(resolution, artifact=path,
            catalog_record=rec, validation_inputs=inputs)


def test_manifest_only_record_construction_cannot_authorize_publication():
    from jobs.monthly_refresh.acs_family_resolution import resolution_record as legacy_record
    with pytest.raises(ValueError, match="manifest-only"):
        legacy_record(manifest={}, catalog_record={})
