"""Offline Phase 2B FRED durable-registry policy smoke."""
from __future__ import annotations
import copy
import tempfile
from pathlib import Path

import pandas as pd

from core.source_artifacts.catalog import (activate_source, add_record, empty_catalog,
    validate_catalog_namespace)
from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.hashing import sha256_file
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import PublicationError, create_receipt
from jobs.monthly_refresh import fred_durable
from jobs.monthly_refresh.fred_durable import SOURCE_ID, accepted_uri, catalog_record


def expect(error, operation):
    try: operation()
    except error: return
    raise AssertionError(f"expected {error.__name__}")


artifact_id = "src__fred_macro__2026-08__r1__8b6f73c37161a06d"
uri = f"artifact://source/fred_macro/{artifact_id}"
manifest = {"artifact_id": artifact_id, "artifact_uri": uri,
    "artifact_content_hash": "8b6f73c37161a06dc91a8950b96b85ea588f220f1e091fefc89575d2529b2f94",
    "data_sha256": "ea0e4eff16ff3aac891e1e187cea3d4dd7dc71d2a3c108d1679d652f2a57336d",
    "provider_release_id": "ordinary-current:identity", "observation_max": "2026-08-31"}
receipt = {"remote_repository": "owner/repo", "release_tag": f"source-artifact/{SOURCE_ID}/{artifact_id}",
    "release_id": 375862480, "asset_id": 527917042, "asset_filename": f"{artifact_id}.tar",
    "package_sha256": "1" * 64, "receipt_id": "publication_receipt__fred",
    "publication_state": "published_immutable_verified"}
record = catalog_record(manifest, receipt)
assert receipt["release_tag"] == f"source-artifact/fred_macro/{artifact_id}"
assert receipt["asset_filename"] == f"{artifact_id}.tar"

# Bootstrap has no misleading pointer. Publication/cataloging is separate from acceptance.
catalog = empty_catalog(); assert accepted_uri(catalog) is None
# add_record validates genuine receipts, so use the already schema-checked record to exercise
# namespace and pointer policy without inventing a fake receipt identity.
catalog["immutable_records"] = [record]
validate_catalog_namespace(catalog, fixture=False)
assert accepted_uri(catalog) is None
active = activate_source(catalog, SOURCE_ID, artifact_id)
assert accepted_uri(active) == uri and catalog["accepted"]["source"] == {}

# A fresh verification attempt may have a different receipt identity without
# changing the governed FRED object or its first-commit receipt provenance.
def valid_receipt(publisher_git_sha):
    return create_receipt(logical_artifact_uri=uri, object_id=artifact_id, object_type="source",
        object_metadata={"source_id":SOURCE_ID}, artifact_content_hash=manifest["artifact_content_hash"],
        package_sha256="1"*64, member_hashes={"data.parquet":manifest["data_sha256"]},
        remote_backend="github_release", remote_repository="owner/repo", release_tag=receipt["release_tag"],
        release_id=receipt["release_id"], asset_id=receipt["asset_id"], asset_filename=receipt["asset_filename"],
        published_at="2026-08-24T00:00:00Z", verified_at="2026-08-24T00:01:00Z",
        publication_state="published_immutable_verified", publisher_git_sha=publisher_git_sha,
        contract_versions=["source_artifact_contract_v1"])

receipt_a=valid_receipt("commit-a"); record_a=catalog_record(manifest,receipt_a)
catalog_a=add_record(empty_catalog(),record_a,receipt_a)
receipt_b=valid_receipt("commit-b"); record_b=catalog_record(manifest,receipt_b)
assert add_record(catalog_a,record_b,receipt_b)==catalog_a
assert catalog_a["immutable_records"][0]["publication_receipt_id"]==receipt_a["receipt_id"]

# Old records remain when a refreshed identity is appended; accepted movement is explicit.
new = copy.deepcopy(record); new["object_id"] = "src__fred_macro__2026-09__r1__" + "2" * 16
new["logical_artifact_uri"] = "artifact://source/fred_macro/" + new["object_id"]
new["release_tag"] = "source-artifact/fred_macro/" + new["object_id"]
new["asset_filename"] = new["object_id"] + ".tar"; new["release_id"] += 1; new["asset_id"] += 1
new["package_sha256"] = "2" * 64; new["artifact_content_hash"] = "3" * 64
new["publication_receipt_id"] = "publication_receipt__new"
both = copy.deepcopy(active); both["immutable_records"].append(new)
assert len(validate_catalog_namespace(both, fixture=False)["immutable_records"]) == 2
assert both["accepted"]["source"][SOURCE_ID] == artifact_id

# Catalog namespaces are mutually exclusive and corruption is never interpreted as bootstrap.
fixture = copy.deepcopy(catalog); fixture["immutable_records"][0]["release_tag"] = fixture["immutable_records"][0]["release_tag"].replace("source-artifact/", "source-artifact-fixture/")
expect(PublicationError, lambda: validate_catalog_namespace(fixture, fixture=False))
expect(PublicationError, lambda: validate_catalog_namespace(catalog, fixture=True))
corrupt = copy.deepcopy(active); corrupt["immutable_records"] = []
expect(RuntimeError, lambda: accepted_uri(corrupt))

# Legacy v1 manifests prove the hosted defect deterministically: execution
# provenance changes manifest/package bytes without changing semantic identity.
# Durable publication must resolve the catalog commit point before upload.
with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    data = pd.DataFrame([{"geo_id":"united_states__nation","metric_id":"fred_gs10",
        "date":"2026-08-31","property_type_id":"all","value":4.0,
        "source_id":SOURCE_ID,"property_type":"all"}])
    common = dict(source_id=SOURCE_ID, source_family="FRED", source_type="revisionary_current_truth",
        provider="Federal Reserve Bank of St. Louis", distribution_channel="FRED API",
        provider_release_id="ordinary-current:fixed", provider_release_timestamp_or_date=None,
        target_month="2026-08", source_request_identity="fixed",
        source_urls_or_endpoint_identity=["api.stlouisfed.org/fred/series/observations"])
    first = create_artifact(root/"first", data, retrieved_at="2026-08-27T16:00:00Z",
        artifact_created_at="2026-08-27T16:00:01Z", git_sha="commit-a", **common)
    second = create_artifact(root/"second", data, retrieved_at="2026-08-27T17:00:00Z",
        artifact_created_at="2026-08-27T17:00:01Z", git_sha="commit-b", **common)
    first_package, second_package = root/"first.tar", root/"second.tar"
    first_info = build_publication_package(root/"first", first_package)
    second_info = build_publication_package(root/"second", second_package)
    assert first["artifact_id"] == second["artifact_id"]
    assert first["artifact_content_hash"] == second["artifact_content_hash"]
    assert first["data_sha256"] == second["data_sha256"]
    assert first["validation_sha256"] == second["validation_sha256"]
    assert sha256_file(root/"first/manifest.json") != sha256_file(root/"second/manifest.json")
    assert first_info["package_sha256"] != second_info["package_sha256"]
    differences = {key for key in first if first.get(key) != second.get(key)}
    assert differences == {"retrieved_at", "artifact_created_at", "git_sha"}

    published_record = {"object_type":"source", "object_id":first["artifact_id"],
        "logical_artifact_uri":first["artifact_uri"], "remote_repository":"owner/repo",
        "release_tag":"source-artifact/fred_macro/"+first["artifact_id"], "release_id":1,
        "asset_id":2, "asset_filename":first["artifact_id"]+".tar",
        "package_sha256":first_info["package_sha256"],
        "artifact_content_hash":first["artifact_content_hash"], "publication_receipt_id":"receipt",
        "publication_state":"published_immutable_verified", "metadata":{"source_id":SOURCE_ID,
        "data_sha256":first["data_sha256"], "provider_release_id":first["provider_release_id"],
        "observation_max":first["observation_max"]}}
    durable_catalog = empty_catalog(); durable_catalog["immutable_records"] = [published_record]
    class CAS:
        def read(self): return durable_catalog, "oid"
    class Resolver:
        def __init__(self, catalog, api, workspace): pass
        def resolve(self, uri): return root/"first"
    original_resolver = fred_durable.GitHubReleaseArtifactResolver
    fred_durable.GitHubReleaseArtifactResolver = Resolver
    try:
        resolved, reused = fred_durable.resolve_published_candidate(
            artifact=root/"second", api=object(), cas=CAS(), workspace=root/"reuse")
    finally:
        fred_durable.GitHubReleaseArtifactResolver = original_resolver
    assert reused and resolved == root/"first"
    rebuilt = root/"rebuilt.tar"; rebuilt_info = build_publication_package(resolved, rebuilt)
    assert rebuilt_info["package_sha256"] == first_info["package_sha256"]

print("Smoke 171 FRED durable registry passed")
