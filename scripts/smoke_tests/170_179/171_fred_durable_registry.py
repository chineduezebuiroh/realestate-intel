"""Offline Phase 2B FRED durable-registry policy smoke."""
from __future__ import annotations
import copy

from core.source_artifacts.catalog import (activate_source, add_record, empty_catalog,
    validate_catalog_namespace)
from core.source_artifacts.publication import PublicationError
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

print("Smoke 171 FRED durable registry passed")
