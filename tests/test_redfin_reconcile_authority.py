import base64
import hashlib
import json
from copy import deepcopy
from pathlib import Path
from urllib.parse import unquote

import pytest

from core.source_artifacts.hashing import canonical_json_bytes
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh import redfin_reconcile_authority as subject


class FakeGitHub:
    repository = "chineduezebuiroh/realestate-intel"

    def __init__(self, objects):
        self.objects = objects
        self.puts = []

    def request(self, method, url, *, payload=None, expected=(200,), **kwargs):
        if url == f"/releases/{subject.RELEASE_ID}":
            return {"id":subject.RELEASE_ID, "draft":False,
                "tag_name":f"source-artifact/redfin/{subject.ARTIFACT_ID}",
                "assets":[{"id":subject.ASSET_ID, "name":f"{subject.ARTIFACT_ID}.tar",
                           "digest":f"sha256:{subject.PACKAGE_SHA256}"}]}, {}
        path = unquote(url.split("/contents/", 1)[1].split("?", 1)[0])
        if method == "GET":
            branch = unquote(url.split("?ref=", 1)[1]); value = self.objects.get((branch, path))
            if value is None: return None, {}
            content = canonical_json_bytes(value)
            return {"content":base64.b64encode(content).decode(),
                    "sha":hashlib.sha1(content).hexdigest()}, {}
        branch = payload["branch"]
        self.objects[(branch, path)] = json.loads(base64.b64decode(payload["content"]))
        self.puts.append((branch, path))
        return {}, {}

    def download_asset(self, asset_id, destination):
        assert asset_id == subject.ASSET_ID
        destination.write_bytes(b"verified by fake GitHub boundary")


@pytest.fixture
def states(monkeypatch):
    catalog = json.loads(Path("config/artifact_catalog.json").read_text())
    readiness = json.loads(Path("config/monthly_refresh_readiness.json").read_text())
    august = next(r for r in catalog["immutable_records"] if r["object_id"] == subject.ARTIFACT_ID)
    source_catalog = deepcopy(catalog)
    main_catalog = deepcopy(catalog)
    main_catalog["immutable_records"] = [r for r in main_catalog["immutable_records"]
                                         if r["object_id"] != subject.ARTIFACT_ID]
    main_catalog["accepted"]["source"]["redfin"] = subject.JULY_ARTIFACT_ID
    source_readiness = deepcopy(readiness)
    main_readiness = deepcopy(readiness)
    main_readiness["records"] = [r for r in main_readiness["records"]
                                 if r["cycle_id"] != subject.CYCLE_ID]
    objects = {(subject.SOURCE_BRANCH, subject.CATALOG_PATH):source_catalog,
        (subject.TARGET_BRANCH, subject.CATALOG_PATH):main_catalog,
        (subject.SOURCE_BRANCH, subject.READINESS_PATH):source_readiness,
        (subject.TARGET_BRANCH, subject.READINESS_PATH):main_readiness}
    api = FakeGitHub(objects)
    real_sha = subject.sha256_file
    monkeypatch.setattr(subject, "sha256_file",
        lambda path: subject.PACKAGE_SHA256 if Path(path).name == "asset.tar" else real_sha(path))
    return api, objects, august


def test_preflight_has_zero_mutation(states):
    api, objects, _ = states; before = deepcopy(objects)
    report = subject.run(api=api, live=False)
    assert report["mode"] == "preflight" and report["catalog_action"] == "add"
    assert objects == before and api.puts == []


def test_live_add_preserves_complete_accepted_mapping(states):
    api, objects, _ = states
    accepted = deepcopy(objects[(subject.TARGET_BRANCH, subject.CATALOG_PATH)]["accepted"])
    report = subject.run(api=api, live=True)
    catalog = objects[(subject.TARGET_BRANCH, subject.CATALOG_PATH)]
    readiness = objects[(subject.TARGET_BRANCH, subject.READINESS_PATH)]
    assert report["mutation_performed"] is True
    assert catalog["accepted"] == accepted
    assert catalog["accepted"]["source"]["redfin"] == subject.JULY_ARTIFACT_ID
    august = next(r for r in readiness["records"] if r["cycle_id"] == subject.CYCLE_ID)
    assert august["consumed"] is False and august["candidate_artifact_id"] == subject.ARTIFACT_ID
    assert any(r["candidate_artifact_id"] == subject.JULY_ARTIFACT_ID for r in readiness["records"])


def test_live_exact_rerun_is_noop(states):
    api, _, _ = states
    subject.run(api=api, live=True); api.puts.clear()
    report = subject.run(api=api, live=True)
    assert report["mutation_performed"] is False and api.puts == []


def test_conflicting_catalog_record_fails_closed(states):
    api, objects, august = states
    bad = deepcopy(august); bad["package_sha256"] = "0" * 64
    objects[(subject.TARGET_BRANCH, subject.CATALOG_PATH)]["immutable_records"].append(bad)
    with pytest.raises(IdentityCollisionError): subject.run(api=api, live=True)
    assert api.puts == []


def test_conflicting_readiness_fails_closed(states):
    api, objects, _ = states
    conflict = next(r for r in objects[(subject.SOURCE_BRANCH, subject.READINESS_PATH)]["records"]
                    if r["cycle_id"] == subject.CYCLE_ID).copy()
    conflict["consumed"] = True
    objects[(subject.TARGET_BRANCH, subject.READINESS_PATH)]["records"].append(conflict)
    objects[(subject.TARGET_BRANCH, subject.READINESS_PATH)]["records"].sort(key=lambda r:r["readiness_id"])
    with pytest.raises(ValueError, match="conflicting Redfin readiness identity"):
        subject.run(api=api, live=True)
    assert api.puts == []
