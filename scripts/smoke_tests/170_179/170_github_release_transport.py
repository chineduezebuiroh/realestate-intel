"""Offline GitHub Release transport, resolver, identity, and CAS smoke."""
from __future__ import annotations
import copy, io, json, tempfile, urllib.error, urllib.request
from email.message import Message
from pathlib import Path

import pandas as pd

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.catalog import add_record, empty_catalog
from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS, GitHubReleaseArtifactPublisher,
    GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.validation import ArtifactValidationError


def expect(error, operation):
    try: operation()
    except error: return
    raise AssertionError(f"expected {error.__name__}")


class FakeAPI:
    repository = "fixture/repo"
    def __init__(self):
        self.release = None; self.asset_bytes = {}; self.next_asset = 22; self.put_conflict = False
        self.tag_calls = 0; self.numeric_calls = 0; self.hide_created_from_tag = False
        self.numeric_missing = False; self.numeric_tag = None
        self.catalog = empty_catalog(); self.catalog_oid = "a" * 40
    def request(self, method, url, *, payload=None, content_type="application/json", expected=(200,)):
        if url.startswith("/releases/tags/"):
            self.tag_calls += 1
            return (copy.deepcopy(self.release), {}) if self.release and not self.hide_created_from_tag else (None, {})
        if method == "POST" and url == "/releases":
            self.release = {"id": 11, "tag_name": payload["tag_name"], "draft": True, "prerelease": True,
                "assets": [], "upload_url": "https://uploads.github.invalid/releases/11/assets{?name,label}",
                "published_at": None}; return copy.deepcopy(self.release), {}
        if method == "POST" and url.startswith("https://uploads.github.invalid"):
            name = url.split("name=")[1]; asset = {"id": self.next_asset, "name": name, "state": "uploaded"}
            self.asset_bytes[self.next_asset] = payload; self.release["assets"].append(asset); return copy.deepcopy(asset), {}
        if method == "PATCH" and url == "/releases/11":
            self.release["draft"] = False; self.release["published_at"] = "2026-08-31T01:00:00Z"
            return copy.deepcopy(self.release), {}
        if method == "GET" and url == "/releases/11":
            self.numeric_calls += 1
            if self.numeric_missing: return None, {}
            release = copy.deepcopy(self.release)
            if self.numeric_tag is not None: release["tag_name"] = self.numeric_tag
            return release, {}
        if method == "GET" and url.startswith("/contents/"):
            import base64
            return {"sha": self.catalog_oid, "content": base64.b64encode(json.dumps(self.catalog).encode()).decode()}, {}
        if method == "PUT" and url.startswith("/contents/"):
            if self.put_conflict: raise PublicationError("HTTP 409")
            import base64
            assert payload["sha"] == self.catalog_oid
            self.catalog = json.loads(base64.b64decode(payload["content"])); return {}, {}
        raise AssertionError((method, url))
    def download_asset(self, asset_id, destination): destination.write_bytes(self.asset_bytes[asset_id])


def fixture(root):
    data = pd.DataFrame([{"geo_id":"US","metric_id":"m","date":"2026-08-31","property_type_id":"all",
        "value":1.0,"source_id":"fixture_source","property_type":"all"}])
    path=root/"artifact"; manifest=create_artifact(path,data,source_id="fixture_source",source_family="Fixture",
        source_type="fixture",provider="Fixture",distribution_channel="generated",provider_release_id="fixed",
        provider_release_timestamp_or_date="2026-08-31",retrieved_at="2026-08-31T00:00:00Z",target_month="2026-08",
        source_request_identity="fixed",source_urls_or_endpoint_identity=["fixture://fixed"],artifact_created_at="2026-08-31T00:00:00Z")
    package=root/"package.tar"; info=build_publication_package(path,package)
    metadata={"logical_artifact_uri":manifest["artifact_uri"],"object_id":manifest["artifact_id"],"object_type":"source",
        "object_metadata":{"source_id":"fixture_source"},"artifact_content_hash":manifest["artifact_content_hash"],
        "member_hashes":{m["path"]:m["sha256"] for m in info["members"]},"publisher_git_sha":"abc",
        "contract_versions":[manifest["artifact_contract_version"],info["package_contract_version"]]}
    return manifest,package,metadata


def record(manifest, receipt):
    return {"object_type":"source","object_id":manifest["artifact_id"],"logical_artifact_uri":manifest["artifact_uri"],
        "remote_repository":receipt["remote_repository"],"release_tag":receipt["release_tag"],"release_id":receipt["release_id"],
        "asset_id":receipt["asset_id"],"asset_filename":receipt["asset_filename"],"package_sha256":receipt["package_sha256"],
        "artifact_content_hash":manifest["artifact_content_hash"],"publication_receipt_id":receipt["receipt_id"],
        "publication_state":receipt["publication_state"],"metadata":{"source_id":"fixture_source",
        "data_sha256":manifest["data_sha256"],"provider_release_id":"fixed","observation_max":"2026-08-31"}}


with tempfile.TemporaryDirectory() as td:
    root=Path(td); manifest,package,metadata=fixture(root); api=FakeAPI(); uri=manifest["artifact_uri"]
    valid_package_bytes=package.read_bytes()
    pub=GitHubReleaseArtifactPublisher(api,fixture=True); assert pub.inspect(uri).state=="absent"
    api.hide_created_from_tag=True
    pub.prepare(uri,package.read_bytes(),metadata); assert api.release["draft"]
    assert api.release["tag_name"].startswith("source-artifact-fixture/fixture_source/")
    # Hosted regression: tag discovery remains unavailable immediately after
    # POST creation, but every later phase uses the POST's numeric Release ID.
    assert api.tag_calls == 1
    pub.upload(uri); pub.verify(uri); receipt=pub.finalize(uri)
    assert api.tag_calls == 1 and api.numeric_calls >= 4
    assert (receipt["release_id"],receipt["asset_id"])==(11,22) and not api.release["draft"]
    # Identical finalized remote identity is recoverable/idempotent.
    api.hide_created_from_tag=False
    again=GitHubReleaseArtifactPublisher(api,fixture=True); again.prepare(uri,package.read_bytes(),metadata)
    discovered_calls=api.tag_calls; again.upload(uri); again.verify(uri)
    assert again.finalize(uri)["asset_id"]==22 and len(api.release["assets"])==1
    assert api.tag_calls == discovered_calls
    # A known numeric identity disappearing or resolving to another tag fails
    # closed; neither path performs tag rediscovery or creates a replacement.
    missing=GitHubReleaseArtifactPublisher(api,fixture=True); missing.prepare(uri,package.read_bytes(),metadata)
    api.numeric_missing=True; expect(PublicationError,lambda:missing.upload(uri)); api.numeric_missing=False
    mismatch=GitHubReleaseArtifactPublisher(api,fixture=True); mismatch.prepare(uri,package.read_bytes(),metadata)
    api.numeric_tag="wrong"; expect(IdentityCollisionError,lambda:mismatch.upload(uri)); api.numeric_tag=None
    # Duplicate names are still a hard collision before byte comparison.
    duplicate=copy.deepcopy(api.release["assets"][0]); duplicate["id"]=23
    api.release["assets"].append(duplicate)
    dup=GitHubReleaseArtifactPublisher(api,fixture=True); dup.prepare(uri,package.read_bytes(),metadata)
    expect(IdentityCollisionError,lambda:dup.upload(uri)); api.release["assets"].pop()
    conflict=GitHubReleaseArtifactPublisher(api,fixture=True); conflict.prepare(uri,package.read_bytes()+b"x",metadata)
    expect(IdentityCollisionError,lambda:conflict.upload(uri))
    badtag=copy.deepcopy(api.release); api.release["tag_name"]="wrong"; expect(IdentityCollisionError,lambda:GitHubReleaseArtifactPublisher(api,fixture=True).prepare(uri,package.read_bytes(),metadata)); api.release=badtag
    rec=record(manifest,receipt); catalog=add_record(empty_catalog(),rec,receipt)
    resolved=GitHubReleaseArtifactResolver(catalog,api,root/"cache").resolve(uri); assert (resolved/"manifest.json").is_file()
    expect(FileNotFoundError,lambda:GitHubReleaseArtifactResolver(empty_catalog(),api,root/"orphan").resolve(uri))
    wrong=copy.deepcopy(catalog); wrong["immutable_records"][0]["release_id"]=99
    expect(AssertionError,lambda:GitHubReleaseArtifactResolver(wrong,api,root/"wrong").resolve(uri))
    original=api.asset_bytes[22]; api.asset_bytes[22]=original+b"tamper"
    expect(ArtifactValidationError,lambda:GitHubReleaseArtifactResolver(catalog,api,root/"tampered").resolve(uri)); api.asset_bytes[22]=original
    updater=GitHubCatalogCAS(api,"artifacts/fixture_registry/catalog.json","monthly-refresh-orchestration")
    api.catalog=empty_catalog(); updated,changed=updater.add(rec,receipt); assert changed and updated["compare_and_swap"]["expected_git_blob_sha"]=="a"*40
    same,changed=updater.add(rec,receipt); assert not changed
    api.catalog=empty_catalog(); api.put_conflict=True; expect(PublicationError,lambda:updater.add(rec,receipt))

# Explicit API failures are surfaced rather than interpreted as absence/retry.
class FailedAPI(FakeAPI):
    def request(self,*args,**kwargs): raise PublicationError("HTTP 401/rate limit")
expect(PublicationError,lambda:GitHubReleaseArtifactPublisher(FailedAPI(),fixture=True).prepare(uri,valid_package_bytes,metadata))

# The production HTTP boundary sends API auth, fails closed on rate/auth errors,
# and strips all credentials when following a signed asset redirect.
class Response(io.BytesIO):
    def __init__(self, body=b"{}", status=200): super().__init__(body); self.status=status; self.headers=Message()
    def getcode(self): return self.status
    def __enter__(self): return self
    def __exit__(self,*args): self.close()
class CaptureOpener:
    def __init__(self): self.request=None
    def open(self,request): self.request=request; return Response()
capture=CaptureOpener(); client=GitHubAPI("fixture/repo","secret-token",opener=capture); client.request("GET","/releases/1")
assert capture.request.get_header("Authorization")=="Bearer secret-token" and capture.request.get_header("X-github-api-version")=="2022-11-28"
class RedirectOpener:
    def open(self,request):
        headers=Message(); headers["Location"]="https://signed.invalid/object"
        raise urllib.error.HTTPError(request.full_url,302,"Found",headers,io.BytesIO())
signed_request=[]; real_urlopen=urllib.request.urlopen
def signed_open(request): signed_request.append(request); return Response(b"remote")
urllib.request.urlopen=signed_open
try:
    with tempfile.TemporaryDirectory() as td: GitHubAPI("fixture/repo","secret-token",opener=RedirectOpener()).download_asset(22,Path(td)/"asset")
finally: urllib.request.urlopen=real_urlopen
assert signed_request[0].get_header("Authorization") is None
class RateOpener:
    def open(self,request): raise urllib.error.HTTPError(request.full_url,403,"rate limit",Message(),io.BytesIO(b'{"message":"rate limit"}'))
expect(PublicationError,lambda:GitHubAPI("fixture/repo","secret-token",opener=RateOpener()).request("GET","/releases/1"))

print("Smoke 170 GitHub Release transport passed")
