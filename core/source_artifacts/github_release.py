"""GitHub Releases transport for governed artifact packages.

The adapter uses exact tags and numeric IDs.  It deliberately does not update
the catalog: remote publication and governance activation are separate steps.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .catalog import add_record, empty_catalog, validate_catalog
from .hashing import canonical_json_bytes, sha256_file
from .package import extract_publication_package
from .publication import (ArtifactPublisher, IdentityCollisionError, PublicationError,
                          RemoteInspection, create_receipt, transition)
from .storage import ArtifactResolver
from .validation import validate_artifact

API_VERSION = "2022-11-28"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class GitHubAPI:
    """Small injectable REST boundary; errors fail closed without token logging."""
    def __init__(self, repository: str, token: str, *, opener: Any = None):
        if repository.count("/") != 1 or not token:
            raise PublicationError("GitHub repository identity and token are required")
        self.repository, self.token = repository, token
        self.api = f"https://api.github.com/repos/{repository}"
        self.opener = opener or urllib.request.build_opener(_NoRedirect())

    def request(self, method: str, url: str, *, payload: Any = None,
                content_type: str = "application/json", expected: tuple[int, ...] = (200,)) -> tuple[Any, dict[str, str]]:
        data = None
        if payload is not None:
            data = canonical_json_bytes(payload) if content_type == "application/json" else payload
        request = urllib.request.Request(url if url.startswith("http") else self.api + url,
            data=data, method=method, headers={"Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": API_VERSION,
            "Content-Type": content_type})
        try:
            response = self.opener.open(request)
        except urllib.error.HTTPError as exc:
            body = exc.read(2048).decode(errors="replace"); exc.close()
            if exc.code in expected:
                if exc.code == 404:
                    return None, dict(exc.headers)
                return (json.loads(body) if body else None), dict(exc.headers)
            raise PublicationError(f"GitHub API {method} failed with HTTP {exc.code}: {body[:300]}") from exc
        with response:
            status = response.getcode(); body = response.read()
            if status not in expected:
                raise PublicationError(f"GitHub API {method} returned unexpected HTTP {status}")
            return (json.loads(body) if body else None), dict(response.headers)

    def download_asset(self, asset_id: int, destination: Path) -> None:
        """Follow GitHub's signed redirect without forwarding bearer credentials."""
        url = f"{self.api}/releases/assets/{asset_id}"
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {self.token}",
            "Accept": "application/octet-stream", "X-GitHub-Api-Version": API_VERSION})
        try:
            response = self.opener.open(request)
        except urllib.error.HTTPError as exc:
            if exc.code != 302:
                exc.close(); raise PublicationError(f"GitHub asset download failed with HTTP {exc.code}") from exc
            location = exc.headers.get("Location"); exc.close()
        else:
            status = response.getcode()
            if status == 200:
                with response, destination.open("wb") as output: shutil.copyfileobj(response, output)
                return
            response.close(); raise PublicationError(f"GitHub asset download returned HTTP {status}")
        if not location:
            raise PublicationError("GitHub asset redirect omitted Location")
        try:
            # A fresh stdlib opener is intentional: no API authorization or
            # headers cross the signed-download trust boundary.
            with urllib.request.urlopen(urllib.request.Request(location)) as source, destination.open("wb") as output:
                shutil.copyfileobj(source, output)
        except Exception:
            destination.unlink(missing_ok=True); raise


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


class GitHubReleaseArtifactPublisher(ArtifactPublisher):
    def __init__(self, api: GitHubAPI, *, fixture: bool = False):
        self.api, self.fixture, self.pending = api, fixture, {}

    def _identity(self, metadata: dict[str, Any]) -> tuple[str, str]:
        source, artifact = metadata["object_metadata"]["source_id"], metadata["object_id"]
        prefix = "source-artifact-fixture" if self.fixture else "source-artifact"
        return f"{prefix}/{source}/{artifact}", f"{artifact}.tar"

    def _release(self, tag: str) -> dict[str, Any] | None:
        encoded = urllib.parse.quote(tag, safe="")
        result, _ = self.api.request("GET", f"/releases/tags/{encoded}", expected=(200, 404))
        return result

    @staticmethod
    def _validate_release(release: Any, expected_tag: str) -> dict[str, Any]:
        if (not isinstance(release, dict) or type(release.get("id")) is not int
                or release["id"] <= 0 or not isinstance(release.get("assets"), list)
                or not isinstance(release.get("upload_url"), str)):
            raise PublicationError("GitHub returned invalid numeric Release identity")
        if release.get("tag_name") != expected_tag:
            raise IdentityCollisionError("GitHub Release tag conflict")
        return release

    def _release_by_id(self, release_id: int, expected_tag: str) -> dict[str, Any] | None:
        if type(release_id) is not int or release_id <= 0:
            raise PublicationError("GitHub numeric Release identity is invalid")
        release, _ = self.api.request("GET", f"/releases/{release_id}", expected=(200, 404))
        if release is None:
            return None
        release = self._validate_release(release, expected_tag)
        if release["id"] != release_id:
            raise PublicationError("GitHub numeric Release lookup returned a different identity")
        return release

    def _pending_release(self, item: dict[str, Any]) -> dict[str, Any]:
        release = self._release_by_id(item["release_id"], item["tag"])
        if release is None:
            raise PublicationError("known GitHub Release numeric identity is missing")
        return release

    @staticmethod
    def _asset(release: dict[str, Any], name: str) -> dict[str, Any] | None:
        if not isinstance(release, dict):
            raise PublicationError("asset lookup requires a validated GitHub Release")
        matches = [a for a in release.get("assets", []) if a.get("name") == name]
        if len(matches) > 1: raise IdentityCollisionError("duplicate GitHub Release asset name")
        return matches[0] if matches else None

    def inspect(self, logical_uri: str) -> RemoteInspection:
        pending = self.pending.get(logical_uri)
        if not pending: return RemoteInspection("absent")
        release = self._pending_release(pending)
        asset = self._asset(release, pending["asset_name"])
        state = "published_immutable_verified" if not release.get("draft") and asset else ("uploaded" if asset else "prepared")
        return RemoteInspection(state, pending["sha"] if asset else None)

    def prepare(self, logical_uri: str, package: bytes, metadata: dict[str, Any]) -> None:
        if logical_uri != metadata.get("logical_artifact_uri"):
            raise PublicationError("logical URI/metadata mismatch")
        tag, name = self._identity(metadata); digest = hashlib.sha256(package).hexdigest()
        # Validate bytes before creating any remote object.  A caller cannot
        # turn an arbitrary tar into a draft merely by supplying metadata.
        with tempfile.TemporaryDirectory() as td:
            local_package = Path(td) / "local.tar"; local_package.write_bytes(package)
            extracted = extract_publication_package(local_package, Path(td) / "artifact", expected_sha256=digest)
            manifest = validate_artifact(extracted)["manifest"]
            if (manifest["artifact_id"] != metadata["object_id"] or manifest["artifact_uri"] != logical_uri
                    or manifest["artifact_content_hash"] != metadata["artifact_content_hash"]):
                raise PublicationError("local package semantic identity mismatch")
            if {n: sha256_file(extracted / n) for n in metadata["member_hashes"]} != metadata["member_hashes"]:
                raise PublicationError("local package member hash mismatch")
        old = self.pending.get(logical_uri)
        if old and old["sha"] != digest: raise IdentityCollisionError("same logical identity has different package bytes")
        release = self._release(tag)
        if release is None:
            release, _ = self.api.request("POST", "/releases", payload={"tag_name": tag,
                "name": f"Governed fixture artifact {metadata['object_id']}" if self.fixture else f"Governed artifact {metadata['object_id']}",
                "draft": True, "prerelease": self.fixture}, expected=(201,))
        release = self._validate_release(release, tag)
        unexpected = [a["name"] for a in release.get("assets", []) if a.get("name") != name]
        if unexpected: raise IdentityCollisionError(f"unexpected assets on exact Release: {unexpected}")
        self.pending[logical_uri] = {"state": "prepared", "bytes": package, "sha": digest,
            "metadata": deepcopy(metadata), "tag": tag, "asset_name": name, "release_id": release["id"]}

    def upload(self, logical_uri: str) -> None:
        item = self.pending[logical_uri]; release = self._pending_release(item)
        asset = self._asset(release, item["asset_name"])
        if asset:
            with tempfile.TemporaryDirectory() as td:
                downloaded = Path(td) / "remote.tar"; self.api.download_asset(int(asset["id"]), downloaded)
                if sha256_file(downloaded) != item["sha"]: raise IdentityCollisionError("existing asset bytes conflict")
        else:
            upload = release["upload_url"].split("{")[0] + "?" + urllib.parse.urlencode({"name": item["asset_name"]})
            asset, _ = self.api.request("POST", upload, payload=item["bytes"], content_type="application/x-tar", expected=(201,))
        if asset.get("name") != item["asset_name"] or not isinstance(asset.get("id"), int):
            raise PublicationError("GitHub upload returned invalid asset identity")
        item.update(state=transition(item["state"], "uploaded"), asset=asset, release=release)

    def verify(self, logical_uri: str) -> None:
        item = self.pending[logical_uri]
        release = self._pending_release(item)
        asset = self._asset(release, item["asset_name"])
        if not asset or asset.get("id") != item["asset"].get("id"):
            raise PublicationError("verified GitHub asset identity is missing or changed")
        with tempfile.TemporaryDirectory() as td:
            package = Path(td) / "remote.tar"; self.api.download_asset(int(item["asset"]["id"]), package)
            extracted = extract_publication_package(package, Path(td) / "artifact", expected_sha256=item["sha"])
            manifest = validate_artifact(extracted)["manifest"]; expected = item["metadata"]
            if manifest["artifact_id"] != expected["object_id"] or manifest["artifact_uri"] != logical_uri or manifest["artifact_content_hash"] != expected["artifact_content_hash"]:
                raise PublicationError("remote package semantic identity mismatch")
            actual_members = {name: sha256_file(extracted / name) for name in expected["member_hashes"]}
            if actual_members != expected["member_hashes"]: raise PublicationError("remote package member hash mismatch")
        item["verified_at"] = _now(); item["state"] = transition(item["state"], "remotely_verified")

    def finalize(self, logical_uri: str) -> dict[str, Any]:
        item = self.pending[logical_uri]
        if item.get("receipt"): return item["receipt"]
        release = self._pending_release(item)
        if release.get("draft"):
            release, _ = self.api.request("PATCH", f"/releases/{release['id']}",
                payload={"draft": False, "prerelease": self.fixture}, expected=(200,))
            release = self._validate_release(release, item["tag"])
            if release["id"] != item["release_id"]:
                raise PublicationError("final GitHub Release identity mismatch")
        # Re-query the exact numeric object after publication rather than
        # trusting either tag discovery or the PATCH response.
        release = self._pending_release(item)
        if release.get("draft") or release["id"] != item["release_id"]:
            raise PublicationError("final GitHub Release identity mismatch")
        asset = self._asset(release, item["asset_name"])
        if not asset or int(asset["id"]) != int(item["asset"]["id"]): raise PublicationError("final asset identity mismatch")
        item["state"] = transition(item["state"], "published_immutable_verified")
        values = deepcopy(item["metadata"]); values.update(remote_backend="github_releases",
            remote_repository=self.api.repository, release_tag=item["tag"], release_id=int(release["id"]),
            asset_id=int(asset["id"]), asset_filename=item["asset_name"], package_sha256=item["sha"],
            published_at=release.get("published_at") or _now(), verified_at=item["verified_at"], publication_state=item["state"])
        item["receipt"] = create_receipt(**values)
        return item["receipt"]


class GitHubReleaseArtifactResolver(ArtifactResolver):
    def __init__(self, catalog: dict[str, Any], api: GitHubAPI, workspace: Path):
        self.catalog, self.api, self.workspace = validate_catalog(catalog), api, workspace

    def resolve(self, uri: str) -> Path:
        records = [r for r in self.catalog["immutable_records"] if r["logical_artifact_uri"] == uri]
        if len(records) != 1: raise FileNotFoundError(f"uncataloged exact artifact URI: {uri}")
        record = records[0]
        if record["remote_repository"] != self.api.repository: raise PublicationError("catalog repository mismatch")
        release, _ = self.api.request("GET", f"/releases/{record['release_id']}")
        if release.get("draft") or release.get("tag_name") != record["release_tag"]: raise PublicationError("catalog Release identity mismatch")
        assets = [a for a in release.get("assets", []) if int(a.get("id", -1)) == record["asset_id"]]
        if len(assets) != 1 or assets[0].get("name") != record["asset_filename"]: raise PublicationError("catalog asset identity mismatch")
        target = self.workspace / record["package_sha256"]
        if target.exists(): return target
        self.workspace.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=self.workspace) as td:
            package = Path(td) / "asset.tar"; self.api.download_asset(record["asset_id"], package)
            extracted = extract_publication_package(package, Path(td) / "extracted", expected_sha256=record["package_sha256"])
            manifest = validate_artifact(extracted)["manifest"]
            if manifest["artifact_id"] != record["object_id"] or manifest["artifact_content_hash"] != record["artifact_content_hash"]:
                raise PublicationError("resolved artifact/catalog semantic mismatch")
            extracted.rename(target)
        return target


class GitHubCatalogCAS:
    """Atomic tracked-JSON update using GitHub Contents API's blob precondition."""
    def __init__(self, api: GitHubAPI, path: str, branch: str): self.api, self.path, self.branch = api, path, branch

    def read(self) -> tuple[dict[str, Any], str | None]:
        encoded = urllib.parse.quote(self.path, safe="/")
        item, _ = self.api.request("GET", f"/contents/{encoded}?ref={urllib.parse.quote(self.branch)}", expected=(200, 404))
        if item is None: return empty_catalog(), None
        import base64
        return validate_catalog(json.loads(base64.b64decode(item["content"]))), item["sha"]

    def add(self, record: dict[str, Any], receipt: dict[str, Any]) -> tuple[dict[str, Any], bool]:
        import base64
        catalog, oid = self.read(); updated = add_record(catalog, record, receipt)
        if updated == catalog: return catalog, False
        updated["compare_and_swap"]["expected_git_blob_sha"] = oid
        validate_catalog(updated)
        payload = {"message": f"Catalog fixture artifact {record['object_id']}",
            "content": base64.b64encode(canonical_json_bytes(updated)).decode(), "branch": self.branch}
        if oid is not None: payload["sha"] = oid
        encoded = urllib.parse.quote(self.path, safe="/")
        try: self.api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200, 201))
        except PublicationError as exc: raise PublicationError("catalog compare-and-swap update failed; refresh and retry") from exc
        return updated, True
