"""Deterministic packages for governed non-source catalog objects."""
from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path, PurePosixPath
from typing import Any

from .catalog import add_record
from .hashing import sha256_file
from .publication import ArtifactPublisher, PublicationError
from .hashing import sha256_json
from .market_artifact import validate_canonical_market_artifact
from .source_set_v2 import validate_source_set_v2


def build_object_package(files: dict[str, Path], output: Path) -> dict[str, Any]:
    if not files or any(PurePosixPath(name).is_absolute() or len(PurePosixPath(name).parts) != 1
                        or ".." in PurePosixPath(name).parts or not path.is_file()
                        for name, path in files.items()):
        raise PublicationError("governed object package members are invalid")
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw, tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name, path in sorted(files.items()):
            data = path.read_bytes(); info = tarfile.TarInfo(name)
            info.size = len(data); info.mtime = 0; info.uid = info.gid = 0
            info.uname = info.gname = ""; info.mode = 0o644; info.pax_headers = {}
            archive.addfile(info, io.BytesIO(data))
    return {"package_sha256":sha256_file(output),
        "member_hashes":{name:sha256_file(path) for name,path in sorted(files.items())}}


def validate_object_package(package: Path, output: Path, *, object_type: str,
                            expected: dict[str, Any]) -> Path:
    allowed = {"source_set":{"source-set.json"},
               "canonical_market":{"canonical-market.json","market.duckdb"}}.get(object_type)
    if allowed is None: raise PublicationError("unsupported governed object package type")
    if output.exists(): raise FileExistsError(output)
    output.mkdir(parents=True)
    try:
        with tarfile.open(package,mode="r:") as archive:
            members=archive.getmembers()
            if {m.name for m in members} != allowed or len(members) != len(allowed):
                raise PublicationError("governed object package allowlist mismatch")
            for member in members:
                if not member.isfile() or member.issym() or member.islnk() or len(PurePosixPath(member.name).parts)!=1:
                    raise PublicationError("unsafe governed object package member")
                stream=archive.extractfile(member)
                if stream is None: raise PublicationError("governed object member unreadable")
                (output/member.name).write_bytes(stream.read())
        if {name:sha256_file(output/name) for name in sorted(allowed)} != expected["member_hashes"]:
            raise PublicationError("governed object member hash mismatch")
        manifest_name="source-set.json" if object_type=="source_set" else "canonical-market.json"
        manifest=json.loads((output/manifest_name).read_text())
        if object_type=="source_set":
            validate_source_set_v2(manifest); object_id=manifest["source_set_id"]
        else:
            validate_canonical_market_artifact(manifest); object_id=manifest["market_artifact_id"]
            if sha256_file(output/"market.duckdb") != manifest["database_sha256"]:
                raise PublicationError("canonical database transport hash mismatch")
        if object_id != expected["object_id"] or sha256_json(manifest) != expected["artifact_content_hash"]:
            raise PublicationError("governed object semantic identity mismatch")
        return output
    except Exception:
        for child in output.iterdir(): child.unlink()
        output.rmdir(); raise


def publish_object(*, publisher: ArtifactPublisher, catalog: dict[str, Any], package: Path,
        logical_uri: str, object_id: str, object_type: str, artifact_content_hash: str,
        object_metadata: dict[str, Any], member_hashes: dict[str, str], remote_repository: str,
        release_tag: str, release_id: int, asset_id: int, asset_filename: str,
        publisher_git_sha: str, published_at: str, contract_versions: list[str]) -> tuple[dict[str, Any], dict[str, Any], bool]:
    """Run the common explicit publisher state machine, then catalog once."""
    metadata = {"logical_artifact_uri":logical_uri, "object_id":object_id,
        "object_type":object_type, "object_metadata":object_metadata,
        "artifact_content_hash":artifact_content_hash, "member_hashes":member_hashes,
        "remote_backend":"governed_artifact_backend", "remote_repository":remote_repository,
        "release_tag":release_tag, "release_id":release_id, "asset_id":asset_id,
        "asset_filename":asset_filename, "published_at":published_at, "verified_at":published_at,
        "publisher_git_sha":publisher_git_sha, "contract_versions":contract_versions}
    publisher.prepare(logical_uri, package.read_bytes(), metadata)
    publisher.upload(logical_uri); publisher.verify(logical_uri); receipt = publisher.finalize(logical_uri)
    record = {"object_type":object_type,"object_id":object_id,"logical_artifact_uri":logical_uri,
        "remote_repository":receipt["remote_repository"],"release_tag":receipt["release_tag"],
        "release_id":receipt["release_id"],"asset_id":receipt["asset_id"],
        "asset_filename":receipt["asset_filename"],"package_sha256":receipt["package_sha256"],
        "artifact_content_hash":artifact_content_hash,"publication_receipt_id":receipt["receipt_id"],
        "publication_state":receipt["publication_state"],"metadata":object_metadata}
    updated = add_record(catalog,record,receipt)
    return updated, receipt, updated != catalog
