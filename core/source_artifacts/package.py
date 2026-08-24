from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path, PurePosixPath

from .hashing import sha256_file
from .validation import ArtifactValidationError, validate_artifact

PACKAGE_VERSION = "source_artifact_publication_package_v1"
FILE_MODE = 0o644


def _allowed_files(artifact_dir: Path) -> list[str]:
    validate_artifact(artifact_dir)
    manifest = json.loads((artifact_dir / "manifest.json").read_text())
    names = {"manifest.json", manifest["data_filename"], manifest["validation_filename"]}
    if manifest.get("lineage_filename"):
        names.add(manifest["lineage_filename"])
    actual = {p.name for p in artifact_dir.iterdir() if p.is_file()}
    if actual != names or any(p.is_dir() or p.is_symlink() for p in artifact_dir.iterdir()):
        raise ArtifactValidationError(f"publication package members differ: expected {sorted(names)}, got {sorted(actual)}")
    return sorted(names)


def build_publication_package(artifact_dir: Path, output: Path) -> dict:
    """Write a deterministic POSIX tar envelope using only the Python stdlib.

    Compression is deliberately deferred until Phase 2 because this repository
    has no pinned Zstandard implementation. The tar bytes are the governed
    package identity and may later be transported with a separately hashed,
    deterministic compression envelope.
    """
    names = _allowed_files(artifact_dir)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("wb") as raw, tarfile.open(fileobj=raw, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for name in names:
            data = (artifact_dir / name).read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mtime = 0
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mode = FILE_MODE
            info.pax_headers = {}
            archive.addfile(info, io.BytesIO(data))
    return {
        "package_contract_version": PACKAGE_VERSION,
        "package_filename": output.name,
        "package_sha256": sha256_file(output),
        "package_size_bytes": output.stat().st_size,
        "members": [{"path": n, "sha256": sha256_file(artifact_dir / n)} for n in names],
    }


def _validate_tar_member(member: tarfile.TarInfo, allowed: set[str]) -> None:
    path = PurePosixPath(member.name)
    if member.name not in allowed or path.is_absolute() or ".." in path.parts or len(path.parts) != 1:
        raise ArtifactValidationError(f"unsafe or unexpected package member: {member.name}")
    if not member.isfile() or member.issym() or member.islnk():
        raise ArtifactValidationError(f"package member is not a regular file: {member.name}")


def extract_publication_package(package: Path, output: Path, *, expected_sha256: str) -> Path:
    if sha256_file(package) != expected_sha256:
        raise ArtifactValidationError("publication package SHA-256 mismatch")
    if output.exists():
        raise FileExistsError(output)
    output.mkdir(parents=True)
    try:
        with tarfile.open(package, mode="r:") as archive:
            members = archive.getmembers()
            names = {m.name for m in members}
            if "manifest.json" not in names:
                raise ArtifactValidationError("package manifest missing")
            manifest_member = next(m for m in members if m.name == "manifest.json")
            _validate_tar_member(manifest_member, {"manifest.json"})
            stream = archive.extractfile(manifest_member)
            if stream is None:
                raise ArtifactValidationError("package manifest unreadable")
            manifest = json.loads(stream.read())
            allowed = {"manifest.json", manifest["data_filename"], manifest["validation_filename"]}
            if manifest.get("lineage_filename"):
                allowed.add(manifest["lineage_filename"])
            if names != allowed or len(members) != len(allowed):
                raise ArtifactValidationError("publication package allowlist mismatch")
            for member in members:
                _validate_tar_member(member, allowed)
                source = archive.extractfile(member)
                if source is None:
                    raise ArtifactValidationError("package member unreadable")
                (output / member.name).write_bytes(source.read())
        validate_artifact(output)
        return output
    except Exception:
        for child in output.iterdir():
            child.unlink()
        output.rmdir()
        raise
