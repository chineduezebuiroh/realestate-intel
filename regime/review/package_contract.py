"""Executable contract for directory and ZIP review packages."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from collections.abc import Callable, Mapping
from zipfile import BadZipFile, ZipFile

from .manifest import ReviewManifest


class ReviewPackageValidationError(ValueError):
    """Raised when a review package violates its runtime contract."""


@dataclass(frozen=True, slots=True)
class ReviewPackageValidationResult:
    """Successful validation details for a review package."""

    package: Path
    archive: bool
    members: tuple[str, ...]
    manifest: ReviewManifest
    hashes_verified: int


def _fail(message: str) -> None:
    raise ReviewPackageValidationError(message)


def _load_manifest(payload: bytes) -> ReviewManifest:
    try:
        decoded = json.loads(payload.decode("utf-8"))
        if not isinstance(decoded, Mapping):
            _fail("manifest.json must contain a JSON object")
        return ReviewManifest.from_dict(decoded)
    except ReviewPackageValidationError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise ReviewPackageValidationError(
            f"Invalid review metadata in manifest.json: {exc}"
        ) from exc


def _required_members(manifest: ReviewManifest) -> tuple[str, ...]:
    required = {"manifest.json"}
    seen: set[str] = set()
    for index, output in enumerate(manifest.outputs):
        raw_path = output.get("path")
        if not isinstance(raw_path, str) or not raw_path.strip():
            _fail(f"manifest outputs[{index}].path must be non-empty text")
        path = PurePosixPath(raw_path)
        if path.is_absolute() or ".." in path.parts or str(path) != raw_path:
            _fail(
                f"manifest outputs[{index}].path is not a safe canonical "
                f"path: {raw_path!r}"
            )
        if raw_path in seen:
            _fail(f"Duplicate manifest output path: {raw_path}")
        seen.add(raw_path)
        required.add(raw_path)
    return tuple(sorted(required))


def _validate(
    *,
    package: Path,
    archive: bool,
    members: tuple[str, ...],
    read_member: Callable[[str], bytes],
) -> ReviewPackageValidationResult:
    if "manifest.json" not in members:
        _fail("Review package is missing required member: manifest.json")
    manifest = _load_manifest(read_member("manifest.json"))
    required = _required_members(manifest)
    missing = sorted(set(required).difference(members))
    if missing:
        _fail(f"Review package is missing manifest members: {missing}")

    hashes_verified = 0
    for output in manifest.outputs:
        payload = read_member(output["path"])
        expected_size = output.get("size_bytes")
        if expected_size is not None and (
            not isinstance(expected_size, int) or expected_size < 0
        ):
            _fail(f"Invalid size_bytes for manifest member {output['path']!r}")
        if expected_size is not None and len(payload) != expected_size:
            _fail(
                f"Size mismatch for {output['path']!r}: "
                f"expected {expected_size}, got {len(payload)}"
            )
        expected = output.get("sha256")
        if expected is None:
            continue
        if not isinstance(expected, str) or len(expected) != 64:
            _fail(f"Invalid sha256 for manifest member {output['path']!r}")
        actual = hashlib.sha256(payload).hexdigest()
        if actual != expected.lower():
            _fail(
                f"SHA-256 mismatch for {output['path']!r}: "
                f"expected {expected.lower()}, got {actual}"
            )
        hashes_verified += 1

    return ReviewPackageValidationResult(
        package=package,
        archive=archive,
        members=members,
        manifest=manifest,
        hashes_verified=hashes_verified,
    )


def validate_review_package(path: str | Path) -> ReviewPackageValidationResult:
    """Validate an exported review-package directory."""
    package = Path(path)
    if not package.is_dir():
        _fail(f"Review package directory does not exist: {package}")
    members = tuple(
        sorted(
            candidate.relative_to(package).as_posix()
            for candidate in package.rglob("*")
            if candidate.is_file()
        )
    )
    return _validate(
        package=package,
        archive=False,
        members=members,
        read_member=lambda member: (package / member).read_bytes(),
    )


def validate_review_zip(path: str | Path) -> ReviewPackageValidationResult:
    """Validate readability, uniqueness, members, metadata, and hashes of a ZIP."""
    package = Path(path)
    try:
        with ZipFile(package, "r") as archive:
            names = tuple(
                info.filename
                for info in archive.infolist()
                if not info.is_dir()
            )
            duplicates = sorted(
                name for name, count in Counter(names).items() if count > 1
            )
            if duplicates:
                _fail(f"Review ZIP contains duplicate members: {duplicates}")
            bad_member = archive.testzip()
            if bad_member is not None:
                _fail(f"Review ZIP member failed CRC validation: {bad_member}")
            return _validate(
                package=package,
                archive=True,
                members=tuple(sorted(names)),
                read_member=archive.read,
            )
    except ReviewPackageValidationError:
        raise
    except (BadZipFile, FileNotFoundError, OSError) as exc:
        raise ReviewPackageValidationError(
            f"Review ZIP is not readable: {package}: {exc}"
        ) from exc
