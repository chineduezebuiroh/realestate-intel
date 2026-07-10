from __future__ import annotations
# regime/artifacts.py

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


DEFAULT_ARTIFACT_ROOT = Path("artifacts/regime/runs")

_VALID_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


class ArtifactError(RuntimeError):
    """Base exception for regime artifact persistence failures."""


class ArtifactNotFoundError(ArtifactError):
    """Raised when a requested run artifact does not exist."""


class ArtifactIntegrityError(ArtifactError):
    """Raised when a persisted artifact does not match its recorded hash."""


def _validate_name(value: str, field_name: str) -> str:
    value = str(value).strip()

    if not value:
        raise ValueError(f"{field_name} cannot be empty")

    if not _VALID_NAME_PATTERN.fullmatch(value):
        raise ValueError(
            f"Invalid {field_name}={value!r}. "
            "Use only letters, numbers, underscores, hyphens, and periods."
        )

    return value


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _json_default(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()

    if isinstance(value, Path):
        return str(value)

    if pd.isna(value):
        return None

    if hasattr(value, "item"):
        return value.item()

    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _atomic_replace(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    os.replace(source, destination)


def _date_metadata(df: pd.DataFrame) -> dict[str, Any]:
    date_columns = [
        column
        for column in ("date", "evaluation_date", "metric_date")
        if column in df.columns
    ]

    out: dict[str, Any] = {}

    for column in date_columns:
        values = pd.to_datetime(df[column], errors="coerce").dropna()

        if values.empty:
            out[column] = {
                "min": None,
                "max": None,
            }
        else:
            out[column] = {
                "min": values.min().isoformat(),
                "max": values.max().isoformat(),
            }

    return out


class RegimeArtifactStore:
    """
    Read and write deterministic regime-engine run artifacts.

    Directory contract:

        <root>/<run_id>/
            manifest.json
            <artifact_name>.parquet
            validation/
                <artifact_name>.parquet
    """

    def __init__(
        self,
        root: str | Path = DEFAULT_ARTIFACT_ROOT,
    ) -> None:
        self.root = Path(root)

    def run_dir(self, run_id: str) -> Path:
        run_id = _validate_name(run_id, "run_id")
        return self.root / run_id

    def manifest_path(self, run_id: str) -> Path:
        return self.run_dir(run_id) / "manifest.json"

    def artifact_path(
        self,
        run_id: str,
        artifact_name: str,
        *,
        validation: bool = False,
    ) -> Path:
        artifact_name = _validate_name(artifact_name, "artifact_name")

        base = self.run_dir(run_id)
        if validation:
            base = base / "validation"

        return base / f"{artifact_name}.parquet"

    def run_exists(self, run_id: str) -> bool:
        return self.run_dir(run_id).is_dir()

    def artifact_exists(
        self,
        run_id: str,
        artifact_name: str,
        *,
        validation: bool = False,
    ) -> bool:
        return self.artifact_path(
            run_id,
            artifact_name,
            validation=validation,
        ).is_file()

    def initialize_run(
        self,
        run_id: str,
        *,
        experiment_id: str,
        metadata: Mapping[str, Any] | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        """
        Create the run directory and initial manifest.

        Existing runs are rejected unless overwrite=True.
        """
        run_id = _validate_name(run_id, "run_id")
        experiment_id = _validate_name(experiment_id, "experiment_id")

        run_dir = self.run_dir(run_id)

        if run_dir.exists() and not overwrite:
            raise ArtifactError(
                f"Run already exists: {run_dir}. "
                "Use a new run_id or explicitly set overwrite=True."
            )

        if overwrite and run_dir.exists():
            # Overwrite is intentionally conservative: recreate the manifest,
            # but do not silently delete unrelated files.
            pass

        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "validation").mkdir(parents=True, exist_ok=True)

        manifest: dict[str, Any] = {
            "schema_version": 1,
            "run_id": run_id,
            "experiment_id": experiment_id,
            "created_at_utc": _utc_now_iso(),
            "updated_at_utc": _utc_now_iso(),
            "status": "initialized",
            "metadata": dict(metadata or {}),
            "artifacts": {},
        }

        self.write_manifest(run_id, manifest)
        return manifest

    def write_dataframe(
        self,
        run_id: str,
        artifact_name: str,
        dataframe: pd.DataFrame,
        *,
        validation: bool = False,
        allow_overwrite: bool = False,
        extra_metadata: Mapping[str, Any] | None = None,
        update_manifest: bool = True,
    ) -> dict[str, Any]:
        """
        Atomically write a DataFrame to Parquet and return its metadata.

        The file is written to a temporary path in the destination directory,
        hashed, and then atomically moved into place.
        """
        if not isinstance(dataframe, pd.DataFrame):
            raise TypeError("dataframe must be a pandas DataFrame")

        path = self.artifact_path(
            run_id,
            artifact_name,
            validation=validation,
        )
        path.parent.mkdir(parents=True, exist_ok=True)

        if path.exists() and not allow_overwrite:
            raise ArtifactError(
                f"Artifact already exists: {path}. "
                "Use a new run_id or explicitly set allow_overwrite=True."
            )

        temp_path: Path | None = None

        try:
            with tempfile.NamedTemporaryFile(
                dir=path.parent,
                prefix=f".{artifact_name}.",
                suffix=".parquet.tmp",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)

            dataframe.to_parquet(
                temp_path,
                index=False,
                engine="pyarrow",
            )

            sha256 = _sha256_file(temp_path)
            size_bytes = temp_path.stat().st_size

            _atomic_replace(temp_path, path)
            temp_path = None
        except Exception as exc:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()

            raise ArtifactError(
                f"Failed to write artifact {artifact_name!r} for run "
                f"{run_id!r}: {exc}"
            ) from exc

        relative_path = path.relative_to(self.run_dir(run_id))

        metadata: dict[str, Any] = {
            "artifact_name": artifact_name,
            "artifact_group": "validation" if validation else "pipeline",
            "relative_path": str(relative_path),
            "format": "parquet",
            "written_at_utc": _utc_now_iso(),
            "sha256": sha256,
            "size_bytes": size_bytes,
            "row_count": int(len(dataframe)),
            "column_count": int(len(dataframe.columns)),
            "columns": [str(column) for column in dataframe.columns],
            "date_ranges": _date_metadata(dataframe),
            "extra_metadata": dict(extra_metadata or {}),
        }

        if update_manifest:
            self.record_artifact(run_id, metadata)

        return metadata

    def read_dataframe(
        self,
        run_id: str,
        artifact_name: str,
        *,
        validation: bool = False,
        verify_hash: bool = True,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        path = self.artifact_path(
            run_id,
            artifact_name,
            validation=validation,
        )

        if not path.is_file():
            raise ArtifactNotFoundError(f"Artifact not found: {path}")

        if verify_hash:
            manifest = self.read_manifest(run_id)
            key = self._artifact_manifest_key(
                artifact_name,
                validation=validation,
            )
            artifact_metadata = manifest.get("artifacts", {}).get(key)

            if artifact_metadata is None:
                raise ArtifactIntegrityError(
                    f"Artifact {key!r} exists on disk but is not recorded "
                    f"in the run manifest."
                )

            expected_hash = artifact_metadata.get("sha256")
            actual_hash = _sha256_file(path)

            if expected_hash != actual_hash:
                raise ArtifactIntegrityError(
                    f"Hash mismatch for {path}: "
                    f"expected {expected_hash}, got {actual_hash}"
                )

        try:
            return pd.read_parquet(
                path,
                columns=columns,
                engine="pyarrow",
            )
        except Exception as exc:
            raise ArtifactError(f"Failed to read artifact {path}: {exc}") from exc

    def write_manifest(
        self,
        run_id: str,
        manifest: Mapping[str, Any],
    ) -> None:
        path = self.manifest_path(run_id)
        path.parent.mkdir(parents=True, exist_ok=True)

        payload = dict(manifest)
        payload["run_id"] = _validate_name(run_id, "run_id")
        payload["updated_at_utc"] = _utc_now_iso()

        temp_path: Path | None = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=path.parent,
                prefix=".manifest.",
                suffix=".json.tmp",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)
                json.dump(
                    payload,
                    temp_file,
                    indent=2,
                    sort_keys=True,
                    default=_json_default,
                )
                temp_file.write("\n")

            _atomic_replace(temp_path, path)
            temp_path = None
        except Exception as exc:
            if temp_path is not None and temp_path.exists():
                temp_path.unlink()

            raise ArtifactError(
                f"Failed to write manifest for run {run_id!r}: {exc}"
            ) from exc

    def read_manifest(self, run_id: str) -> dict[str, Any]:
        path = self.manifest_path(run_id)

        if not path.is_file():
            raise ArtifactNotFoundError(f"Manifest not found: {path}")

        try:
            with path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
        except Exception as exc:
            raise ArtifactError(f"Failed to read manifest {path}: {exc}") from exc

        if manifest.get("run_id") != run_id:
            raise ArtifactIntegrityError(
                f"Manifest run_id mismatch in {path}: "
                f"expected {run_id!r}, got {manifest.get('run_id')!r}"
            )

        return manifest

    def record_artifact(
        self,
        run_id: str,
        artifact_metadata: Mapping[str, Any],
    ) -> None:
        manifest = self.read_manifest(run_id)

        artifact_name = str(artifact_metadata["artifact_name"])
        validation = artifact_metadata.get("artifact_group") == "validation"
        key = self._artifact_manifest_key(
            artifact_name,
            validation=validation,
        )

        artifacts = dict(manifest.get("artifacts", {}))
        artifacts[key] = dict(artifact_metadata)

        manifest["artifacts"] = artifacts
        self.write_manifest(run_id, manifest)

    def update_manifest(
        self,
        run_id: str,
        *,
        status: str | None = None,
        metadata_updates: Mapping[str, Any] | None = None,
        top_level_updates: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        manifest = self.read_manifest(run_id)

        if status is not None:
            manifest["status"] = str(status)

        if metadata_updates:
            metadata = dict(manifest.get("metadata", {}))
            metadata.update(dict(metadata_updates))
            manifest["metadata"] = metadata

        if top_level_updates:
            protected = {"run_id", "artifacts"}
            illegal = protected.intersection(top_level_updates)

            if illegal:
                raise ValueError(
                    f"Cannot update protected manifest fields: {sorted(illegal)}"
                )

            manifest.update(dict(top_level_updates))

        self.write_manifest(run_id, manifest)
        return self.read_manifest(run_id)

    def verify_run(self, run_id: str) -> pd.DataFrame:
        """
        Verify all manifest-recorded artifact files and hashes.

        Returns one row per recorded artifact.
        """
        manifest = self.read_manifest(run_id)
        records: list[dict[str, Any]] = []

        for key, metadata in manifest.get("artifacts", {}).items():
            relative_path = metadata.get("relative_path")
            path = self.run_dir(run_id) / str(relative_path)

            exists = path.is_file()
            expected_hash = metadata.get("sha256")
            actual_hash = _sha256_file(path) if exists else None

            records.append(
                {
                    "artifact_key": key,
                    "relative_path": relative_path,
                    "exists": exists,
                    "hash_matches": exists and actual_hash == expected_hash,
                    "expected_sha256": expected_hash,
                    "actual_sha256": actual_hash,
                    "row_count": metadata.get("row_count"),
                    "size_bytes": metadata.get("size_bytes"),
                }
            )

        return pd.DataFrame(records)

    def list_runs(self) -> pd.DataFrame:
        if not self.root.exists():
            return pd.DataFrame(
                columns=[
                    "run_id",
                    "experiment_id",
                    "status",
                    "created_at_utc",
                    "updated_at_utc",
                    "artifact_count",
                ]
            )

        rows: list[dict[str, Any]] = []

        for run_dir in sorted(path for path in self.root.iterdir() if path.is_dir()):
            manifest_path = run_dir / "manifest.json"

            if not manifest_path.is_file():
                continue

            try:
                manifest = self.read_manifest(run_dir.name)
            except ArtifactError:
                continue

            rows.append(
                {
                    "run_id": manifest.get("run_id"),
                    "experiment_id": manifest.get("experiment_id"),
                    "status": manifest.get("status"),
                    "created_at_utc": manifest.get("created_at_utc"),
                    "updated_at_utc": manifest.get("updated_at_utc"),
                    "artifact_count": len(manifest.get("artifacts", {})),
                }
            )

        return pd.DataFrame(rows)

    @staticmethod
    def _artifact_manifest_key(
        artifact_name: str,
        *,
        validation: bool,
    ) -> str:
        prefix = "validation/" if validation else ""
        return f"{prefix}{artifact_name}"
