from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

import pandas as pd

from .io import (
    sha256_file,
    write_csv,
    write_json,
)


@dataclass(frozen=True)
class ReviewArtifactWriter:
    output_dir: Path

    def __init__(self, output_dir: str | Path) -> None:
        object.__setattr__(self, "output_dir", Path(output_dir))

    def prepare(self, *, clear_existing: bool = False) -> Path:
        if clear_existing and self.output_dir.exists():
            for path in sorted(self.output_dir.rglob("*"), reverse=True):
                if path.is_file() or path.is_symlink():
                    path.unlink()
                elif path.is_dir():
                    path.rmdir()

        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self.output_dir

    def write_table(
        self,
        name: str,
        frame: pd.DataFrame,
        *,
        subdir: str = "tables",
        index: bool = False,
    ) -> Path:
        return write_csv(
            frame,
            self.output_dir / subdir / f"{name}.csv",
            index=index,
        )

    def write_json(
        self,
        name: str,
        payload: Mapping[str, Any],
        *,
        subdir: str | None = None,
    ) -> Path:
        target_dir = self.output_dir if subdir is None else self.output_dir / subdir
        return write_json(payload, target_dir / f"{name}.json")

    def write_manifest(self, payload: Mapping[str, Any]) -> Path:
        return self.write_json("manifest", payload)

    def write_decision_summary(self, payload: Mapping[str, Any]) -> Path:
        return self.write_json("decision_summary", payload)

    def write_tables(
        self,
        tables: Mapping[str, pd.DataFrame],
        *,
        subdir: str = "tables",
        index: bool = False,
    ) -> dict[str, Path]:
        return {
            name: self.write_table(name, frame, subdir=subdir, index=index)
            for name, frame in sorted(tables.items())
        }

    def build_output_manifest(
        self,
        *,
        exclude_names: tuple[str, ...] = ("manifest.json",),
    ) -> list[dict[str, Any]]:
        outputs: list[dict[str, Any]] = []
        if not self.output_dir.exists():
            return outputs

        for path in sorted(self.output_dir.rglob("*")):
            if not path.is_file():
                continue
            relative_path = path.relative_to(self.output_dir).as_posix()
            if relative_path in exclude_names:
                continue
            outputs.append(
                {
                    "path": relative_path,
                    "sha256": sha256_file(path),
                    "size_bytes": path.stat().st_size,
                }
            )
        return outputs

    def write_zip(self, path: str | Path | None = None) -> Path:
        """Write the current review package as a deterministic ZIP bundle."""
        if not self.output_dir.is_dir():
            raise FileNotFoundError(
                f"Review package directory does not exist: {self.output_dir}"
            )

        zip_path = Path(path) if path is not None else self.output_dir.with_suffix(".zip")
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        package_files = sorted(
            candidate
            for candidate in self.output_dir.rglob("*")
            if candidate.is_file() and candidate.resolve() != zip_path.resolve()
        )
        with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as archive:
            for candidate in package_files:
                member = candidate.relative_to(self.output_dir).as_posix()
                info = ZipInfo(member, date_time=(1980, 1, 1, 0, 0, 0))
                info.compress_type = ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, candidate.read_bytes())
        return zip_path
