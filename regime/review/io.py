from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import pandas as pd


def _atomic_replace_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="wb",
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
        delete=False,
    ) as handle:
        temp_path = Path(handle.name)
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())

    try:
        temp_path.replace(path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def write_json(payload: Mapping[str, Any], path: str | Path) -> Path:
    output_path = Path(path)
    serialized = (
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            allow_nan=False,
            default=str,
        )
        + "\n"
    ).encode("utf-8")
    _atomic_replace_bytes(output_path, serialized)
    return output_path


def write_csv(
    frame: pd.DataFrame,
    path: str | Path,
    *,
    index: bool = False,
) -> Path:
    output_path = Path(path)
    serialized = frame.to_csv(index=index, lineterminator="\n").encode("utf-8")
    _atomic_replace_bytes(output_path, serialized)
    return output_path


def sha256_file(path: str | Path) -> str:
    input_path = Path(path)
    digest = hashlib.sha256()
    with input_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
