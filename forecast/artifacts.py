from __future__ import annotations
# forecast/artifacts.py

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class DesignMatrixArtifact:
    df: pd.DataFrame
    y: pd.Series
    X: pd.DataFrame
    audit: dict
    parquet_sha256: str


def sha256_file(path: Path) -> str:
    b = path.read_bytes()
    return hashlib.sha256(b).hexdigest()


def load_design_matrix_artifact(parquet_path: str | Path, audit_path: str | Path) -> DesignMatrixArtifact:
    parquet_path = Path(parquet_path)
    audit_path = Path(audit_path)

    audit = json.loads(audit_path.read_text())
    sha = sha256_file(parquet_path)

    # Tripwire: parquet must match audit
    expected_sha = audit.get("design_matrix_sha256")
    if expected_sha and sha != expected_sha:
        raise SystemExit(f"[sarimax_exog] REFUSING: parquet sha mismatch: {sha} != {expected_sha}")

    df = pd.read_parquet(parquet_path)
    if "y" not in df.columns:
        raise SystemExit("[sarimax_exog] REFUSING: design matrix missing 'y' column")

    y = df["y"].copy()
    X = df.drop(columns=["y"]).copy()

    feature_ids = audit.get("feature_ids")
    if feature_ids is None:
        raise SystemExit("[sarimax_exog] REFUSING: audit missing feature_ids")

    if X.columns.tolist() != list(feature_ids):
        raise SystemExit("[sarimax_exog] REFUSING: X columns do not match audit feature_ids order")

    n_features = audit.get("n_features")
    if n_features is not None and X.shape[1] != int(n_features):
        raise SystemExit(f"[sarimax_exog] REFUSING: X has {X.shape[1]} cols but audit says {n_features}")

    return DesignMatrixArtifact(df=df, y=y, X=X, audit=audit, parquet_sha256=sha)
