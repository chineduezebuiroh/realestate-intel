from __future__ import annotations
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


def load_design_matrix_artifact(parquet_path: str | Path, audit_path: str | Path) -> DesignMatrixArtifact:
    parquet_path = Path(parquet_path)
    audit_path = Path(audit_path)

    audit = json.loads(audit_path.read_text())
    b = parquet_path.read_bytes()
    sha = hashlib.sha256(b).hexdigest()

    if audit.get("design_matrix_sha256") and sha != audit["design_matrix_sha256"]:
        raise SystemExit(f"[sarimax_exog] REFUSING: parquet sha mismatch: {sha} != {audit['design_matrix_sha256']}")

    df = pd.read_parquet(parquet_path)
    if "y" not in df.columns:
        raise SystemExit("[sarimax_exog] REFUSING: design matrix missing 'y'")

    y = df["y"].copy()
    X = df.drop(columns=["y"]).copy()

    feature_ids = audit.get("feature_ids")
    if feature_ids is None:
        raise SystemExit("[sarimax_exog] REFUSING: audit missing feature_ids")

    if X.shape[1] != int(audit.get("n_features", X.shape[1])):
        raise SystemExit(f"[sarimax_exog] REFUSING: X n_features={X.shape[1]} != audit n_features={audit.get('n_features')}")

    if X.columns.tolist() != list(feature_ids):
        raise SystemExit("[sarimax_exog] REFUSING: X columns do not match audit feature_ids order")

    return DesignMatrixArtifact(df=df, y=y, X=X, audit=audit, parquet_sha256=sha)
