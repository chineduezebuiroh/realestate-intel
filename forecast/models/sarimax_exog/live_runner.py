from __future__ import annotations

from typing import Optional
from pathlib import Path
import json

from forecast.models.sarimax_exog.bridge_runner import run_bridge_from_design_matrix_artifact


def _parse_asof_from_name(name: str) -> str:
    # expects ...__asof=YYYY-MM-DD.json
    part = name.split("__asof=", 1)[1]
    return part.replace(".json", "")


def _find_latest_design_matrix_artifact(runs_root: str = "runs") -> tuple[str, str, dict]:
    """
    Finds the most recent design matrix artifact by scanning runs/.
    Returns: (parquet_path, audit_json_path, audit_dict)
    """
    root = Path(runs_root)
    if not root.exists():
        raise FileNotFoundError(f"[sarimax_exog_live] runs root not found: {runs_root}")

    # heuristic: look for audit jsons, then choose most recent mtime
    audits = [p for p in root.rglob("*.json") if p.name.startswith("design_matrix__anchor=") and "__asof=" in p.name]
    if not audits:
        raise FileNotFoundError("[sarimax_exog_live] no design matrix audit jsons found under runs/")

    audits.sort(key=lambda p: _parse_asof_from_name(p.name), reverse=True)
    audit_path = audits[0]
    parquet_path = str(audit_path).replace(".json", ".parquet")

    with open(audit_path, "r") as f:
        audit = json.load(f)

    return parquet_path, str(audit_path), audit


def run_live_latest_artifact(
    *,
    metric_id: str,
    geo_id: str,
    property_type_id: str,
    freq: str = "M",
    horizon: int = 12,
    batch_id: str,
    data_asof: str,
    runs_root: str = "runs",
) -> int:
    parquet_path, audit_path, audit = _find_latest_design_matrix_artifact(runs_root=runs_root)

    # anchor comes from audit (live should not guess)
    anchor_date = audit.get("anchor_date")
    if not anchor_date:
        raise ValueError("[sarimax_exog_live] audit missing anchor_date")

    return run_bridge_from_design_matrix_artifact(
        metric_id=metric_id,
        geo_id=geo_id,
        property_type_id=property_type_id,
        freq=freq,
        design_matrix_parquet_path=parquet_path,
        design_matrix_audit_json_path=audit_path,
        anchor_date=anchor_date,
        horizon=horizon,
        batch_id=batch_id,
        data_asof=data_asof,
        run_kind="live",
        is_active=True,
    )
