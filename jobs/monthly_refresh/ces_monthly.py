"""Routine governed CES current-truth acquisition and reconciliation."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from core.source_artifacts.reconciliation import preserve_prior
from core.source_artifacts.validation import validate_artifact
from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.ces_bootstrap import create_bootstrap_artifact
from sources.bls_ces.artifact import (TransientCESAcquisitionError, acquire,
    build_request_plan, canonicalize, governed_config_hashes, load_series_spec)

SOURCE_ID = "ces"


def _git_sha() -> str:
    value = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True)
    return value.stdout.strip() if value.returncode == 0 else "unknown"


def _validate_prior(frame: pd.DataFrame, plan: dict[str, Any]) -> None:
    expected = {(item["geo_id"], item["metric_id"]) for item in plan["series"]}
    actual = set(zip(frame.geo_id.astype(str), frame.metric_id.astype(str)))
    if actual != expected:
        raise ValueError("accepted CES prior contains unexpected governed identity")


def run(*, output_root: Path, prior_artifact: Path, start_year: int, end_year: int,
        api_key: str, acquire_fn: Callable[..., Any] = acquire,
        retrieved_at: str | None = None, repository_root: Path = Path(".")) -> dict[str, Any]:
    """Build a candidate; explicit bounds make ordinary revision overlap semantic."""
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / "run_report.json"
    try:
        prior_manifest = validate_artifact(prior_artifact, expected_source_id=SOURCE_ID)["manifest"]
        prior = pd.read_parquet(prior_artifact / "data.parquet")
        plan = build_request_plan(load_series_spec(repository_root / "config/ces_series.generated.csv"),
            start_year=start_year, end_year=end_year, acquisition_mode="ordinary_overlap",
            config_hashes=governed_config_hashes(repository_root))
        _validate_prior(prior, plan)
        acquired = acquire_fn(plan, api_key=api_key)
        current, diagnostics = canonicalize(plan, acquired)
        if diagnostics["missing_mandatory_series"] or diagnostics["target_month"] is None:
            raise ValueError("CES mandatory series completeness failed")
        if diagnostics["target_month"] < str(prior_manifest["target_month"]):
            raise ValueError("CES mandatory target month regressed relative to accepted prior")
        combined = preserve_prior(prior, current)
        if pd.to_datetime(combined.date).max() < pd.to_datetime(prior.date).max():
            raise ValueError("CES accepted mandatory history regressed")
        artifact = output_root / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        retrieval = retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        manifest = create_bootstrap_artifact(artifact, combined, plan, diagnostics,
            retrieved_at=retrieval, lineage_contract="ces_monthly_source_v1")
        prior_keys = prior.set_index(["geo_id", "metric_id", "date", "property_type_id"])["value"]
        current_keys = current.set_index(["geo_id", "metric_id", "date", "property_type_id"])["value"]
        overlap = prior_keys.index.intersection(current_keys.index)
        changed = bool(any(prior_keys.loc[key] != current_keys.loc[key] for key in overlap)
                       or len(current_keys.index.difference(prior_keys.index)))
        report = {"schema_version":"ces_monthly_source_run_v1", "source_id":SOURCE_ID,
            "run_status":"refreshed" if changed else "unchanged", "git_sha":_git_sha(),
            "target_month":diagnostics["target_month"], "observation_max":manifest["observation_max"],
            "prior_artifact_id":prior_manifest["artifact_id"],
            "resulting_artifact_id":manifest["artifact_id"],
            "resulting_artifact_content_hash":manifest["artifact_content_hash"],
            "data_sha256":manifest["data_sha256"], "validation_status":"passed",
            "source_change_detected":changed, "request_plan":plan,
            "prior_only_preserved_key_count":len(prior_keys.index.difference(current_keys.index)),
            "accepted_pointer_changed":False}
        write_canonical_json(report_path, report); return report
    except Exception as exc:
        write_canonical_json(report_path, {"schema_version":"ces_monthly_source_run_v1",
            "source_id":SOURCE_ID,"run_status":"failed","validation_status":"failed",
            "retryability":"retryable" if isinstance(exc, TransientCESAcquisitionError) else "terminal",
            "error":f"{type(exc).__name__}: {exc}","accepted_pointer_changed":False})
        raise


def main() -> int:
    parser=argparse.ArgumentParser(); parser.add_argument("--output-root",type=Path,required=True)
    parser.add_argument("--prior-artifact",type=Path,required=True); parser.add_argument("--start-year",type=int,required=True)
    parser.add_argument("--end-year",type=int,required=True); args=parser.parse_args()
    key=os.environ.get("BLS_API_KEY","")
    if not key.strip(): parser.error("BLS_API_KEY is required")
    print(json.dumps(run(output_root=args.output_root,prior_artifact=args.prior_artifact,
        start_year=args.start_year,end_year=args.end_year,api_key=key),sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
