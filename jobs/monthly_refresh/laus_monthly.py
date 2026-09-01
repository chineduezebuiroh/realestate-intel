"""Routine governed LAUS current-truth acquisition and reconciliation."""
from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from core.source_artifacts.reconciliation import preserve_prior
from core.source_artifacts.hashing import write_canonical_json
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.laus_bootstrap import create_bootstrap_artifact
from sources.bls_laus.artifact import (SOURCE_ID, TransientLAUSAcquisitionError,
    acquire, build_request_plan, canonicalize, governed_config_hashes, load_registry)


def _validate_prior(frame: pd.DataFrame, plan: dict[str, Any]) -> None:
    expected = {(item["geo_id"], item["metric_id"]) for item in plan["series"]}
    actual = set(zip(frame.geo_id.astype(str), frame.metric_id.astype(str)))
    if actual != expected:
        raise ValueError("accepted LAUS prior contains unexpected governed identity")


def run(*, output_root: Path, prior_artifact: Path, acquisition_mode: str,
        end_year: int, api_key: str, acquire_fn: Callable[..., Any] = acquire,
        retrieved_at: str | None = None) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    report_path = output_root / "run_report.json"
    try:
        prior_manifest = validate_artifact(prior_artifact, expected_source_id=SOURCE_ID)["manifest"]
        prior = pd.read_parquet(prior_artifact / "data.parquet")
        plan = build_request_plan(acquisition_mode=acquisition_mode, end_year=end_year,
                                  registry=load_registry(), config_hashes=governed_config_hashes())
        _validate_prior(prior, plan)
        current, diagnostics, _ = canonicalize(plan, acquire_fn(plan, api_key=api_key),
                                                prior_target_month=prior_manifest["target_month"])
        combined = preserve_prior(prior, current)
        artifact = output_root / "artifact"
        if artifact.exists(): shutil.rmtree(artifact)
        retrieval = retrieved_at or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        manifest = create_bootstrap_artifact(artifact, combined, plan, diagnostics,
                                             retrieved_at=retrieval)
        prior_keys = prior.set_index(["geo_id","metric_id","date","property_type_id"])["value"]
        current_keys = current.set_index(["geo_id","metric_id","date","property_type_id"])["value"]
        overlap = prior_keys.index.intersection(current_keys.index)
        changed = bool(any(prior_keys.loc[k] != current_keys.loc[k] for k in overlap)
                       or len(current_keys.index.difference(prior_keys.index)))
        report = {"schema_version":"laus_monthly_source_run_v1","source_id":SOURCE_ID,
            "run_status":"refreshed" if changed else "unchanged",
            "acquisition_mode":acquisition_mode,"target_month":diagnostics["target_month"],
            "observation_max":manifest["observation_max"],"prior_artifact_id":prior_manifest["artifact_id"],
            "resulting_artifact_id":manifest["artifact_id"],
            "resulting_artifact_content_hash":manifest["artifact_content_hash"],
            "validation_status":"passed","source_change_detected":changed,
            "accepted_pointer_changed":False}
        write_canonical_json(report_path, report); return report
    except Exception as exc:
        write_canonical_json(report_path,{"schema_version":"laus_monthly_source_run_v1",
            "source_id":SOURCE_ID,"run_status":"failed","validation_status":"failed",
            "retryability":"retryable" if isinstance(exc,TransientLAUSAcquisitionError) else "terminal",
            "error":f"{type(exc).__name__}: {exc}","accepted_pointer_changed":False})
        raise


def main() -> int:
    p=argparse.ArgumentParser(); p.add_argument("--output-root",type=Path,required=True)
    p.add_argument("--prior-artifact",type=Path,required=True); p.add_argument("--acquisition-mode",choices=("ordinary_overlap","annual_deep"),required=True)
    p.add_argument("--end-year",type=int,required=True); a=p.parse_args()
    key=os.environ.get("BLS_API_KEY","")
    if not key.strip(): p.error("BLS_API_KEY is required")
    print(json.dumps(run(output_root=a.output_root,prior_artifact=a.prior_artifact,
        acquisition_mode=a.acquisition_mode,end_year=a.end_year,api_key=key),sort_keys=True)); return 0

if __name__ == "__main__": raise SystemExit(main())
