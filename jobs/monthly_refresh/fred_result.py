"""Fail-closed construction of the FRED monthly source execution result."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _read_object(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def build_result(*, root: Path, cycle_id: str, acquire_outcome: str,
                 publish_outcome: str) -> dict[str, Any]:
    """Convert every Actions step outcome, including broken evidence, to the source contract."""
    run = _read_object(root / "run_report.json")
    manifest = _read_object(root / "artifact" / "manifest.json")
    publication = _read_object(root / "publication.json")
    failure = _read_object(root / "publication_failure.json")
    result = {"schema_version": "monthly_source_execution_result_v1", "source_id": "fred_macro",
        "cycle_id": cycle_id, "status": "failed", "candidate_artifact_id": None,
        "artifact_content_hash": None, "package_sha256": None, "publication_state": "not_published",
        "validation_status": "failed", "provider_release_id": None, "observation_max": None,
        "prior_artifact_id": None, "source_change_detected": False, "retryability": "terminal",
        "evidence_uri": "actions://fred_macro/run_report.json"}

    if run:
        result.update(candidate_artifact_id=run.get("resulting_artifact_id"),
            artifact_content_hash=run.get("resulting_artifact_content_hash"),
            observation_max=run.get("observation_max"), prior_artifact_id=run.get("prior_artifact_id"),
            source_change_detected=run.get("source_change_detected", False))
    if manifest:
        result["provider_release_id"] = manifest.get("provider_release_id")

    if acquire_outcome != "success":
        if run and run.get("run_status") == "failed" and run.get("retryability") in {"retryable", "terminal"}:
            result["retryability"] = run["retryability"]
        return result

    # A successful acquisition must have both a successful report and its
    # validated manifest. Missing/malformed evidence is a terminal invariant violation.
    required_run = ("resulting_artifact_id", "resulting_artifact_content_hash", "observation_max",
                    "prior_artifact_id", "source_change_detected")
    if not run or run.get("run_status") not in {"refreshed", "unchanged"} or not manifest \
            or any(key not in run for key in required_run) or not manifest.get("provider_release_id"):
        return result

    if publish_outcome != "success":
        if failure and failure.get("retryability") in {"retryable", "terminal"}:
            result["retryability"] = failure["retryability"]
        return result

    # The step outcome is authoritative; files are secondary invariants.
    if not publication or not publication.get("package_sha256") \
            or publication.get("publication_state") != "published_immutable_verified":
        return result
    result.update(status="succeeded", package_sha256=publication["package_sha256"],
        publication_state="published_verified", validation_status="passed",
        retryability="not_applicable",
        evidence_uri="artifact://source/fred_macro/" + run["resulting_artifact_id"])
    return result
