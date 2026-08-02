"""Smoke Test 88: atomic calibration-evidence handoff and readiness contract."""

from __future__ import annotations

import json
import runpy
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory

from regime.review.calibration.inventory_campaign import (
    AUTHORITATIVE_PRODUCER_CODE_IDENTITY,
    COMPLETION_MARKER,
    EVIDENCE_CONTRACT_VERSION,
    invalidate_authoritative_evidence_readiness,
    load_phase_a_foundation_evidence,
    persist_phase_a_foundation_evidence,
    validate_current_authoritative_evidence,
)


def _expect_error(call, text: str) -> None:
    try:
        call()
    except (ValueError, FileNotFoundError, RuntimeError) as exc:
        assert text in str(exc), str(exc)
        return
    raise AssertionError(f"Expected failure containing {text!r}")


def main() -> int:
    fixture = runpy.run_path("scripts/smoke_tests/80_89/85_inventory_review_bundle.py")
    scoring_fixture = runpy.run_path("scripts/smoke_tests/80_89/83_inventory_candidate_scoring.py")
    evidence = scoring_fixture["_evidence"]()
    evidence = replace(evidence, system_evidence=fixture["_system_evidence"](evidence),
                       decomposition_evidence=fixture["_decomposition_evidence"](evidence))
    campaign = evidence.campaign

    with TemporaryDirectory() as temporary:
        artifact_root = Path(temporary) / "runs"
        campaigns = artifact_root / "calibration_campaigns"
        canonical = persist_phase_a_foundation_evidence(evidence, campaigns)
        marker_path = canonical / COMPLETION_MARKER
        manifest_path = canonical / "evidence_manifest.json"
        assert marker_path.is_file() and manifest_path.is_file()
        marker = json.loads(marker_path.read_text())
        assert marker["evidence_manifest_sha256"]
        assert marker["producer_code_identity"] == AUTHORITATIVE_PRODUCER_CODE_IDENTITY
        current = validate_current_authoritative_evidence(
            campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
            artifact_root=artifact_root, source_run_id=campaign.baseline_run_id,
        )
        assert current.decomposition_evidence.primary_decomposition_axes == ("supply",)
        assert current.decomposition_evidence.supporting_coordinate_axes == ("supply", "demand")
        assert "axis_scope_lineage" in current.decomposition_evidence.tables
        original_manifest = manifest_path.read_bytes()

        manifest_payload = json.loads(original_manifest)
        manifest_payload["files"].append({"kind": "unknown", "name": "intruder", "sha256": "0" * 64})
        manifest_path.write_text(json.dumps(manifest_payload))
        _expect_error(lambda: load_phase_a_foundation_evidence(
            campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
            artifact_root=artifact_root,
        ), "Unknown persisted evidence artifact kind")
        manifest_path.write_bytes(original_manifest)

        # A new producer attempt invalidates readiness without destroying the
        # older governed package. Injected staging failures cannot publish.
        invalidate_authoritative_evidence_readiness(
            artifact_root=artifact_root, campaign_id=campaign.campaign_id,
            campaign_version=campaign.campaign_version,
        )
        _expect_error(lambda: persist_phase_a_foundation_evidence(
            evidence, campaigns, _failure_point="write"), "injected staging write failure")
        assert manifest_path.read_bytes() == original_manifest and not marker_path.exists()
        assert not list(canonical.parent.glob(f".{canonical.name}.staging-*"))
        load_phase_a_foundation_evidence(
            campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
            artifact_root=artifact_root,
        )
        _expect_error(lambda: validate_current_authoritative_evidence(
            campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
            artifact_root=artifact_root, source_run_id=campaign.baseline_run_id,
        ), "producer completion marker absent")

        _expect_error(lambda: persist_phase_a_foundation_evidence(
            evidence, campaigns, _failure_point="manifest"), "injected pre-manifest failure")
        assert manifest_path.read_bytes() == original_manifest and not marker_path.exists()

        persist_phase_a_foundation_evidence(evidence, campaigns)
        valid_marker = json.loads(marker_path.read_text())
        cases = (
            ("producer_code_identity", "stale", "producer identity stale"),
            ("evidence_contract_version", "stale", "contract-version mismatch"),
            ("source_run_id", "wrong", "source-run mismatch"),
            ("evidence_manifest_sha256", "0" * 64, "manifest hash mismatch"),
        )
        for field, value, message in cases:
            changed = dict(valid_marker); changed[field] = value
            marker_path.write_text(json.dumps(changed))
            _expect_error(lambda: validate_current_authoritative_evidence(
                campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
                artifact_root=artifact_root, source_run_id=campaign.baseline_run_id,
            ), message)
        marker_path.write_text(json.dumps(valid_marker))
        artifact = next((canonical / "phase_a").glob("*.parquet"))
        artifact_bytes = artifact.read_bytes(); artifact.write_bytes(artifact_bytes + b"corrupt")
        _expect_error(lambda: validate_current_authoritative_evidence(
            campaign_id=campaign.campaign_id, campaign_version=campaign.campaign_version,
            artifact_root=artifact_root, source_run_id=campaign.baseline_run_id,
        ), "artifact hash mismatch")
        artifact.write_bytes(artifact_bytes)
        assert valid_marker["evidence_contract_version"] == EVIDENCE_CONTRACT_VERSION

    print("SMOKE TEST 88 — INVENTORY EVIDENCE HANDOFF: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
