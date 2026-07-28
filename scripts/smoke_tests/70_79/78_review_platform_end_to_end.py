"""Smoke Test 78: deterministic review-platform package closeout."""

from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import pandas as pd

from regime.review import (
    DecisionSummary,
    GeographySelectionPolicy,
    ReviewArtifactWriter,
    ReviewBundle,
    ReviewManifest,
    ReviewPackageValidationError,
    select_review_geographies,
    validate_review_package,
    validate_review_zip,
    write_review_bundle,
)


def _assert_validation_fails(action) -> None:
    try:
        action()
    except ReviewPackageValidationError:
        return
    raise AssertionError("Expected ReviewPackageValidationError")


def main() -> int:
    print("[review_e2e] constructing deterministic campaign")
    candidates = pd.DataFrame(
        [
            ("district_of_columbia_dc__county", "county", 99, 0.0),
            ("montgomery_county_md__county", "county", 1, 0.8),
            ("fairfax_county_va__county", "county", 2, 0.7),
            ("washington_dc__cbsa_metro", "cbsa_metro", 1, 1.0),
        ],
        columns=["geo_id", "geo_type", "selection_rank", "selection_value"],
    )
    candidates["selection_reason"] = "highest_candidate_delta"
    candidates["selection_metric"] = "candidate_delta"
    selection = select_review_geographies(
        candidates,
        policy=GeographySelectionPolicy(max_automatic_geographies=1),
        manual_geo_ids=("fairfax_county_va__county",),
    )
    assert set(selection.selected_geographies["geo_id"]) == {
        "district_of_columbia_dc__county",
        "montgomery_county_md__county",
        "fairfax_county_va__county",
    }
    assert selection.selected_geographies["selected_manually"].sum() == 1

    with tempfile.TemporaryDirectory(prefix="review-platform-e2e-") as temp:
        root = Path(temp)
        package_dir = root / "review_export"
        writer = ReviewArtifactWriter(package_dir)
        bundle = ReviewBundle(campaign_id="section_8b_closeout_smoke")
        bundle.add_table(
            "candidate_roles",
            pd.DataFrame(
                {
                    "role": ["baseline", "incumbent", "challenger"],
                    "run_id": ["baseline_v1", "incumbent_v1", "challenger_v2"],
                }
            ),
        )
        manifest = ReviewManifest(
            schema_version="1.0",
            campaign_id=bundle.campaign_id,
            run_id="section_8b_closeout_smoke_001",
            created_at=datetime(2026, 7, 28, tzinfo=timezone.utc).isoformat(),
            framework_version="8b",
            source_run_id="incumbent_v1",
            challenger_run_id="challenger_v2",
            metadata={
                "baseline_run_id": "baseline_v1",
                "incumbent_run_id": "incumbent_v1",
                "campaign_type": "calibration_review",
            },
        )
        decision = DecisionSummary(
            recommendation="needs_review",
            rationale="Deterministic closeout smoke; no promotion decision.",
            metadata={"campaign_id": bundle.campaign_id},
        )

        print("[review_e2e] orchestrating package export")
        write_review_bundle(
            bundle=bundle,
            writer=writer,
            manifest=manifest,
            decision=decision,
            geography_selection=selection,
        )
        directory_result = validate_review_package(package_dir)
        assert directory_result.manifest.campaign_id == bundle.campaign_id
        assert directory_result.hashes_verified == len(manifest.outputs)

        print("[review_e2e] creating and validating ZIP bundle")
        zip_path = writer.write_zip(root / "review_export.zip")
        zip_result = validate_review_zip(zip_path)
        assert zip_result.members == directory_result.members
        assert zip_result.hashes_verified == directory_result.hashes_verified

        table_path = package_dir / "tables" / "candidate_roles.csv"
        original_table = table_path.read_bytes()
        table_path.write_bytes(original_table + b"tampered")
        _assert_validation_fails(lambda: validate_review_package(package_dir))
        table_path.write_bytes(original_table)

        repository_root = Path(__file__).resolve().parents[3]
        assert not package_dir.resolve().is_relative_to(repository_root)
        assert not zip_path.resolve().is_relative_to(repository_root)
        print(f"[review_e2e] validated_members={len(zip_result.members)}")
        print(f"[review_e2e] verified_hashes={zip_result.hashes_verified}")

    print("=" * 100)
    print("SMOKE TEST 78 — REVIEW PLATFORM END TO END: PASS")
    print("=" * 100)
    print("temporary_outputs_cleaned=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
