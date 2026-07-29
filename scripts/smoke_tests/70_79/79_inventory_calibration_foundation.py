"""Smoke Test 79: Inventory calibration campaign foundation."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from regime.review import (
    DecisionSummary,
    GeneratedPlot,
    ReviewArtifactWriter,
    ReviewManifest,
    ReviewResult,
    validate_review_package,
    validate_review_zip,
    write_review_bundle,
)
from regime.review.calibration import (
    CalibrationCampaign,
    PromotionGate,
    PromotionGateResult,
    build_inventory_calibration_campaign,
)
from regime.review.calibration.inventory_campaign import assemble_review_results


def _expect_value_error(action) -> None:
    try:
        action()
    except ValueError:
        return
    raise AssertionError("Expected ValueError")


def main() -> int:
    campaign = build_inventory_calibration_campaign(
        campaign_id="inventory_calibration_foundation_v1",
        campaign_version="1.0",
        baseline_run_id="macro_regime_v1_bps120_sources",
        incumbent_run_id="macro_regime_v1_bps120_sources",
        manual_geo_ids=("district_of_columbia_dc__county",),
    )
    assert campaign.candidate_policy_ids == (
        "inventory_ma3_structural",
        "inventory_ma6_structural",
        "inventory_ma9_structural",
        "inventory_ma12_structural",
    )
    json.dumps(campaign.to_dict(), sort_keys=True)
    _expect_value_error(lambda: CalibrationCampaign(**{
        **campaign.to_dict(), "candidate_policy_ids": ("duplicate", "duplicate")
    }))
    for non_finite in (float("nan"), float("inf"), float("-inf")):
        _expect_value_error(lambda value=non_finite: CalibrationCampaign(**{
            **campaign.to_dict(), "metadata": {"invalid": value}
        }))
    _expect_value_error(lambda: CalibrationCampaign(**{
        **campaign.to_dict(), "campaign_phase": "unsupported"
    }))
    _expect_value_error(lambda: CalibrationCampaign(**{
        **campaign.to_dict(), "metadata": {1: "collision", "1": "value"}
    }))

    gate = PromotionGate(
        gate_id="fixture_contract_completeness",
        gate_version="1.0",
        title="Fixture contract completeness",
        description="Confirms that fixture evidence can be packaged.",
        campaign_phase="phase_a",
        severity="informational",
        required_evidence=("candidate_contracts",),
        evaluation_scope={"metric": "active_inventory"},
    )
    normalized_gate = PromotionGate(**{
        **gate.to_dict(), "campaign_phase": " PHASE_A ",
        "severity": " INFORMATIONAL ",
    })
    assert normalized_gate.campaign_phase == "phase_a"
    assert normalized_gate.severity == "informational"
    _expect_value_error(lambda: PromotionGate(**{
        **gate.to_dict(), "campaign_phase": "unsupported"
    }))
    _expect_value_error(lambda: PromotionGate(**{
        **gate.to_dict(), "severity": "unsupported"
    }))
    _expect_value_error(lambda: PromotionGate(**{
        **gate.to_dict(), "required_evidence": ("same", "same")
    }))
    _expect_value_error(lambda: PromotionGate(**{
        **gate.to_dict(), "thresholds": {"invalid": float("nan")}
    }))
    results = [
        PromotionGateResult(
            gate_id=gate.gate_id,
            gate_version=gate.gate_version,
            candidate_policy_id=candidate,
            status="not_evaluated",
            severity=gate.severity,
            measured_values={},
            thresholds={},
            evaluation_scope=gate.evaluation_scope,
            evidence_references=("candidate_contracts",),
            rationale="Analytical evidence and thresholds are not yet implemented.",
        )
        for candidate in campaign.candidate_policy_ids
    ]
    json.dumps([result.to_dict() for result in results], sort_keys=True)
    result_payload = results[0].to_dict()
    normalized_result = PromotionGateResult(**{
        **result_payload, "status": " NOT_EVALUATED ",
        "severity": " INFORMATIONAL ",
    })
    assert normalized_result.status == "not_evaluated"
    assert normalized_result.severity == "informational"
    _expect_value_error(lambda: PromotionGateResult(**{
        **result_payload, "status": "unsupported"
    }))
    _expect_value_error(lambda: PromotionGateResult(**{
        **result_payload, "severity": "unsupported"
    }))
    _expect_value_error(lambda: PromotionGateResult(**{
        **result_payload, "evidence_references": ("same", "same")
    }))
    _expect_value_error(lambda: PromotionGateResult(**{
        **result_payload, "exceptions": ("same", "same")
    }))

    contracts = ReviewResult()
    contract_frame = pd.DataFrame({
        "candidate_policy_id": list(campaign.candidate_policy_ids),
        "target_metric": campaign.target_metric,
        "transform_strategy": "ma_structural",
    })
    contracts.add_table("candidate_contracts", contract_frame)
    gates = ReviewResult()
    gates.add_table("promotion_gate_results", pd.DataFrame([item.to_dict() for item in results]))
    contracts.add_plot(GeneratedPlot(name="contract_plot", path=Path("plots/contract.png")))
    original = contract_frame.copy(deep=True)
    original_plots = list(contracts.plots)
    bundle = assemble_review_results(campaign.campaign_id, {"gates": gates, "contracts": contracts})
    repeated = assemble_review_results(campaign.campaign_id, {"contracts": contracts, "gates": gates})
    pd.testing.assert_frame_equal(contract_frame, original)
    assert contracts.plots == original_plots
    assert [table.subdirectory for table in bundle.tables] == ["tables/contracts", "tables/gates"]
    assert [table.subdirectory for table in repeated.tables] == [table.subdirectory for table in bundle.tables]
    assert [plot.name for plot in repeated.plots] == [plot.name for plot in bundle.plots]

    duplicate = ReviewResult(tables={"candidate_contracts": pd.DataFrame({"x": [1]})})
    _expect_value_error(lambda: assemble_review_results(
        campaign.campaign_id, {"one": contracts, "two": duplicate}
    ))
    for unsafe_name in ("../escape", "nested/path", ".", "has space", ""):
        _expect_value_error(lambda name=unsafe_name: assemble_review_results(
            campaign.campaign_id, {name: contracts}
        ))
    duplicate_plot = ReviewResult(plots=[
        GeneratedPlot(name="contract_plot", path=Path("plots/other.png"))
    ])
    _expect_value_error(lambda: assemble_review_results(
        campaign.campaign_id, {"one": contracts, "two": duplicate_plot}
    ))

    with tempfile.TemporaryDirectory(prefix="inventory-calibration-foundation-") as temp:
        root = Path(temp)
        writer = ReviewArtifactWriter(root / "review_package")
        package_bundle = assemble_review_results(
            campaign.campaign_id,
            {
                "contracts": ReviewResult(tables=contracts.tables),
                "gates": gates,
            },
        )
        manifest = ReviewManifest(
            schema_version="1.0",
            campaign_id=campaign.campaign_id,
            run_id="inventory_calibration_foundation_fixture_001",
            created_at=datetime(2026, 7, 29, tzinfo=timezone.utc).isoformat(),
            framework_version="8c-foundation",
            source_run_id=campaign.incumbent_run_id,
            metadata=campaign.to_dict(),
        )
        decision = DecisionSummary(
            recommendation="needs_review",
            rationale="Analytical evidence and thresholds are not yet implemented; no production candidate has been selected.",
            metadata={"fixture_gate": gate.to_dict()},
        )
        write_review_bundle(bundle=package_bundle, writer=writer, manifest=manifest, decision=decision)
        directory = validate_review_package(writer.output_dir)
        zip_path = writer.write_zip(root / "review_package.zip")
        archive = validate_review_zip(zip_path)
        assert directory.members == archive.members
        assert json.loads((writer.output_dir / "decision_summary.json").read_text())["recommendation"] == "needs_review"
        print(f"fixture_artifacts={root} (temporary; cleaned on exit)")

    print("SMOKE TEST 79 — INVENTORY CALIBRATION FOUNDATION: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
