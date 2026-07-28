from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(
    0,
    str(Path(__file__).resolve().parents[3]),
)

import pandas as pd

from regime.review import (
    DecisionSummary,
    ReviewArtifactWriter,
    ReviewBundle,
    ReviewManifest,
)


OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/"
    "_smoke_review_framework_contracts"
)


def _assert_raises(
    expected_exception: type[BaseException],
    func,
) -> None:
    try:
        func()
    except expected_exception:
        return

    raise AssertionError(
        f"Expected {expected_exception.__name__}"
    )


def main() -> int:
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    writer = ReviewArtifactWriter(OUTPUT_DIR)
    writer.prepare()

    bundle = ReviewBundle(
        campaign_id="review_framework_contracts_smoke"
    )

    bundle.add_table(
        "coverage",
        pd.DataFrame(
            {
                "geo_id": [
                    "district_of_columbia_dc__county",
                ],
                "coverage_ratio": [1.0],
            }
        ),
    )
    bundle.add_table(
        "candidate_summary",
        pd.DataFrame(
            {
                "candidate_id": ["baseline", "challenger"],
                "mean_score": [0.10, 0.15],
            }
        ),
    )
    bundle.add_plot(
        "chronology",
        "plots/chronology.png",
        section="Chronology",
    )

    _assert_raises(
        ValueError,
        lambda: bundle.add_table(
            "coverage",
            pd.DataFrame({"x": [1]}),
        ),
    )

    for table in bundle.tables:
        writer.write_table(
            table.name,
            table.dataframe,
            subdir=table.subdirectory,
        )

    outputs = writer.build_output_manifest()

    manifest = ReviewManifest(
        schema_version="1.0",
        campaign_id=bundle.campaign_id,
        run_id="review_framework_contracts_smoke_001",
        created_at=datetime.now(
            timezone.utc
        ).isoformat(),
        framework_version="8a.2",
        source_run_id="macro_regime_v1_baseline",
        challenger_run_id="macro_regime_v1_challenger",
        outputs=outputs,
        metadata={
            "plot_contract_count": len(bundle.plots),
            "table_contract_count": len(bundle.tables),
        },
    )

    manifest_round_trip = ReviewManifest.from_dict(
        manifest.to_dict()
    )

    if manifest_round_trip != manifest:
        raise AssertionError(
            "ReviewManifest round trip failed"
        )

    manifest.write(writer)

    decision = DecisionSummary(
        recommendation="needs_review",
        rationale=(
            "Framework contract smoke test does not "
            "make a production promotion decision."
        ),
        reviewer=None,
        approved=False,
        notes=[
            "Contract serialization validated.",
            "Artifact paths validated.",
        ],
        metadata={
            "campaign_id": bundle.campaign_id,
        },
    )

    decision_round_trip = DecisionSummary.from_dict(
        decision.to_dict()
    )

    if decision_round_trip != decision:
        raise AssertionError(
            "DecisionSummary round trip failed"
        )

    decision.write(writer)

    written_manifest = json.loads(
        (
            OUTPUT_DIR / "manifest.json"
        ).read_text(encoding="utf-8")
    )
    written_decision = json.loads(
        (
            OUTPUT_DIR / "decision_summary.json"
        ).read_text(encoding="utf-8")
    )

    if (
        written_manifest["campaign_id"]
        != bundle.campaign_id
    ):
        raise AssertionError(
            "Written manifest campaign_id differs"
        )

    if (
        written_decision["recommendation"]
        != "needs_review"
    ):
        raise AssertionError(
            "Written decision recommendation differs"
        )

    _assert_raises(
        ValueError,
        lambda: DecisionSummary(
            recommendation="needs_review",
            rationale="Invalid approved state.",
            approved=True,
        ),
    )

    final_outputs = writer.build_output_manifest(
        exclude_names=(
            "manifest.json",
            "decision_summary.json",
        )
    )

    if len(final_outputs) != 2:
        raise AssertionError(
            "Unexpected persisted table count: "
            f"{len(final_outputs)}"
        )

    print("=" * 100)
    print(
        "SMOKE TEST 73 — REVIEW FRAMEWORK "
        "CONTRACTS: PASS"
    )
    print("=" * 100)
    print(f"campaign_id={bundle.campaign_id}")
    print(f"tables={len(bundle.tables)}")
    print(f"plots={len(bundle.plots)}")
    print(f"artifacts={len(final_outputs) + 2}")
    print(f"output_dir={OUTPUT_DIR}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
