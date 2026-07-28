from __future__ import annotations

import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from regime.review import (
    DecisionSummary,
    ReviewArtifactWriter,
    ReviewBundle,
    ReviewManifest,
    write_review_bundle,
)

OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/"
    "_smoke_review_bundle_orchestration"
)


def main():

    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    writer = ReviewArtifactWriter(OUTPUT_DIR)

    bundle = ReviewBundle(
        campaign_id="bundle_smoke"
    )

    bundle.add_table(
        "coverage",
        pd.DataFrame(
            {
                "geo_id": ["dc"],
                "score": [1.0],
            }
        ),
    )

    bundle.add_plot(
        "chronology",
        "plots/chronology.png",
    )

    manifest = ReviewManifest(
        schema_version="1.0",
        campaign_id=bundle.campaign_id,
        run_id="bundle_smoke_001",
        created_at=datetime.now(
            timezone.utc
        ).isoformat(),
        framework_version="8a.3",
        source_run_id="baseline",
    )

    decision = DecisionSummary(
        recommendation="needs_review",
        rationale="Smoke test.",
    )

    write_review_bundle(
        bundle=bundle,
        writer=writer,
        manifest=manifest,
        decision=decision,
    )

    assert (OUTPUT_DIR / "manifest.json").exists()
    assert (OUTPUT_DIR / "decision_summary.json").exists()
    assert (
        OUTPUT_DIR
        / "tables"
        / "coverage.csv"
    ).exists()

    assert len(manifest.outputs) == 2

    print("=" * 100)
    print(
        "SMOKE TEST 74 — REVIEW BUNDLE ORCHESTRATION: PASS"
    )
    print("=" * 100)
    print(f"tables={bundle.table_count}")
    print(f"plots={bundle.plot_count}")
    print(f"outputs={len(manifest.outputs)}")
    print(f"output_dir={OUTPUT_DIR}")


if __name__ == "__main__":
    main()