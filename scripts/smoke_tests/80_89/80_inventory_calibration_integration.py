"""Smoke Test 80: artifact-backed Inventory campaign integration."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from regime.artifacts import DEFAULT_ARTIFACT_ROOT, RegimeArtifactStore
from regime.experiments.in_memory_challenger import build_in_memory_smoothing_challenger
from regime.review.calibration import build_inventory_calibration_campaign


RUN_ID = "macro_regime_v1_bps120_sources"
TARGET_KEYS = {
    "redfin_inventory_level",
    "redfin_inventory_short",
    "redfin_inventory_long",
}


def main() -> int:
    campaign = build_inventory_calibration_campaign(
        campaign_id="inventory_calibration_integration_v1",
        campaign_version="1.0",
        baseline_run_id=RUN_ID,
        incumbent_run_id=RUN_ID,
    )
    if not (Path(DEFAULT_ARTIFACT_ROOT) / RUN_ID).is_dir():
        print("SMOKE TEST 80 — SKIP: authoritative baseline artifact directory is unavailable")
        return 0

    store = RegimeArtifactStore()
    source = store.read_dataframe(RUN_ID, "source_metrics")
    baseline = store.read_dataframe(RUN_ID, "features")
    baseline_snapshot = baseline.copy(deep=True)
    challenger = build_in_memory_smoothing_challenger(
        baseline_features=baseline,
        source_metrics=source,
        experiment_id=campaign.candidate_policy_ids[0],
    )
    assert not challenger.smoothing_lineage.empty
    assert TARGET_KEYS.issubset(set(challenger.features["feature_key"]))
    key_columns = [column for column in ("geo_id", "date", "feature_key") if column in baseline]
    non_target_before = baseline[~baseline["feature_key"].isin(TARGET_KEYS)].sort_values(key_columns).reset_index(drop=True)
    non_target_after = challenger.features[~challenger.features["feature_key"].isin(TARGET_KEYS)].sort_values(key_columns).reset_index(drop=True)
    pd.testing.assert_frame_equal(non_target_before, non_target_after)
    pd.testing.assert_frame_equal(baseline, baseline_snapshot)
    assert campaign.metadata["resolved_candidate_ids"]["ma3"] == campaign.candidate_policy_ids[0]
    print("SMOKE TEST 80 — INVENTORY CALIBRATION INTEGRATION: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
