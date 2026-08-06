"""Validate the post-merge immutable frozen-Supply production run."""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from regime._05_dimension_scorer import score_dimensions
from regime.artifacts import RegimeArtifactStore

RUN = "macro_regime_v1_frozen_supply_20260806"
EXPERIMENT = "supply_metric_weight_promotion_2026_08_06"
COUNTIES = {
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
}
DESCENDANTS = ("axis_scores", "coordinates", "geometry", "regime_assignments")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--artifact-root", type=Path, required=True)
    p.add_argument("--run-id", default=RUN)
    p.add_argument("--prior-run-id", required=True)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.run_id != RUN or args.run_id == args.prior_run_id:
        raise ValueError("Run identity is not the governed new immutable identity")
    store = RegimeArtifactStore(args.artifact_root)
    if not store.run_exists(args.prior_run_id):
        raise FileNotFoundError("Prior settled-MA12 run is required for comparison")
    manifest = store.read_manifest(args.run_id)
    assert manifest["status"] == "complete"
    assert manifest["run_id"] == RUN and manifest["experiment_id"] == EXPERIMENT
    metadata = manifest["metadata"]
    assert metadata.get("config_hashes") and metadata.get("ma_transform_policy_snapshot")
    assert metadata.get("smoothing_experiment_id") is None
    assert metadata["promotion_contract"] == EXPERIMENT
    assert metadata["supply_freeze_contract"] == "supply_dimension_frozen_v1"
    verification = store.verify_run(args.run_id)
    assert not verification.empty and verification.exists.all() and verification.hash_matches.all()

    aligned = store.read_dataframe(args.run_id, "aligned_metric_scores")
    persisted = store.read_dataframe(args.run_id, "dimension_scores")
    rebuilt = score_dimensions(aligned)
    supply = persisted[persisted.dimension.eq("supply")]
    assert COUNTIES <= set(supply.geo_id)
    keys = ["geo_id", "date", "dimension"]
    check = supply.merge(rebuilt[rebuilt.dimension.eq("supply")], on=keys, suffixes=("_persisted", "_rebuilt"), validate="one_to_one")
    assert np.allclose(check.dimension_score_persisted, check.dimension_score_rebuilt, equal_nan=True)
    sums = set(np.round(check.metric_weight_sum_rebuilt.astype(float), 10))
    assert sums <= {0.2, 0.4, 0.6, 0.8, 1.0} and 1.0 in sums

    features = store.read_dataframe(args.run_id, "features")
    expected = {"redfin_inventory_level", "redfin_inventory_short", "redfin_inventory_long", "bps_total_units_level", "bps_total_units_short", "bps_total_units_long", "permit_intensity_level", "permit_intensity_short", "permit_intensity_long"}
    assert expected <= set(features.feature_key)

    prior_dimensions = store.read_dataframe(args.prior_run_id, "dimension_scores")
    non_supply = persisted[~persisted.dimension.eq("supply")].sort_values(keys).reset_index(drop=True)
    prior_non_supply = prior_dimensions[~prior_dimensions.dimension.eq("supply")].sort_values(keys).reset_index(drop=True)
    pd.testing.assert_frame_equal(non_supply, prior_non_supply, check_dtype=False)
    prior_manifest = store.read_manifest(args.prior_run_id)
    for name in DESCENDANTS:
        store.read_dataframe(args.run_id, name)  # hash-verified and present
        new_hash = manifest["artifacts"][name]["sha256"]
        old_hash = prior_manifest["artifacts"][name]["sha256"]
        assert new_hash != old_hash, f"{name} was not recomputed as a changed descendant"
    print("[validate_frozen_supply_run] OK")


if __name__ == "__main__":
    main()
