from __future__ import annotations
# scripts/smoke_tests/10_19/16_pipeline_runner.py

import argparse

from regime.artifacts import RegimeArtifactStore


DEFAULT_RUN_ID = "baseline_raw_pct_equal_supply_weights"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify a persisted regime pipeline run."
    )
    parser.add_argument(
        "--run-id",
        default=DEFAULT_RUN_ID,
        help="Persisted regime run to verify.",
    )
    return parser.parse_args()


REQUIRED_PIPELINE_ARTIFACTS = [
    "features",
    "normalized_features",
    "metric_scores",
    "aligned_metric_scores",
    "dimension_scores",
    "axis_scores",
    "coordinates",
    "geometry",
    "regime_assignments",
]


REQUIRED_VALIDATION_ARTIFACTS = [
    "historical_trajectory",
    "transition_events",
    "transition_audit",
    "seasonality_transition_counts_by_month",
    "seasonality_transition_calendar",
    "seasonality_monthly_movement",
    "seasonality_monthly_diagnostics",
    "supply_contribution_metric_summary",
    "supply_contribution_by_geo_metric",
    "supply_contribution_transition_metric_events",
    "supply_contribution_top_metric_events",
]


def main() -> int:
    args = parse_args()
    run_id = args.run_id

    store = RegimeArtifactStore()

    manifest = store.read_manifest(run_id)

    print("[pipeline_runner] run_id:", manifest["run_id"])
    print("[pipeline_runner] experiment_id:", manifest["experiment_id"])
    print("[pipeline_runner] status:", manifest["status"])
    print("[pipeline_runner] artifact count:", len(manifest["artifacts"]))

    if manifest["status"] != "complete":
        raise AssertionError(
            f"Expected complete run, found {manifest['status']!r}"
        )

    for artifact_name in REQUIRED_PIPELINE_ARTIFACTS:
        if not store.artifact_exists(run_id, artifact_name):
            raise AssertionError(
                f"Missing pipeline artifact: {artifact_name}"
            )

    for artifact_name in REQUIRED_VALIDATION_ARTIFACTS:
        if not store.artifact_exists(
            run_id,
            artifact_name,
            validation=True,
        ):
            raise AssertionError(
                f"Missing validation artifact: {artifact_name}"
            )

    verification = store.verify_run(run_id)

    print("\n[pipeline_runner] verification:")
    print(verification.to_string(index=False))

    if not verification["exists"].all():
        raise AssertionError("One or more artifacts are missing")

    if not verification["hash_matches"].all():
        raise AssertionError("One or more artifact hashes failed")

    regimes = store.read_dataframe(
        run_id,
        "regime_assignments",
    )

    trajectory = store.read_dataframe(
        run_id,
        "historical_trajectory",
        validation=True,
    )

    print("\n[pipeline_runner] regime assignments:", len(regimes))
    print("[pipeline_runner] validation trajectory:", len(trajectory))
    print("[pipeline_runner] geos:", regimes["geo_id"].nunique())
    print(
        "[pipeline_runner] date range:",
        regimes["date"].min(),
        "→",
        regimes["date"].max(),
    )

    print("\n[pipeline_runner] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
