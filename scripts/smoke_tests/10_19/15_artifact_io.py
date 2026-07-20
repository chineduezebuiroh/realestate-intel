from __future__ import annotations
# scripts/smoke_tests/10_19/15_artifact_io.py

import shutil
from pathlib import Path

import pandas as pd

from regime.artifacts import RegimeArtifactStore


TEST_ROOT = Path("artifacts/regime/_smoke_test")
TEST_RUN_ID = "artifact_io_smoke_test"


def main() -> int:
    if TEST_ROOT.exists():
        shutil.rmtree(TEST_ROOT)

    store = RegimeArtifactStore(TEST_ROOT)

    manifest = store.initialize_run(
        TEST_RUN_ID,
        experiment_id="artifact_io_test",
        metadata={
            "purpose": "Validate regime artifact writer and reader",
        },
    )

    print("[artifact_io] initialized manifest:")
    print(manifest)

    pipeline_df = pd.DataFrame(
        {
            "geo_id": [
                "district_of_columbia_dc__county",
                "alameda_county_ca__county",
            ],
            "date": pd.to_datetime(
                [
                    "2026-05-31",
                    "2026-05-31",
                ]
            ),
            "axis_score": [0.125, -0.250],
        }
    )

    validation_df = pd.DataFrame(
        {
            "geo_id": [
                "district_of_columbia_dc__county",
                "alameda_county_ca__county",
            ],
            "date": pd.to_datetime(
                [
                    "2026-05-31",
                    "2026-05-31",
                ]
            ),
            "transition_count": [4, 3],
        }
    )

    pipeline_metadata = store.write_dataframe(
        TEST_RUN_ID,
        "axis_scores",
        pipeline_df,
    )

    validation_metadata = store.write_dataframe(
        TEST_RUN_ID,
        "transition_audit",
        validation_df,
        validation=True,
    )

    print("\n[artifact_io] pipeline metadata:")
    print(pipeline_metadata)

    print("\n[artifact_io] validation metadata:")
    print(validation_metadata)

    pipeline_read = store.read_dataframe(
        TEST_RUN_ID,
        "axis_scores",
    )

    validation_read = store.read_dataframe(
        TEST_RUN_ID,
        "transition_audit",
        validation=True,
    )

    pd.testing.assert_frame_equal(
        pipeline_read,
        pipeline_df,
        check_dtype=True,
    )

    pd.testing.assert_frame_equal(
        validation_read,
        validation_df,
        check_dtype=True,
    )

    store.update_manifest(
        TEST_RUN_ID,
        status="complete",
        metadata_updates={
            "smoke_test_passed": True,
        },
    )

    final_manifest = store.read_manifest(TEST_RUN_ID)

    print("\n[artifact_io] final manifest:")
    print(final_manifest)

    verification = store.verify_run(TEST_RUN_ID)

    print("\n[artifact_io] verification:")
    print(verification.to_string(index=False))

    if verification.empty:
        raise AssertionError("Expected recorded artifacts")

    if not verification["exists"].all():
        raise AssertionError("One or more artifacts are missing")

    if not verification["hash_matches"].all():
        raise AssertionError("One or more artifact hashes do not match")

    runs = store.list_runs()

    print("\n[artifact_io] listed runs:")
    print(runs.to_string(index=False))

    if TEST_RUN_ID not in set(runs["run_id"]):
        raise AssertionError("Smoke-test run was not listed")

    shutil.rmtree(TEST_ROOT)

    print("\n[artifact_io] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
