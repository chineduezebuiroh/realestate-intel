from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import numpy as np
import pandas as pd

from regime.review import (
    GeographySelectionPolicy,
    ReviewArtifactWriter,
    assert_finite,
    assert_no_duplicate_keys,
    assert_non_empty,
    assert_required_columns,
    select_review_geographies,
)


OUTPUT_DIR = Path(
    "artifacts/regime/review_exports/_smoke_review_framework_core"
)


def _assert_raises(expected_exception: type[BaseException], func) -> None:
    try:
        func()
    except expected_exception:
        return
    raise AssertionError(f"Expected {expected_exception.__name__}")


def main() -> int:
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)

    valid = pd.DataFrame(
        {
            "geo_id": [
                "district_of_columbia_dc__county",
                "alpha_county_md__county",
            ],
            "date": pd.to_datetime(["2026-05-31", "2026-05-31"]),
            "score": [0.25, -0.50],
        }
    )

    assert_non_empty(valid, frame_name="valid")
    assert_required_columns(
        valid,
        ("geo_id", "date", "score"),
        frame_name="valid",
    )
    assert_finite(valid, ("score",), frame_name="valid", allow_null=False)
    assert_no_duplicate_keys(
        valid,
        ("geo_id", "date"),
        frame_name="valid",
    )

    _assert_raises(
        AssertionError,
        lambda: assert_required_columns(
            valid,
            ("missing_column",),
            frame_name="valid",
        ),
    )

    duplicates = pd.concat([valid, valid.iloc[[0]]], ignore_index=True)
    _assert_raises(
        AssertionError,
        lambda: assert_no_duplicate_keys(
            duplicates,
            ("geo_id", "date"),
            frame_name="duplicates",
        ),
    )

    non_finite = valid.copy()
    non_finite.loc[0, "score"] = np.inf
    _assert_raises(
        AssertionError,
        lambda: assert_finite(
            non_finite,
            ("score",),
            frame_name="non_finite",
            allow_null=False,
        ),
    )

    candidates = pd.DataFrame(
        {
            "geo_id": [
                "district_of_columbia_dc__county",
                "alpha_county_md__county",
                "beta_county_va__county",
                "gamma_metro__cbsa",
            ],
            "geo_type": ["county", "county", "county", "cbsa_metro"],
            "selection_reason": [
                "highest_candidate_delta",
                "highest_candidate_delta",
                "highest_volatility",
                "highest_candidate_delta",
            ],
            "selection_rank": [99, 1, 2, 1],
            "selection_metric": [
                "candidate_delta",
                "candidate_delta",
                "volatility",
                "candidate_delta",
            ],
            "selection_value": [0.0, 0.9, 0.8, 10.0],
        }
    )

    selected = select_review_geographies(
        candidates,
        policy=GeographySelectionPolicy(max_automatic_geographies=2),
    )

    selected_geo_ids = set(selected["geo_id"])
    if "district_of_columbia_dc__county" not in selected_geo_ids:
        raise AssertionError("Mandatory DC county was not selected")

    automatic = selected[selected["selected_automatically"]]
    if set(automatic["geo_type"]) != {"county"}:
        raise AssertionError(
            "Automatic review selection was not restricted to counties"
        )
    if "gamma_metro__cbsa" in selected_geo_ids:
        raise AssertionError(
            "CBSA was incorrectly selected for targeted review"
        )

    writer = ReviewArtifactWriter(OUTPUT_DIR)
    writer.prepare()
    writer.write_table("selected_geographies", selected)
    writer.write_json("test_payload", {"status": "ok", "rows": len(selected)})

    outputs = writer.build_output_manifest()
    if len(outputs) != 2:
        raise AssertionError(f"Unexpected output count: {len(outputs)}")

    writer.write_manifest(
        {
            "schema_version": "1.0",
            "campaign_id": "review_framework_core_smoke",
            "outputs": outputs,
        }
    )

    written_manifest = json.loads(
        (OUTPUT_DIR / "manifest.json").read_text(encoding="utf-8")
    )
    if written_manifest["campaign_id"] != "review_framework_core_smoke":
        raise AssertionError("Manifest round trip failed")

    print("=" * 100)
    print("SMOKE TEST 72 — REVIEW FRAMEWORK CORE: PASS")
    print("=" * 100)
    print(f"selected_geographies={len(selected)}")
    print(f"artifacts={len(outputs) + 1}")
    print(f"output_dir={OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
