"""Focused smoke coverage for the correlation-only permit diagnostic."""
from pathlib import Path
import hashlib
import tempfile

import numpy as np
import pandas as pd

from regime.diagnostics.permit_redundancy import (
    METRICS, REVIEW_GEOGRAPHIES, build_permit_redundancy_evidence,
    write_permit_redundancy_bundle,
)


def main() -> None:
    policy_paths = tuple(Path("config") / name for name in (
        "feature_registry.csv", "metric_dimension_registry.csv", "axis_registry.csv",
        "normalization_registry.csv", "metric_smoothing_experiments.csv"))
    before = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in policy_paths}
    rows = []
    dates = pd.to_datetime(["2022-01-31", "2022-02-28", "2022-03-31", "2022-04-30"])
    for gi, geo in enumerate(REVIEW_GEOGRAPHIES):
        activity = np.array([-.8, -.2, .4, .9]) + gi * .001
        intensity = activity * .75 + np.array([.1, -.05, .02, -.03])
        for date, left, right in zip(dates, activity, intensity):
            rows.extend((
                {"geo_id": geo, "evaluation_date": date, "canonical_metric_key": METRICS[0], "metric_score": left},
                {"geo_id": geo, "evaluation_date": date, "canonical_metric_key": METRICS[1], "metric_score": right},
            ))
    aligned = pd.DataFrame(rows)
    # Remove one side only. The unmatched date must not enter counts or correlations.
    aligned = aligned[~(
        aligned.geo_id.eq(REVIEW_GEOGRAPHIES[0]) & aligned.evaluation_date.eq(dates[1])
        & aligned.canonical_metric_key.eq(METRICS[1]))].copy()
    evidence = build_permit_redundancy_evidence(aligned)
    assert evidence.review_geographies == REVIEW_GEOGRAPHIES
    correlations = evidence.tables["per_geography_correlations"]
    assert tuple(correlations.geo_id) == REVIEW_GEOGRAPHIES
    assert correlations.iloc[0].observation_count == 3
    assert correlations.iloc[0].first_overlapping_observation == dates[0]
    assert correlations.iloc[0].last_overlapping_observation == dates[-1]
    paired = evidence.tables["paired_chronology"]
    assert not ((paired.geo_id.eq(REVIEW_GEOGRAPHIES[0])) & paired.date.eq(dates[1])).any()
    expected = np.corrcoef(np.delete(np.array([-.8, -.2, .4, .9]), 1),
                           np.delete(np.array([-.8, -.2, .4, .9]) * .75 + np.array([.1, -.05, .02, -.03]), 1))[0, 1]
    assert abs(correlations.iloc[0].pearson_correlation - expected) < 1e-12
    first = build_permit_redundancy_evidence(aligned)
    pd.testing.assert_frame_equal(first.tables["summary"], evidence.tables["summary"])
    with tempfile.TemporaryDirectory() as tmp:
        one, two = Path(tmp) / "one", Path(tmp) / "two"
        _, zip_one, count = write_permit_redundancy_bundle(evidence, one)
        _, zip_two, _ = write_permit_redundancy_bundle(evidence, two)
        assert zip_one.read_bytes() == zip_two.read_bytes()
        assert count == 11
        assert len(tuple((one / "figures").glob("*.svg"))) == len(REVIEW_GEOGRAPHIES)
        assert '"promotion_state": "none"' in (one / "manifest.json").read_text()
    after = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in policy_paths}
    assert before == after
    print("SMOKE TEST 95 — PERMIT REDUNDANCY DIAGNOSTIC: PASS")


if __name__ == "__main__":
    main()
