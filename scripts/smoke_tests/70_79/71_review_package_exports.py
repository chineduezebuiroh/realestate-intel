"""
Smoke Test 71
Review package public API contract.
"""

from regime.review import (
    DEFAULT_MANDATORY_REVIEW_GEOS,
    DecisionRecommendation,
    DecisionSummary,
    GeographySelectionPolicy,
    ReviewArtifactWriter,
    ReviewBundle,
    ReviewManifest,
    ReviewPlot,
    ReviewResult,
    ReviewTable,
    context_review_geographies,
    assert_expected_values,
    assert_finite,
    assert_no_duplicate_keys,
    assert_non_empty,
    assert_required_columns,
    assert_same_dates,
    assert_same_geographies,
    select_review_geographies,
    sha256_file,
    write_csv,
    write_json,
    write_review_bundle,
    GeneratedPlot,
)

print("=" * 100)
print("SMOKE TEST 71 — REVIEW PACKAGE EXPORTS: PASS")
print("=" * 100)
print(f"mandatory_geos={len(DEFAULT_MANDATORY_REVIEW_GEOS)}")
print(f"exports={23}")