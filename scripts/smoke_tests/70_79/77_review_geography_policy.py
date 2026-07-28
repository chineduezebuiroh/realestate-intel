"""
SMOKE TEST 77
Review Geography Policy
"""

import pandas as pd

from regime.review import (
    GeographySelectionPolicy,
    select_review_geographies,
)

candidates = pd.DataFrame(
    [
        {
            "geo_id": "district_of_columbia_dc__county",
            "geo_type": "county",
            "selection_reason": "highest_candidate_delta",
            "selection_rank": 0,
            "selection_metric": "delta",
            "selection_value": 10,
        },
        {
            "geo_id": "montgomery_county_md__county",
            "geo_type": "county",
            "selection_reason": "highest_candidate_delta",
            "selection_rank": 1,
            "selection_metric": "delta",
            "selection_value": 9,
        },
        {
            "geo_id": "washington_arlington_alexandria_dc_va_md_wv__cbsa_metro",
            "geo_type": "cbsa_metro",
            "selection_reason": "coverage_exception",
            "selection_rank": 2,
            "selection_metric": "delta",
            "selection_value": 1,
        },
    ]
)

selection = select_review_geographies(
    candidates,
    policy=GeographySelectionPolicy(),
)

selected = selection.selected_geographies

assert (
    "district_of_columbia_dc__county"
    in selected.geo_id.values
)

assert (
    "montgomery_county_md__county"
    in selected.geo_id.values
)

assert (
    "washington_arlington_alexandria_dc_va_md_wv__cbsa_metro"
    in selection.aggregate_geographies.geo_id.values
)

assert len(selection.rationale) == len(selected)

assert (
    selection.metadata["mandatory_geo_ids"]
    == ["district_of_columbia_dc__county"]
)

print("=" * 100)
print("SMOKE TEST 77 — REVIEW GEOGRAPHY POLICY: PASS")
print("=" * 100)
print(f"selected={len(selected)}")
print(f"context={len(selection.aggregate_geographies)}")
print(f"rationale={len(selection.rationale)}")