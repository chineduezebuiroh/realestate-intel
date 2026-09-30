"""Narrow preparation boundary for the August 2026 FRED-unemployment correction.

The module intentionally has no provider client, publication adapter, or CLI
that can mutate durable state.  A later production change may wrap the pure
recovery primitive in the hosted JSON/blob CAS loop after real artifacts exist.
"""
from __future__ import annotations

from typing import Any, Mapping

from core.source_artifacts.forward_correction import (create_correction_record,
    preflight_correction)
from core.source_artifacts.hashing import sha256_json
from core.source_artifacts.publication import PublicationError

CYCLE_ID = "monthly_cycle__2026-08__a9e022a980d29cd7"
ACCEPTED_SOURCE_SET = "source_set__2026-08__v2__fde2413d93948bee"
ACCEPTED_CANONICAL = "market__2026-08__r1__cfe00e7ec9a9be8e"
ORIGINAL_PROMOTION = "cohort_promotion__1ea61a82ff16b1e492ba0f6f"
ACCEPTED_SOURCES = {
    "fred_macro": "src__fred_macro__2026-09__r1__29a8ac35a657cfce",
    "ces": "src__ces__2026-08__r1__c39c12b32234bd93",
    "laus": "src__laus__2026-07__r1__a1b60a2a16d2b99f",
    "redfin": "src__redfin__2026-08__r1__f2ca39c3c36a9c2b",
    "bps": "src__bps__2026-08__r1__a25be34c54e12251",
    "acs": "src__acs__2024-12__r1__366cf814efe424ef",
    "bea_gdp_qtr": "src__bea_gdp_qtr__2026-03__r1__9290d93e61b8e5dd",
    "bea_gdp_ann": "src__bea_gdp_ann__2024-12__r1__1c51a5b7a95bdc27",
    "census_nrc": "src__census_nrc__2026-08__r2__569f6385f5eb6d72",
}


def prepare(*, catalog: dict[str, Any], readiness: dict[str, Any],
        original_promotion: dict[str, Any], fred_unemp_artifact_id: str,
        corrected_source_set_id: str, corrected_source_set_hash: str,
        corrected_canonical_id: str, corrected_canonical_hash: str,
        family_resolution_lineage: Mapping[str, Any],
        governed_hashes: Mapping[str, str]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return an immutable plan and zero-mutation preflight report."""
    if original_promotion.get("promotion_id") != ORIGINAL_PROMOTION:
        raise PublicationError("not the exact accepted August original promotion")
    accepted = catalog.get("accepted", {})
    if (accepted.get("source_set") != ACCEPTED_SOURCE_SET or
            accepted.get("canonical_market") != ACCEPTED_CANONICAL or
            accepted.get("serving_market") is not None or
            accepted.get("source") != ACCEPTED_SOURCES):
        raise PublicationError("not the exact accepted August correction origin")
    if (not fred_unemp_artifact_id.startswith("src__fred_unemp__") or
            not corrected_source_set_id.startswith("source_set__2026-08__v2__") or
            not corrected_canonical_id.startswith("market__2026-08__r2__")):
        raise PublicationError("August correction target identity shape mismatch")
    readiness_matches = [item for item in readiness.get("records", [])
                         if item.get("cycle_id") == CYCLE_ID]
    if len(readiness_matches) != 1:
        raise PublicationError("exact August readiness does not resolve once")
    readiness_record = readiness_matches[0]
    expected = {**ACCEPTED_SOURCES, "fred_unemp": None}
    target = {**ACCEPTED_SOURCES, "fred_unemp": fred_unemp_artifact_id}
    record = create_correction_record(cycle_id=CYCLE_ID,
        expected_source_set=ACCEPTED_SOURCE_SET, expected_canonical=ACCEPTED_CANONICAL,
        original_promotion_id=ORIGINAL_PROMOTION,
        expected_source_pointers=expected, target_source_pointers=target,
        corrected_source_set=corrected_source_set_id,
        corrected_source_set_hash=corrected_source_set_hash,
        corrected_canonical=corrected_canonical_id,
        corrected_canonical_hash=corrected_canonical_hash,
        readiness_evidence={"readiness_id":readiness_record.get("readiness_id"),
            "record_sha256":sha256_json(readiness_record),
            "consumed_by_promotion_id":ORIGINAL_PROMOTION},
        family_resolution_lineage=family_resolution_lineage,
        governed_hashes=governed_hashes)
    return record, preflight_correction(record, catalog, readiness, original_promotion)
