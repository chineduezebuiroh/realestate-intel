from __future__ import annotations

import pytest

from core.source_artifacts.promotion import OPERATION_ORDER, create_promotion_record
from core.source_artifacts.publication import PublicationError
from jobs.monthly_refresh.cohort_promotion import (
    LOGICAL_COHORT_SOURCES,
    PHYSICAL_FAMILY_SOURCES,
)


def promotion_targets() -> dict[str, str]:
    return {source: f"src__{source}__2026-07__r1__{'a' * 16}"
            for source in sorted(LOGICAL_COHORT_SOURCES)}


def promotion(targets: dict[str, str]) -> dict:
    return create_promotion_record(
        cycle_id="monthly_cycle__2026-07__fixture",
        source_set_id="source_set__2026-07__v2__" + "b" * 16,
        source_set_semantic_sha256="c" * 64,
        canonical_artifact_id="market__2026-07__r1__" + "d" * 16,
        canonical_artifact_hash="e" * 64,
        expected_source_pointers={source: None for source in targets},
        target_source_pointers=targets,
        expected_source_set=None,
        expected_canonical=None,
        readiness_id="redfin-readiness-fixture",
        resolution_id="family-resolution-fixture",
    )


def test_acs_is_one_logical_cohort_member_and_physical_products_are_not_members():
    assert LOGICAL_COHORT_SOURCES == {
        "acs", "bea_gdp_ann", "bea_gdp_qtr", "bps", "census_nrc", "ces",
        "fred_macro", "fred_unemp", "laus", "redfin"
    }
    assert {"census_acs1", "census_acs5"}.isdisjoint(LOGICAL_COHORT_SOURCES)
    assert {"census_acs1", "census_acs5"}.issubset(PHYSICAL_FAMILY_SOURCES)


def test_acceptance_plan_targets_logical_acs_and_preserves_shared_order():
    record = promotion(promotion_targets())
    assert record["target_source_pointers"]["acs"].startswith("src__acs__")
    assert PHYSICAL_FAMILY_SOURCES.isdisjoint(record["target_source_pointers"])
    assert tuple(record["operation_order"]) == OPERATION_ORDER
    assert record["operation_order"][-1] == "consume_redfin"


@pytest.mark.parametrize("physical", ["census_acs1", "census_acs5"])
def test_physical_acs_pointer_is_rejected_from_acceptance_planning(physical: str):
    targets = promotion_targets()
    targets[physical] = targets.pop("acs")
    with pytest.raises(PublicationError, match="physical family pointers"):
        promotion(targets)
