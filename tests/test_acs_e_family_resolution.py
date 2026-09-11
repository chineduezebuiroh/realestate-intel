from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.acs_family_resolution import (
    PHYSICAL_TO_LOGICAL, add_record, build_family_artifact, resolve_frames,
)
from sources.census_acs.artifact import CONTRACT_VERSION


def row(source: str, suffix: str, geo: str, value: float, year: int = 2024) -> dict:
    return {"geo_id": geo, "metric_id": f"{source}_{suffix}", "date": date(year, 12, 31),
            "property_type_id": "all", "value": value, "source_id": source, "property_type": "all"}


P1 = {"source_id": "census_acs1", "artifact_id": "one", "artifact_content_hash": "1" * 64, "package_sha256": "a" * 64}
P5 = {"source_id": "census_acs5", "artifact_id": "five", "artifact_content_hash": "5" * 64, "package_sha256": "b" * 64}


def test_observation_resolution_and_lineage_diagnostics():
    one = pd.DataFrame([row("census_acs1", "pop_total", "one__state", 10),
                        row("census_acs1", "median_household_income", "both__state", 50)])
    five = pd.DataFrame([row("census_acs5", "pop_total", "five__county", 5),
                         row("census_acs5", "median_household_income", "both__state", 45)])
    output, lineage, diagnostic = resolve_frames(one, five, acs1_parent=P1, acs5_parent=P5)
    assert set(output.metric_id) == {"census_acs_pop_total", "census_acs_median_household_income"}
    assert dict(zip(output.geo_id, output.value)) == {"both__state": 50, "five__county": 5, "one__state": 10}
    both = lineage.set_index("geo_id").loc["both__state"]
    assert (both.winning_physical_source_id, both.winning_physical_metric_id) == (
        "census_acs1", "census_acs1_median_household_income")
    assert both.winning_parent_artifact_id == "one" and both.acs1_present and both.acs5_present
    assert both.acs1_value == 50 and both.acs5_value == 45
    assert both.resolution_reason == "ACS1_PREFERRED_AVAILABLE"
    fallback = lineage.set_index("geo_id").loc["five__county"]
    assert fallback.resolution_reason == "ACS5_FALLBACK_ACS1_UNAVAILABLE"
    assert diagnostic["acs1_only_logical_key_count"] == diagnostic["acs5_only_logical_key_count"] == 1
    assert diagnostic["overlap_logical_key_count"] == diagnostic["overlap_differing_value_count"] == 1
    assert diagnostic["acs1_wins_count"] == 2 and diagnostic["acs5_fallback_count"] == 1
    assert diagnostic["output_duplicate_key_count"] == 0


def test_equal_overlap_is_diagnostic_not_equivalence_requirement():
    one = pd.DataFrame([row("census_acs1", "pop_total", "x__state", 10)])
    five = pd.DataFrame([row("census_acs5", "pop_total", "x__state", 10)])
    _, _, diagnostic = resolve_frames(one, five, acs1_parent=P1, acs5_parent=P5)
    assert diagnostic["overlap_equal_value_count"] == 1
    assert diagnostic["overlap_differing_value_count"] == 0


def test_unknown_metric_and_duplicate_physical_key_fail_closed():
    valid = pd.DataFrame([row("census_acs5", "pop_total", "x__state", 1)])
    unknown = pd.DataFrame([row("census_acs1", "new_variable", "x__state", 1)])
    with pytest.raises(ValueError, match="unknown ACS physical metric"):
        resolve_frames(unknown, valid, acs1_parent=P1, acs5_parent=P5)
    duplicate = pd.DataFrame([row("census_acs1", "pop_total", "x__state", 1)] * 2)
    with pytest.raises(ValueError, match="duplicate physical canonical key"):
        resolve_frames(duplicate, valid, acs1_parent=P1, acs5_parent=P5)


def make_parent(path: Path, source: str, rows: list[dict], root: Path) -> tuple[dict, dict]:
    manifest = create_artifact(path, pd.DataFrame(rows), source_id=source, source_family=source,
        source_type="government_survey", provider="Census", distribution_channel="api",
        provider_release_id=f"acs/{'acs1' if source.endswith('1') else 'acs5'}:2024",
        provider_release_timestamp_or_date="2024", retrieved_at="2025-01-01T00:00:00Z",
        target_month="2024-12", source_request_identity=source,
        source_urls_or_endpoint_identity=["https://example.invalid"], source_contract_version=CONTRACT_VERSION,
        raw_source_lineage={"coverage": {"provider_ineligible": ["missing__county"], "excluded_geographies": []}},
        config_hashes={}, artifact_created_at="2025-01-01T00:00:00Z")
    record = {"object_type": "source", "object_id": manifest["artifact_id"],
        "artifact_content_hash": manifest["artifact_content_hash"], "package_sha256": "f" * 64,
        "publication_state": "published_immutable_verified", "metadata": {"source_id": source, "data_sha256": manifest["data_sha256"]}}
    return manifest, record


def test_exact_parents_deterministically_build_and_preserve_contract(tmp_path: Path):
    root = Path(".")
    one_manifest, one_record = make_parent(tmp_path / "one", "census_acs1",
        [row("census_acs1", "pop_total", "alameda_county_ca__county", 10)], root)
    five_manifest, five_record = make_parent(tmp_path / "five", "census_acs5",
        [row("census_acs5", "pop_total", "alameda_county_ca__county", 9)], root)
    first = build_family_artifact(acs1_artifact=tmp_path / "one", acs5_artifact=tmp_path / "five",
        acs1_record=one_record, acs5_record=five_record, output=tmp_path / "out1", repository_root=root)
    second = build_family_artifact(acs1_artifact=tmp_path / "one", acs5_artifact=tmp_path / "five",
        acs1_record=one_record, acs5_record=five_record, output=tmp_path / "out2", repository_root=root)
    assert first["manifest"]["artifact_id"] == second["manifest"]["artifact_id"]
    assert first["manifest"]["artifact_content_hash"] == second["manifest"]["artifact_content_hash"]
    assert first["manifest"]["identity_context"]["physical_to_logical_metric_mapping"] == PHYSICAL_TO_LOGICAL
    lineage = pd.read_parquet(tmp_path / "out1/lineage.parquet").iloc[0]
    assert lineage.winning_parent_artifact_content_hash == one_manifest["artifact_content_hash"]
    assert first["diagnostics"]["provider_ineligible"]["census_acs5"] == ["missing__county"]
    bad = dict(one_record); bad["artifact_content_hash"] = "0" * 64
    with pytest.raises(ValueError, match="artifact/catalog identity mismatch"):
        build_family_artifact(acs1_artifact=tmp_path / "one", acs5_artifact=tmp_path / "five",
            acs1_record=bad, acs5_record=five_record, output=tmp_path / "bad", repository_root=root)


def test_excluded_geography_cannot_enter_logical_family(tmp_path: Path):
    _, one_record = make_parent(tmp_path / "one", "census_acs1",
        [row("census_acs1", "pop_total", "anaheim_ca_metro_area__cbsa_metro", 10)], Path("."))
    _, five_record = make_parent(tmp_path / "five", "census_acs5",
        [row("census_acs5", "pop_total", "alameda_county_ca__county", 9)], Path("."))
    with pytest.raises(ValueError, match="outside governed ACS geography contract"):
        build_family_artifact(acs1_artifact=tmp_path / "one", acs5_artifact=tmp_path / "five",
            acs1_record=one_record, acs5_record=five_record, output=tmp_path / "out", repository_root=Path("."))


def test_resolution_record_is_create_once_and_side_effect_free():
    proposed = {"schema_version": "acs_family_resolution_record_v1", "resolution_id": "r",
        "accepted_pointer_changed": False, "source_set_created": False, "duckdb_mutated": False,
        "serving_db_mutated": False, "provider_discovery_performed": False}
    value, changed = add_record(None, proposed)
    assert changed and value == proposed
    assert add_record(value, proposed) == (proposed, False)
    with pytest.raises(IdentityCollisionError, match="contradictory"):
        add_record({**proposed, "resolution_id": "other"}, proposed)
    for flag in ("accepted_pointer_changed", "source_set_created", "duckdb_mutated", "serving_db_mutated", "provider_discovery_performed"):
        assert proposed[flag] is False
