from __future__ import annotations

import copy
from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import pytest

from core.source_artifacts.artifact import create_artifact
from core.source_artifacts.assembly_v2 import assemble_source_set_v2
from core.source_artifacts.source_set_v2 import (create_source_set_v2, governed_config_hashes,
    source_set_semantic_sha256)
from core.source_artifacts.storage import LocalArtifactResolver
from jobs.monthly_refresh.acs_family_resolution import resolve_frames as resolve_acs
from jobs.monthly_refresh.bps_family_resolution import resolve_frames as resolve_bps


GEO = "alameda_county_ca__county"
HASHES = ("1" * 64, "2" * 64, "3" * 64, "4" * 64)


def _row(source: str, metric: str, value: float) -> dict:
    return {"geo_id": GEO, "metric_id": metric, "date": date(2024, 12, 31),
            "property_type_id": "all", "value": value, "source_id": source,
            "property_type": "all"}


def _artifact(root: Path, source: str, frame: pd.DataFrame) -> tuple[dict, Path]:
    directory = root / ("artifact-" + source)
    manifest = create_artifact(directory, frame, source_id=source, source_family=source,
        source_type="fixture", provider="fixture", distribution_channel="fixture",
        provider_release_id="fixture-" + source, provider_release_timestamp_or_date=None,
        retrieved_at=None, target_month="2024-12", source_request_identity="fixture:" + source,
        source_urls_or_endpoint_identity=["fixture://" + source], artifact_created_at="fixed")
    return manifest, directory


def _entry(manifest: dict, number: int) -> dict:
    return {"source_id":manifest["source_id"], "artifact_id":manifest["artifact_id"],
        "logical_artifact_uri":manifest["artifact_uri"], "package_sha256":str(number) * 64,
        "artifact_content_hash":manifest["artifact_content_hash"],
        "provider_release_id":manifest["provider_release_id"],
        "observation_max":manifest["observation_max"], "validation_status":"passed",
        "monthly_status":"refreshed", "release_tag":"fixture", "asset_id":number,
        "publication_receipt_id":"fixture", "cycle_check_succeeded":True,
        "carried_forward":False, "carry_forward_policy_allowed":False}


def _family(logical: str, entry: dict, physical: tuple[str, ...]) -> dict:
    return {"logical_source_id":logical, "resolution_id":logical + "-resolution",
        "output_artifact_id":entry["artifact_id"],
        "output_content_hash":entry["artifact_content_hash"],
        "output_package_sha256":entry["package_sha256"],
        "physical_sources":[{"source_id":source, "artifact_id":"src__" + source,
            "artifact_content_hash":HASHES[index % len(HASHES)],
            "package_sha256":HASHES[(index + 1) % len(HASHES)]}
            for index, source in enumerate(sorted(physical))]}


def _source_set(path: Path, entries: list[dict], families: list[dict]) -> dict:
    logical = sorted(entry["source_id"] for entry in entries)
    physical = sorted((set(logical) - {f["logical_source_id"] for f in families}) |
                      {p["source_id"] for f in families for p in f["physical_sources"]})
    family_map = {"schema_version":"source_family_resolution_map_v1",
        "cycle_id":"monthly_cycle__2024-12__fixture", "physical_source_inventory":physical,
        "logical_source_inventory":logical, "families":families} if families else {}
    return create_source_set_v2(path, target_month="2024-12", created_at="fixed",
        builder_git_sha="fixture", entries=entries, config_hashes=governed_config_hashes(),
        family_resolution=family_map)


def _assemble(tmp_path: Path, frames: dict[str, pd.DataFrame], family_sources: set[str]) -> tuple[dict, Path, dict]:
    artifacts, entries = {}, []
    for number, (source, frame) in enumerate(sorted(frames.items()), 1):
        manifest, directory = _artifact(tmp_path, source, frame)
        artifacts[manifest["artifact_uri"]] = directory
        entries.append(_entry(manifest, number))
    families = []
    if "acs" in family_sources:
        families.append(_family("acs", next(e for e in entries if e["source_id"] == "acs"),
                                ("census_acs1", "census_acs5")))
    if "bps" in family_sources:
        families.append(_family("bps", next(e for e in entries if e["source_id"] == "bps"),
                                ("census_bps", "census_bps_provisional")))
    source_set = _source_set(tmp_path / "source-set.json", entries, families)
    database = tmp_path / "candidate.duckdb"
    result = assemble_source_set_v2(source_set, database, LocalArtifactResolver(artifacts))
    return result, database, source_set


def test_real_acs_resolution_and_bps_family_pass_logical_assembly(tmp_path: Path):
    acs1 = pd.DataFrame([
        _row("census_acs1", "census_acs1_pop_total", 10),
        _row("census_acs1", "census_acs1_median_household_income", 20),
    ])
    acs5 = pd.DataFrame([
        _row("census_acs5", "census_acs5_pop_total", 9),
        _row("census_acs5", "census_acs5_median_household_income", 19),
    ])
    acs, _, _ = resolve_acs(acs1, acs5,
        acs1_parent={"artifact_id":"acs1", "artifact_content_hash":"1" * 64},
        acs5_parent={"artifact_id":"acs5", "artifact_content_hash":"5" * 64})
    compiled = pd.DataFrame([_row("census_bps", "census_bp_total_units", 30)])
    provisional = pd.DataFrame([_row("census_bps_provisional", "census_bp_total_units", 31)])
    bps, _, _ = resolve_bps(compiled, provisional)

    result, database, _ = _assemble(tmp_path, {"acs":acs, "bps":bps}, {"acs", "bps"})

    assert result["sources"] == ["acs", "bps"]
    connection = duckdb.connect(str(database), read_only=True)
    try:
        assert {row[0] for row in connection.execute(
            "select distinct source_id from fact_timeseries").fetchall()} == {"acs", "bps"}
        assert {row[0] for row in connection.execute(
            "select source_id from source_artifact_metadata").fetchall()} == {"acs", "bps"}
        assert connection.execute("select count(*) from fact_timeseries where source_id like 'census_%'").fetchone()[0] == 0
    finally:
        connection.close()


@pytest.mark.parametrize(("source", "families", "metric"), [
    ("acs", {"acs"}, "unregistered_acs_metric"),
    ("ces", set(), "census_acs_pop_total"),
    ("bps", {"bps"}, "census_acs_pop_total"),
    ("acs", set(), "census_acs_pop_total"),
])
def test_logical_metric_ownership_fails_closed(tmp_path: Path, source: str,
                                                families: set[str], metric: str):
    frame = pd.DataFrame([_row(source, metric, 1)])
    with pytest.raises(ValueError, match=f"unauthorized metric ownership for {source}"):
        _assemble(tmp_path, {source:frame}, families)


def test_contradictory_direct_and_logical_ownership_fails_closed(tmp_path: Path):
    frame = pd.DataFrame([_row("acs", "census_acs_pop_total", 1)])
    manifest, directory = _artifact(tmp_path, "acs", frame)
    entry = _entry(manifest, 1)
    source_set = _source_set(tmp_path / "source-set.json", [entry],
                             [_family("acs", entry, ("census_acs1", "census_acs5"))])
    direct = pd.read_csv("config/source_metric_registry.csv", dtype=str)
    direct.loc[len(direct)] = {column:"" for column in direct.columns}
    direct.loc[len(direct) - 1, ["metric_key", "source_id", "metric_id"]] = [
        "contradiction", "census_acs1", "census_acs_pop_total"]
    direct_path = tmp_path / "direct.csv"
    direct.to_csv(direct_path, index=False)
    with pytest.raises(ValueError, match="contradictory direct/logical metric ownership"):
        assemble_source_set_v2(source_set, tmp_path / "candidate.duckdb",
            LocalArtifactResolver({manifest["artifact_uri"]:directory}), metric_registry=direct_path)


def test_assembly_rejects_ungoverned_canonical_artifact_without_aliasing(tmp_path: Path):
    frame = pd.DataFrame([_row("ces", "ces_total_nonfarm_sa", 1)]).assign(
        geo_id="midwest_region__region")
    with pytest.raises(ValueError, match="ungoverned geography in ces"):
        _assemble(tmp_path, {"ces":frame}, set())


def test_physical_family_source_cannot_enter_logical_inventory(tmp_path: Path):
    acs = pd.DataFrame([_row("acs", "census_acs_pop_total", 1)])
    _, _, source_set = _assemble(tmp_path / "valid", {"acs":acs}, {"acs"})
    invalid = copy.deepcopy(source_set)
    physical = copy.deepcopy(invalid["sources"][0])
    physical.update(source_id="census_acs1", artifact_id="src__census_acs1",
                    logical_artifact_uri="artifact://source/census_acs1/src__census_acs1")
    invalid["sources"].append(physical)
    invalid["sources"].sort(key=lambda entry: entry["source_id"])
    invalid["included_source_inventory"].append("census_acs1")
    invalid["included_source_inventory"].sort()
    invalid["required_source_inventory"] = list(invalid["included_source_inventory"])
    invalid["family_resolution"]["logical_source_inventory"] = list(invalid["included_source_inventory"])
    invalid["source_set_id"] = "source_set__2024-12__v2__" + source_set_semantic_sha256(invalid)[:16]
    with pytest.raises(Exception, match="family physical member identity invalid"):
        assemble_source_set_v2(invalid, tmp_path / "invalid.duckdb", LocalArtifactResolver({}))
