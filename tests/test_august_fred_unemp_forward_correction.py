from __future__ import annotations

import copy
from pathlib import Path

import duckdb
import pytest

from core.source_artifacts.catalog import empty_catalog
from core.source_artifacts.forward_correction import (FRED_GEOS, SOURCES, UNCHANGED,
    add_correction_record, authorization_token, build_corrected_source_set,
    create_correction_record, preflight_correction, recover_correction,
    validate_canonical_factual_addition)
from core.source_artifacts.hashing import sha256_file, sha256_json
from core.source_artifacts.market_artifact import (CORRECTION_VERSION,
    create_canonical_market_manifest)
from core.source_artifacts.promotion import LEGACY_SOURCE_TRANSITION_ORDER, LEGACY_VERSION
from core.source_artifacts.publication import IdentityCollisionError, PublicationError
from core.source_artifacts.source_set_v2 import (create_source_set_v2,
    governed_config_hashes)

CYCLE = "monthly_cycle__2026-08__a9e022a980d29cd7"
OLD_SET = "source_set__2026-08__v2__fde2413d93948bee"
OLD_MARKET = "market__2026-08__r1__cfe00e7ec9a9be8e"
PROMOTION = "cohort_promotion__1ea61a82ff16b1e492ba0f6f"
OLD_IDS = {
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
FRED_ID = "src__fred_unemp__2026-08__r1__fixture000000000"
NEW_SET = "source_set__2026-08__v2__fixture00000000"
NEW_MARKET = "market__2026-08__r2__fixture000000000"


def _catalog_record(kind: str, object_id: str, number: int, source: str | None = None):
    metadata = ({"source_id": source, "data_sha256": f"{number % 16:x}" * 64,
                 "provider_release_id": "fixture", "observation_max": "2026-08-31"}
                if source else {})
    if kind == "canonical_market" and object_id == NEW_MARKET:
        metadata = {"supersedes_artifact_id": OLD_MARKET}
    if kind == "source_set" and object_id == NEW_SET:
        metadata = {"supersedes_artifact_id": OLD_SET}
    return {"object_type":kind, "object_id":object_id,
        "logical_artifact_uri":f"artifact://{kind}/{object_id}", "remote_repository":"fixture/repo",
        "release_tag":f"{kind}/{object_id}", "release_id":number, "asset_id":number,
        "asset_filename":f"{object_id}.tar", "package_sha256":f"{(number+1)%10}"*64,
        "artifact_content_hash":f"{(number+2)%10}"*64,
        "publication_receipt_id":f"receipt-{number}",
        "publication_state":"published_immutable_verified", "metadata":metadata}


def _state():
    catalog = empty_catalog()
    records = []
    for number, (source, artifact) in enumerate(OLD_IDS.items(), 1):
        records.append(_catalog_record("source", artifact, number, source))
    records.append(_catalog_record("source", FRED_ID, 20, "fred_unemp"))
    records += [_catalog_record("source_set", OLD_SET, 30),
                _catalog_record("source_set", NEW_SET, 31),
                _catalog_record("canonical_market", OLD_MARKET, 32),
                _catalog_record("canonical_market", NEW_MARKET, 33)]
    catalog["immutable_records"] = sorted(records, key=lambda r:(r["object_type"], r["object_id"]))
    catalog["accepted"] = {"source":copy.deepcopy(OLD_IDS), "source_set":OLD_SET,
                           "canonical_market":OLD_MARKET, "serving_market":None}
    readiness_record = {"readiness_id":f"redfin_readiness__{CYCLE}", "cycle_id":CYCLE,
        "source_id":"redfin", "candidate_artifact_id":OLD_IDS["redfin"], "consumed":True}
    readiness = {"records":[readiness_record]}
    expected = {source:OLD_IDS.get(source) for source in SOURCES}
    target = {**expected, "fred_unemp":FRED_ID}
    original_semantic = {"schema_version":LEGACY_VERSION, "cycle_id":CYCLE,
        "source_set_id":OLD_SET, "source_set_semantic_sha256":"a"*64,
        "canonical_artifact_id":OLD_MARKET, "canonical_artifact_hash":"b"*64,
        "expected_source_pointers":{source:None for source in LEGACY_SOURCE_TRANSITION_ORDER},
        "target_source_pointers":{source:OLD_IDS[source] for source in LEGACY_SOURCE_TRANSITION_ORDER},
        "expected_source_set":None, "expected_canonical":None,
        "readiness_id":readiness_record["readiness_id"], "resolution_id":"bps-resolution",
        "operation_order":["accept_source_set","accept_canonical_market","accept_sources","consume_redfin"]}
    original = {**original_semantic,
                "promotion_id":"cohort_promotion__" + sha256_json(original_semantic)[:24]}
    # The production ID is bound explicitly; fixture semantics retain a valid identity.
    record = create_correction_record(cycle_id=CYCLE, expected_source_set=OLD_SET,
        expected_canonical=OLD_MARKET, original_promotion_id=original["promotion_id"],
        expected_source_pointers=expected, target_source_pointers=target,
        corrected_source_set=NEW_SET, corrected_source_set_hash="c"*64,
        corrected_canonical=NEW_MARKET, corrected_canonical_hash="d"*64,
        readiness_evidence={"readiness_id":readiness_record["readiness_id"],
                            "record_sha256":sha256_json(readiness_record),
                            "consumed_by_promotion_id":original["promotion_id"]},
        family_resolution_lineage={"bps":"bps-resolution", "acs":"acs-resolution"},
        governed_hashes={"policy":"e"*64})
    next(r for r in catalog["immutable_records"] if r["object_id"] == NEW_SET)[
        "artifact_content_hash"] = "c"*64
    next(r for r in catalog["immutable_records"] if r["object_id"] == NEW_MARKET)[
        "artifact_content_hash"] = "d"*64
    return catalog, readiness, original, record


def test_preflight_accepts_historical_v1_and_mutates_nothing(monkeypatch):
    monkeypatch.setattr("socket.create_connection", lambda *_a, **_k:
                        (_ for _ in ()).throw(AssertionError("provider/network acquisition attempted")))
    production_paths = [Path("config/artifact_catalog.json"),
                        Path("config/monthly_refresh_readiness.json")]
    production_bytes = {path:path.read_bytes() for path in production_paths}
    catalog, readiness, original, record = _state()
    before = copy.deepcopy((catalog, readiness))
    report = preflight_correction(record, catalog, readiness, original)
    assert (catalog, readiness) == before
    assert report["accepted_state_mutated"] is report["readiness_mutated"] is False
    assert report["provider_acquisition_performed"] is False
    assert report["authorization_token"] == authorization_token(record)
    assert set(record["target_source_pointers"]) == set(SOURCES)
    assert all(record["target_source_pointers"][s] == OLD_IDS[s] for s in UNCHANGED)
    assert record["target_source_pointers"]["fred_unemp"] == FRED_ID
    assert {path:path.read_bytes() for path in production_paths} == production_bytes


@pytest.mark.parametrize("mutation", ["unconsumed", "wrong_cycle", "wrong_artifact", "changed_record"])
def test_preflight_rejects_nonhistorical_readiness(mutation):
    catalog, readiness, original, record = _state()
    item = readiness["records"][0]
    if mutation == "unconsumed": item["consumed"] = False
    elif mutation == "wrong_cycle": item["cycle_id"] = "other"
    elif mutation == "wrong_artifact": item["candidate_artifact_id"] = FRED_ID
    else: item["extra"] = "drift"
    with pytest.raises(PublicationError, match="readiness|promotion"):
        preflight_correction(record, catalog, readiness, original)


def test_authorized_recovery_is_ordered_idempotent_and_never_touches_readiness_or_serving():
    catalog, readiness, _original, record = _state()
    token = authorization_token(record)
    with pytest.raises(PublicationError, match="authorization"):
        recover_correction(record, catalog, readiness, supplied_authorization="stale")
    partial, ready_after, progress = recover_correction(record, catalog, readiness,
        supplied_authorization=token, max_operations=1)
    assert partial["accepted"]["source_set"] == NEW_SET
    assert partial["accepted"]["canonical_market"] == OLD_MARKET
    assert ready_after == readiness and progress["next_operation"] == "accept_corrected_canonical"
    complete, ready_final, progress = recover_correction(record, partial, ready_after,
        supplied_authorization=token)
    assert progress["complete"] is True
    assert complete["accepted"]["canonical_market"] == NEW_MARKET
    assert complete["accepted"]["source"]["fred_unemp"] == FRED_ID
    assert complete["accepted"]["serving_market"] is None
    assert all(complete["accepted"]["source"][s] == OLD_IDS[s] for s in UNCHANGED)
    assert ready_final == readiness
    repeated, repeated_ready, repeated_progress = recover_correction(record, complete, ready_final,
        supplied_authorization=token)
    assert repeated == complete and repeated_ready == readiness and repeated_progress["complete"]


def test_stale_and_conflicting_pointer_movement_fails_closed():
    catalog, readiness, original, record = _state()
    stale = copy.deepcopy(catalog); stale["accepted"]["source_set"] = NEW_SET
    with pytest.raises(PublicationError, match="stale"):
        preflight_correction(record, stale, readiness, original)
    moved = copy.deepcopy(catalog); moved["accepted"]["source"]["laus"] = FRED_ID
    with pytest.raises((IdentityCollisionError, PublicationError), match="unchanged|dangling"):
        recover_correction(record, moved, readiness,
            supplied_authorization=authorization_token(record))
    conflict = copy.deepcopy(catalog); conflict["accepted"]["source_set"] = NEW_SET
    conflict["accepted"]["canonical_market"] = "third-party"
    # Keep catalog structurally valid so the correction CAS detects the conflict.
    conflict["immutable_records"].append(_catalog_record("canonical_market", "third-party", 44))
    conflict["immutable_records"].sort(key=lambda r:(r["object_type"], r["object_id"]))
    with pytest.raises(IdentityCollisionError, match="canonical_market"):
        recover_correction(record, conflict, readiness,
            supplied_authorization=authorization_token(record))


def _entry(source: str, artifact: str):
    return {"source_id":source, "artifact_id":artifact,
        "logical_artifact_uri":f"artifact://source/{source}/{artifact}",
        "package_sha256":"1"*64, "artifact_content_hash":"2"*64,
        "provider_release_id":"fixture", "observation_max":"2026-08-31",
        "validation_status":"passed", "monthly_status":"refreshed",
        "release_tag":"fixture", "asset_id":1, "publication_receipt_id":"receipt",
        "cycle_check_succeeded":True, "carried_forward":False,
        "carry_forward_policy_allowed":False}


def test_corrected_source_set_adds_exactly_one_member_and_preserves_old(tmp_path):
    old_path = tmp_path / "old.json"
    old = create_source_set_v2(old_path, target_month="2026-08",
        created_at="2026-08-01T00:00:00Z", builder_git_sha="fixture",
        entries=[_entry(source, OLD_IDS[source]) for source in UNCHANGED],
        config_hashes=governed_config_hashes(Path(".")))
    old_bytes = old_path.read_bytes()
    corrected = build_corrected_source_set(tmp_path / "corrected.json", accepted_source_set=old,
        fred_unemp_entry=_entry("fred_unemp", FRED_ID),
        created_at="2026-08-02T00:00:00Z", builder_git_sha="fixture")
    assert old_path.read_bytes() == old_bytes
    assert corrected["included_source_inventory"] == sorted(SOURCES)
    assert len(corrected["sources"]) == 10
    assert {e["artifact_id"] for e in corrected["sources"]} - set(OLD_IDS.values()) == {FRED_ID}


def _database(path: Path, include_fred: bool):
    connection = duckdb.connect(str(path))
    rows = [("california__state", "laus_unemployment_rate_nsa", "2026-08-31", "all", 4.0, "laus", "all")]
    if include_fred:
        rows += [(geo, "fred_unemployment_rate_sa", "2026-08-31", "all", 4.5,
                  "fred_unemp", "all") for geo in FRED_GEOS]
    connection.execute("CREATE TABLE fact_timeseries(geo_id VARCHAR, metric_id VARCHAR, date DATE, "
        "property_type_id VARCHAR, value DOUBLE, source_id VARCHAR, property_type VARCHAR)")
    connection.executemany("INSERT INTO fact_timeseries VALUES (?,?,?,?,?,?,?)", rows)
    connection.close()


def test_corrected_canonical_is_r2_supersession_with_exact_factual_addition(tmp_path):
    old_db, new_db = tmp_path/"old.duckdb", tmp_path/"new.duckdb"
    _database(old_db, False); _database(new_db, True)
    proof = validate_canonical_factual_addition(accepted_database=old_db,
                                                corrected_database=new_db)
    assert proof["added_geographies"] == sorted(FRED_GEOS)
    manifest = create_canonical_market_manifest(tmp_path/"manifest.json",
        database_path=new_db, supersedes_artifact_id=OLD_MARKET,
        source_set_id="source_set__2026-08__v2__" + "a"*16,
        source_set_semantic_sha256="1"*64, source_set_package_sha256="2"*64,
        canonical_assembly_contract_version="canonical_market_assembly_v1",
        canonical_schema_identity="canonical-timeseries-v1", config_hashes={},
        builder_contract_identity="fixture", dependency_lock_identity="fixture",
        assembly_revision=2, compressed_package_sha256=sha256_file(new_db),
        table_inventory=["fact_timeseries"], row_count=7, source_count=2,
        geography_count=6, metric_count=2, first_date="2026-08-31", last_date="2026-08-31",
        duplicate_key_count=0, validation_status="passed", assembly_warnings=[],
        builder_git_sha="fixture", built_at="2026-08-31T00:00:00Z")
    assert manifest["schema_version"] == CORRECTION_VERSION
    assert manifest["assembly_revision"] == 2 and manifest["supersedes_artifact_id"] == OLD_MARKET


def test_correction_record_is_create_once():
    _catalog, _readiness, _original, record = _state()
    assert add_correction_record(None, record) == (record, True)
    assert add_correction_record(record, record) == (record, False)
    changed = copy.deepcopy(record); changed["governed_hashes"]["policy"] = "0"*64
    with pytest.raises(PublicationError):
        add_correction_record(record, changed)
