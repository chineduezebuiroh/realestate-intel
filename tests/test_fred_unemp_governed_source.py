from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from core.source_artifacts.artifact import artifact_package_sha256
from core.source_artifacts.publication import IdentityCollisionError
from jobs.monthly_refresh.cycle_results import add_record
from jobs.monthly_refresh.fred_unemp import (EXPECTED_TARGETS, MEMBER, acquire_histories,
    candidate, discover_pin, normalized_snapshot, recover_pinned_snapshot,
    resolve_targets, snapshot_frame)
from jobs.monthly_refresh.fred_unemp_hosted import authority_fingerprint, execute_source
from jobs.monthly_refresh.source_inputs import FilePinStore, add_pin
from sources.fred_unemp.artifact import METRIC_ID, SOURCE_ID, governed_config_hashes

CYCLE = "monthly_cycle__2026-08__fred_unemp_test"
STAMP = "2026-09-25T00:00:00Z"


def histories(value: float = 4.0) -> dict[str, list[dict]]:
    return {series: [{"date": "2026-07-01", "value": value},
                     {"date": "2026-08-01", "value": value + 0.1}]
            for series in EXPECTED_TARGETS.values()}


def pin_and_paths(tmp_path: Path, *, values: dict | None = None):
    return discover_pin(cycle_id=CYCLE, workspace=tmp_path / "input",
        repository_root=Path("."), retrieved_at=STAMP,
        acquire=lambda targets: values or histories())


def test_exact_targets_and_registry_contract():
    assert resolve_targets() == EXPECTED_TARGETS
    registry = pd.read_csv("config/source_metric_registry.csv")
    owned = registry.loc[registry.source_id.eq(SOURCE_ID), "metric_id"].tolist()
    assert owned == [METRIC_ID]
    dimension = pd.read_csv("config/metric_dimension_registry.csv")
    laus = dimension.loc[dimension.metric_key.eq("laus_unemployment_rate")].iloc[0]
    fred = dimension.loc[dimension.metric_key.eq("fred_unemployment_rate")].iloc[0]
    assert laus.source_priority == 1 and fred.source_priority == 2
    assert "Preferred" in laus.notes and "Fallback national/state" in fred.notes


def test_manifest_hash_and_exact_request_map_participate_in_pin(tmp_path):
    pin, _ = pin_and_paths(tmp_path)
    evidence = pin["members"][MEMBER]["evidence"]
    assert evidence["targets"] == EXPECTED_TARGETS
    assert evidence["config_hashes"]["config/geo_manifest.generated.csv"] == governed_config_hashes()[
        "config/geo_manifest.generated.csv"]
    changed = copy.deepcopy(pin); changed["members"][MEMBER]["evidence"]["targets"]["x"] = "BAD"
    assert changed["pin_id"] == pin["pin_id"]  # demonstrates why candidate evidence validation is required
    with pytest.raises(ValueError, match="configuration drift|inventory"):
        # The serialized snapshot remains authoritative and cannot be substituted by altered evidence.
        bad_snapshot = normalized_snapshot({**EXPECTED_TARGETS, "x": "BAD"}, histories(),
                                           config_hashes=governed_config_hashes())
        snapshot_frame(bad_snapshot)


@pytest.mark.parametrize("mutation", ["missing", "duplicate", "unexpected", "empty"])
def test_missing_duplicate_unexpected_or_empty_series_fail(mutation):
    snap = normalized_snapshot(EXPECTED_TARGETS, histories(), config_hashes=governed_config_hashes())
    if mutation == "missing": snap["members"].pop()
    elif mutation == "duplicate": snap["members"].append(copy.deepcopy(snap["members"][0]))
    elif mutation == "unexpected": snap["members"][0]["series_id"] = "BAD"
    else: snap["members"][0]["observations"] = []
    with pytest.raises(ValueError, match="missing|duplicate|unexpected"):
        snapshot_frame(snap)


def test_partial_provider_success_and_missing_secret_fail_clearly():
    class Client:
        def get_series(self, series):
            if series == "VAUR": return pd.Series(dtype=float)
            return pd.Series([4.0], index=[pd.Timestamp("2026-08-01")])
    with pytest.raises(RuntimeError, match="no history for VAUR"):
        acquire_histories(EXPECTED_TARGETS, key="fixture", client=Client())
    with pytest.raises(RuntimeError, match="FRED_API_KEY is required"):
        acquire_histories(EXPECTED_TARGETS, key="", client=Client())


def test_month_end_normalization_and_series_evidence():
    snap = normalized_snapshot(EXPECTED_TARGETS, histories(), config_hashes=governed_config_hashes())
    frame, evidence = snapshot_frame(snap)
    assert set(frame.date.astype(str)) == {"2026-07-31", "2026-08-31"}
    assert set(frame.source_id) == {SOURCE_ID} and set(frame.metric_id) == {METRIC_ID}
    assert evidence["present_series_count"] == evidence["expected_series_count"] == 6
    assert {item["series_id"] for item in evidence["series"]} == set(EXPECTED_TARGETS.values())


def test_immutable_serialization_recovery_and_hash_mismatch(tmp_path):
    pin, paths = pin_and_paths(tmp_path)
    recovered = tmp_path / "recovered.json"; recover_pinned_snapshot(pin, recovered)
    assert recovered.read_bytes() == paths[MEMBER].read_bytes()
    corrupt = copy.deepcopy(pin); corrupt["members"][MEMBER]["content_base64"] = "YWJj"
    with pytest.raises(ValueError, match="hash mismatch"):
        recover_pinned_snapshot(corrupt, tmp_path / "bad.json")


def test_reconciliation_revision_prior_preservation_and_unchanged_reuse(tmp_path):
    pin1, paths1 = pin_and_paths(tmp_path / "one")
    first = candidate(pin=pin1, paths=paths1, output=tmp_path / "artifact1", cycle_id=CYCLE,
        repository_root=Path("."), git_sha="a" * 40, artifact_created_at=STAMP)
    prior_data = pd.read_parquet(tmp_path / "artifact1/data.parquet")
    extra = prior_data.iloc[[0]].copy(); extra["date"] = pd.to_datetime(["2026-06-30"]).date
    extended = pd.concat([prior_data, extra], ignore_index=True)
    extended.to_parquet(tmp_path / "artifact1/data.parquet", index=False)
    # Rebuild a valid prior artifact containing a prior-only key.
    from core.source_artifacts.artifact import create_artifact
    import shutil
    shutil.rmtree(tmp_path / "artifact1")
    create_artifact(tmp_path / "artifact1", extended, source_id=SOURCE_ID, source_family="test",
        source_type="revisionary_current_truth", provider="test", distribution_channel="test",
        provider_release_id="prior", provider_release_timestamp_or_date=None, retrieved_at=STAMP,
        target_month="2026-08", source_request_identity="prior", source_urls_or_endpoint_identity=[],
        config_hashes=governed_config_hashes(), git_sha="a" * 40, artifact_created_at=STAMP)
    changed_histories = histories(); changed_histories["UNRATE"][1]["value"] = 9.9
    pin2, paths2 = pin_and_paths(tmp_path / "two", values=changed_histories)
    second = candidate(pin=pin2, paths=paths2, output=tmp_path / "artifact2", cycle_id=CYCLE,
        prior_artifact=tmp_path / "artifact1", repository_root=Path("."), git_sha="a" * 40,
        artifact_created_at=STAMP)
    data2 = pd.read_parquet(tmp_path / "artifact2/data.parquet")
    assert second["source_change_detected"] is True
    assert (pd.to_datetime(data2.date) == pd.Timestamp("2026-06-30")).any()
    assert data2.loc[(data2.geo_id == "united_states__nation") &
                     (pd.to_datetime(data2.date) == pd.Timestamp("2026-08-31")), "value"].item() == 9.9
    pin3, paths3 = pin_and_paths(tmp_path / "three", values=changed_histories)
    third = candidate(pin=pin3, paths=paths3, output=tmp_path / "artifact3", cycle_id=CYCLE,
        prior_artifact=tmp_path / "artifact2", repository_root=Path("."))
    assert third["source_change_detected"] is False
    assert third["manifest"]["artifact_id"] == second["manifest"]["artifact_id"]


def test_candidate_content_and_package_identity_are_deterministic(tmp_path):
    pin, paths = pin_and_paths(tmp_path)
    values = []
    for name in ("a", "b"):
        result = candidate(pin=pin, paths=paths, output=tmp_path / name, cycle_id=CYCLE,
            repository_root=Path("."), git_sha="b" * 40, artifact_created_at=STAMP)
        values.append((result["manifest"]["artifact_id"], result["manifest"]["artifact_content_hash"],
                       artifact_package_sha256(tmp_path / name)))
    assert values[0] == values[1]


def test_contradictory_pins_and_results_fail(tmp_path):
    first, _ = pin_and_paths(tmp_path / "one")
    second, _ = pin_and_paths(tmp_path / "two", values=histories(7.0))
    with pytest.raises(IdentityCollisionError): add_pin(first, second)
    base = {"schema_version":"monthly_source_cycle_result_v1", "cycle_id":CYCLE,
        "source_id":SOURCE_ID, "result_contract":"monthly_source_execution_result_v1",
        "policy_schema_version":"monthly_refresh_policy_v2", "result":{
            "schema_version":"monthly_source_execution_result_v1", "cycle_id":CYCLE,
            "source_id":SOURCE_ID, "status":"succeeded", "validation_status":"passed",
            "publication_state":"published_verified", "accepted_pointer_changed":False,
            "candidate_artifact_id":"src__fred_unemp__2026-08__r1__x", "artifact_content_hash":"a"*64,
            "package_sha256":"b"*64, "provider_release_id":"ordinary-current:x",
            "prior_artifact_id":None}}
    changed = copy.deepcopy(base); changed["result"]["candidate_artifact_id"] += "y"
    with pytest.raises(IdentityCollisionError): add_record(base, changed)


@pytest.mark.parametrize("mode", ["resume", "replay"])
def test_resume_and_replay_recover_pin_without_provider_calls(tmp_path, mode):
    pin, _ = pin_and_paths(tmp_path / "seed")
    store = FilePinStore(tmp_path / "authority"); store.put(pin)
    calls = {"discover": 0, "publish": 0, "record": 0}
    def forbidden(): calls["discover"] += 1; raise AssertionError("provider called")
    def publish(path, source):
        calls["publish"] += 1
        manifest=json.loads((path/"manifest.json").read_text())
        record={"object_id":manifest["artifact_id"], "artifact_content_hash":manifest["artifact_content_hash"],
            "package_sha256":"c"*64, "logical_artifact_uri":manifest["artifact_uri"],
            "metadata":{"provider_release_id":manifest["provider_release_id"],
                        "observation_max":manifest["observation_max"]}}
        return {"record":record,"catalog":{}}
    execute_source(mode=mode, cycle_id=CYCLE, workspace=tmp_path/mode, pin_store=store,
        existing_result=None, discover=forbidden, build=candidate, publish=publish,
        record=lambda result,catalog: calls.__setitem__("record", calls["record"]+1))
    assert calls == {"discover":0,"publish":1,"record":1}


def test_normal_acquires_once_then_pin_prevents_rediscovery(tmp_path):
    store = FilePinStore(tmp_path / "authority"); calls = {"discover":0}
    def discover(): calls["discover"] += 1; return pin_and_paths(tmp_path / "provider")
    def publish(path, source):
        manifest=json.loads((path/"manifest.json").read_text())
        return {"record":{"object_id":manifest["artifact_id"],
            "artifact_content_hash":manifest["artifact_content_hash"], "package_sha256":"d"*64,
            "logical_artifact_uri":manifest["artifact_uri"], "metadata":{
                "provider_release_id":manifest["provider_release_id"],"observation_max":manifest["observation_max"]}},
            "catalog":{}}
    execute_source(mode="normal", cycle_id=CYCLE, workspace=tmp_path/"run1", pin_store=store,
        existing_result=None, discover=discover, build=candidate, publish=publish,
        record=lambda result,catalog: None)
    execute_source(mode="normal", cycle_id=CYCLE, workspace=tmp_path/"run2", pin_store=store,
        existing_result=None, discover=discover, build=candidate, publish=publish,
        record=lambda result,catalog: None)
    assert calls["discover"] == 1


def test_valid_existing_result_reused_without_any_execution(tmp_path):
    result={"source_id":SOURCE_ID,"cycle_id":CYCLE,"status":"succeeded",
            "accepted_pointer_changed":False,"candidate_artifact_id":"artifact"}
    value=execute_source(mode="resume", cycle_id=CYCLE, workspace=tmp_path,
        pin_store=None, existing_result={"result":result}, discover=lambda: (_ for _ in ()).throw(AssertionError()),
        build=lambda **kwargs: (_ for _ in ()).throw(AssertionError()),
        publish=lambda *args: (_ for _ in ()).throw(AssertionError()),
        record=lambda *args: (_ for _ in ()).throw(AssertionError()))
    assert value == {"result":result,"reused":True}


def test_source_authority_fingerprint_covers_all_accepted_and_readiness():
    catalog={"accepted":{"source":{"fred_macro":"x"},"source_set":"s","canonical_market":"c",
                         "serving_market":"v"}}
    readiness={"records":[{"readiness_id":"r","consumed":False}]}
    baseline=authority_fingerprint(catalog,readiness)
    for mutation in (lambda c,r: c["accepted"]["source"].update(fred_unemp="y"),
                     lambda c,r: c["accepted"].update(source_set="z"),
                     lambda c,r: c["accepted"].update(canonical_market="z"),
                     lambda c,r: c["accepted"].update(serving_market="z"),
                     lambda c,r: r["records"][0].update(consumed=True)):
        c,r=copy.deepcopy(catalog),copy.deepcopy(readiness); mutation(c,r)
        assert authority_fingerprint(c,r) != baseline


def test_hosted_workflow_is_independent_and_main_authoritative():
    workflow = Path(".github/workflows/fred-unemp-monthly-source.yml").read_text()
    master = Path(".github/workflows/monthly-refresh-production.yml").read_text()
    assert "actions/checkout@v4" in workflow
    assert "DURABLE_AUTHORITY_BRANCH: main" in workflow
    assert "jobs.monthly_refresh.fred_unemp_hosted" in workflow
    assert "fred-unemp-monthly-source.yml" not in master
