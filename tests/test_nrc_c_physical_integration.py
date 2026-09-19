from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import pytest
from jobs.monthly_refresh.nrc_hosted import execute_source
from jobs.monthly_refresh.nrc_monthly import MEMBERS, candidate, discover_pin, recover_pinned_workbooks
from jobs.monthly_refresh.source_inputs import FilePinStore
from sources.census_nrc.parser import CENSUS_INPUTS, PARSER_CONTRACT_VERSION
from tests.test_nrc_b_verify import workbook

def acquisition(url: str):
    kind = next(k for k, (candidate_url, _) in CENSUS_INPUTS.items() if candidate_url == url)
    return workbook(kind, unavailable=(kind == "completions")), {"status_code": 200, "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}

def pinned(tmp_path, cycle="cycle"):
    return discover_pin(cycle_id=cycle, workspace=tmp_path / "discovery", acquire=acquisition, retrieved_at="2026-09-19T00:00:00Z")

def test_two_member_pin_candidate_contract_lineage_and_stable_identity(tmp_path):
    pin, paths = pinned(tmp_path); assert set(pin["members"]) == MEMBERS
    for member in pin["members"].values(): assert len(member["sha256"]) == 64 and member["size_bytes"] > 0 and member["content_base64"]
    first = candidate(pin=pin, paths=paths, output=tmp_path / "one", cycle_id="cycle")
    second = candidate(pin=pin, paths=paths, output=tmp_path / "two", cycle_id="cycle")
    frame = pd.read_parquet(tmp_path / "one/data.parquet")
    assert set(frame.source_id) == {"census_nrc"}
    assert set(frame.metric_id) == {"census_housing_starts_total_saar", "census_housing_completions_total_saar"}
    assert set(frame.geo_id) == {"us_nation", "us_region_northeast", "us_region_midwest", "us_region_south", "us_region_west"}
    assert set(frame.property_type_id) == set(frame.property_type) == {"all"}
    assert frame.value.max() == 1501 and set(frame.date.astype(str)) == {"2026-01-31", "2026-02-28"}
    regional_completions = frame[(frame.metric_id == "census_housing_completions_total_saar") &
                                 (frame.geo_id == "us_region_northeast")]
    assert regional_completions.date.astype(str).tolist() == ["2026-02-28"]
    assert {x["kind"] for x in first["evidence"]["input_members"]} == MEMBERS
    assert first["manifest"]["artifact_id"] == second["manifest"]["artifact_id"]
    assert first["evidence"]["parser_contract_version"] == PARSER_CONTRACT_VERSION

def test_pinned_byte_recovery_and_hash_mismatch_fail_closed(tmp_path):
    pin, paths = pinned(tmp_path); recovered = recover_pinned_workbooks(pin, tmp_path / "recovered")
    assert all(recovered[k].read_bytes() == paths[k].read_bytes() for k in MEMBERS)
    recovered["starts"].write_bytes(b"changed")
    with pytest.raises(ValueError, match="pin mismatch"): candidate(pin=pin, paths=recovered, output=tmp_path / "bad", cycle_id="cycle")
    corrupt = json.loads(json.dumps(pin)); corrupt["members"]["starts"]["content_base64"] = "YQ=="
    with pytest.raises(ValueError, match="hash mismatch"): recover_pinned_workbooks(corrupt, tmp_path / "corrupt")

def test_normal_pins_before_execution_and_resume_replay_do_not_discover(tmp_path):
    store = FilePinStore(tmp_path / "store"); calls = []
    def discover(): calls.append("discover"); return pinned(tmp_path / "normal")
    def build(**kwargs):
        calls.append("build"); assert store.get("cycle", "census_nrc") == kwargs["pin"]
        return candidate(**kwargs)
    sequence = 0
    def publish(path: Path, source_id: str):
        nonlocal sequence; sequence += 1; manifest = json.loads((path / "manifest.json").read_text())
        return {"record": {"object_id": manifest["artifact_id"], "artifact_content_hash": manifest["artifact_content_hash"], "package_sha256": str(sequence) * 64, "logical_artifact_uri": manifest["artifact_uri"], "metadata": {"provider_release_id": manifest["provider_release_id"], "observation_max": manifest["observation_max"]}}, "catalog": {}}
    for mode in ("normal", "resume", "replay"):
        execute_source(mode=mode, cycle_id="cycle", workspace=tmp_path / mode, pin_store=store, discover=discover if mode == "normal" else lambda: (_ for _ in ()).throw(AssertionError("rediscovery")), build=build, publish=publish, record=lambda *_: calls.append("record"))
    assert calls == ["discover", "build", "record", "build", "record", "build", "record"]

def test_current_workbook_revision_is_accepted_as_provider_truth(tmp_path):
    pin, paths = pinned(tmp_path)
    original = candidate(pin=pin, paths=paths, output=tmp_path / "original", cycle_id="cycle")
    def revised(url: str):
        payload, metadata = acquisition(url)
        if url == CENSUS_INPUTS["starts"][0]: payload = workbook("starts", unavailable=True)
        return payload, metadata
    changed_pin, changed_paths = discover_pin(cycle_id="later", workspace=tmp_path / "later", acquire=revised, retrieved_at="2026-10-01T00:00:00Z")
    changed = candidate(pin=changed_pin, paths=changed_paths, output=tmp_path / "changed", cycle_id="later")
    assert changed["manifest"]["artifact_id"] != original["manifest"]["artifact_id"]
