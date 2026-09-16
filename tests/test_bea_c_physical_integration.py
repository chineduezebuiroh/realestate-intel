from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from jobs.monthly_refresh.acs_hosted import execute_product
from jobs.monthly_refresh.bea_monthly import (acquire_snapshot, candidate, discover_pin,
    retrieve_pinned_snapshot, semantic_identity)
from jobs.monthly_refresh.source_inputs import FilePinStore
from sources.bea.artifact import PRODUCTS, canonicalize, geography_plan, request_params


def periods(source_id):
    if source_id == "bea_gdp_qtr": return [f"{year}Q{q}" for year in range(2005, 2027) for q in range(1, 5) if (year, q) <= (2026, 1)]
    return [str(year) for year in range(2001, 2025)]


def snapshot(source_id, mutation=0):
    plan = geography_plan(source_id); rows = []
    for geo in plan:
        if geo["availability"] != "AVAILABLE_DIRECT": continue
        for index, period in enumerate(periods(source_id)):
            rows.append({"GeoFips": geo["provider_geo_fips"], "TimePeriod": period,
                "DataValue": f"{1000 + index + mutation if not rows else 1000 + index:,}",
                "CL_UNIT": PRODUCTS[source_id]["provider_unit"], "UNIT_MULT": "6", "LineCode": "1",
                "LineDescription": "All industry total", "TableName": PRODUCTS[source_id]["table"]})
    return {"contract_version": "bea_gdp_physical_source_v1", "source_id": source_id,
        "request": request_params(source_id, plan), "applicability": plan, "rows": rows,
        "provider_metadata": {"dataset": "Regional", "table": PRODUCTS[source_id]["table"],
            "line_code": "1", "frequency": PRODUCTS[source_id]["frequency"], "unit_contract": PRODUCTS[source_id]["unit"]},
        "parser_contract_version": "bea-b-observation-parser-v1"}


class Response:
    status_code = 200
    def __init__(self, payload, padding=""): self.payload, self.content = payload, json.dumps(payload).encode() + padding.encode()
    def raise_for_status(self): pass
    def json(self): return self.payload


class Session:
    def __init__(self, source_id, padding=""): self.source_id, self.padding, self.calls = source_id, padding, []
    def post(self, url, *, data, timeout):
        self.calls.append((url, dict(data)))
        snap = snapshot(self.source_id)
        return Response({"BEAAPI": {"Results": {"Data": snap["rows"]}}}, self.padding)


def test_frozen_requests_and_full_history_candidates():
    quarterly = snapshot("bea_gdp_qtr"); annual = snapshot("bea_gdp_ann")
    assert quarterly["request"] == {"method": "GetData", "DataSetName": "Regional", "TableName": "SQGDP9", "LineCode": "1", "Year": "ALL", "GeoFips": "00000,06000,11000,24000,34000,51000"}
    assert annual["request"]["TableName"] == "CAGDP9" and annual["request"]["Year"] == "ALL"
    qframe, qdiag = canonicalize("bea_gdp_qtr", quarterly); aframe, adiag = canonicalize("bea_gdp_ann", annual)
    assert len(qframe) == 510 and qframe.geo_id.nunique() == 6
    assert {str(x) for x in qframe.date.unique()} == {str(pd.Period(p, freq="Q").end_time.date()) for p in periods("bea_gdp_qtr")}
    assert len(aframe) == 3096 and aframe.geo_id.nunique() == 129
    assert adiag["applicability_count"] == 169 and adiag["provider_unavailable_count"] == 40
    assert adiag["synthesized_observation_count"] == 0 and qdiag["returned_geography_count"] == 6
    for source_id, frame in (("bea_gdp_qtr", qframe), ("bea_gdp_ann", aframe)):
        assert set(frame.metric_id) == {PRODUCTS[source_id]["metric_id"]}; assert set(frame.source_id) == {source_id}
        assert set(frame.property_type_id) == {"all"} and set(frame.property_type) == {"all"}
        assert not frame.duplicated(["geo_id", "metric_id", "date", "property_type_id"]).any()


def test_credentials_and_raw_transport_do_not_enter_semantic_identity(tmp_path, monkeypatch):
    secret = "never-persist-bea-secret"; monkeypatch.setenv("BEA_API_KEY", secret)
    pins = []
    for suffix, padding in (("one", ""), ("two", "raw-envelope-noise")):
        session = Session("bea_gdp_qtr", padding)
        def acquire(**kwargs): return acquire_snapshot(**kwargs, session=session)
        pin, paths = discover_pin(cycle_id="cycle", source_id="bea_gdp_qtr", workspace=tmp_path / suffix,
                                  acquire=acquire, retrieved_at="2026-09-16T00:00:00Z")
        pins.append(pin); persisted = json.dumps(pin) + paths["snapshot"].read_text()
        assert secret not in persisted and "UserID" not in persisted and "raw_response_sha256" not in persisted
        assert session.calls[0][1]["UserID"] == secret
    assert pins[0] == pins[1]
    assert pins[0]["provider_release_id"] == pins[1]["provider_release_id"]


def test_content_revision_changes_semantic_and_candidate_identity(tmp_path):
    base, changed = snapshot("bea_gdp_qtr"), snapshot("bea_gdp_qtr")
    changed["rows"][0]["DataValue"] = "999999"
    assert semantic_identity("bea_gdp_qtr", base) != semantic_identity("bea_gdp_qtr", changed)
    artifacts = []
    for name, snap in (("base", base), ("changed", changed)):
        def acquire(**kwargs):
            Path(kwargs["output"]).parent.mkdir(parents=True, exist_ok=True)
            from core.source_artifacts.hashing import write_canonical_json
            write_canonical_json(kwargs["output"], snap); return snap, {}
        pin, paths = discover_pin(cycle_id="cycle", source_id="bea_gdp_qtr", workspace=tmp_path / name,
                                  acquire=acquire, retrieved_at="2026-09-16T00:00:00Z")
        artifacts.append(candidate(pin=pin, paths=paths, output=tmp_path / f"artifact-{name}", cycle_id="cycle", repository_root=Path(".")))
    assert artifacts[0]["manifest"]["artifact_id"] != artifacts[1]["manifest"]["artifact_id"]


def test_fail_closed_geography_sentinel_and_duplicate():
    value = snapshot("bea_gdp_ann"); value["rows"] = value["rows"][24:]
    with pytest.raises(ValueError, match="geography membership"): canonicalize("bea_gdp_ann", value)
    value = snapshot("bea_gdp_ann"); unavailable = next(x for x in value["applicability"] if x["availability"] == "PROVIDER_UNAVAILABLE")
    value["rows"].append({**value["rows"][0], "GeoFips": unavailable["provider_geo_fips"]})
    with pytest.raises(ValueError, match="geography membership"): canonicalize("bea_gdp_ann", value)
    value = snapshot("bea_gdp_qtr"); value["rows"][0]["DataValue"] = "(NA)"
    with pytest.raises(ValueError, match="unknown BEA DataValue"): canonicalize("bea_gdp_qtr", value)
    value = snapshot("bea_gdp_qtr"); value["rows"].append(dict(value["rows"][0]))
    with pytest.raises(ValueError, match="duplicate"): canonicalize("bea_gdp_qtr", value)


def test_resume_replay_use_embedded_pin_without_discovery(tmp_path):
    source_id, cycle = "bea_gdp_ann", "cycle"; snap = snapshot(source_id)
    def acquire(**kwargs):
        from core.source_artifacts.hashing import write_canonical_json
        Path(kwargs["output"]).parent.mkdir(parents=True, exist_ok=True); write_canonical_json(kwargs["output"], snap); return snap, {}
    pin, _ = discover_pin(cycle_id=cycle, source_id=source_id, workspace=tmp_path / "seed", acquire=acquire,
                          retrieved_at="2026-09-16T00:00:00Z")
    store = FilePinStore(tmp_path / "store"); store.put(pin); executions = []
    for mode in ("resume", "replay"):
        execute_product(source_id=source_id, mode=mode, cycle_id=cycle, workspace=tmp_path / mode,
            pin_store=store, discover=lambda: (_ for _ in ()).throw(AssertionError("provider rediscovered")),
            retrieve=retrieve_pinned_snapshot,
            build=lambda **kw: executions.append(mode) or {"manifest": {}},
            publish=lambda path, source: {"record": {"object_id": "id", "artifact_content_hash": "a"*64,
                "package_sha256": "b"*64, "logical_artifact_uri": "artifact://x",
                "metadata": {"provider_release_id": pin["provider_release_id"], "observation_max": "2024-12-31"}}, "catalog": {}},
            record=lambda *_: None)
    assert executions == ["resume", "replay"]


def test_normal_mode_discovers_persists_then_executes(tmp_path):
    source_id, cycle = "bea_gdp_qtr", "cycle"; snap = snapshot(source_id); calls = []
    def discover():
        calls.append("discover")
        def acquire(**kwargs):
            from core.source_artifacts.hashing import write_canonical_json
            Path(kwargs["output"]).parent.mkdir(parents=True, exist_ok=True); write_canonical_json(kwargs["output"], snap); return snap, {}
        return discover_pin(cycle_id=cycle, source_id=source_id, workspace=tmp_path / "discovery", acquire=acquire,
                            retrieved_at="2026-09-16T00:00:00Z")
    store = FilePinStore(tmp_path / "store")
    result = execute_product(source_id=source_id, mode="normal", cycle_id=cycle, workspace=tmp_path / "normal",
        pin_store=store, discover=discover, retrieve=retrieve_pinned_snapshot,
        build=lambda **kw: calls.append("execute") or {"manifest": {}},
        publish=lambda path, source: {"record": {"object_id": "id", "artifact_content_hash": "a"*64,
            "package_sha256": "b"*64, "logical_artifact_uri": "artifact://x",
            "metadata": {"provider_release_id": "p", "observation_max": "2026-03-31"}}, "catalog": {}}, record=lambda *_: None)
    assert calls == ["discover", "execute"] and store.get(cycle, source_id) == result["pin"]
