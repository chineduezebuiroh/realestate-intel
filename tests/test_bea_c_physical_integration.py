from __future__ import annotations

import copy
import json
from pathlib import Path

import pandas as pd
import pytest

from jobs.monthly_refresh.bea_hosted import execute_source
from jobs.monthly_refresh.bea_monthly import (MEMBER, acquire_provider, candidate, discover_pin,
    recover_pinned_snapshot)
from jobs.monthly_refresh.source_inputs import FilePinStore
from sources.bea.artifact import (OBSERVATION_METADATA, SOURCES, build_snapshot,
    canonicalize_snapshot, expected_periods, governed_geographies, request_plan,
    snapshot_bytes, stable_metadata)


def provider_rows(source_id: str, *, mutation: float = 0.0):
    direct = [g for g in governed_geographies(source_id) if g["availability"] == "AVAILABLE_DIRECT"]
    rows = []
    metadata = stable_metadata(source_id)
    for geo_number, geo in enumerate(direct):
        for period_number, period in enumerate(expected_periods(source_id)):
            value = 1000 + geo_number * 100 + period_number
            if not rows: value += mutation
            # Faithful Regional GetData observation shape: table/line identity is
            # request-level metadata and is not repeated on each observation.
            rows.append({"GeoFips": geo["provider_geo_fips"], "TimePeriod": period,
                "DataValue": f"{value:,.1f}", "CL_UNIT": metadata["unit"],
                "UNIT_MULT": metadata["unit_multiplier"]})
    return rows


class Response:
    status_code = 200
    def __init__(self, rows, envelope="one"):
        self._payload = {"BEAAPI": {"Results": {"Data": rows, "Note": envelope}}}
        self.content = json.dumps(self._payload, indent=2 if envelope == "one" else None).encode()
    def json(self): return self._payload
    def raise_for_status(self): pass


class Session:
    def __init__(self, source_id, envelope="one"): self.source_id, self.envelope, self.calls = source_id, envelope, []
    def post(self, url, *, data, timeout):
        self.calls.append((url, dict(data), timeout)); return Response(provider_rows(self.source_id), self.envelope)


@pytest.mark.parametrize("source_id,table,count", [("bea_gdp_qtr", "SQGDP9", 6), ("bea_gdp_ann", "CAGDP9", 169)])
def test_frozen_request_and_credentials_never_persist(tmp_path, source_id, table, count):
    plan = request_plan(source_id); assert plan["DataSetName"] == "Regional" and plan["TableName"] == table
    assert plan["LineCode"] == "1" and plan["Year"] == "ALL" and len(plan["GeoFips"].split(",")) == count
    secret = "credential-must-not-persist"; session = Session(source_id)
    pin, paths, lineage = discover_pin(cycle_id="cycle", source_id=source_id, workspace=tmp_path,
        acquire=lambda source_id, root: acquire_provider(source_id, root=root, key=secret, session=session),
        retrieved_at="2026-09-16T00:00:00Z")
    persisted = json.dumps(pin) + paths[MEMBER].read_text() + json.dumps(lineage)
    assert secret not in persisted and "UserID" not in persisted
    assert session.calls[0][1]["UserID"] == secret


@pytest.mark.parametrize("source_id,geos,rows,unavailable", [
    ("bea_gdp_qtr", 6, 510, 0), ("bea_gdp_ann", 129, 3096, 40)])
def test_candidate_contract_and_no_synthesis(tmp_path, source_id, geos, rows, unavailable):
    snapshot = build_snapshot(source_id, provider_rows(source_id)); raw = tmp_path / "snapshot.json"
    raw.write_bytes(snapshot_bytes(snapshot))
    pin, paths, _ = discover_pin(cycle_id="cycle", source_id=source_id, workspace=tmp_path / "pin",
        acquire=lambda *_args, **_kwargs: (snapshot, {"raw_response_sha256": "a" * 64}),
        retrieved_at="2026-09-16T00:00:00Z")
    built = candidate(pin=pin, paths=paths, output=tmp_path / "artifact", cycle_id="cycle")
    frame = pd.read_parquet(tmp_path / "artifact" / "data.parquet")
    assert len(frame) == rows and frame.geo_id.nunique() == geos
    assert not frame[["geo_id", "metric_id", "date", "property_type_id"]].duplicated().any()
    assert set(frame.source_id) == {source_id} and set(frame.metric_id) == {SOURCES[source_id]["metric_id"]}
    assert set(frame.property_type_id) == {"all"} and set(frame.property_type) == {"all"}
    assert built["evidence"]["coverage"]["provider_unavailable_count"] == unavailable
    assert built["evidence"]["coverage"]["applicability_count"] == (169 if source_id.endswith("ann") else 6)
    assert set(frame.date.astype(str)) == {r["date"] for r in snapshot["normalized_observations"]}


def test_raw_only_change_is_stable_and_content_mutation_changes_identity(tmp_path):
    source = "bea_gdp_qtr"; rows = provider_rows(source)
    class Variant(Session):
        def post(self, url, *, data, timeout):
            self.calls.append((url, dict(data), timeout)); return Response(list(reversed(rows)), self.envelope)
    pins, candidates = [], []
    for label in ("one", "two"):
        pin, paths, lineage = discover_pin(cycle_id="same", source_id=source, workspace=tmp_path / label,
            acquire=lambda source_id, root, label=label: acquire_provider(source_id, root=root, key="runtime", session=Variant(source, label)),
            retrieved_at="2026-09-16T00:00:00Z")
        built = candidate(pin=pin, paths=paths, output=tmp_path / f"candidate-{label}", cycle_id="same",
                          acquisition_lineage=lineage)
        pins.append(pin); candidates.append(built["manifest"])
    assert pins[0]["members"][MEMBER]["sha256"] == pins[1]["members"][MEMBER]["sha256"]
    assert pins[0]["pin_id"] == pins[1]["pin_id"]
    assert candidates[0]["artifact_id"] == candidates[1]["artifact_id"]
    changed = build_snapshot(source, provider_rows(source, mutation=1.0))
    changed_pin, changed_paths, _ = discover_pin(cycle_id="changed", source_id=source, workspace=tmp_path / "changed",
        acquire=lambda *_args, **_kwargs: (changed, {}), retrieved_at="2026-09-16T00:00:00Z")
    changed_candidate = candidate(pin=changed_pin, paths=changed_paths, output=tmp_path / "candidate-changed", cycle_id="changed")
    assert changed_pin["provider_release_id"] != pins[0]["provider_release_id"]
    assert changed_candidate["manifest"]["artifact_id"] != candidates[0]["artifact_id"]


def test_unknown_sentinel_and_geography_changes_fail_closed():
    rows = provider_rows("bea_gdp_ann")
    bad = copy.deepcopy(rows); bad[0]["DataValue"] = "(NA)"
    with pytest.raises(ValueError, match="sentinel"): build_snapshot("bea_gdp_ann", bad)
    unexpected = copy.deepcopy(rows); unexpected[0]["GeoFips"] = "99999"
    with pytest.raises(ValueError, match="unexpected BEA provider geography"): build_snapshot("bea_gdp_ann", unexpected)
    missing = rows[1:]
    with pytest.raises(ValueError, match="history is incomplete"): build_snapshot("bea_gdp_ann", missing)
    unavailable = next(g for g in governed_geographies("bea_gdp_ann") if g["availability"] == "PROVIDER_UNAVAILABLE")
    appeared = copy.deepcopy(rows); appeared[0]["GeoFips"] = unavailable["provider_geo_fips"]
    with pytest.raises(ValueError, match="became available"): build_snapshot("bea_gdp_ann", appeared)


@pytest.mark.parametrize("source_id", ["bea_gdp_qtr", "bea_gdp_ann"])
def test_live_row_shape_and_request_level_table_line_contract(source_id):
    rows = provider_rows(source_id)
    assert all("LineCode" not in row and "LineDescription" not in row and "Unit" not in row for row in rows)
    snapshot = build_snapshot(source_id, rows)
    assert snapshot["sanitized_request_plan"]["TableName"] == SOURCES[source_id]["table"]
    assert snapshot["sanitized_request_plan"]["LineCode"] == "1"
    assert snapshot["stable_provider_metadata"]["line_description"] == "All industry total"
    assert snapshot["stable_provider_metadata"] == stable_metadata(source_id)

    wrong_unit = copy.deepcopy(rows); wrong_unit[0]["CL_UNIT"] = "Thousands of current dollars"
    with pytest.raises(ValueError, match="unit metadata changed"):
        build_snapshot(source_id, wrong_unit)
    wrong_multiplier = copy.deepcopy(rows)
    wrong_multiplier[0]["UNIT_MULT"] = "3" if stable_metadata(source_id)["unit_multiplier"] != "3" else "6"
    with pytest.raises(ValueError, match="unit metadata changed"):
        build_snapshot(source_id, wrong_multiplier)
    optional_wrong_line = copy.deepcopy(rows); optional_wrong_line[0]["LineCode"] = "2"
    with pytest.raises(ValueError, match="contract metadata changed"):
        build_snapshot(source_id, optional_wrong_line)
    optional_wrong_table = copy.deepcopy(rows); optional_wrong_table[0]["TableName"] = "WRONG"
    with pytest.raises(ValueError, match="contract metadata changed"):
        build_snapshot(source_id, optional_wrong_table)

    drifted_table = copy.deepcopy(snapshot)
    drifted_table["sanitized_request_plan"]["TableName"] = "WRONG"
    with pytest.raises(ValueError, match="snapshot contract mismatch"):
        canonicalize_snapshot(drifted_table, source_id)
    drifted_line = copy.deepcopy(snapshot)
    drifted_line["sanitized_request_plan"]["LineCode"] = "2"
    with pytest.raises(ValueError, match="snapshot contract mismatch"):
        canonicalize_snapshot(drifted_line, source_id)


def test_source_specific_native_units_are_not_interchangeable_or_rescaled():
    assert OBSERVATION_METADATA == {
        "bea_gdp_qtr": {"unit": "Millions of chained 2017 dollars", "unit_multiplier": "6"},
        "bea_gdp_ann": {"unit": "Thousands of chained 2017 dollars", "unit_multiplier": "3"},
    }
    for source_id, other_source in (("bea_gdp_qtr", "bea_gdp_ann"),
                                    ("bea_gdp_ann", "bea_gdp_qtr")):
        rows = provider_rows(source_id)
        native_value = rows[0]["DataValue"]
        snapshot = build_snapshot(source_id, rows)
        assert snapshot["normalized_observations"][0]["value"] == native_value.replace(",", "").rstrip("0").rstrip(".")
        canonical, _ = canonicalize_snapshot(snapshot, source_id)
        first = snapshot["normalized_observations"][0]
        canonical_value = canonical.loc[
            (canonical.geo_id == first["geo_id"]) & (canonical.date.astype(str) == first["date"]), "value"
        ].iloc[0]
        assert canonical_value == float(native_value.replace(",", ""))
        swapped = copy.deepcopy(rows)
        swapped[0]["CL_UNIT"] = OBSERVATION_METADATA[other_source]["unit"]
        swapped[0]["UNIT_MULT"] = OBSERVATION_METADATA[other_source]["unit_multiplier"]
        with pytest.raises(ValueError, match="unit metadata changed"):
            build_snapshot(source_id, swapped)


def test_normal_persists_then_executes_and_resume_replay_never_discover(tmp_path):
    source = "bea_gdp_qtr"; store = FilePinStore(tmp_path / "store"); calls = []
    def discover():
        calls.append("discover")
        snapshot = build_snapshot(source, provider_rows(source))
        return discover_pin(cycle_id="cycle", source_id=source, workspace=tmp_path / "discovery",
            acquire=lambda *_args, **_kwargs: (snapshot, {}), retrieved_at="2026-09-16T00:00:00Z")
    def build(**kwargs):
        calls.append("build"); assert store.get("cycle", source) == kwargs["pin"]
        return candidate(repository_root=Path("."), **kwargs)
    sequence = 0
    def publish(path, source_id):
        nonlocal sequence; sequence += 1
        manifest = json.loads((path / "manifest.json").read_text())
        return {"record": {"object_id": manifest["artifact_id"], "artifact_content_hash": manifest["artifact_content_hash"],
            "package_sha256": str(sequence) * 64, "logical_artifact_uri": manifest["artifact_uri"],
            "metadata": {"provider_release_id": manifest["provider_release_id"], "observation_max": manifest["observation_max"]}}, "catalog": {}}
    execute_source(source_id=source, mode="normal", cycle_id="cycle", workspace=tmp_path / "normal",
        pin_store=store, discover=discover, build=build, publish=publish, record=lambda *_: None)
    for mode in ("resume", "replay"):
        execute_source(source_id=source, mode=mode, cycle_id="cycle", workspace=tmp_path / mode,
            pin_store=store, discover=lambda: (_ for _ in ()).throw(AssertionError("BEA reacquired")),
            build=build, publish=publish, record=lambda *_: None)
    assert calls == ["discover", "build", "build", "build"]
    recovered = tmp_path / "recovered.json"; recover_pinned_snapshot(store.get("cycle", source), recovered)
    assert recovered.read_bytes() == snapshot_bytes(build_snapshot(source, provider_rows(source)))
