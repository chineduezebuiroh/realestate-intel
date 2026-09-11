from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.acs_hosted import execute_product
from jobs.monthly_refresh.acs_monthly import acquire_snapshot, candidate, discover_pin
from jobs.monthly_refresh.source_inputs import FilePinStore
from sources.census_acs.artifact import CONTRACT_VERSION, canonicalize


PLAN = [
    {"geo_id": "united_states__nation", "level": "nation", "census_code": "1"},
    {"geo_id": "alpha__county", "level": "county", "census_code": "01001"},
    {"geo_id": "big_stone_gap__cbsa_metro", "level": "cbsa_metro", "census_code": "13720"},
]
EXCLUDED = [{"geo_id": "division__cbsa_metro", "level": "cbsa_metro", "census_code": "11244",
             "classification": "CANONICAL_CONCEPT_MISMATCH",
             "disposition": "EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT"}]


class Response:
    def __init__(self, status: int, payload=None): self.status_code, self._payload = status, payload
    def raise_for_status(self):
        if self.status_code >= 400: raise RuntimeError("http failure")
    def json(self): return self._payload


class Session:
    def __init__(self, income="70000", absent=frozenset({"13720"})):
        self.income, self.absent, self.calls = income, absent, []
    def get(self, url, *, params, timeout):
        self.calls.append((url, dict(params)))
        code = params["for"].split(":")[-1]
        if code in self.absent: return Response(204)
        return Response(200, [["NAME", "B01003_001E", "B19013_001E"], ["x", "100", self.income]])


def _patch_plan(monkeypatch):
    monkeypatch.setattr("jobs.monthly_refresh.acs_monthly.geography_plan", lambda: (PLAN, EXCLUDED))


def test_discovery_pin_membership_and_credential_non_persistence(tmp_path, monkeypatch):
    _patch_plan(monkeypatch); secret = "do-not-persist-this-key"; session = Session()
    monkeypatch.setenv("CENSUS_API_KEY", secret)
    pin, paths = discover_pin(cycle_id="cycle", source_id="census_acs1", workspace=tmp_path,
        vintages=lambda source: [2025, 2024],
        acquire=lambda **kw: acquire_snapshot(**kw, session=session), retrieved_at="2026-01-01T00:00:00Z")
    assert pin["provider_release_id"] == "acs/acs1:2025"
    assert pin["members"]["snapshot"]["evidence"]["available_membership"] == ["alpha__county", "united_states__nation"]
    assert pin["members"]["snapshot"]["evidence"]["provider_ineligible"] == ["big_stone_gap__cbsa_metro"]
    persisted = json.dumps(pin) + paths["snapshot"].read_text()
    assert secret not in persisted and "key=" not in persisted
    assert all(call[1]["key"] == secret for call in session.calls)
    assert all(secret not in member["request_url"] for member in json.loads(paths["snapshot"].read_text())["members"])


def test_products_are_independent_and_content_changes_identity(tmp_path, monkeypatch):
    _patch_plan(monkeypatch)
    one, one_paths = discover_pin(cycle_id="cycle", source_id="census_acs1", workspace=tmp_path / "one",
        vintages=lambda _: [2024], acquire=lambda **kw: acquire_snapshot(**kw, session=Session(), key="runtime"),
        retrieved_at="2026-01-01T00:00:00Z")
    five, _ = discover_pin(cycle_id="cycle", source_id="census_acs5", workspace=tmp_path / "five",
        vintages=lambda _: [2023], acquire=lambda **kw: acquire_snapshot(**kw, session=Session(absent=frozenset()), key="runtime"),
        retrieved_at="2026-01-01T00:00:00Z")
    changed, changed_paths = discover_pin(cycle_id="other-cycle", source_id="census_acs1", workspace=tmp_path / "changed",
        vintages=lambda _: [2024], acquire=lambda **kw: acquire_snapshot(**kw, session=Session(income="70001"), key="runtime"),
        retrieved_at="2026-01-01T00:00:00Z")
    assert one["provider_release_id"] == "acs/acs1:2024" and five["provider_release_id"] == "acs/acs5:2023"
    assert one["members"]["snapshot"]["evidence"]["available_membership"] != five["members"]["snapshot"]["evidence"]["available_membership"]
    assert one["members"]["snapshot"]["sha256"] != changed["members"]["snapshot"]["sha256"]
    original_candidate = candidate(pin=one, paths=one_paths, output=tmp_path / "candidate-one",
                                   cycle_id="cycle", repository_root=Path("."))
    revised_candidate = candidate(pin=changed, paths=changed_paths, output=tmp_path / "candidate-changed",
                                  cycle_id="other-cycle", repository_root=Path("."))
    assert original_candidate["manifest"]["artifact_id"] != revised_candidate["manifest"]["artifact_id"]


def test_canonical_semantics_no_synthesis_and_candidate_identity(tmp_path, monkeypatch):
    _patch_plan(monkeypatch); snapshot_path = tmp_path / "snapshot"
    snapshot = acquire_snapshot(source_id="census_acs5", vintage=2024, output=snapshot_path,
                                session=Session(), key="runtime")
    frame, diagnostics = canonicalize("census_acs5", 2024, snapshot)
    assert set(frame.metric_id) == {"census_acs5_pop_total", "census_acs5_median_household_income"}
    assert set(frame.date.astype(str)) == {"2024-12-31"}
    assert set(frame.property_type_id) == {"all"} and set(frame.property_type) == {"all"}
    assert "big_stone_gap__cbsa_metro" not in set(frame.geo_id)
    assert diagnostics["excluded_geographies"] == EXCLUDED
    assert "division__cbsa_metro" not in set(frame.geo_id)


def test_resume_and_replay_never_discover(tmp_path):
    cycle, source = "cycle", "census_acs1"; store = FilePinStore(tmp_path)
    snapshot = {"contract_version": CONTRACT_VERSION, "source_id": source, "product": "acs/acs1",
        "vintage": 2024, "members": [], "excluded_geographies": []}
    raw = tmp_path / "seed"; raw.write_text(json.dumps(snapshot))
    from jobs.monthly_refresh.source_inputs import provider_pin
    import hashlib
    pin = provider_pin(cycle_id=cycle, source_id=source, provider_release_id="acs/acs1:2024",
        members={"snapshot": {"url": "https://api.census.gov/data/2024/acs/acs1",
            "retrieved_at": "2026-01-01T00:00:00Z", "sha256": hashlib.sha256(raw.read_bytes()).hexdigest()}})
    store.put(pin); executions = []
    for mode in ("resume", "replay"):
        execute_product(source_id=source, mode=mode, cycle_id=cycle, workspace=tmp_path / mode,
            pin_store=store, discover=lambda: (_ for _ in ()).throw(AssertionError("rediscovered")),
            retrieve=lambda p, path: path.write_bytes(raw.read_bytes()),
            build=lambda **kw: executions.append(mode) or {"manifest": {}},
            publish=lambda path, s: {"record": {"object_id": "id", "artifact_content_hash": "a"*64,
                "package_sha256": "b"*64, "logical_artifact_uri": "artifact://x",
                "metadata": {"provider_release_id": "acs/acs1:2024", "observation_max": "2024-12-31"}},
                "catalog": {}}, record=lambda *_: None)
    assert executions == ["resume", "replay"]
