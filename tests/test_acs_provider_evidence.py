"""Full governed membership with offline synthetic HTTP responses only."""
import copy
import hashlib
import json
from pathlib import Path

import pytest
import yaml

from jobs.monthly_refresh import acs_provider_evidence as pilot
from sources.census_acs.artifact import VARIABLES, geography_plan
from sources.census_acs.historical import validate_evidence

KEY = "fixture-credential-must-never-be-persisted"
FIXTURE = json.loads(Path("tests/fixtures/census_acs_historical/provider.json").read_text())


class Response:
    def __init__(self, payload=None, *, status=200, raw=None):
        self.status_code = status
        self.body = raw if raw is not None else json.dumps(payload).encode()
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def iter_content(self, chunk_size): yield self.body


class Session:
    def __init__(self, mode="success"):
        self.mode, self.calls = mode, []
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def get(self, url, *, params, timeout, stream, allow_redirects):
        assert timeout == 60 and stream and not allow_redirects
        self.calls.append((url, params))
        if url.endswith("data.json"):
            assert params is None
            dataset = {"c_vintage": 2023, "c_dataset": ["acs", "acs5"],
                       "identifier": pilot.ENDPOINT}
            if self.mode == "wrong_year": dataset["c_vintage"] = 2024
            if self.mode == "wrong_product": dataset["c_dataset"] = ["acs", "acs1"]
            return Response({"dataset": [dataset]})
        if url.endswith("variables.json"):
            metadata = copy.deepcopy(FIXTURE["metadata"])
            if self.mode == "malformed_metadata": metadata["variables"]["B01003_001E"].pop("concept")
            return Response(metadata)
        assert url == pilot.ENDPOINT and params["key"] == KEY
        assert params["get"] == "NAME," + ",".join(VARIABLES)
        if len(self.calls) == 3:
            if self.mode == "204": return Response(status=204, raw=b"")
            if self.mode == "nonempty_204": return Response(status=204, raw=b"unexpected")
            if self.mode == "missing_geography": return Response([])
            if self.mode == "http400": return Response({"error": "unknown geography"}, status=400)
            if self.mode == "redirect": return Response(status=302, raw=b"redirect rejected")
            if self.mode == "credential_echo": return Response(raw=KEY.encode())
            if self.mode == "transport_error": raise RuntimeError("key=" + KEY)
            if self.mode == "oversized": return Response(raw=b"x" * (1024 * 1024 + 1))
        geo = dict(part.rsplit(":", 1) for part in [params["for"], *([params["in"]] if "in" in params else [])])
        header = ["NAME", "B01003_001E", "B19013_001E", *geo]
        values = ["fixture geography", "100", "70000", *geo.values()]
        if len(self.calls) == 3:
            if self.mode == "wrong_geography": values[-1] = "99999"
            if self.mode == "unknown_sentinel": values[1] = "-222222222"
            if self.mode == "sentinel": values[1] = "-666666666"
            if self.mode == "null": values[2] = None
        return Response([header, values])


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    import socket
    def forbidden(*args, **kwargs): raise AssertionError("network forbidden in fixture tests")
    monkeypatch.setattr(socket.socket, "connect", forbidden)


def run(tmp_path, mode="success"):
    session = Session(mode)
    output = tmp_path / "pilot"
    summary = pilot.run_pilot(output, credential=KEY, session=session)
    for f in output.rglob("*"):
        if f.is_file(): assert KEY.encode() not in f.read_bytes()
    return output, summary, session


def test_complete_governed_evidence_and_hashes(tmp_path):
    output, summary, session = run(tmp_path)
    assert summary["validated"] and summary["captured_responses"] == 225
    assert summary["requested_members"] == 223 and summary["excluded_members"] == 10
    assert summary["accounting"] == {"available": 446}
    assert summary["production_writes"] is False
    value = json.loads((output / "provider-evidence.json").read_text())["2023"]
    plan, excluded = geography_plan()
    assert validate_evidence(value, source_id="census_acs5", vintage=2023, plan=plan, excluded=excluded)
    records = json.loads((output / "response-manifest.json").read_text())["responses"]
    for record in records:
        body = (output / record["body_path"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == record["body_sha256"]
        assert "key=" not in record["request_url"]
    assert len(session.calls) == 225


@pytest.mark.parametrize("mode,classification,count", [
    ("204", "provider_no_content", 2), ("sentinel", "provider_sentinel:-666666666", 1),
    ("null", "provider_null", 1)])
def test_exact_observed_omission_receipts(tmp_path, mode, classification, count):
    output, summary, _ = run(tmp_path, mode)
    assert summary["validated"] and summary["accounting"][classification] == count
    value = json.loads((output / "provider-evidence.json").read_text())["2023"]
    assert len(value["omission_evidence"]) == 1
    receipt = value["omission_evidence"][0]
    assert receipt["classification"] == classification
    records = json.loads((output / "response-manifest.json").read_text())["responses"]
    assert receipt["request_url"] == records[2]["request_url"]
    assert receipt["response_sha256"] == records[2]["decoded_response_sha256"]
    if mode == "204":
        assert records[2]["http_status"] == 204 and records[2]["body_size_bytes"] == 0


@pytest.mark.parametrize("mode", ["missing_geography", "wrong_geography", "nonempty_204", "http400",
    "malformed_metadata", "unknown_sentinel", "wrong_year", "wrong_product", "credential_echo",
    "transport_error", "oversized", "redirect"])
def test_invalid_or_incompatible_provider_retains_diagnostics_not_evidence(tmp_path, mode):
    output, summary, session = run(tmp_path, mode)
    expected_calls = 225 if mode in {"missing_geography", "wrong_geography", "unknown_sentinel"} else 3
    assert not summary["validated"] and len(session.calls) <= expected_calls
    assert not (output / "provider-evidence.json").exists()
    assert summary["failure"] == "provider_evidence_inaccessible_or_invalid"
    assert (output / "response-manifest.json").exists()
    if mode == "http400":
        records = json.loads((output / "response-manifest.json").read_text())["responses"]
        assert records[-1]["http_status"] == 400
        assert records[-1]["decoded_response_sha256"] is None


def test_missing_snapshot_member_cannot_be_approved(tmp_path):
    output, _, _ = run(tmp_path)
    value = json.loads((output / "provider-evidence.json").read_text())["2023"]
    value["snapshot"]["members"].pop()
    plan, excluded = geography_plan()
    with pytest.raises(ValueError, match="missing"):
        validate_evidence(value, source_id="census_acs5", vintage=2023, plan=plan, excluded=excluded)


def test_fixed_membership_change_fails_before_network(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot, "geography_plan", lambda path: ([], []))
    _, summary, session = run(tmp_path)
    assert not summary["validated"] and not session.calls


def test_missing_key_fails_before_network(tmp_path):
    session = Session()
    summary = pilot.run_pilot(tmp_path / "pilot", credential="", session=session)
    assert not summary["validated"] and not session.calls


def test_request_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot, "MAX_REQUESTS", 2)
    _, summary, session = run(tmp_path)
    assert not summary["validated"] and len(session.calls) == 2


def test_time_bound(tmp_path, monkeypatch):
    clock = iter([0, pilot.MAX_SECONDS + 1])
    monkeypatch.setattr(pilot.time, "monotonic", lambda: next(clock))
    _, summary, session = run(tmp_path)
    assert not summary["validated"] and not session.calls


def test_workflow_is_manual_main_only_read_only():
    workflow = yaml.safe_load(Path(".github/workflows/acs5-2023-provider-evidence.yml").read_text())
    trigger = workflow.get("on", workflow.get(True))
    assert set(trigger) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["evidence"]
    assert "refs/heads/main" in job["if"] and "chineduezebuiroh/realestate-intel" in job["if"]
    assert job["timeout-minutes"] == 20
    checkout = job["steps"][0]["with"]
    assert checkout == {"ref": "main", "fetch-depth": 0, "persist-credentials": False}
    capture = next(s for s in job["steps"] if s.get("name") == "Capture and validate exact provider evidence")
    assert "GITHUB_TOKEN" not in capture["env"]
    assert capture["env"]["CENSUS_API_KEY"] == "${{ secrets.CENSUS_API_KEY }}"
