"""Full governed membership with offline synthetic HTTP responses only."""
import copy
import hashlib
import json
import zipfile
from pathlib import Path

import pytest
import yaml

from jobs.monthly_refresh import acs_provider_evidence as pilot
from jobs.monthly_refresh.acs_evidence_review import verify_pilot_archive
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
    def __init__(self, mode="success", source="census_acs5", year=2023):
        self.mode, self.calls = mode, []
        self.source, self.year = source, year
        self.endpoint = f"{pilot.API}/{year}/{pilot.PRODUCTS[source]}"
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def get(self, url, *, params, timeout, stream, allow_redirects):
        assert timeout == 60 and stream and not allow_redirects
        self.calls.append((url, params))
        if url.endswith("data.json"):
            assert params is None
            dataset = {"c_vintage": self.year, "c_dataset": pilot.PRODUCTS[self.source].split("/"),
                       "identifier": self.endpoint}
            if self.mode == "wrong_year": dataset["c_vintage"] = self.year + 1
            if self.mode == "wrong_product": dataset["c_dataset"] = ["acs", "wrong"]
            return Response({"dataset": [] if self.mode == "unsupported" else [dataset]})
        if url.endswith("variables.json"):
            metadata = copy.deepcopy(FIXTURE["metadata"])
            if self.mode == "malformed_metadata": metadata["variables"]["B01003_001E"].pop("concept")
            return Response(metadata)
        assert url == self.endpoint and params["key"] == KEY
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
            if self.mode == "changed": values[1] = "101"
        return Response([header, values])


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    import socket
    def forbidden(*args, **kwargs): raise AssertionError("network forbidden in fixture tests")
    monkeypatch.setattr(socket.socket, "connect", forbidden)


def run(tmp_path, mode="success"):
    session = Session(mode)
    output = tmp_path / "pilot"
    summary = pilot.capture_year(output, credential=KEY, session=session)
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
    summary = pilot.capture_year(tmp_path / "pilot", credential="", session=session)
    assert not summary["validated"] and not session.calls


def test_request_bound(tmp_path, monkeypatch):
    monkeypatch.setattr(pilot, "MAX_REQUESTS", 2)
    _, summary, session = run(tmp_path)
    assert not summary["validated"] and len(session.calls) == 2


def test_time_bound(tmp_path, monkeypatch):
    clock = [0]
    monkeypatch.setattr(pilot.time, "monotonic", lambda: clock[0])
    class SlowSession(Session):
        def get(self, *args, **kwargs):
            response = super().get(*args, **kwargs)
            clock[0] = pilot.MAX_SECONDS + 1
            return response
    session = SlowSession()
    summary = pilot.capture_year(tmp_path / "slow", credential=KEY, session=session)
    assert not summary["validated"] and len(session.calls) == 1


def test_workflow_is_manual_main_only_read_only():
    workflow = yaml.safe_load(Path(".github/workflows/acs-provider-evidence.yml").read_text())
    trigger = workflow.get("on", workflow.get(True))
    assert set(trigger) == {"workflow_dispatch"}
    assert workflow["permissions"] == {"contents": "read"}
    job = workflow["jobs"]["evidence"]
    assert "refs/heads/main" in job["if"] and "chineduezebuiroh/realestate-intel" in job["if"]
    assert job["timeout-minutes"] == 40
    assert set(trigger["workflow_dispatch"]["inputs"]) == {"source_id", "vintages", "batch_size"}
    checkout = job["steps"][0]["with"]
    assert checkout == {"ref": "main", "fetch-depth": 0, "persist-credentials": False}
    capture = next(s for s in job["steps"] if s.get("name") == "Capture and validate exact provider evidence")
    assert "GITHUB_TOKEN" not in capture["env"]
    assert capture["env"]["CENSUS_API_KEY"] == "${{ secrets.CENSUS_API_KEY }}"


@pytest.mark.parametrize("source", ["census_acs1", "census_acs5"])
def test_successful_batch_identity_and_independent_year_maps(tmp_path, source):
    def capture(name):
        sessions = iter([Session(source=source, year=y) for y in [2021, 2022]])
        return pilot.capture_batch(tmp_path / name, source_id=source, vintages=[2022, 2021],
            batch_size=2, credential=KEY, session_factory=lambda: next(sessions))
    first, second = capture("a"), capture("b")
    assert first["validated"] and first["http_requests"] == 450
    assert first["batch_id"] == second["batch_id"]
    assert [e["capture_id"] for e in first["entries"]] == [e["capture_id"] for e in second["entries"]]
    combined = json.loads((tmp_path / "a/provider-evidence.json").read_text())
    assert set(combined) == {"2021", "2022"}
    for y in [2021, 2022]:
        assert combined[str(y)]["source_id"] == source
        assert set(json.loads((tmp_path / f"a/{y}/provider-evidence.json").read_text())) == {str(y)}


def test_failure_isolation_partial_recovery_and_no_credential_artifacts(tmp_path):
    sessions = iter([Session("credential_echo", year=2021), Session("204", year=2022)])
    result = pilot.capture_batch(tmp_path / "partial", source_id="census_acs5", vintages=[2021, 2022],
        batch_size=2, credential=KEY, session_factory=lambda: next(sessions))
    assert not result["validated"] and result["failed_vintages"] == [2021]
    assert result["successful_vintages"] == [2022]
    assert set(json.loads((tmp_path / "partial/provider-evidence.json").read_text())) == {"2022"}
    retry = pilot.capture_batch(tmp_path / "retry", source_id="census_acs5", vintages=[2021],
        batch_size=1, credential=KEY, session_factory=lambda: Session(year=2021))
    assert retry["validated"] and retry["http_requests"] == 225
    # Recovery is a fresh explicit failed-year capture; prior success is preserved.
    assert json.loads((tmp_path / "partial/2022/summary.json").read_text())["capture_id"] == result["entries"][1]["capture_id"]
    for f in tmp_path.rglob("*"):
        if f.is_file(): assert KEY.encode() not in f.read_bytes()


@pytest.mark.parametrize("years,batch", [([2023, 2023], 2), (list(range(2010, 2016)), 5),
    ([2004], 1), ([2101], 1), ([True], 1), ([], 1), ([2023], 0), ([2022, 2023], 1)])
def test_invalid_batch_rejected_before_outputs_or_transport(tmp_path, years, batch):
    with pytest.raises(ValueError):
        pilot.capture_batch(tmp_path / "invalid", source_id="census_acs5", vintages=years,
            batch_size=batch, credential=KEY, session_factory=lambda: pytest.fail("unexpected transport"))
    assert not (tmp_path / "invalid").exists()


def test_unadvertised_year_does_not_fall_back_or_hide_next_year(tmp_path):
    sessions = iter([Session("unsupported", source="census_acs1", year=2020),
                     Session(source="census_acs1", year=2021)])
    result = pilot.capture_batch(tmp_path / "unsupported", source_id="census_acs1", vintages=[2020, 2021],
        batch_size=2, credential=KEY, session_factory=lambda: next(sessions))
    assert result["failed_vintages"] == [2020] and result["successful_vintages"] == [2021]
    assert not (tmp_path / "unsupported/2020/provider-evidence.json").exists()


def test_configuration_drift_rejects_evidence(tmp_path, monkeypatch):
    original = pilot.governed_config_hashes
    changed = [False]
    monkeypatch.setattr(pilot, "governed_config_hashes", lambda root: {**original(root), **({"drift": "x"} if changed[0] else {})})
    class DriftingSession(Session):
        def get(self, *args, **kwargs):
            response = super().get(*args, **kwargs)
            if len(self.calls) == 225: changed[0] = True
            return response
    sessions = iter([DriftingSession(year=2021), Session(year=2022)])
    result = pilot.capture_batch(tmp_path / "drift", source_id="census_acs5", vintages=[2021, 2022],
        batch_size=2, credential=KEY, session_factory=lambda: next(sessions))
    assert result["failed_vintages"] == [2021, 2022] and not result["successful_vintages"]
    assert not list((tmp_path / "drift").rglob("provider-evidence.json"))


def test_batch_time_and_output_bounds(tmp_path, monkeypatch):
    budget = pilot.CaptureBudget(5)
    monkeypatch.setattr(pilot.time, "monotonic", lambda: budget.started + pilot.MAX_BATCH_SECONDS + 1)
    with pytest.raises(ValueError, match="time"): budget.check()
    monkeypatch.setattr(pilot, "MAX_OUTPUT_BYTES", 16)
    monkeypatch.setattr(pilot, "AUDIT_RESERVE", 4)
    budget.write(tmp_path / "data", b"x" * 12)
    with pytest.raises(ValueError, match="output"): budget.write(tmp_path / "overflow", b"x")
    budget.write(tmp_path / "audit", b"x" * 4, audit=True)
    assert budget.output_bytes == 16


def test_session_setup_failure_isolated_per_year(tmp_path):
    def broken(): raise RuntimeError("secret=" + KEY)
    result = pilot.capture_batch(tmp_path / "setup", source_id="census_acs5", vintages=[2021, 2022],
        batch_size=2, credential=KEY, session_factory=broken)
    assert result["failed_vintages"] == [2021, 2022] and result["http_requests"] == 0
    assert KEY not in (tmp_path / "setup/batch-summary.json").read_text()


def test_provider_drift_changes_identity_and_existing_output_is_never_overwritten(tmp_path):
    original = pilot.capture_batch(tmp_path / "original", source_id="census_acs5", vintages=[2023],
        batch_size=1, credential=KEY, session_factory=Session)
    changed = pilot.capture_batch(tmp_path / "changed", source_id="census_acs5", vintages=[2023],
        batch_size=1, credential=KEY, session_factory=lambda: Session("changed"))
    assert original["entries"][0]["capture_id"] != changed["entries"][0]["capture_id"]
    before = (tmp_path / "original/batch-summary.json").read_bytes()
    with pytest.raises(FileExistsError):
        pilot.capture_batch(tmp_path / "original", source_id="census_acs5", vintages=[2023],
            batch_size=1, credential=KEY, session_factory=Session)
    assert (tmp_path / "original/batch-summary.json").read_bytes() == before


def test_aggregation_failure_does_not_hide_individual_success(tmp_path, monkeypatch):
    write = pilot.write_json
    def bounded(path, value, *args, **kwargs):
        if path == tmp_path / "aggregate/provider-evidence.json":
            raise ValueError("output limit")
        return write(path, value, *args, **kwargs)
    monkeypatch.setattr(pilot, "write_json", bounded)
    result = pilot.capture_batch(tmp_path / "aggregate", source_id="census_acs5", vintages=[2023],
        batch_size=1, credential=KEY, session_factory=Session)
    assert not result["validated"] and result["aggregation_failure"] == "batch_output_limit"
    assert result["successful_vintages"] == [2023]
    assert (tmp_path / "aggregate/2023/provider-evidence.json").is_file()


def test_cli_partial_batch_exits_nonzero_and_reports_every_year(tmp_path, monkeypatch, capsys):
    sessions = iter([Session("unsupported", year=2021), Session(year=2022)])
    capture = pilot.capture_batch
    monkeypatch.setattr(pilot, "capture_batch", lambda *a, **kw: capture(*a, **kw, session_factory=lambda: next(sessions)))
    monkeypatch.setenv("CENSUS_API_KEY", KEY)
    monkeypatch.setattr("sys.argv", ["capture", "--source-id", "census_acs5", "--vintages", "2021,2022",
        "--batch-size", "2", "--output", str(tmp_path / "cli")])
    assert pilot.main() == 1
    logged = capsys.readouterr().out
    assert KEY not in logged
    assert json.loads(logged)["failed_vintages"] == [2021]
    assert json.loads(logged)["successful_vintages"] == [2022]


def pilot_archive(tmp_path, mode="204", mutate=None):
    output, _, _ = run(tmp_path, mode)
    files = {str(f.relative_to(output)): f.read_bytes() for f in output.rglob("*") if f.is_file()}
    summary = json.loads(files["summary.json"])
    # Replay the original successful pilot's wrapper convention as well.
    summary["schema_version"] = "acs_provider_evidence_pilot_summary_v1"
    files["summary.json"] = json.dumps(summary).encode()
    if mutate: mutate(files)
    archive = tmp_path / "capture.zip"
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as z:
        for name, body in files.items(): z.writestr(name, body)
    return archive, hashlib.sha256(archive.read_bytes()).hexdigest()


def test_offline_exact_archive_reconstructs_provider_evidence(tmp_path):
    archive, sha = pilot_archive(tmp_path)
    body, report = verify_pilot_archive(archive, expected_sha256=sha)
    assert json.loads(body)["2023"]["source_id"] == "census_acs5"
    assert report["validated"] and report["responses_verified"] == 225
    assert len(report["omission_receipt_ids"]) == 1
    assert report["approval_status"] == "REVIEW_CANDIDATE_ONLY"


@pytest.mark.parametrize("mutation", ["raw", "receipt", "config", "path", "manifest", "archive"])
def test_archive_corruption_or_drift_never_produces_review_candidate(tmp_path, mutation):
    def change(files):
        if mutation == "raw": files["responses/002.body"] = b"tampered"
        if mutation == "path": files["../escape"] = b"tampered"
        if mutation in {"receipt", "config"}:
            summary = json.loads(files["summary.json"])
            if mutation == "config": summary["config_hashes"] = {}
            else:
                evidence = json.loads(files["provider-evidence.json"])
                evidence["2023"]["omission_evidence"] = []
                files["provider-evidence.json"] = json.dumps(evidence).encode()
                summary["evidence_sha256"] = hashlib.sha256(files["provider-evidence.json"]).hexdigest()
            files["summary.json"] = json.dumps(summary).encode()
        if mutation == "manifest":
            manifest = json.loads(files["response-manifest.json"])
            manifest["responses"][2]["body_size_bytes"] = 12
            files["response-manifest.json"] = json.dumps(manifest).encode()
            summary = json.loads(files["summary.json"])
            summary["response_manifest_sha256"] = hashlib.sha256(files["response-manifest.json"]).hexdigest()
            files["summary.json"] = json.dumps(summary).encode()
    archive, sha = pilot_archive(tmp_path, mutate=change)
    if mutation == "archive": sha = "0" * 64
    with pytest.raises(ValueError): verify_pilot_archive(archive, expected_sha256=sha)
