"""Smoke 167: deterministic, fail-closed prior Actions artifact resolution."""
from __future__ import annotations

import tempfile
import urllib.error
import urllib.request
import zipfile
from io import BytesIO
from pathlib import Path

import pandas as pd
import jobs.monthly_refresh.fred_prior_actions as resolver
from jobs.monthly_refresh import fred_macro

def expect(kind, call):
    try:
        call()
    except kind:
        return
    raise AssertionError(f"expected {kind.__name__}")


def fact(rows):
    return pd.DataFrame([{"geo_id": "united_states__nation", "metric_id": "fred_cpi_urban_sa_index",
        "date": date, "property_type_id": "all", "value": value, "source_id": "fred_macro",
        "property_type": "all"} for date, value in rows])


assert resolver.select_prior_run([], 9) is None
runs = [{"id": 9, "status": "completed", "conclusion": "success", "created_at": "2026-08-03"},
        {"id": 7, "status": "completed", "conclusion": "failure", "created_at": "2026-08-02"},
        {"id": 6, "status": "completed", "conclusion": "success", "created_at": "2026-08-01"}]
assert resolver.select_prior_run(runs, 9)["id"] == 6
expect(RuntimeError, lambda: resolver.select_fred_artifact([]))
expect(RuntimeError, lambda: resolver.select_fred_artifact([{"name": "fred-governed-artifact-a", "expired": False}, {"name": "fred-governed-artifact-b", "expired": False}]))

with tempfile.TemporaryDirectory() as td:
    root = Path(td)
    base = fact([("2026-06-30", 1.0), ("2026-07-31", 2.0)])
    fred_macro.run(target_month="2026-07", output_root=root / "evidence", acquire=lambda: base,
                   retrieved_at="2026-08-01T00:00:00Z", git_sha="fixture")
    (root / "evidence/workflow_summary.json").write_text("{}")
    explicit = resolver.resolve(repository="owner/repo", current_run_id=10, token="unused",
                                download_root=root / "unused", explicit_path=root / "evidence/artifact")
    assert explicit["resolution"] == "explicit" and explicit["path"].name == "artifact"
    tampered = root / "evidence/artifact/data.parquet"
    tampered.write_bytes(tampered.read_bytes() + b"tamper")
    expect(Exception, lambda: resolver.resolve(repository="owner/repo", current_run_id=10, token="unused",
                                               download_root=root / "unused2", explicit_path=root / "evidence/artifact"))

    # Automatic API behavior is exercised without network by replacing only the transport boundary.
    package = root / "package-source"
    fred_macro.run(target_month="2026-07", output_root=package, acquire=lambda: base,
                   retrieved_at="2026-08-01T00:00:00Z", git_sha="fixture")
    (package / "workflow_summary.json").write_text("{}")
    archive = root / "fixture.zip"
    with zipfile.ZipFile(archive, "w") as bundle:
        for path in package.rglob("*"):
            if path.is_file(): bundle.write(path, path.relative_to(package))
    original_json = resolver._request_json
    def fake_json(url, token):
        if "/runs?" in url:
            return {"workflow_runs": [{"id": 8, "status": "completed", "conclusion": "success", "created_at": "2026-08-01"}]}
        return {"artifacts": [{"name": "fred-governed-artifact-inferred-sha", "expired": False,
                               "archive_download_url": "https://api.github.invalid/archive"}]}
    resolver._request_json = fake_json

    # Exercise the real two-step transport with deterministic offline responses.
    original_build_opener, original_urlopen = urllib.request.build_opener, urllib.request.urlopen
    api_requests, signed_requests = [], []
    signed_url = "https://signed.example.invalid/temporary-secret"
    class RedirectOpener:
        def open(self, request):
            api_requests.append(request)
            raise urllib.error.HTTPError(request.full_url, 302, "Found", {"Location": signed_url}, None)
    class SignedResponse(BytesIO):
        def __enter__(self): return self
        def __exit__(self, *args): self.close()
    urllib.request.build_opener = lambda *handlers: RedirectOpener()
    def signed_urlopen(request):
        signed_requests.append(request)
        return SignedResponse(archive.read_bytes())
    urllib.request.urlopen = signed_urlopen
    automatic = resolver.resolve(repository="owner/repo", current_run_id=10, token="fixture", download_root=root / "download")
    assert automatic["resolution"] == "actions_artifact" and automatic["prior_workflow_run_id"] == 8
    assert api_requests[0].get_header("Authorization") == "Bearer fixture"
    assert api_requests[0].get_header("Accept") == "application/vnd.github+json"
    assert api_requests[0].get_header("X-github-api-version") == "2022-11-28"
    assert signed_requests[0].full_url == signed_url
    assert signed_requests[0].get_header("Authorization") is None
    repeat = fred_macro.run(output_root=root / "repeat", prior_artifact=automatic["path"], acquire=lambda: base, git_sha="fixture")
    assert repeat["run_status"] == "unchanged" and repeat["prior_artifact_id"] == automatic["artifact_id"]
    resolver._request_json = lambda url, token: {"workflow_runs": []}
    assert resolver.resolve(repository="owner/repo", current_run_id=10, token="fixture", download_root=root / "bootstrap")["resolution"] == "bootstrap"
    resolver._request_json = original_json

    # Redirect metadata and both download stages fail closed.
    destination = root / "failure.zip"
    def redirect_error(headers=None, code=302):
        class Opener:
            def open(self, request):
                raise urllib.error.HTTPError(request.full_url, code, "failure", headers or {}, None)
        return Opener()
    urllib.request.build_opener = lambda *handlers: redirect_error()
    expect(RuntimeError, lambda: resolver._download("https://api.github.invalid/archive", "fixture", destination))
    class UnexpectedResponse(BytesIO):
        def getcode(self): return 200
    class UnexpectedOpener:
        def open(self, request): return UnexpectedResponse(b"not a redirect")
    urllib.request.build_opener = lambda *handlers: UnexpectedOpener()
    expect(RuntimeError, lambda: resolver._download("https://api.github.invalid/archive", "fixture", destination))
    urllib.request.build_opener = lambda *handlers: redirect_error({"Location": signed_url})
    urllib.request.urlopen = lambda request: (_ for _ in ()).throw(urllib.error.URLError("signed failure"))
    expect(urllib.error.URLError, lambda: resolver._download("https://api.github.invalid/archive", "fixture", destination))
    assert not destination.exists()
    urllib.request.urlopen = lambda request: SignedResponse(b"")
    expect(RuntimeError, lambda: resolver._download("https://api.github.invalid/archive", "fixture", destination))
    assert not destination.exists()
    urllib.request.build_opener, urllib.request.urlopen = original_build_opener, original_urlopen

print("FRED prior Actions resolver smoke: ok")
