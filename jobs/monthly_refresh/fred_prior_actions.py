"""Resolve the prior governed FRED artifact from temporary Actions storage."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

from core.source_artifacts.artifact import artifact_package_sha256
from core.source_artifacts.hashing import write_canonical_json
from core.source_artifacts.validation import validate_artifact

WORKFLOW = "fred-monthly-artifact.yml"
BRANCH = "monthly-refresh-orchestration"
ARTIFACT_PREFIX = "fred-governed-artifact-"
EXPECTED_PACKAGE_FILES = ("run_report.json", "workflow_summary.json")


def select_prior_run(runs: list[dict], current_run_id: int) -> dict | None:
    """Select the newest successful, completed, non-current workflow run."""
    candidates = [run for run in runs if int(run["id"]) != current_run_id
                  and run.get("status") == "completed" and run.get("conclusion") == "success"]
    return max(candidates, key=lambda run: (run.get("created_at", ""), int(run["id"])), default=None)


def select_fred_artifact(artifacts: list[dict]) -> dict:
    candidates = [item for item in artifacts if item.get("name", "").startswith(ARTIFACT_PREFIX)
                  and not item.get("expired", False)]
    if len(candidates) != 1:
        raise RuntimeError(f"expected exactly one non-expired FRED governed artifact; found {len(candidates)}")
    return candidates[0]


def _request_json(url: str, token: str) -> dict:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
    with urllib.request.urlopen(request) as response:
        return json.load(response)


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Keep the GitHub archive redirect available for explicit handling."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _download(url: str, token: str, destination: Path) -> None:
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"})
    opener = urllib.request.build_opener(_NoRedirect())
    try:
        response = opener.open(request)
    except urllib.error.HTTPError as error:
        if error.code != 302:
            raise RuntimeError(f"GitHub artifact download endpoint returned HTTP {error.code}; expected 302") from error
        location = error.headers.get("Location")
        error.close()
    else:
        status = response.getcode()
        response.close()
        raise RuntimeError(f"GitHub artifact download endpoint returned HTTP {status}; expected 302")
    if not location:
        raise RuntimeError("GitHub artifact download redirect did not include a Location header")

    # The Location is a short-lived, credential-bearing signed URL.  Deliberately
    # create a new request without the GitHub bearer token (or any API headers).
    signed_request = urllib.request.Request(location)
    try:
        with urllib.request.urlopen(signed_request) as signed_response, destination.open("wb") as output:
            shutil.copyfileobj(signed_response, output)
    except Exception:
        destination.unlink(missing_ok=True)
        raise
    if destination.stat().st_size == 0:
        destination.unlink()
        raise RuntimeError("downloaded Actions artifact ZIP is empty")


def validate_artifact_directory(artifact: Path) -> dict:
    validated = validate_artifact(artifact, expected_source_id="fred_macro")
    manifest = validated["manifest"]
    if manifest.get("artifact_status") != "complete" or manifest.get("validation_status") != "passed":
        raise RuntimeError("prior FRED governed artifact status is not complete and passed")
    return {"path": artifact, "artifact_id": manifest["artifact_id"],
            "artifact_package_sha256": artifact_package_sha256(artifact)}


def validate_package(package_root: Path) -> dict:
    missing = [name for name in EXPECTED_PACKAGE_FILES if not (package_root / name).is_file()]
    artifact = package_root / "artifact"
    if missing or not artifact.is_dir():
        raise RuntimeError(f"downloaded FRED evidence has invalid structure; missing: {missing + ([] if artifact.is_dir() else ['artifact/'])}")
    return validate_artifact_directory(artifact)


def resolve(*, repository: str, current_run_id: int, token: str, download_root: Path,
            explicit_path: Path | None = None) -> dict:
    if explicit_path is not None:
        validated = validate_artifact_directory(explicit_path)
        return {"resolution": "explicit", "prior_workflow_run_id": None,
                "prior_actions_artifact_name": None, **validated}

    api = f"https://api.github.com/repos/{repository}"
    payload = _request_json(f"{api}/actions/workflows/{WORKFLOW}/runs?branch={BRANCH}&status=completed&per_page=100", token)
    prior_run = select_prior_run(payload.get("workflow_runs", []), current_run_id)
    if prior_run is None:
        return {"resolution": "bootstrap", "path": None, "prior_workflow_run_id": None,
                "prior_actions_artifact_name": None, "artifact_id": None, "artifact_package_sha256": None}
    artifacts = _request_json(f"{api}/actions/runs/{prior_run['id']}/artifacts?per_page=100", token).get("artifacts", [])
    selected = select_fred_artifact(artifacts)
    download_root.mkdir(parents=True, exist_ok=True)
    archive = download_root / "prior.zip"
    _download(selected["archive_download_url"], token, archive)
    package_root = download_root / "package"
    package_root.mkdir()
    with zipfile.ZipFile(archive) as bundle:
        if any(Path(name).is_absolute() or ".." in Path(name).parts for name in bundle.namelist()):
            raise RuntimeError("unsafe path in downloaded Actions artifact")
        bundle.extractall(package_root)
    validated = validate_package(package_root)
    return {"resolution": "actions_artifact", "prior_workflow_run_id": int(prior_run["id"]),
            "prior_actions_artifact_name": selected["name"], **validated}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True); parser.add_argument("--current-run-id", required=True, type=int)
    parser.add_argument("--download-root", required=True, type=Path); parser.add_argument("--explicit-path", type=Path)
    parser.add_argument("--provenance-output", required=True, type=Path); parser.add_argument("--github-output", required=True, type=Path)
    args = parser.parse_args()
    result = resolve(repository=args.repository, current_run_id=args.current_run_id,
        token=os.environ.get("GITHUB_TOKEN", ""), download_root=args.download_root, explicit_path=args.explicit_path)
    serializable = {**result, "path": str(result["path"]) if result["path"] else None}
    write_canonical_json(args.provenance_output, serializable)
    with args.github_output.open("a") as output:
        output.write(f"prior_artifact={serializable['path'] or ''}\n")
        output.write(f"resolution={serializable['resolution']}\n")
    print(json.dumps({key: value for key, value in serializable.items() if key != "path"}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
