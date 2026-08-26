"""One-command governed Redfin monthly candidate producer (Phase 3A).

The routine path clones accepted local state and never promotes either the
local state or ``accepted.source.redfin``.  Remote boundaries are injectable so
the complete path can be exercised offline without weakening production mode.
"""
from __future__ import annotations

import argparse
import base64
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable

import duckdb

from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
    GitHubReleaseArtifactPublisher, GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.production import RESULT_VERSION, cycle_id, validate_source_result
from jobs.monthly_refresh.readiness import add_readiness, empty_readiness, make_record
from sources.redfin.governance import GovernanceError, RAW_ROOT
from sources.redfin.inbox import register_incoming
from sources.redfin.state import emit_artifact, reconcile_governed_drop
from sources.redfin.storage import atomic_json, raw_files, read_json
from sources.redfin.validate import validate_drop

SOURCE_ID = "redfin"
REPOSITORY = "chineduezebuiroh/realestate-intel"
BRANCH = "monthly-refresh-orchestration"
CATALOG_PATH = "config/artifact_catalog.json"
READINESS_PATH = "config/monthly_refresh_readiness.json"
POLICY_PATH = Path("config/monthly_refresh_policy.json")
ACCEPTED_STATE = Path("data/redfin/state/canonical_redfin.duckdb")
JULY_ARTIFACT_ID = "src__redfin__2026-07__r1__b10214595868c2ff"
JULY_DATA_SHA256 = "0ed5c374372bdcc8f5969dcbd6cd5015c55f2f84a2506687dea00081ccb49924"
WORKSPACE_ROOT = Path("data/redfin/state/candidates")
EVIDENCE_ROOT = Path("artifacts/audit/redfin_monthly")
LEDGER_PATH = Path("data/redfin/state/monthly_source_ledger.json")
LEDGER_STATES = {"registered", "validated", "candidate_running", "candidate_ready",
                 "failed_retryable", "failed_terminal"}


def drop_content_hash(metadata: dict[str, Any]) -> str:
    return sha256_json({"drop_id": metadata["drop_id"], "files": metadata["files"]})


def accepted_record(catalog: dict[str, Any]) -> dict[str, Any]:
    artifact_id = catalog.get("accepted", {}).get("source", {}).get(SOURCE_ID)
    matches = [r for r in catalog.get("immutable_records", [])
               if r.get("object_type") == "source" and r.get("object_id") == artifact_id
               and r.get("metadata", {}).get("source_id") == SOURCE_ID]
    if artifact_id is None:
        raise GovernanceError("accepted.source.redfin is absent; run the separate governed Redfin July bootstrap before a monthly run")
    if len(matches) != 1:
        raise GovernanceError("accepted.source.redfin does not identify exactly one immutable record")
    return matches[0]


def _ledger(path: Path) -> dict[str, Any]:
    return read_json(path) if path.exists() else {"schema_version": "redfin_monthly_cycle_ledger_v1", "cycles": {}}


def _save_ledger(path: Path, payload: dict[str, Any]) -> None:
    if any(item.get("state") not in LEDGER_STATES for item in payload["cycles"].values()):
        raise GovernanceError("invalid Redfin monthly ledger state")
    atomic_json(path, payload)


def _update(path: Path, cycle: str, **values: Any) -> dict[str, Any]:
    ledger = _ledger(path); item = ledger["cycles"].setdefault(cycle, {})
    pinned = item.get("candidate_artifact_id")
    if pinned and values.get("candidate_artifact_id") not in (None, pinned):
        raise GovernanceError("candidate identity drift for existing Redfin cycle")
    item.update(values); _save_ledger(path, ledger); return item


def _candidate_rows(db: Path) -> int:
    con = duckdb.connect(str(db), read_only=True)
    try: return con.execute("select count(*) from canonical_redfin").fetchone()[0]
    finally: con.close()


def _publication_metadata(manifest: dict[str, Any], package: dict[str, Any], git_sha: str) -> dict[str, Any]:
    return {"logical_artifact_uri": manifest["artifact_uri"], "object_id": manifest["artifact_id"],
        "object_type": "source", "object_metadata": {"source_id": SOURCE_ID},
        "artifact_content_hash": manifest["artifact_content_hash"],
        "member_hashes": {m["path"]: m["sha256"] for m in package["members"]},
        "publisher_git_sha": git_sha,
        "contract_versions": [manifest["artifact_contract_version"], package["package_contract_version"]]}


def _catalog_record(manifest: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    return {"object_type":"source", "object_id":manifest["artifact_id"],
        "logical_artifact_uri":manifest["artifact_uri"], "remote_repository":receipt["remote_repository"],
        "release_tag":receipt["release_tag"], "release_id":receipt["release_id"],
        "asset_id":receipt["asset_id"], "asset_filename":receipt["asset_filename"],
        "package_sha256":receipt["package_sha256"], "artifact_content_hash":manifest["artifact_content_hash"],
        "publication_receipt_id":receipt["receipt_id"], "publication_state":receipt["publication_state"],
        "metadata":{"source_id":SOURCE_ID, "data_sha256":manifest["data_sha256"],
                    "provider_release_id":manifest["provider_release_id"], "observation_max":manifest["observation_max"]}}


def publish_candidate(artifact: Path, workspace: Path, api: GitHubAPI,
                      cas: GitHubCatalogCAS, git_sha: str) -> dict[str, Any]:
    validation = validate_artifact(artifact, expected_source_id=SOURCE_ID); manifest = validation["manifest"]
    package_path = workspace / f"{manifest['artifact_id']}.tar"
    package = build_publication_package(artifact, package_path)
    publisher = GitHubReleaseArtifactPublisher(api); uri = manifest["artifact_uri"]
    publisher.prepare(uri, package_path.read_bytes(), _publication_metadata(manifest, package, git_sha))
    publisher.upload(uri); publisher.verify(uri); receipt = publisher.finalize(uri)
    catalog, changed = cas.add(_catalog_record(manifest, receipt), receipt)
    resolved = GitHubReleaseArtifactResolver(catalog, api, workspace / "remote-proof").resolve(uri)
    remote = validate_artifact(resolved, expected_source_id=SOURCE_ID)["manifest"]
    if remote["artifact_content_hash"] != manifest["artifact_content_hash"]:
        raise GovernanceError("remote Redfin candidate identity mismatch")
    return {"receipt": receipt, "catalog_changed": changed, "package_sha256": sha256_file(package_path),
            "publication_state": "published_verified", "accepted_pointer_changed": False}


def bootstrap_accepted(*, artifact: Path, accepted_state: Path, workspace: Path,
                       api: GitHubAPI, cas: GitHubCatalogCAS, git_sha: str) -> dict[str, Any]:
    """Explicit one-time July registry migration; never called by :func:`run`."""
    manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
    if (manifest["artifact_id"] != JULY_ARTIFACT_ID or manifest["data_sha256"] != JULY_DATA_SHA256
            or manifest["target_month"] != "2026-07"):
        raise GovernanceError("bootstrap input is not the exact proven July Redfin artifact")
    if not accepted_state.is_file(): raise GovernanceError("accepted July Redfin state is absent")
    con = duckdb.connect(str(accepted_state), read_only=True)
    try:
        mismatch = con.execute("""SELECT count(*) FROM (
          (SELECT geo_id,metric_id,date,property_type_id,value,property_type FROM canonical_redfin
           EXCEPT ALL SELECT geo_id,metric_id,date,property_type_id,value,property_type FROM read_parquet(?))
          UNION ALL
          (SELECT geo_id,metric_id,date,property_type_id,value,property_type FROM read_parquet(?)
           EXCEPT ALL SELECT geo_id,metric_id,date,property_type_id,value,property_type FROM canonical_redfin))""",
          [str(artifact / "data.parquet")] * 2).fetchone()[0]
    finally: con.close()
    if mismatch: raise GovernanceError("accepted local state does not reproduce the proven July artifact")
    publication = publish_candidate(artifact, workspace, api, cas, git_sha)
    _, changed = cas.activate_source(SOURCE_ID, JULY_ARTIFACT_ID)
    return {**publication, "accepted_pointer_changed": changed, "artifact_id": JULY_ARTIFACT_ID,
            "bootstrap_operation": True}


def run(*, accepted_state: Path = ACCEPTED_STATE, raw_root: Path = RAW_ROOT,
        workspace_root: Path = WORKSPACE_ROOT, ledger_path: Path = LEDGER_PATH,
        evidence_root: Path = EVIDENCE_ROOT, policy_path: Path = POLICY_PATH,
        catalog: dict[str, Any], publisher: Callable[[Path, Path], dict[str, Any]],
        readiness_writer: Callable[[dict[str, Any]], bool] | None = None,
        repository_root: Path = Path("."), git_sha: str = "unknown") -> dict[str, Any]:
    """Execute or resume one exact governed drop. ``publisher`` must publish and catalog."""
    prior = accepted_record(catalog)
    incoming = raw_root / "incoming"
    if raw_files(incoming):
        registration = register_incoming(raw_root, clear_incoming=True)
        drop_id = registration["drop_id"]
    else:
        ready = [(key, value) for key, value in _ledger(ledger_path)["cycles"].items()
                 if value.get("state") in {"registered", "validated", "candidate_running", "candidate_ready",
                                           "failed_retryable", "failed_terminal"}]
        if not ready:
            return {"schema_version":"redfin_monthly_not_ready_v1", "source_id":SOURCE_ID,
                    "status":"not_ready", "reason":"managed inbox contains no governed drop"}
        _, last = sorted(ready)[-1]; drop_id = last["drop_id"]
        registration = {"status":"already_registered", "drop_id":drop_id,
                        "path":str(raw_root / "drops" / drop_id)}
    metadata = read_json(raw_root / "drops" / drop_id / "metadata.json")
    content_hash = drop_content_hash(metadata); policy_hash = sha256_file(policy_path)
    cycle = cycle_id(redfin_drop_id=drop_id, redfin_drop_hash=content_hash,
                     target_month=drop_id, policy_sha256=policy_hash)
    evidence = evidence_root / cycle; evidence.mkdir(parents=True, exist_ok=True)
    _update(ledger_path, cycle, state="registered", drop_id=drop_id,
            drop_content_hash=content_hash, target_month=drop_id, policy_sha256=policy_hash,
            prior_artifact_id=prior["object_id"])
    write_canonical_json(evidence / "registration.json", {**registration, "drop_content_hash":content_hash})
    try:
        validated = validate_drop(drop_id, raw_root)
        _update(ledger_path, cycle, state="validated")
        write_canonical_json(evidence / "drop_validation.json", validated)
        cycle_workspace = workspace_root / cycle; state = cycle_workspace / "candidate_redfin.duckdb"
        artifact = cycle_workspace / "artifact"
        item = _ledger(ledger_path)["cycles"][cycle]
        # Publication/catalog retries intentionally leave the state at a
        # transitional or retry marker.  A pinned, still-valid artifact is the
        # durable resume point; rebuilding it would risk needless drift.
        reusable = bool(item.get("candidate_artifact_id") and artifact.is_dir() and state.is_file())
        if not reusable:
            _update(ledger_path, cycle, state="candidate_running")
            cycle_workspace.mkdir(parents=True, exist_ok=True); temporary = state.with_suffix(".building.duckdb")
            temporary.unlink(missing_ok=True)
            if not accepted_state.is_file():
                raise GovernanceError(f"accepted Redfin state absent: {accepted_state}; routine runs never bootstrap it")
            accepted_before = sha256_file(accepted_state); shutil.copy2(accepted_state, temporary)
            reconcile_governed_drop(temporary, drop_id, root=raw_root,
                                    geo_manifest=repository_root / "config/geo_manifest.generated.csv")
            temporary.replace(state)
            manifest = emit_artifact(state, artifact, target_month=drop_id,
                registered_at=metadata.get("registered_at"), raw_root=raw_root,
                repository_root=repository_root, artifact_created_at=metadata.get("registered_at"),
                # The executing checkout is publication-attempt evidence, not
                # a governed Redfin build input.  The actual SHA remains on the
                # publication receipt and must not perturb package bytes.
                git_sha="operational-evidence-excluded")
            validation = validate_artifact(artifact, expected_source_id=SOURCE_ID)
            if validation["rows"] != _candidate_rows(state): raise GovernanceError("candidate state/artifact row parity mismatch")
            if sha256_file(accepted_state) != accepted_before: raise GovernanceError("accepted Redfin state changed during candidate construction")
            _update(ledger_path, cycle, state="candidate_ready", candidate_artifact_id=manifest["artifact_id"],
                    artifact_content_hash=manifest["artifact_content_hash"], candidate_state_sha256=sha256_file(state))
            write_canonical_json(evidence / "candidate_state.json", {"accepted_state":str(accepted_state),
                "accepted_state_sha256":accepted_before, "candidate_state":str(state),
                "candidate_state_sha256":sha256_file(state), "rows":validation["rows"]})
            write_canonical_json(evidence / "artifact_validation.json", validation)
        manifest = validate_artifact(artifact, expected_source_id=SOURCE_ID)["manifest"]
        pinned = _ledger(ledger_path)["cycles"][cycle].get("candidate_artifact_id")
        if pinned != manifest["artifact_id"]: raise GovernanceError("candidate artifact drift from ledger pin")
        publication = publisher(artifact, cycle_workspace / "publication")
        write_canonical_json(evidence / "publication_receipt.json", publication)
        if readiness_writer is not None:
            readiness_writer({"drop_id":drop_id, "drop_content_hash":content_hash,
                "target_month":drop_id, "cycle_id":cycle, "candidate_artifact_id":manifest["artifact_id"]})
        source_changed = manifest["data_sha256"] != prior["metadata"]["data_sha256"]
        result = {"schema_version":RESULT_VERSION, "source_id":SOURCE_ID, "cycle_id":cycle,
            "status":"succeeded", "candidate_artifact_id":manifest["artifact_id"],
            "artifact_content_hash":manifest["artifact_content_hash"], "package_sha256":publication["package_sha256"],
            "publication_state":"published_verified", "validation_status":"passed",
            "provider_release_id":manifest["provider_release_id"], "observation_max":manifest["observation_max"],
            "prior_artifact_id":prior["object_id"], "source_change_detected":source_changed,
            "retryability":"not_applicable", "evidence_uri":str(evidence / "source_execution_result.json")}
        validate_source_result(result, expected_cycle_id=cycle)
        write_canonical_json(evidence / "source_execution_result.json", result)
        _update(ledger_path, cycle, state="candidate_ready", package_sha256=publication["package_sha256"],
                publication_state="published_verified", evidence_uri=result["evidence_uri"])
        return result
    except Exception as exc:
        terminal = isinstance(exc, (GovernanceError, ValueError))
        _update(ledger_path, cycle, state="failed_terminal" if terminal else "failed_retryable",
                failure_type=type(exc).__name__, failure_message=str(exc))
        write_canonical_json(evidence / "failure.json", {"retryability":"terminal" if terminal else "retryable",
                                                          "error_type":type(exc).__name__, "message":str(exc)})
        raise


def _gh_token() -> str:
    try:
        process = subprocess.run(["gh", "auth", "token"], check=True, capture_output=True, text=True)
    except (FileNotFoundError, subprocess.CalledProcessError) as exc:
        raise RuntimeError("GitHub CLI authentication unavailable; install gh and run `gh auth login`") from exc
    token = process.stdout.strip()
    if not token: raise RuntimeError("GitHub CLI returned no token; run `gh auth login`")
    return token


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", default=REPOSITORY); parser.add_argument("--branch", default=BRANCH)
    parser.add_argument("--accepted-state", type=Path, default=ACCEPTED_STATE)
    parser.add_argument("--raw-root", type=Path, default=RAW_ROOT); parser.add_argument("--workspace", type=Path, default=WORKSPACE_ROOT)
    parser.add_argument("--ledger", type=Path, default=LEDGER_PATH); parser.add_argument("--evidence-root", type=Path, default=EVIDENCE_ROOT)
    parser.add_argument("--bootstrap-accepted-artifact", type=Path,
        help="explicit one-time migration of the exact proven July artifact; does not start a monthly cycle")
    args = parser.parse_args()
    if args.repository != REPOSITORY or args.branch != BRANCH:
        parser.error(f"production runner is governed for {REPOSITORY}@{BRANCH}")
    token = _gh_token(); api = GitHubAPI(args.repository, token)
    cas = GitHubCatalogCAS(api, CATALOG_PATH, args.branch, fixture=False); catalog, _ = cas.read()
    git_sha = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True, text=True).stdout.strip()
    def publish(artifact: Path, workspace: Path) -> dict[str, Any]:
        workspace.mkdir(parents=True, exist_ok=True)
        return publish_candidate(artifact, workspace, api, cas, git_sha)
    def write_readiness(identity: dict[str, Any]) -> bool:
        # Re-read the catalog after its CAS commit; it is authoritative for all
        # publication fields duplicated in the tiny catalyst record.
        current_catalog, _ = cas.read()
        artifact = next((r for r in current_catalog["immutable_records"]
            if r["object_id"] == identity["candidate_artifact_id"]), None)
        if artifact is None: raise GovernanceError("catalog commit not visible; readiness cannot be advertised")
        record = make_record(drop_id=identity["drop_id"], drop_content_hash=identity["drop_content_hash"],
            target_month=identity["target_month"], cycle=identity["cycle_id"], artifact=artifact)
        encoded = READINESS_PATH.replace("/", "%2F")
        item, _ = api.request("GET", f"/contents/{encoded}?ref={args.branch}", expected=(200,404))
        state = json.loads(base64.b64decode(item["content"])) if item else empty_readiness()
        updated, changed = add_readiness(state, record, catalog=current_catalog, policy_path=POLICY_PATH)
        if not changed: return False
        payload = {"message":f"Record Redfin readiness {record['readiness_id']}",
            "content":base64.b64encode((json.dumps(updated,sort_keys=True,separators=(",",":"))+"\n").encode()).decode(),
            "branch":args.branch}
        if item: payload["sha"] = item["sha"]
        api.request("PUT", f"/contents/{encoded}", payload=payload, expected=(200,201))
        return True
    if args.bootstrap_accepted_artifact:
        result = bootstrap_accepted(artifact=args.bootstrap_accepted_artifact,
            accepted_state=args.accepted_state, workspace=args.workspace / "bootstrap",
            api=api, cas=cas, git_sha=git_sha)
        print(json.dumps(result, sort_keys=True)); return 0
    result = run(accepted_state=args.accepted_state, raw_root=args.raw_root, workspace_root=args.workspace,
        ledger_path=args.ledger, evidence_root=args.evidence_root, catalog=catalog, publisher=publish,
        readiness_writer=write_readiness, git_sha=git_sha)
    print(json.dumps(result, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
