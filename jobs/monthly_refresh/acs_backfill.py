"""Bounded physical ACS backfill planning and plan-authorized execution.

No family resolution, acceptance, promotion, or bootstrap operations live here.
The CLI defaults to offline provider evidence plus read-only main inventory.
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Callable, Mapping

from core.source_artifacts.github_release import (GitHubAPI, GitHubCatalogCAS,
                                                 GitHubReleaseArtifactResolver)
from core.source_artifacts.hashing import sha256_file, sha256_json, write_canonical_json
from core.source_artifacts.package import build_publication_package
from core.source_artifacts.publication import IdentityCollisionError
from core.source_artifacts.validation import validate_artifact
from jobs.monthly_refresh.acs_hosted import execute_product, publication_result
from jobs.monthly_refresh.acs_monthly import candidate, snapshot_to_pin
from jobs.monthly_refresh.bps_hosted import CATALOG_PATH, publish_candidate
from jobs.monthly_refresh.cycle_results import GitHubCycleResultStore, governed_record
from jobs.monthly_refresh.source_inputs import GitHubPinStore, validate_pin, verify_member_bytes
from sources.census_acs.artifact import PRODUCTS, VARIABLES, geography_plan, governed_config_hashes
from sources.census_acs.historical import (HistoricalAcquisitionFailure, acquire_historical_snapshot,
                                           validate_evidence, validate_snapshot)

REPOSITORY = "chineduezebuiroh/realestate-intel"
BASELINE = "282ba2d4dcb0fa18d1e945fc5f3cb16957d9da32"
PLAN_VERSION = "acs_historical_execution_plan_v1"
AUTH_VERSION = "acs_historical_execution_authorization_v1"
MAX_BATCH = 5


def read_json(path: Path) -> dict:
    def unique(pairs):
        out = {}
        for key, value in pairs:
            if key in out:
                raise ValueError("duplicate JSON key")
            out[key] = value
        return out
    return json.loads(path.read_text(), object_pairs_hook=unique)


def scope(source_id: str, vintages: list[int], batch_size: int) -> list[int]:
    if (type(source_id) is not str or source_id not in PRODUCTS or type(vintages) is not list
            or type(batch_size) is not int or not 1 <= batch_size <= MAX_BATCH
            or not 1 <= len(vintages) <= batch_size or len(set(vintages)) != len(vintages)
            or any(type(y) is not int or not 2005 <= y <= 2100 for y in vintages)):
        raise ValueError("scope must be one exact physical product and 1..5 unique bounded vintages")
    return sorted(vintages)


def runtime_identity() -> dict:
    """Bind serialization implementations, including wheel member checksums."""
    import importlib.metadata
    import sys
    implementations = {}
    for name in ("pandas", "numpy", "pyarrow"):
        distribution = importlib.metadata.distribution(name)
        record = distribution.read_text("RECORD")
        if record is None:
            raise ValueError(f"serialization implementation evidence unavailable: {name}")
        import hashlib
        if distribution.files is None:
            raise ValueError("installed serialization file inventory unavailable")
        for member in distribution.files:
            if member.hash is None:
                continue
            if member.hash.mode != "sha256":
                raise ValueError("unsupported installed serialization hash algorithm")
            path = Path(distribution.locate_file(member))
            actual = base64.urlsafe_b64encode(bytes.fromhex(sha256_file(path))).decode().rstrip("=")
            if actual != member.hash.value:
                raise ValueError(f"installed serialization implementation bytes drifted: {name}")
        implementations[name] = {"version": distribution.version,
                                  "installed_record_sha256": hashlib.sha256(record.encode()).hexdigest()}
    return {"python_version": sys.version, "python_executable_sha256": sha256_file(Path(sys.executable)),
            "implementations": implementations}


def repository_identity(root: Path) -> dict:
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()
    remote = git("remote", "get-url", "origin")
    if remote not in {f"https://github.com/{REPOSITORY}.git", f"https://github.com/{REPOSITORY}",
                      f"git@github.com:{REPOSITORY}.git", f"git@github.com:{REPOSITORY}"}:
        raise ValueError("backfill repository identity mismatch")
    subprocess.run(["git", "-C", str(root), "merge-base", "--is-ancestor", BASELINE, "HEAD"], check=True)
    paths = git("ls-files", "core", "jobs", "sources", "requirements.txt",
                ".github/workflows/acs-historical-backfill.yml").splitlines()
    # Include new implementation files during local review; production checkouts
    # naturally include them in ls-files after review and merge.
    paths += [str(p.relative_to(root)) for folder in ("core", "jobs", "sources")
              for p in (root / folder).rglob("*.py") if str(p.relative_to(root)) not in paths]
    workflow = ".github/workflows/acs-historical-backfill.yml"
    if (root / workflow).exists() and workflow not in paths:
        paths.append(workflow)
    files = {p: sha256_file(root / p) for p in sorted(set(paths)) if (root / p).is_file()}
    configs = governed_config_hashes(root)
    governance = root / "config/acs_historical_governance.json"
    configs["config/acs_historical_governance.json"] = sha256_file(governance) if governance.exists() else "absent"
    return {"repository": REPOSITORY, "baseline": BASELINE, "code_sha256": sha256_json(files),
            "config_hashes": configs, "serialization_runtime": runtime_identity()}


def _identified(kind: str, body: dict, field: str) -> dict:
    return {**body, field: kind + "__" + sha256_json(body)}


def verify_identity(value: dict, kind: str, field: str) -> None:
    if not isinstance(value, dict):
        raise ValueError(f"{field} must be a complete identity object")
    body = {k: v for k, v in value.items() if k != field}
    if value.get(field) != kind + "__" + sha256_json(body):
        raise ValueError(f"{field} content identity mismatch")


def cycle_identity(plan_id: str, source_id: str, vintage: int) -> str:
    return f"acs_backfill__{source_id}__{vintage}__{sha256_json(plan_id)[:24]}"


def reconcile(*, catalog: Mapping[str, Any], source_id: str, vintages: list[int],
              resolve: Callable[[str], Path], approved_gaps: Mapping[int, dict] | None = None) -> list[dict]:
    """Inventory is authoritative only when supplied by the main control plane.

    Every advertised parent must resolve and verify. Unreachable packages remain
    unknown; multiple acquisition identities remain conflicts even if equivalent.
    Gap authority is an injected *already verified* governance-reader result.
    """
    result = []
    for year in vintages:
        records = [r for r in catalog.get("immutable_records", []) if r.get("object_type") == "source"
                   and r.get("metadata", {}).get("source_id") == source_id
                   and r.get("metadata", {}).get("provider_release_id") == f"{PRODUCTS[source_id]}:{year}"]
        parents, failures = [], []
        for record in records:
            try:
                path = resolve(record["logical_artifact_uri"])
                from jobs.monthly_refresh.acs_family_resolution import _parent
                frame, manifest, _ = _parent(record, path, source_id)
                import pandas as pd
                if (not pd.to_datetime(frame.date).eq(pd.Timestamp(year, 12, 31)).all()
                        or not frame.property_type_id.eq("all").all() or not frame.property_type.eq("all").all()
                        or set(frame.metric_id) - {f"{source_id}_{suffix}" for suffix in VARIABLES.values()}
                        or manifest.get("source_contract_version") is not None):
                    raise ValueError("not an ordinary single-vintage acquisition parent")
                with tempfile.TemporaryDirectory() as td:
                    package = build_publication_package(path, Path(td) / "parent.tar")
                if (record.get("publication_state") != "published_immutable_verified"
                        or manifest["artifact_id"] != record["object_id"]
                        or manifest["artifact_uri"] != record["logical_artifact_uri"]
                        or manifest["artifact_content_hash"] != record["artifact_content_hash"]
                        or manifest["provider_release_id"] != f"{PRODUCTS[source_id]}:{year}"
                        or manifest["target_month"] != f"{year}-12"
                        or package["package_sha256"] != record["package_sha256"]):
                    raise ValueError("parent immutable identity mismatch")
                parents.append({"artifact_id": record["object_id"],
                    "artifact_content_hash": record["artifact_content_hash"],
                    "package_sha256": record["package_sha256"], "logical_artifact_uri": record["logical_artifact_uri"],
                    "cycle_id": manifest.get("raw_source_lineage", {}).get("cycle_id")})
            except Exception as exc:
                failures.append({"artifact_id": record.get("object_id"), "requirement": type(exc).__name__})
        gap = (approved_gaps or {}).get(year)
        status = ("conflict" if len(records) > 1 or (records and gap) else
                  "unknown_unverified" if failures else "verified_existing_parent" if parents else
                  "approved_gap" if gap else "missing_acquisition")
        result.append({"vintage": year, "classification": status,
                       "parents": sorted(parents, key=lambda p: p["artifact_id"]),
                       "verification_requirements": failures, "approved_gap": gap})
    return result


def make_plan(*, source_id: str, vintages: list[int], batch_size: int, identity: dict,
              code_commit: str, inventory: list[dict], evidence: Mapping[str, dict],
              geography: list[dict], excluded: list[dict], authority: dict) -> dict:
    years = scope(source_id, vintages, batch_size)
    if [i["vintage"] for i in inventory] != years:
        raise ValueError("inventory scope mismatch")
    entries = []
    for item in inventory:
        provider = evidence.get(str(item["vintage"]))
        requirement = None
        if item["classification"] == "missing_acquisition":
            try:
                if provider is None:
                    raise ValueError("official historical provider preflight required")
                validate_evidence(provider, source_id=source_id, vintage=item["vintage"],
                                  plan=geography, excluded=excluded)
            except Exception as exc:
                requirement = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        entries.append({**item, "provider_evidence": provider, "verification_requirement": requirement})
    return _identified(PLAN_VERSION, {"schema_version": PLAN_VERSION, "source_id": source_id,
        "vintages": years, "batch_size": batch_size, "repository_identity": identity,
        "code_commit": code_commit, "authority": authority, "geography": geography, "excluded": excluded,
        "entries": entries}, "plan_id")


def authorize(plan: dict, *, reviewed_plan_id: str, identity: dict, actor: str) -> dict:
    verify_plan(plan, identity)
    if plan["plan_id"] != reviewed_plan_id or not actor or len(actor) > 100:
        raise ValueError("exact reviewed plan and authorizing actor required")
    for entry in plan["entries"]:
        if entry["classification"] in {"conflict", "unknown_unverified"} or entry["verification_requirement"]:
            raise ValueError("plan has unresolved verification requirements")
    return _identified(AUTH_VERSION, {"schema_version": AUTH_VERSION, "plan_id": reviewed_plan_id,
        "repository_identity": identity, "actor": actor, "action": "acquire_physical_only"}, "authorization_id")


def verify_plan(plan: dict, identity: dict) -> None:
    verify_identity(plan, PLAN_VERSION, "plan_id")
    if set(plan) != {"schema_version", "source_id", "vintages", "batch_size", "repository_identity",
                     "code_commit", "authority", "geography", "excluded", "entries", "plan_id"}:
        raise ValueError("execution plan schema mismatch")
    if plan.get("schema_version") != PLAN_VERSION or plan.get("repository_identity") != identity:
        raise ValueError("repository/code/configuration/governance/serialization-runtime drift; restore reviewed identity or review a new plan")
    years = scope(plan["source_id"], plan["vintages"], plan["batch_size"])
    if plan["vintages"] != years or [e["vintage"] for e in plan["entries"]] != years:
        raise ValueError("plan entries do not exactly match vintage scope")
    if not re.fullmatch(r"[0-9a-f]{40}", plan["code_commit"]):
        raise ValueError("plan requires exact code commit")
    if (plan.get("authority", {}).get("repository") != REPOSITORY
            or plan["authority"].get("branch") != "main"
            or not re.fullmatch(r"[0-9a-f]{40}", plan["authority"].get("commit", ""))):
        raise ValueError("plan lacks exact main preflight authority")
    for entry in plan["entries"]:
        if set(entry) != {"vintage", "classification", "parents", "verification_requirements",
                          "approved_gap", "provider_evidence", "verification_requirement"}:
            raise ValueError("execution plan entry schema mismatch")
        classification = entry.get("classification")
        if classification not in {"missing_acquisition", "verified_existing_parent", "approved_gap",
                                   "conflict", "unknown_unverified"}:
            raise ValueError("unknown plan inventory classification")
        if classification == "verified_existing_parent" and len(entry.get("parents", [])) != 1:
            raise ValueError("verified parent must resolve exactly once")
        if classification == "missing_acquisition" and (entry.get("parents") or entry.get("approved_gap")
                                                        or entry.get("verification_requirements")):
            raise ValueError("missing acquisition has contradictory reconciliation evidence")
        if classification == "approved_gap" and (entry.get("parents") or not entry.get("approved_gap")):
            raise ValueError("approved gap lacks exclusive evidence")
        if classification == "missing_acquisition" and not entry["verification_requirement"]:
            validate_evidence(entry["provider_evidence"], source_id=plan["source_id"], vintage=entry["vintage"],
                              plan=plan["geography"], excluded=plan["excluded"])


def acquire_with_diagnostics(**kwargs):
    """Preserve only the historical transport's explicitly sanitized failure."""
    try:
        return acquire_historical_snapshot(**kwargs)
    except HistoricalAcquisitionFailure:
        raise
    except Exception:
        raise ValueError("historical acquisition failed; provider details redacted") from None


def exact_pin(*, cycle_id: str, source_id: str, vintage: int, workspace: Path,
              evidence: dict, geography: list[dict], excluded: list[dict],
              acquire: Callable[..., dict] = acquire_historical_snapshot,
              retrieved_at: str | None = None) -> tuple[dict, dict[str, Path]]:
    path = workspace / "snapshot.json"
    snapshot = acquire(source_id=source_id, vintage=vintage, output=path)
    checked = validate_snapshot(source_id=source_id, vintage=vintage, snapshot=snapshot,
                                plan=geography, excluded=excluded, omissions=evidence.get("omission_evidence", []))
    if checked["snapshot_sha256"] != evidence["snapshot_sha256"]:
        raise ValueError("provider response drift from reviewed preflight; new review required")
    return snapshot_to_pin(cycle_id=cycle_id, source_id=source_id, vintage=vintage,
                           snapshot=snapshot, path=path, retrieved_at=retrieved_at, historical_evidence=evidence)


def verify_historical_pin(pin: Mapping, *, evidence: dict, source_id: str,
                          vintage: int, cycle_id: str) -> None:
    """Bind a durable pin to reviewed snapshot bytes using the common pin builder."""
    with tempfile.TemporaryDirectory() as td:
        path = Path(td) / "snapshot"
        path.write_text(json.dumps(evidence["snapshot"], sort_keys=True, separators=(",", ":")) + "\n")
        expected, _ = snapshot_to_pin(cycle_id=cycle_id, source_id=source_id, vintage=vintage,
            snapshot=evidence["snapshot"], path=path,
            retrieved_at=pin["members"]["snapshot"]["retrieved_at"], historical_evidence=evidence)
    if dict(pin) != expected:
        raise ValueError("historical durable pin differs from exact reviewed snapshot evidence")


def exact_publish(*, artifact: Path, source_id: str, api: Any, cas: Any,
                  workspace: Path, git_sha: str) -> dict:
    """Strengthen only historical reuse: never accept different package bytes."""
    manifest = validate_artifact(artifact, expected_source_id=source_id)["manifest"]
    package = build_publication_package(artifact, workspace / "exact.tar")
    catalog, _ = cas.read()
    matches = [r for r in catalog["immutable_records"] if r.get("object_type") == "source"
               and r.get("object_id") == manifest["artifact_id"]]
    if matches and (len(matches) != 1 or matches[0]["package_sha256"] != package["package_sha256"]):
        raise IdentityCollisionError(
            f"historical exact-byte recovery blocked: artifact={manifest['artifact_id']} "
            f"local_package_sha256={package['package_sha256']}; preserve remote release, inspect original package")
    try:
        return publish_candidate(artifact=artifact, source_id=source_id, api=api, cas=cas,
                                 workspace=workspace, git_sha=git_sha, logical_source_id=source_id)
    except IdentityCollisionError:
        raise IdentityCollisionError(
            f"historical immutable release collision: artifact={manifest['artifact_id']} "
            f"local_package_sha256={package['package_sha256']}; preserve remote release, "
            "retrieve original package and compare member hashes before recovery") from None


def check_reconciliation(plan: dict, observed: list[dict]) -> None:
    if [i["vintage"] for i in observed] != plan["vintages"]:
        raise ValueError("current reconciliation differs from exact plan scope")
    for entry, now in zip(plan["entries"], observed):
        if entry["classification"] == "missing_acquisition":
            cycle = cycle_identity(plan["plan_id"], plan["source_id"], entry["vintage"])
            if now["classification"] != "missing_acquisition" and not (
                    now["classification"] == "verified_existing_parent" and len(now["parents"]) == 1
                    and now["parents"][0]["cycle_id"] == cycle):
                raise ValueError("acquisition inventory changed or became unverified; review required")
        elif any(now.get(k) != entry.get(k) for k in ("classification", "parents", "approved_gap")):
            raise ValueError("selected parent/gap inventory drift")


def execute_batch(*, plan: dict, authorization: dict, reviewed_plan_id: str, identity: dict,
                  workspace: Path, pin_store: Any, current_inventory: Callable[[], list[dict]],
                  acquire: Callable[..., dict], retrieve: Callable[[Mapping, Path], Any],
                  publish: Callable[[Path, str], Mapping], record: Callable[[Mapping, Mapping], Any],
                  repository_root: Path = Path("."),
                  existing_publication: Callable[[str], tuple[Path, Mapping]] | None = None,
                  progress: dict | None = None) -> list[dict]:
    """Sequential, stop on first failure; retries retain the same cycle and pin.

    Authorization is separate from the plan and binds the exact reviewed identity.
    Hosted caller rechecks main code/config authority before every mutation path.
    """
    progress = progress if progress is not None else {}
    progress.update(boundary="authorization", completed_vintages=[])
    verify_plan(plan, identity)
    verify_identity(authorization, AUTH_VERSION, "authorization_id")
    if (authorization.get("schema_version") != AUTH_VERSION or authorization.get("action") != "acquire_physical_only"
            or authorization.get("plan_id") != plan["plan_id"] or reviewed_plan_id != plan["plan_id"]
            or authorization.get("repository_identity") != identity or not authorization.get("actor")):
        raise ValueError("execution requires separate exact plan-bound authorization")
    if any(e["verification_requirement"] or e["classification"] in {"conflict", "unknown_unverified"}
           for e in plan["entries"]):
        raise ValueError("unresolved plan cannot execute")
    source = plan["source_id"]
    outputs = []
    for entry in plan["entries"]:
        progress.update(vintage=entry["vintage"], boundary="inventory")
        observed = current_inventory()
        check_reconciliation(plan, observed)
        now = next(i for i in observed if i["vintage"] == entry["vintage"])
        cycle = cycle_identity(plan["plan_id"], source, entry["vintage"])
        if entry["classification"] != "missing_acquisition":
            if any(now.get(k) != entry.get(k) for k in ("classification", "parents", "approved_gap")):
                raise ValueError("selected parent/gap inventory drift")
            outputs.append({"vintage": entry["vintage"], "status": "verified_skip"})
            continue
        if now["classification"] != "missing_acquisition":
            if not (now["classification"] == "verified_existing_parent"
                    and len(now["parents"]) == 1 and now["parents"][0]["cycle_id"] == cycle):
                raise ValueError("acquisition inventory changed or became unverified; review required")
        year = entry["vintage"]
        stage = workspace / cycle
        progress["boundary"] = "pin_read"
        existing = pin_store.get(cycle, source)
        if existing is not None:
            validate_pin(existing, cycle_id=cycle, source_id=source, required_members={"snapshot"})
            if existing["provider_release_id"] != f"{PRODUCTS[source]}:{year}":
                raise ValueError("historical persisted vintage mismatch")
            verify_historical_pin(existing, evidence=entry["provider_evidence"], source_id=source,
                                  vintage=year, cycle_id=cycle)
        if now["classification"] == "verified_existing_parent":
            if existing is None:
                raise ValueError("published historical cycle has no durable pin; investigate authority")
            if existing_publication is not None:
                parent = now["parents"][0]
                path, catalog = existing_publication(parent["logical_artifact_uri"])
                manifest = validate_artifact(path, expected_source_id=source)["manifest"]
                package = build_publication_package(path, stage / "verified-existing.tar")
                matches = [r for r in catalog["immutable_records"] if r.get("object_type") == "source"
                           and r.get("object_id") == parent["artifact_id"]]
                if (len(matches) != 1 or package["package_sha256"] != parent["package_sha256"]
                        or matches[0]["package_sha256"] != parent["package_sha256"]
                        or manifest["artifact_id"] != parent["artifact_id"]
                        or manifest["artifact_content_hash"] != parent["artifact_content_hash"]
                        or manifest.get("raw_source_lineage", {}).get("provider_pin") != existing
                        or manifest.get("raw_source_lineage", {}).get("cycle_id") != cycle
                        or manifest.get("git_sha") != plan["code_commit"]
                        or manifest.get("config_hashes") != governed_config_hashes(repository_root)
                        or manifest.get("artifact_created_at") != existing["members"]["snapshot"]["retrieved_at"]):
                    raise ValueError("published historical recovery package contradicts durable plan/pin identity")
                result = publication_result(item=matches[0], source_id=source, cycle_id=cycle)
                progress["boundary"] = "result_write"
                record(result, catalog)
                progress["completed_vintages"].append(year)
                outputs.append({"result": result, "pin": existing, "reused_verified_package": True})
                continue
        def scope_check():
            entries = current_inventory()
            try:
                check_reconciliation(plan, entries)
            except ValueError:
                raise ValueError("acquisition inventory drift before publication; review required") from None
            selected = [i for i in entries if i["vintage"] == year]
            if len(selected) != 1:
                raise ValueError("current acquisition scope does not resolve once")
            item = selected[0]
            if item["classification"] != "missing_acquisition" and not (
                    item["classification"] == "verified_existing_parent" and len(item["parents"]) == 1
                    and item["parents"][0]["cycle_id"] == cycle):
                raise ValueError("acquisition inventory drift before publication; review required")
        def discover():
            progress["boundary"] = "provider_discovery"
            scope_check()
            return exact_pin(cycle_id=cycle, source_id=source, vintage=year, workspace=stage / "discovery",
                evidence=entry["provider_evidence"], geography=plan["geography"], excluded=plan["excluded"], acquire=acquire)
        def checked_retrieve(pin, path):
            progress["boundary"] = "pinned_retrieval"
            try:
                retrieve(pin, path)
            except Exception:
                raise ValueError("pinned historical retrieval inaccessible; restore exact original bytes") from None
            verify_member_bytes(pin, {"snapshot": path})
        def build(**kwargs):
            progress["boundary"] = "candidate_build"
            scope_check()
            pin = kwargs["pin"]
            verify_historical_pin(pin, evidence=entry["provider_evidence"], source_id=source,
                                  vintage=year, cycle_id=cycle)
            snapshot = read_json(kwargs["paths"]["snapshot"])
            checked = validate_snapshot(source_id=source, vintage=year, snapshot=snapshot,
                                        plan=plan["geography"], excluded=plan["excluded"],
                                        omissions=entry["provider_evidence"].get("omission_evidence", []))
            if checked["snapshot_sha256"] != entry["provider_evidence"]["snapshot_sha256"]:
                raise ValueError("pinned historical response differs from reviewed plan")
            # Stable timestamp and code attribution are fixed before first upload.
            return candidate(**kwargs, repository_root=repository_root, git_sha=plan["code_commit"],
                             artifact_created_at=pin["members"]["snapshot"]["retrieved_at"])
        def checked_publish(path, source_id):
            progress["boundary"] = "publication"
            scope_check()
            return publish(path, source_id)
        def checked_record(result, catalog):
            progress["boundary"] = "result_write"
            return record(result, catalog)
        outputs.append(execute_product(source_id=source, mode="resume" if existing else "normal",
            cycle_id=cycle, workspace=stage, pin_store=pin_store, discover=discover,
            retrieve=checked_retrieve, build=build, publish=checked_publish, record=checked_record))
        progress["completed_vintages"].append(year)
    progress["boundary"] = "complete"
    return outputs


def _main_snapshot(api: Any) -> str:
    commit, _ = api.request("GET", "/commits/main")
    sha = commit.get("sha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise ValueError("main authority commit unavailable")
    return sha


def _authority_files(api: Any, root: Path, sha: str) -> None:
    """Detect remote configuration drift even when durable state advances HEAD."""
    from urllib.parse import quote
    configs = repository_identity(root)["config_hashes"]
    for name, digest in configs.items():
        remote, _ = api.request("GET", f"/contents/{quote(name, safe='/')}?ref={sha}", expected=(200, 404))
        if remote is None:
            if digest != "absent":
                raise ValueError("main configuration authority absent")
        else:
            import hashlib
            if digest != hashlib.sha256(base64.b64decode(remote["content"])).hexdigest():
                raise ValueError("main configuration/governance drift")


def _code_authority(api: Any, code_commit: str, sha: str) -> None:
    comparison, _ = api.request("GET", f"/compare/{code_commit}...{sha}")
    if comparison.get("merge_base_commit", {}).get("sha") != code_commit:
        raise ValueError("reviewed code is not main history")
    files = comparison.get("files")
    if not isinstance(files, list) or len(files) >= 300:
        raise ValueError("complete main code comparison unavailable")
    for item in files:
        for path in (item.get("filename", ""), item.get("previous_filename", "")):
            if path.startswith(("core/", "jobs/", "sources/")) or path in {
                    "requirements.txt", ".github/workflows/acs-historical-backfill.yml"}:
                raise ValueError("main execution code drift; review a new plan")


def execution_context() -> str:
    """The supported write CLI runs only in the manual main workflow."""
    if (os.environ.get("GITHUB_ACTIONS") != "true"
            or os.environ.get("GITHUB_REPOSITORY") != REPOSITORY
            or os.environ.get("GITHUB_REF") != "refs/heads/main"
            or os.environ.get("GITHUB_WORKFLOW_REF") !=
                f"{REPOSITORY}/.github/workflows/acs-historical-backfill.yml@refs/heads/main"
            or not re.fullmatch(r"[1-9][0-9]{0,19}", os.environ.get("GITHUB_RUN_ID", ""))):
        raise ValueError("execution CLI requires the dedicated manual main workflow; local authorization cannot bypass preflight")
    return os.environ["GITHUB_RUN_ID"]


def download_review_archive(api: Any, artifact_id: int) -> bytes:
    """Read a signed Actions archive without forwarding API credentials."""
    import urllib.request
    from urllib.parse import urlsplit
    _, headers = api.request("GET", f"/actions/artifacts/{artifact_id}/zip", expected=(302,))
    location = headers.get("Location") or headers.get("location")
    parsed = urlsplit(location or "")
    if (parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password
            or parsed.port not in (None, 443) or parsed.fragment):
        raise ValueError("invalid authorization artifact download location")
    try:
        with urllib.request.urlopen(urllib.request.Request(location), timeout=60) as response:
            data = response.read(16 * 1024 * 1024 + 1)
    except Exception:
        raise ValueError("authorization archive inaccessible; download details redacted") from None
    if len(data) > 16 * 1024 * 1024:
        raise ValueError("authorization archive exceeds bounded size")
    return data


def verify_authorization_run(api: Any, *, run_id: str, plan: dict, authorization: dict,
                             archive_reader: Callable = download_review_archive) -> None:
    """A run ID/actor and self-consistent hashes alone are not authorization."""
    import hashlib
    import io
    import stat
    import zipfile
    if not re.fullmatch(r"[1-9][0-9]{0,19}", run_id):
        raise ValueError("successful separate authorization workflow run required")
    run, _ = api.request("GET", f"/actions/runs/{run_id}")
    if (run.get("path") != ".github/workflows/acs-historical-backfill.yml"
            or run.get("head_branch") != "main" or run.get("event") != "workflow_dispatch"
            or run.get("conclusion") != "success"
            or run.get("repository", {}).get("full_name") != REPOSITORY
            or authorization.get("actor") != run.get("actor", {}).get("login")):
        raise ValueError("authorization artifact run lacks main workflow authority")
    head = run.get("head_sha", "")
    if not re.fullmatch(r"[0-9a-f]{40}", head):
        raise ValueError("authorization code commit unavailable")
    _code_authority(api, plan["code_commit"], head)
    listing, _ = api.request("GET", f"/actions/runs/{run_id}/artifacts?per_page=100")
    artifacts = listing.get("artifacts", [])
    matches = [a for a in artifacts if a.get("name") == "acs-backfill-authorize"]
    if listing.get("total_count") != len(artifacts) or len(matches) != 1:
        raise ValueError("authorization artifact must resolve exactly once with complete evidence")
    artifact = matches[0]
    if (artifact.get("expired") is not False or type(artifact.get("id")) is not int
            or artifact["id"] <= 0 or not re.fullmatch(r"sha256:[0-9a-f]{64}", artifact.get("digest", ""))
            or artifact.get("workflow_run", {}).get("id") != int(run_id)
            or artifact["workflow_run"].get("head_sha") != head):
        raise ValueError("authorization artifact immutable provenance unavailable")
    raw = archive_reader(api, artifact["id"])
    if len(raw) > 16 * 1024 * 1024 or "sha256:" + hashlib.sha256(raw).hexdigest() != artifact["digest"]:
        raise ValueError("authorization archive digest/size mismatch")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) != 2 or {m.filename for m in members} != {"plan.json", "authorization.json"}:
            raise ValueError("authorization archive members duplicate, missing or unexpected")
        for member in members:
            if (member.is_dir() or stat.S_ISLNK(member.external_attr >> 16)
                    or member.file_size > 8 * 1024 * 1024 or member.flag_bits & 1):
                raise ValueError("unsafe authorization archive member")
        with tempfile.TemporaryDirectory() as td:
            archived = {}
            for name in ("plan.json", "authorization.json"):
                path = Path(td) / name; path.write_bytes(archive.read(name))
                archived[name] = read_json(path)
    if archived["plan.json"] != plan or archived["authorization.json"] != authorization:
        raise ValueError("supplied plan/authorization differ from the independently authorized run artifact")


class PlanAuthority:
    """Physical-write fence, derived after run-artifact authorization verification.

    This confers no historical governance approval. It supplies the existing
    ACS single-parent non-fast-forward write primitive with current plan identity.
    """
    def __init__(self, *, api, plan, root, inventory_check=None):
        self.api, self.plan, self.root, self.inventory_check = api, plan, root, inventory_check
        self.write_events = []

    def require_destination(self, repository):
        if repository != REPOSITORY or self.api.repository != REPOSITORY:
            raise ValueError("physical backfill destination repository mismatch")

    def current_commit(self, repository):
        self.require_destination(repository)
        verify_plan(self.plan, repository_identity(self.root))
        tip = _main_snapshot(self.api)
        _authority_files(self.api, self.root, tip)
        _code_authority(self.api, self.plan["code_commit"], tip)
        if self.inventory_check is not None:
            check_reconciliation(self.plan, self.inventory_check(tip))
        return tip


class FencedPhysicalCatalog:
    """Existing immutable catalog registration plus exact physical-scope fencing."""
    def __init__(self, store, authority):
        authority.require_destination(store.api.repository)
        if store.branch != "main":
            raise ValueError("physical historical catalog requires main")
        self.store, self.authority = store, authority

    def read(self):
        return self.store.read()

    def add(self, record, receipt):
        if hasattr(self.authority, "execution_progress"):
            self.authority.execution_progress["boundary"] = "catalog_write"
        from core.source_artifacts.catalog import add_record, validate_catalog_namespace
        from jobs.monthly_refresh.acs_authoritative_write import write_current
        from copy import deepcopy
        meta = record.get("metadata", {})
        plan = self.authority.plan
        allowed = {f"{PRODUCTS[plan['source_id']]}:{year}" for year in plan["vintages"]}
        if (record.get("object_type") != "source" or meta.get("source_id") != plan["source_id"]
                or meta.get("provider_release_id") not in allowed):
            raise ValueError("physical acquisition authorization cannot register other products/vintages/object types")
        catalog, oid = self.store.read()
        matches = [r for r in catalog["immutable_records"] if r["object_type"] == "source"
                   and r["metadata"].get("source_id") == meta["source_id"]
                   and r["metadata"].get("provider_release_id") == meta["provider_release_id"]]
        if len(matches) > 1 or any(r["object_id"] != record["object_id"] for r in matches):
            raise IdentityCollisionError("concurrent same-vintage acquisition identity; preserve releases and review inventory")
        updated = add_record(catalog, record, receipt)
        self.authority.current_commit(self.store.api.repository)
        if updated == catalog:
            return catalog, False
        updated = deepcopy(updated); updated["compare_and_swap"]["expected_git_blob_sha"] = oid
        validate_catalog_namespace(updated, fixture=self.store.fixture)
        commit = write_current(api=self.store.api, branch="main", path=self.store.path, value=updated,
            expected_sha=oid, message=f"Catalog historical physical artifact {record['object_id']}",
            governance=self.authority)
        from jobs.monthly_refresh.acs_authoritative_write import verify_at_commit
        blob = verify_at_commit(self.store.api, self.store.path, commit, updated)
        if hasattr(self.authority, "write_events"):
            self.authority.write_events[-1].update(state="verified_durable", blob_sha=blob)
        self.authority.current_commit(self.store.api.repository)
        return updated, True


def check_physical_key(authority, source, cycle, release):
    plan = authority.plan
    allowed = {f"{PRODUCTS[plan['source_id']]}:{year}": year for year in plan["vintages"]}
    if (source != plan["source_id"] or release not in allowed
            or cycle != cycle_identity(plan["plan_id"], source, allowed[release])):
        raise ValueError("physical acquisition write outside authorized product/vintage/cycle scope")


class HistoricalReadback:
    """Read current authority by immutable snapshot, including lifecycle get()."""
    def _current_read(self, path):
        from jobs.monthly_refresh.acs_authoritative_write import read_at_commit
        tip = self.authority.current_commit(self.api.repository)
        return read_at_commit(self.api, path, tip, allow_missing=True)

    def _verify_write(self, path, value, commit):
        from jobs.monthly_refresh.acs_authoritative_write import verify_at_commit
        blob = verify_at_commit(self.api, path, commit, value)
        self.last_commit_sha = commit
        if hasattr(self.authority, "write_events"):
            self.authority.write_events[-1].update(state="verified_durable", blob_sha=blob)
        # Historical correctness never substitutes for current permission or
        # current content. This also rejects deletion/replacement after writing.
        if self._current_read(path)[0] != value:
            raise ValueError("historical current state contradicts verified write")


class FencedPinStore(HistoricalReadback, GitHubPinStore):
    def __init__(self, api, authority):
        super().__init__(api, "main"); self.authority = authority

    def _read(self, cycle_id, source_id):
        from jobs.monthly_refresh.source_inputs import pin_path
        return self._current_read(pin_path(cycle_id, source_id))

    def put(self, pin):
        if hasattr(self.authority, "execution_progress"):
            self.authority.execution_progress["boundary"] = "pin_write"
        from jobs.monthly_refresh.acs_authoritative_write import write_current
        from jobs.monthly_refresh.source_inputs import add_pin, pin_path
        check_physical_key(self.authority, pin["source_id"], pin["cycle_id"], pin["provider_release_id"])
        validate_pin(pin, cycle_id=pin["cycle_id"], source_id=pin["source_id"], required_members={"snapshot"})
        existing, oid = self._read(pin["cycle_id"], pin["source_id"])
        value, changed = add_pin(existing, pin)
        self.authority.current_commit(self.api.repository)
        if changed:
            commit = write_current(api=self.api, branch="main", path=pin_path(pin["cycle_id"], pin["source_id"]),
                value=value, expected_sha=oid, message=f"Pin historical {pin['source_id']} for {pin['cycle_id']}",
                governance=self.authority)
            self._verify_write(pin_path(pin["cycle_id"], pin["source_id"]), value, commit)
        return value, changed


class FencedResultStore(HistoricalReadback, GitHubCycleResultStore):
    def __init__(self, api, authority):
        super().__init__(api, "main"); self.authority = authority

    def _read(self, path):
        return self._current_read(path)

    def put(self, proposed):
        from jobs.monthly_refresh.acs_authoritative_write import write_current
        from jobs.monthly_refresh.cycle_results import add_record, record_path
        check_physical_key(self.authority, proposed["source_id"], proposed["cycle_id"],
                           proposed["result"]["provider_release_id"])
        path = record_path(proposed["cycle_id"], proposed["source_id"])
        existing, oid = self._read(path)
        value, changed = add_record(existing, proposed)
        self.authority.current_commit(self.api.repository)
        if changed:
            commit = write_current(api=self.api, branch="main", path=path, value=value, expected_sha=oid,
                message=f"Record historical {proposed['source_id']} for {proposed['cycle_id']}", governance=self.authority)
            self._verify_write(path, value, commit)
        return value, changed


def failure_diagnostic(progress, writes):
    return {"schema_version": "acs_historical_execution_failure_v1", "status": "failed",
            "boundary": progress.get("boundary", "authorization"),
            "vintage": progress.get("vintage"),
            "completed_vintages": list(progress.get("completed_vintages", [])),
            "writes": [{key: event[key] for key in ("path", "commit_sha", "state", "blob_sha")
                        if key in event} for event in writes],
            "publication_outcome": "reconcile_inventory_and_exact_release_before_retry"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--action", choices=("preflight", "authorize", "execute"), default="preflight")
    parser.add_argument("--source-id", choices=sorted(PRODUCTS))
    parser.add_argument("--vintages", default="")
    parser.add_argument("--batch-size", type=int)
    parser.add_argument("--evidence", type=Path)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--authorization", type=Path)
    parser.add_argument("--reviewed-plan-id", default="")
    parser.add_argument("--actor", default="")
    parser.add_argument("--authorization-run-id", default="")
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.action != "preflight" and (args.source_id is not None or args.vintages
            or args.batch_size is not None or args.evidence is not None):
        raise ValueError("authorization/execution scope overrides forbidden; scope comes only from the reviewed plan")
    if args.action == "execute":
        execution_context()
        if not args.plan or not args.authorization or not args.reviewed_plan_id or not args.authorization_run_id:
            raise ValueError("execution requires reviewed plan, separate run authorization and exact plan identity")
    root = Path.cwd(); identity = repository_identity(root)
    token = os.environ.get("GITHUB_TOKEN", "")
    api = GitHubAPI(REPOSITORY, token, read_only=args.action != "execute")
    sha = _main_snapshot(api); _authority_files(api, root, sha)
    head = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    if args.action == "preflight" and head != sha:
        raise ValueError("hosted planning requires checked-out main authority HEAD")
    cas = GitHubCatalogCAS(api, CATALOG_PATH, "main")
    def inventory(source, years, at_commit=None):
        inventory_commit = at_commit or _main_snapshot(api)
        catalog, _ = GitHubCatalogCAS(api, CATALOG_PATH, inventory_commit).read()
        gaps = {}
        governance_path = root / "config/acs_historical_governance.json"
        if governance_path.exists():
            from jobs.monthly_refresh.acs_scope import ACSGovernance
            registry = read_json(governance_path)
            authority_id = registry.get("current_scope", {}).get(source)
            if authority_id:
                governance = ACSGovernance.load_production(REPOSITORY, token, snapshot={
                    "repository": REPOSITORY, "commit": inventory_commit,
                    "registry_sha256": sha256_json(registry)})
                approved = governance.scope(source, authority_id, root)
                gaps = {g["vintage"]: g for g in approved["governed_gaps"]}
        # Fresh resolver workspace prevents stale local cache reuse.
        with tempfile.TemporaryDirectory(dir=args.workspace) as td:
            resolver = GitHubReleaseArtifactResolver(catalog, api, Path(td))
            return reconcile(catalog=catalog, source_id=source, vintages=years, resolve=resolver.resolve, approved_gaps=gaps)
    args.workspace.mkdir(parents=True, exist_ok=True)
    if args.action == "preflight":
        if not re.fullmatch(r"\d{4}(,\d{4}){0,4}", args.vintages):
            raise ValueError("vintages must be 1..5 comma-separated exact years")
        batch_size = 1 if args.batch_size is None else args.batch_size
        years = scope(args.source_id, [int(y) for y in args.vintages.split(",")], batch_size)
        geography, excluded = geography_plan()
        evidence = read_json(args.evidence) if args.evidence else {}
        value = make_plan(source_id=args.source_id, vintages=years, batch_size=batch_size,
            identity=identity, code_commit=head, inventory=inventory(args.source_id, years, sha), evidence=evidence,
            geography=geography, excluded=excluded, authority={"repository": REPOSITORY, "branch": "main", "commit": sha})
    else:
        if args.plan is None:
            raise ValueError("reviewed plan file required")
        plan = read_json(args.plan)
        verify_plan(plan, identity)
        geography, excluded = geography_plan()
        if plan["geography"] != geography or plan["excluded"] != excluded:
            raise ValueError("plan geography/exclusions differ from governed configuration")
        # The reviewed code commit must remain in main history. Exact file identity
        # permits durable pin/result commits without authorizing code changes.
        _code_authority(api, plan["code_commit"], sha)
        if args.action == "authorize":
            current_entries = inventory(plan["source_id"], plan["vintages"])
            for old, current_entry in zip(plan["entries"], current_entries):
                if any(old.get(k) != current_entry.get(k) for k in
                       ("vintage", "classification", "parents", "approved_gap")):
                    raise ValueError("inventory changed since plan review; preflight again")
            value = authorize(plan, reviewed_plan_id=args.reviewed_plan_id, identity=identity, actor=args.actor)
        else:
            if args.authorization is None:
                raise ValueError("separate authorization file required")
            authorization = read_json(args.authorization)
            verify_authorization_run(api, run_id=args.authorization_run_id, plan=plan,
                                     authorization=authorization)
            authority = PlanAuthority(api=api, plan=plan, root=root,
                inventory_check=lambda tip: inventory(plan["source_id"], plan["vintages"], tip))
            def authority_check():
                return authority.current_commit(REPOSITORY)
            pins = FencedPinStore(api, authority); results = FencedResultStore(api, authority)
            cas = FencedPhysicalCatalog(cas, authority)
            policy = read_json(root / "config/monthly_refresh_policy.json")
            def publish(path, source):
                authority_check()
                return exact_publish(artifact=path, source_id=source, api=api, cas=cas,
                    workspace=args.workspace / "publication", git_sha=plan["code_commit"])
            def current():
                authority_check()
                return inventory(plan["source_id"], plan["vintages"])
            def record(result, catalog):
                authority_check()
                return results.put(governed_record(result, policy, catalog))
            def existing_publication(uri):
                catalog, _ = cas.read()
                directory = Path(tempfile.mkdtemp(dir=args.workspace))
                path = GitHubReleaseArtifactResolver(catalog, api, directory).resolve(uri)
                return path, catalog
            def retrieve_pinned_snapshot(pin, output):
                year = int(pin["provider_release_id"].rsplit(":", 1)[1])
                return acquire_historical_snapshot(source_id=pin["source_id"], vintage=year, output=output)
            progress = {}
            authority.execution_progress = progress
            try:
                value = {"plan_id": plan["plan_id"], "executions": execute_batch(plan=plan,
                    authorization=authorization, reviewed_plan_id=args.reviewed_plan_id,
                    identity=identity, workspace=args.workspace, pin_store=pins, current_inventory=current,
                    acquire=acquire_with_diagnostics, retrieve=retrieve_pinned_snapshot, publish=publish,
                    record=record, existing_publication=existing_publication, progress=progress)}
            except Exception:
                # Fixed labels/identities only: never serialize exception text,
                # pins, provider responses, credentials or signed asset URLs.
                write_canonical_json(args.output, failure_diagnostic(progress, authority.write_events))
                raise
    write_canonical_json(args.output, value)
    print(json.dumps({"action": args.action, "output": str(args.output), "plan_id": value.get("plan_id")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
