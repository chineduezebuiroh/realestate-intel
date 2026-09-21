#!/usr/bin/env python3
"""Collect read-only evidence about the authoritative durable source state.

The audit reads the integration checkout, ``origin/main``, and authenticated
GitHub GET endpoints.  It writes exclusively below
``/tmp/realestate-intel-durable-audit`` and never invokes source execution or
control-plane mutation interfaces.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import urllib.parse
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

REPO = "chineduezebuiroh/realestate-intel"
EXPECTED_BRANCH = "monthly-refresh-orchestration"
EXPECTED_HEAD = "860b077cb3c4c637f5b9404e3b7cb843cf9688f2"
DEFAULT_ROOT = Path("/Users/chineduezebuiroh/Desktop/GitHub Projects/realestate-intel")
AUDIT_ROOT = Path("/tmp/realestate-intel-durable-audit")

DISPUTED = {
    "census_nrc": (
        "src__census_nrc__2026-08__r1__94a8e9dc77b9063b",
        "src__census_nrc__2026-08__r1__e02eda146c92a3d3",
    ),
    "bea_gdp_qtr": (
        "src__bea_gdp_qtr__2026-03__r1__9290d93e61b8e5dd",
        "src__bea_gdp_qtr__2026-03__r1__6060ef25ef70eb62",
    ),
    "bea_gdp_ann": (
        "src__bea_gdp_ann__2024-12__r1__1c51a5b7a95bdc27",
        "src__bea_gdp_ann__2024-12__r1__384cf1e026bce84e",
    ),
}
AUDITED_SOURCES = {
    "redfin", "fred_macro", "ces", "laus", "census_bps",
    "census_bps_provisional", "bps", "census_acs1", "census_acs5",
    "acs", "bea_gdp_qtr", "bea_gdp_ann", "census_nrc",
    "census_nrc_fred",
}
MAIN_PREFIXES = (
    "config/monthly_source_input_pins/",
    "config/monthly_source_cycle_results/",
    "config/bps_family_resolutions/",
    "config/acs_family_resolutions/",
    "config/cohort_promotion_records/",
    "config/source_publication_receipts/",
)
MAIN_FILES = {
    "config/artifact_catalog.json",
    "config/monthly_refresh_readiness.json",
}


def _json_dump(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def command_is_read_only(command: list[str]) -> bool:
    """Return whether a subprocess belongs to the audit's explicit allowlist."""
    if not command:
        return False
    if command[0] == "git":
        if command[1:4] == ["fetch", "origin", "--prune"]:
            return command[4:] == ["--tags"]
        return len(command) > 1 and command[1] in {
            "branch", "rev-parse", "remote", "status", "show-ref", "tag",
            "ls-tree", "show",
        }
    if command[:2] == ["gh", "auth"]:
        return command[2:] == ["status"]
    if command[:2] == ["gh", "api"]:
        forbidden = {"--method", "-X", "--input", "-f", "--field", "-F", "--raw-field"}
        return not any(arg in forbidden or arg.startswith("--method=") for arg in command[2:])
    return False


def _run(
    command: list[str], *, cwd: Path, text: bool = True, check: bool = True,
) -> subprocess.CompletedProcess[Any]:
    if not command_is_read_only(command):
        raise RuntimeError(f"refusing non-read-only command: {command!r}")
    print("+", " ".join(command), file=sys.stderr, flush=True)
    return subprocess.run(
        command, cwd=cwd, check=check, capture_output=True, text=text,
    )


def _stdout(command: list[str], root: Path) -> str:
    return _run(command, cwd=root).stdout


def _git(root: Path, *args: str) -> str:
    return _stdout(["git", *args], root).strip()


def _gh_json(root: Path, endpoint: str) -> Any:
    return json.loads(_stdout(["gh", "api", endpoint], root))


def _gh_paginated_items(root: Path, endpoint: str, jq: str) -> list[Any]:
    output = _stdout(["gh", "api", "--paginate", endpoint, "--jq", jq], root)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def _tracked_status(root: Path) -> str:
    return _git(root, "status", "--porcelain", "--untracked-files=no")


def _assert_context(root: Path) -> dict[str, str]:
    if _git(root, "branch", "--show-current") != EXPECTED_BRANCH:
        raise RuntimeError(f"expected branch {EXPECTED_BRANCH}")
    if _git(root, "rev-parse", "HEAD") != EXPECTED_HEAD:
        raise RuntimeError(f"expected HEAD {EXPECTED_HEAD}")
    status = _tracked_status(root)
    if status:
        raise RuntimeError(f"tracked working tree is not clean:\n{status}")
    origin = _git(root, "remote", "get-url", "origin")
    _run(["gh", "auth", "status"], cwd=root)
    access = _gh_json(root, f"repos/{REPO}")
    return {
        "repository_root": str(root), "branch": EXPECTED_BRANCH,
        "head": EXPECTED_HEAD, "origin_url": origin,
        "github_full_name": access["full_name"],
        "github_default_branch": access["default_branch"],
    }


def _prepare_output() -> None:
    if AUDIT_ROOT != Path("/tmp/realestate-intel-durable-audit"):
        raise RuntimeError("unsafe audit root")
    shutil.rmtree(AUDIT_ROOT, ignore_errors=True)
    for name in ("context", "github", "main", "releases", "packages", "extracted", "reports"):
        (AUDIT_ROOT / name).mkdir(parents=True, exist_ok=True)


def _snapshot_main(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    paths = _git(root, "ls-tree", "-r", "--name-only", "origin/main", "config").splitlines()
    relevant = sorted(
        path for path in paths
        if path in MAIN_FILES or any(path.startswith(prefix) for prefix in MAIN_PREFIXES)
    )
    (AUDIT_ROOT / "main/relevant-paths.txt").write_text("\n".join(relevant) + "\n")
    for path in relevant:
        destination = AUDIT_ROOT / "main" / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(_run(["git", "show", f"origin/main:{path}"], cwd=root, text=False).stdout)

    catalog_path = AUDIT_ROOT / "main/config/artifact_catalog.json"
    readiness_path = AUDIT_ROOT / "main/config/monthly_refresh_readiness.json"
    catalog = json.loads(catalog_path.read_text())
    readiness = json.loads(readiness_path.read_text())

    api_values = {}
    for filename in MAIN_FILES:
        response = _gh_json(root, f"repos/{REPO}/contents/{filename}?ref=main")
        import base64
        content = base64.b64decode(response["content"])
        destination = AUDIT_ROOT / "github" / (Path(filename).name + ".main")
        destination.write_bytes(content)
        local = (AUDIT_ROOT / "main" / filename).read_bytes()
        if content != local:
            raise RuntimeError(f"origin/main and GitHub Contents API differ for {filename}")
        api_values[filename] = hashlib.sha256(content).hexdigest()

    _json_dump(AUDIT_ROOT / "reports/main-authority.json", {
        "origin_main": _git(root, "rev-parse", "origin/main"),
        "catalog_blob": _git(root, "rev-parse", "origin/main:config/artifact_catalog.json"),
        "readiness_blob": _git(root, "rev-parse", "origin/main:config/monthly_refresh_readiness.json"),
        "api_verified_sha256": api_values,
    })
    return catalog, readiness


def _release_summary(release: dict[str, Any]) -> dict[str, Any]:
    return {
        key: release.get(key) for key in (
            "id", "tag_name", "name", "draft", "prerelease", "created_at",
            "published_at", "target_commitish", "html_url", "body",
        )
    } | {"assets": [{k: asset.get(k) for k in (
        "id", "name", "size", "content_type", "state", "created_at",
        "updated_at", "browser_download_url",
    )} for asset in release.get("assets", [])]}


def _download_and_validate(
    root: Path, releases: list[dict[str, Any]], catalog: dict[str, Any], runs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    from core.source_artifacts.package import extract_publication_package
    from core.source_artifacts.validation import validate_artifact

    catalog_by_id = {
        item["object_id"]: item for item in catalog.get("immutable_records", [])
        if item.get("object_type") == "source"
    }
    summaries = []
    for release in releases:
        tag = release.get("tag_name", "")
        parts = tag.split("/")
        if len(parts) != 3 or parts[0] != "source-artifact" or parts[1] not in DISPUTED:
            continue
        artifact_id = parts[2]
        for asset in release.get("assets", []):
            directory = AUDIT_ROOT / "packages" / artifact_id
            directory.mkdir(parents=True, exist_ok=True)
            destination = directory / asset["name"]
            response = _run(
                ["gh", "api", "-H", "Accept: application/octet-stream",
                 f"repos/{REPO}/releases/assets/{asset['id']}"],
                cwd=root, text=False,
            )
            destination.write_bytes(response.stdout)
            if destination.stat().st_size != asset["size"]:
                raise RuntimeError(f"asset size mismatch: {destination}")
            base = {
                "artifact_id_from_tag": artifact_id, "asset": asset,
                "path": str(destination), "package_sha256": _sha256(destination),
            }
            if destination.suffix.lower() != ".tar":
                summaries.append(base | {"validated": False, "reason": "not a .tar package"})
                continue
            extracted = AUDIT_ROOT / "extracted" / artifact_id
            if extracted.exists():
                raise RuntimeError(f"duplicate publication tar for {artifact_id}")
            extract_publication_package(destination, extracted, expected_sha256=base["package_sha256"])
            validation = validate_artifact(extracted)
            manifest = validation["manifest"]
            if manifest["artifact_id"] != artifact_id:
                raise RuntimeError(f"tag/manifest artifact mismatch for {artifact_id}")
            data_path = extracted / manifest["data_filename"]
            data = pd.read_parquet(data_path)
            lineage = None
            if manifest.get("lineage_filename"):
                path = extracted / manifest["lineage_filename"]
                frame = pd.read_parquet(path)
                lineage = {
                    "filename": path.name, "sha256": _sha256(path),
                    "row_count": len(frame), "columns": list(frame.columns),
                    "distinct_values": {
                        column: sorted(frame[column].dropna().astype(str).unique().tolist())
                        for column in frame.columns
                        if column in {"source_url", "url", "source_sha256", "raw_sha256",
                                      "byte_length", "content_length", "parser_contract_version",
                                      "workbook_type", "member", "member_name"}
                    },
                }
            with tarfile.open(destination, "r:") as archive:
                members = [{"name": m.name, "size": m.size, "mtime": m.mtime,
                            "sha256": hashlib.sha256(archive.extractfile(m).read()).hexdigest()}
                           for m in archive.getmembers()]
            producer_shas = {value for value in (
                manifest.get("git_sha"), manifest.get("producer_git_sha"),
                manifest.get("code_git_sha"), release.get("target_commitish"),
            ) if value and value != "unknown"}
            summaries.append(base | {
                "validated": True, "validation": validation,
                "manifest": manifest, "package_members": members,
                "data": {
                    "sha256": _sha256(data_path), "row_count": len(data),
                    "observation_min": str(pd.to_datetime(data.date).dt.date.min()),
                    "observation_max": str(pd.to_datetime(data.date).dt.date.max()),
                    "geography_inventory": sorted(data.geo_id.astype(str).unique().tolist()),
                    "metric_inventory": sorted(data.metric_id.astype(str).unique().tolist()),
                    "property_type_inventory": sorted(data.property_type_id.astype(str).unique().tolist()),
                    "duplicate_governed_key_count": int(data.duplicated(
                        ["geo_id", "metric_id", "property_type_id", "date"]
                    ).sum()),
                },
                "lineage": lineage, "release": _release_summary(release),
                "catalog_record": catalog_by_id.get(artifact_id),
                "matching_workflow_runs": [run for run in runs if run.get("head_sha") in producer_shas],
            })
    return summaries


def _control_plane_map() -> dict[str, Any]:
    wanted = set(DISPUTED)
    artifacts = {item for pair in DISPUTED.values() for item in pair}
    result = {"pins": [], "cycle_results": []}
    for kind, directory in (
        ("pins", AUDIT_ROOT / "main/config/monthly_source_input_pins"),
        ("cycle_results", AUDIT_ROOT / "main/config/monthly_source_cycle_results"),
    ):
        if not directory.is_dir():
            continue
        for path in sorted(directory.rglob("*.json")):
            value = json.loads(path.read_text())
            serialized = json.dumps(value)
            if value.get("source_id") in wanted or any(item in serialized for item in artifacts):
                result[kind].append({"path": str(path.relative_to(AUDIT_ROOT / "main")), "value": value})
    return result


def _compare_pairs() -> dict[str, Any]:
    report = {}
    keys = ["geo_id", "metric_id", "property_type_id", "date"]
    for label, pair in DISPUTED.items():
        loaded = []
        for artifact_id in pair:
            directory = AUDIT_ROOT / "extracted" / artifact_id
            if not directory.is_dir():
                loaded.append(None)
                continue
            manifest = json.loads((directory / "manifest.json").read_text())
            data = pd.read_parquet(directory / manifest["data_filename"])
            data = data.sort_values(keys, kind="mergesort").reset_index(drop=True)
            loaded.append((manifest, data))
        if any(item is None for item in loaded):
            report[label] = {
                "artifacts": list(pair), "comparison_available": False,
                "package_available": [item is not None for item in loaded],
            }
            continue
        (left_manifest, left), (right_manifest, right) = loaded  # type: ignore[misc]
        left_keys = left[keys].astype(str)
        right_keys = right[keys].astype(str)
        left_set, right_set = set(map(tuple, left_keys.values)), set(map(tuple, right_keys.values))
        report[label] = {
            "artifacts": list(pair), "comparison_available": True,
            "data_semantically_equal": list(left.columns) == list(right.columns) and left.equals(right),
            "left_row_count": len(left), "right_row_count": len(right),
            "left_only_key_count": len(left_set - right_set),
            "right_only_key_count": len(right_set - left_set),
            "manifest_differences": {
                key: {"left": left_manifest.get(key), "right": right_manifest.get(key)}
                for key in sorted(set(left_manifest) | set(right_manifest))
                if left_manifest.get(key) != right_manifest.get(key)
            },
        }
    return report


def _catalog_reports(catalog: dict[str, Any], readiness: dict[str, Any]) -> None:
    accepted = catalog.get("accepted", {})
    accepted_sources = accepted.get("source", {})
    names = ["redfin", "fred_macro", "ces", "laus", "bps", "acs",
             "bea_gdp_qtr", "bea_gdp_ann", "census_nrc"]
    forbidden = ["census_bps", "census_bps_provisional", "census_acs1",
                 "census_acs5", "census_nrc_fred"]
    _json_dump(AUDIT_ROOT / "reports/accepted-pointers.json", {
        "source_set": accepted.get("source_set"),
        "canonical_market": accepted.get("canonical_market"),
        "serving_market": accepted.get("serving_market"),
        "source": {name: accepted_sources.get(name) for name in names},
        "forbidden_physical_or_legacy": {name: accepted_sources.get(name) for name in forbidden},
        "all_accepted_source_pointers": accepted_sources,
    })
    inventory = []
    for record in catalog.get("immutable_records", []):
        source_id = record.get("metadata", {}).get("source_id")
        if record.get("object_type") != "source" or source_id not in AUDITED_SOURCES:
            continue
        inventory.append(record | {
            "source_id": source_id,
            "accepted": accepted_sources.get(source_id) == record.get("object_id"),
        })
    inventory.sort(key=lambda item: (item["source_id"], item["object_id"]))
    _json_dump(AUDIT_ROOT / "reports/final-cohort-catalog-inventory.json", inventory)
    _json_dump(AUDIT_ROOT / "reports/redfin-accepted.json", {
        "accepted_redfin": accepted_sources.get("redfin"),
        "catalog_records": [item for item in inventory if item["object_id"] == accepted_sources.get("redfin")],
    })
    records = readiness.get("records", [])
    _json_dump(AUDIT_ROOT / "reports/redfin-readiness.json", {
        "all_records": records,
        "july_records": [item for item in records if
            item.get("cycle_id") == "monthly_cycle__2026-07__7cab1c5df177a1e4" or
            item.get("readiness_id") == "redfin_readiness__monthly_cycle__2026-07__7cab1c5df177a1e4"],
        "unconsumed_records": [item for item in records if item.get("consumed") is False],
        "eligible_shape_unconsumed_records": [item for item in records if
            item.get("consumed") is False and item.get("source_id") == "redfin" and
            item.get("validation_status") == "passed" and
            item.get("publication_state") == "published_immutable_verified"],
    })


def _family_reports() -> None:
    records = []
    missing = []
    for name in ("bps_family_resolutions", "acs_family_resolutions"):
        directory = AUDIT_ROOT / "main/config" / name
        if not directory.is_dir():
            missing.append(str(directory))
            continue
        for path in sorted(directory.rglob("*.json")):
            records.append({"path": str(path.relative_to(AUDIT_ROOT / "main")),
                            "value": json.loads(path.read_text())})
    _json_dump(AUDIT_ROOT / "reports/family-resolution-summary.json",
               {"records": records, "missing_optional_directories": missing})


def _release_catalog_consistency(
    releases: list[dict[str, Any]], catalog: dict[str, Any],
) -> list[dict[str, Any]]:
    by_tag = {item.get("tag_name"): item for item in releases}
    rows = []
    for record in catalog.get("immutable_records", []):
        source = record.get("metadata", {}).get("source_id")
        if record.get("object_type") != "source" or source not in DISPUTED:
            continue
        release = by_tag.get(record.get("release_tag"))
        assets = [] if release is None else [
            item for item in release.get("assets", []) if item.get("id") == record.get("asset_id")
        ]
        rows.append({
            "source_id": source, "artifact_id": record.get("object_id"),
            "catalog_record": record, "release_found": release is not None,
            "release_id_matches": bool(release and release.get("id") == record.get("release_id")),
            "exact_asset_found": len(assets) == 1,
            "release": None if release is None else _release_summary(release),
        })
    return rows


def run(root: Path) -> None:
    context = _assert_context(root)
    _prepare_output()
    _json_dump(AUDIT_ROOT / "context/context.json", context)

    _run(["git", "fetch", "origin", "--prune", "--tags"], cwd=root)
    context.update({
        "origin_main": _git(root, "rev-parse", "origin/main"),
        "origin_monthly_refresh_orchestration": _git(root, "rev-parse", "origin/monthly-refresh-orchestration"),
    })
    _json_dump(AUDIT_ROOT / "context/context.json", context)
    (AUDIT_ROOT / "context/show-ref.txt").write_text(_git(root, "show-ref") + "\n")
    (AUDIT_ROOT / "context/source-artifact-tags.txt").write_text(
        _git(root, "tag", "--list", "source-artifact/*") + "\n"
    )

    catalog, readiness = _snapshot_main(root)
    releases = _gh_paginated_items(root, f"repos/{REPO}/releases?per_page=100", ".[]")
    runs = _gh_paginated_items(root, f"repos/{REPO}/actions/runs?per_page=100", ".workflow_runs[]")
    tag_refs = _gh_paginated_items(root, f"repos/{REPO}/git/matching-refs/tags/source-artifact/", ".[]")
    _json_dump(AUDIT_ROOT / "github/releases.json", releases)
    _json_dump(AUDIT_ROOT / "github/workflow-runs.json", runs)
    _json_dump(AUDIT_ROOT / "github/source-artifact-tag-refs.json", tag_refs)

    relevant_releases = [item for item in releases if any(
        item.get("tag_name", "").startswith(f"source-artifact/{source}/") for source in DISPUTED
    )]
    _json_dump(AUDIT_ROOT / "reports/nrc-releases.json",
               [_release_summary(item) for item in relevant_releases if "/census_nrc/" in item["tag_name"]])
    _json_dump(AUDIT_ROOT / "reports/bea-releases.json",
               [_release_summary(item) for item in relevant_releases if "/bea_gdp_" in item["tag_name"]])
    by_tag = {item.get("tag_name"): item for item in releases}
    _json_dump(AUDIT_ROOT / "reports/exact-disputed-release-lookups.json", [
        {"source_id": source, "artifact_id": artifact, "tag": tag,
         "exists": tag in by_tag,
         "release": None if tag not in by_tag else _release_summary(by_tag[tag])}
        for source, artifacts in DISPUTED.items() for artifact in artifacts
        for tag in [f"source-artifact/{source}/{artifact}"]
    ])

    package_report = _download_and_validate(root, relevant_releases, catalog, runs)
    _json_dump(AUDIT_ROOT / "reports/package-validation-summary.json", package_report)
    _json_dump(AUDIT_ROOT / "reports/nrc-bea-control-plane-map.json", _control_plane_map())
    _json_dump(AUDIT_ROOT / "reports/disputed-pair-comparisons.json", _compare_pairs())
    _catalog_reports(catalog, readiness)
    _family_reports()
    _json_dump(AUDIT_ROOT / "reports/release-catalog-consistency.json",
               _release_catalog_consistency(releases, catalog))

    relevant_runs = [run for run in runs if any(term in (
        (run.get("name") or "") + " " + (run.get("display_title") or "")
    ).lower() for term in ("nrc", "bea", "census_nrc", "bea_gdp", "monthly source"))]
    _json_dump(AUDIT_ROOT / "reports/relevant-source-workflow-runs.json", relevant_runs)
    workflow_jobs = []
    for run_record in relevant_runs:
        jobs = _gh_paginated_items(
            root, f"repos/{REPO}/actions/runs/{run_record['id']}/jobs?per_page=100", ".jobs[]"
        )
        workflow_jobs.append({"run_id": run_record["id"], "jobs": jobs})
    _json_dump(AUDIT_ROOT / "reports/relevant-source-workflow-jobs.json", workflow_jobs)

    final_status = _tracked_status(root)
    if final_status:
        raise RuntimeError(f"tracked repository state changed during audit:\n{final_status}")
    (AUDIT_ROOT / "reports/final-tracked-git-status.txt").write_text(final_status)

    primary = sorted(str(path) for path in (AUDIT_ROOT / "reports").iterdir())
    print("DURABLE_STATE_RECONCILIATION_COMPLETE")
    print(f"evidence_root={AUDIT_ROOT}")
    print("primary_report_paths:")
    for path in primary:
        print(path)
    print("final_tracked_git_status:")
    print(final_status or "CLEAN")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args(list(argv) if argv is not None else None)
    run(args.repo_root.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
