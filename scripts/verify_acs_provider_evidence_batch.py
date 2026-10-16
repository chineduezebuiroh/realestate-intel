"""Offline review of bounded hosted ACS capture archives; no acquisition authority.

This adapter leaves the fixed pilot verifier and acquisition code unchanged.
Provider interpretation is exclusively historical.preflight/validate_evidence.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import zipfile
from collections import Counter
from pathlib import Path

import requests

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_backfill import scope
from jobs.monthly_refresh.acs_monthly import API, _safe_url
from jobs.monthly_refresh.acs_provider_evidence import capture_identity
from sources.census_acs.artifact import PRODUCTS, geography_plan, governed_config_hashes, request_params
from sources.census_acs.historical import checked_response, preflight, validate_evidence

REPOSITORY = "chineduezebuiroh/realestate-intel"


def verify_batch_archive(archive: Path, *, expected_sha256: str, source_id: str,
                         vintages: list[int], run_id: int, artifact_id: int,
                         capture_commit: str, root=Path("."), failed_vintages: list[int] | None = None) -> tuple[dict[str, bytes], dict]:
    years = scope(source_id, vintages, len(vintages))
    if (not re.fullmatch(r"[0-9a-f]{64}", expected_sha256)
            or not re.fullmatch(r"[0-9a-f]{40}", capture_commit)
            or run_id <= 0 or artifact_id <= 0
            or archive.stat().st_size > 256 * 1024 * 1024
            or sha256_file(archive) != expected_sha256):
        raise ValueError("exact archive/provenance identity mismatch")
    failed = [] if failed_vintages is None else scope(source_id, failed_vintages, len(failed_vintages))
    if not set(failed) < set(years) and failed:
        raise ValueError("explicit failed scope must leave successful vintages")
    successful = [y for y in years if y not in failed]
    names = {"batch-summary.json", "provider-evidence.json"}
    for year in years:
        names |= {f"{year}/{n}" for n in ("summary.json", "provider-evidence.json", "response-manifest.json")}
        names |= {f"{year}/responses/{i:03d}.body" for i in range(225)}
    dirs = {f"{y}/" for y in years} | {f"{y}/responses/" for y in years}
    with zipfile.ZipFile(archive) as z:
        infos = z.infolist()
        # Failed capture directories contain audit receipts only, never evidence.
        for year in failed:
            item = z.getinfo(f"{year}/summary.json")
            if item.file_size > 256 * 1024:
                raise ValueError("failed summary size limit")
            count = json.loads(z.read(item)).get("captured_responses")
            if type(count) is not int or not 0 <= count <= 225:
                raise ValueError("failed response count limit")
            names.remove(f"{year}/provider-evidence.json")
            names -= {f"{year}/responses/{i:03d}.body" for i in range(count, 225)}
        files = [i for i in infos if not i.is_dir()]
        if (len(infos) > len(names) + len(dirs)
                or len({i.filename for i in infos}) != len(infos)
                or {i.filename for i in files} != names
                or any(i.is_dir() and i.filename not in dirs for i in infos)
                or any(stat.S_ISLNK(i.external_attr >> 16) or i.flag_bits & 1 for i in infos)
                or sum(i.file_size for i in files) > 256 * 1024 * 1024
                or any(i.file_size > 32 * 1024 * 1024 for i in files)):
            raise ValueError("archive member/size allowlist mismatch")
        if z.testzip() is not None:
            raise ValueError("archive CRC failure")
        data = {i.filename: z.read(i) for i in files}
    config = governed_config_hashes(root)
    plan, excluded = geography_plan(root / "config/geo_manifest.generated.csv")
    if len(plan) != 223 or len(excluded) != 10:
        raise ValueError("governed membership drift")
    batch = json.loads(data["batch-summary.json"])
    hosted = batch.get("hosted_run", {})
    if (batch.get("schema_version") != "acs_provider_capture_batch_v1"
            or batch.get("source_id") != source_id or batch.get("vintages") != years
            or batch.get("code_commit") != capture_commit or batch.get("config_hashes") != config
            or batch.get("validated") is not (not failed) or batch.get("production_writes") is not False
            or batch.get("failed_vintages") != failed or batch.get("successful_vintages") != successful
            or type(batch.get("http_requests")) is not int or not 0 <= batch["http_requests"] <= 225 * len(years)
            or batch.get("aggregation_failure") is not None
            or hosted.get("GITHUB_REPOSITORY") != REPOSITORY
            or hosted.get("GITHUB_RUN_ID") != str(run_id)
            or hosted.get("GITHUB_WORKFLOW_REF") != f"{REPOSITORY}/.github/workflows/acs-provider-evidence.yml@refs/heads/main"
            or not re.fullmatch(r"[1-9][0-9]*", str(hosted.get("GITHUB_RUN_ATTEMPT", "")))):
        raise ValueError("batch scope/configuration/provenance mismatch")
    entries = batch.get("entries", [])
    if len(entries) != len(years):
        raise ValueError("batch entry inventory mismatch")
    identity = {"source_id": source_id, "vintages": years, "config_hashes": config,
                "validated": not failed, "aggregation_failure": batch.get("aggregation_failure"),
                "entries": [{k: e.get(k) for k in ("vintage", "validated", "capture_id", "response_manifest_sha256")} for e in entries]}
    if batch.get("batch_id") != "acs_provider_capture_batch_v1__" + sha256_json(identity):
        raise ValueError("batch identity mismatch")
    digest = lambda body: hashlib.sha256(body).hexdigest()
    candidates, reports, combined, failed_reports = {}, [], {}, []
    captured = 0
    for year, entry in zip(years, entries):
        prefix = f"{year}/"
        summary = json.loads(data[prefix + "summary.json"])
        manifest = json.loads(data[prefix + "response-manifest.json"])
        captured += summary.get("captured_responses", 0)
        if year in failed:
            if (summary != entry or summary.get("validated") is not False
                    or summary.get("source_id") != source_id or summary.get("vintage") != year
                    or summary.get("schema_version") != "acs_provider_capture_summary_v1"
                    or summary.get("code_commit") != capture_commit or summary.get("config_hashes") != config
                    or summary.get("production_writes") is not False or summary.get("credential_persisted") is not False
                    or summary.get("requested_members") != 223 or summary.get("excluded_members") != 10
                    or summary.get("failure") != "provider_evidence_inaccessible_or_invalid"
                    or summary.get("capture_id") is not None or summary.get("evidence_sha256") is not None
                    or summary.get("response_manifest_sha256") != digest(data[prefix + "response-manifest.json"])
                    or manifest.get("source_id") != source_id or manifest.get("vintage") != year
                    or len(manifest.get("responses", [])) != summary["captured_responses"]):
                raise ValueError("failed capture audit mismatch")
            expected_urls = [API + ".json", f"{API}/{year}/{PRODUCTS[source_id]}/variables.json"] + [
                _safe_url(year, PRODUCTS[source_id], request_params(g)) for g in plan]
            for i, record in enumerate(manifest["responses"]):
                name = f"responses/{i:03d}.body"
                body = data[prefix + name]
                if (record.get("request_url") != expected_urls[i] or record.get("body_path") != name or record.get("body_size_bytes") != len(body)
                        or record.get("body_sha256") != digest(body)):
                    raise ValueError("failed capture raw response integrity mismatch")
                if record.get("decoded_response_sha256") is not None:
                    response = requests.Response()
                    response.status_code, response._content, response.encoding = record["http_status"], body, "utf-8"
                    payload = checked_response(response, allow_no_content=i >= 2)
                    if sha256_json(payload) != record["decoded_response_sha256"]:
                        raise ValueError("failed capture decoded identity mismatch")
            failed_reports.append({"vintage": year, "adopted": False,
                "raw_bodies_hash_verified": summary["captured_responses"],
                "response_manifest_sha256": summary["response_manifest_sha256"]})
            continue
        original = data[prefix + "provider-evidence.json"]
        evidence = json.loads(original)
        if (entry != summary or summary.get("schema_version") != "acs_provider_capture_summary_v1"
                or summary.get("source_id") != source_id or summary.get("vintage") != year
                or summary.get("code_commit") != capture_commit or summary.get("config_hashes") != config
                or summary.get("validated") is not True or summary.get("production_writes") is not False
                or summary.get("credential_persisted") is not False
                or summary.get("requested_members") != 223 or summary.get("excluded_members") != 10
                or summary.get("captured_responses") != 225
                or summary.get("evidence_file") != "provider-evidence.json"
                or summary.get("evidence_sha256") != digest(original)
                or summary.get("response_manifest_sha256") != digest(data[prefix + "response-manifest.json"])
                or manifest.get("source_id") != source_id or manifest.get("vintage") != year
                or len(manifest.get("responses", [])) != 225 or set(evidence) != {str(year)}
                or summary.get("capture_id") != capture_identity(summary)):
            raise ValueError("year source/configuration/file identity mismatch")
        records = manifest["responses"]

        class Replay:
            index = 0
            def get(self, url, *, params=None, timeout):
                record = records[self.index]
                name = f"responses/{self.index:03d}.body"
                self.index += 1
                body = data[prefix + name]
                safe = _safe_url(year, PRODUCTS[source_id], params) if params is not None else url
                if (record.get("request_url") != safe or record.get("body_path") != name
                        or record.get("body_size_bytes") != len(body) or record.get("body_sha256") != digest(body)):
                    raise ValueError("raw response identity mismatch")
                response = requests.Response()
                response.status_code, response._content, response.encoding = record["http_status"], body, "utf-8"
                payload = checked_response(response, allow_no_content=params is not None)
                if record.get("decoded_response_sha256") != sha256_json(payload):
                    raise ValueError("decoded response identity mismatch")
                return response

        replay = Replay()
        rebuilt = preflight(source_id=source_id, vintage=year, transport=replay, plan=plan, excluded=excluded)
        validate_evidence(evidence[str(year)], source_id=source_id, vintage=year, plan=plan, excluded=excluded)
        if (replay.index != 225 or rebuilt != evidence[str(year)]
                or summary.get("snapshot_sha256") != rebuilt["snapshot_sha256"]
                or summary.get("accounting") != dict(Counter(a["classification"] for a in rebuilt["accounting"]))
                or summary.get("omission_receipt_ids") != [r["evidence_id"] for r in rebuilt["omission_evidence"]]):
            raise ValueError("evidence differs from exact year-keyed captured responses")
        candidates[f"provider-{source_id.removeprefix('census_')}-{year}.json"] = original
        combined.update(evidence)
        reports.append({"vintage": year, "capture_id": summary["capture_id"], "evidence_sha256": digest(original),
                        "response_manifest_sha256": summary["response_manifest_sha256"],
                        "snapshot_sha256": rebuilt["snapshot_sha256"], "responses_verified": 225,
                        "accounting": summary["accounting"], "omission_receipt_ids": summary["omission_receipt_ids"]})
    if not captured <= batch["http_requests"] <= captured + len(failed):
        raise ValueError("batch request accounting mismatch")
    if json.loads(data["provider-evidence.json"]) != combined or governed_config_hashes(root) != config:
        raise ValueError("aggregate evidence/configuration mismatch")
    aggregate_name = f"provider-{source_id.removeprefix('census_')}-" + "-".join(map(str, successful)) + ".json"
    # Hosted preflight accepts one map; retain the original aggregate bytes too.
    if len(successful) > 1:
        candidates[aggregate_name] = data["provider-evidence.json"]
    return candidates, {"aggregate_evidence_sha256": digest(data["provider-evidence.json"]), "schema_version": "acs_provider_batch_review_v1", "repository": REPOSITORY,
        "source_id": source_id, "vintages": successful, "captured_vintages": years,
        "capture_validated": not failed, "failed_captures_not_adopted": failed_reports,
        "run_id": run_id, "artifact_id": artifact_id,
        "capture_commit": capture_commit, "archive_sha256": expected_sha256,
        "config_hashes": config, "batch_id": batch["batch_id"], "entries": reports,
        "responses_verified": 225 * len(successful), "validated": True,
        "approval_status": "REVIEW_CANDIDATE_ONLY"}



def combine_reviewed_evidence(reviews: list[tuple[dict[str, bytes], dict]], *,
                              source_id: str, vintages: list[int], root=Path(".")) -> tuple[bytes, dict]:
    """Combine only verified year maps; never select a winner for duplicate years."""
    years = scope(source_id, vintages, len(vintages))
    config = governed_config_hashes(root)
    plan, excluded = geography_plan(root / "config/geo_manifest.generated.csv")
    merged, origins = {}, []
    for candidates, report in reviews:
        if (report.get("validated") is not True or report.get("approval_status") != "REVIEW_CANDIDATE_ONLY"
                or report.get("source_id") != source_id or report.get("config_hashes") != config):
            raise ValueError("review source/configuration mismatch")
        if [e["vintage"] for e in report["entries"]] != report["vintages"]:
            raise ValueError("review year inventory mismatch")
        for entry in report["entries"]:
            year = entry["vintage"]
            if str(year) in merged or year not in years:
                raise ValueError("duplicate or unexpected reviewed vintage")
            body = candidates[f"provider-{source_id.removeprefix('census_')}-{year}.json"]
            if hashlib.sha256(body).hexdigest() != entry["evidence_sha256"]:
                raise ValueError("reviewed candidate byte mismatch")
            value = json.loads(body)
            if set(value) != {str(year)}:
                raise ValueError("reviewed candidate year mismatch")
            validate_evidence(value[str(year)], source_id=source_id, vintage=year, plan=plan, excluded=excluded)
            merged.update(value)
            origins.append({"vintage": year, **{k: report[k] for k in
                ("run_id", "artifact_id", "archive_sha256", "capture_commit", "batch_id")}, **entry})
    if set(merged) != set(map(str, years)):
        raise ValueError("missing reviewed vintage")
    body = (json.dumps(merged, sort_keys=True, indent=2) + "\n").encode()
    return body, {"schema_version": "acs_provider_combined_review_v1", "source_id": source_id,
        "vintages": years, "config_hashes": config, "origins": sorted(origins, key=lambda x: x["vintage"]),
        "combined_evidence_sha256": hashlib.sha256(body).hexdigest(),
        "validated": True, "approval_status": "REVIEW_CANDIDATE_ONLY"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--failed-vintages", help="Explicit failed capture years to audit and exclude, never adopt")
    parser.add_argument("--expected-sha256", required=True)
    parser.add_argument("--source-id", choices=sorted(PRODUCTS), required=True)
    parser.add_argument("--vintages", required=True)
    parser.add_argument("--run-id", type=int, required=True)
    parser.add_argument("--artifact-id", type=int, required=True)
    parser.add_argument("--capture-commit", required=True)
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"\d{4}(,\d{4}){0,4}", args.vintages):
            raise ValueError("explicit bounded years required")
        if args.failed_vintages is not None and not re.fullmatch(r"\d{4}(,\d{4}){0,3}", args.failed_vintages):
            raise ValueError("explicit bounded failed year list required")
        candidates, report = verify_batch_archive(args.archive, expected_sha256=args.expected_sha256,
            source_id=args.source_id, vintages=[int(y) for y in args.vintages.split(",")],
            run_id=args.run_id, artifact_id=args.artifact_id, capture_commit=args.capture_commit,
            failed_vintages=None if args.failed_vintages is None else [int(y) for y in args.failed_vintages.split(",")])
    except Exception:
        print("Exact ACS batch verification failed; no candidates emitted")
        return 1
    args.output.mkdir(parents=True, exist_ok=False)
    for name, body in candidates.items():
        (args.output / name).write_bytes(body)
    (args.output / "verification.json").write_text(json.dumps(report, sort_keys=True, indent=2) + "\n")
    if report["failed_captures_not_adopted"]:
        print("Failed capture vintages excluded: " + ",".join(str(e["vintage"]) for e in report["failed_captures_not_adopted"]))
    print("Exact ACS batch verified; local review candidates only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
