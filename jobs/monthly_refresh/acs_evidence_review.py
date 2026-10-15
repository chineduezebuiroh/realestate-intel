"""Offline verification of the exact hosted ACS5 2023 pilot archive.

Produces local review candidates only. Hashes and source references confer no
approval; the separately reviewed main change is the evidence-adoption boundary.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import zipfile
from pathlib import Path
from collections import Counter

import requests

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_monthly import _safe_url
from sources.census_acs.artifact import geography_plan, governed_config_hashes
from sources.census_acs.historical import checked_response, preflight, validate_evidence

PILOT_ARCHIVE_SHA256 = "6d96f15258f4e718b3612d1afea635195dde71dc39770831189bd1c26de22b1d"


def verify_pilot_archive(archive: Path, *, expected_sha256: str, root=Path(".")) -> tuple[bytes, dict]:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or sha256_file(archive) != expected_sha256:
        raise ValueError("exact hosted ZIP SHA-256 mismatch")
    if archive.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("archive compressed size limit")
    with zipfile.ZipFile(archive) as z:
        infos = z.infolist()
        files = [i for i in infos if not i.is_dir()]
        allowed = {"provider-evidence.json", "summary.json", "response-manifest.json"} | {
            f"responses/{i:03d}.body" for i in range(225)}
        if (len({i.filename for i in infos}) != len(infos)
                or {i.filename for i in files} != allowed
                or any(i.is_dir() and i.filename != "responses/" for i in infos)
                or any(stat.S_ISLNK(i.external_attr >> 16) for i in infos)
                or sum(i.file_size for i in files) > 256 * 1024 * 1024
                or any(i.file_size > 32 * 1024 * 1024 for i in files)):
            raise ValueError("archive member/size allowlist mismatch")
        if z.testzip() is not None:
            raise ValueError("archive CRC failure")
        data = {i.filename: z.read(i) for i in files}
    digest = lambda body: hashlib.sha256(body).hexdigest()
    summary = json.loads(data["summary.json"])
    manifest = json.loads(data["response-manifest.json"])
    evidence = json.loads(data["provider-evidence.json"])
    config = governed_config_hashes(root)
    plan, excluded = geography_plan(root / "config/geo_manifest.generated.csv")
    if (summary.get("schema_version") not in {"acs_provider_evidence_pilot_summary_v1", "acs_provider_capture_summary_v1"}
            or summary.get("validated") is not True or summary.get("production_writes") is not False
            or summary.get("credential_persisted") is not False
            or summary.get("source_id") != "census_acs5" or summary.get("vintage") != 2023
            or summary.get("config_hashes") != config
            or summary.get("requested_members") != len(plan) or len(plan) != 223
            or summary.get("excluded_members") != len(excluded) or len(excluded) != 10
            or summary.get("captured_responses") != 225
            or summary.get("evidence_sha256") != digest(data["provider-evidence.json"])
            or summary.get("response_manifest_sha256") != digest(data["response-manifest.json"])
            or manifest.get("source_id") != "census_acs5" or manifest.get("vintage") != 2023
            or len(manifest.get("responses", [])) != 225 or set(evidence) != {"2023"}):
        raise ValueError("pilot source/configuration/file identity mismatch")
    records = manifest["responses"]

    class Replay:
        """Replay exact raw bytes; existing preflight retains all interpretation."""
        index = 0
        def get(self, url, *, params=None, timeout):
            record = records[self.index]
            body_path = f"responses/{self.index:03d}.body"
            self.index += 1
            body = data[body_path]
            safe = _safe_url(2023, "acs/acs5", params) if params is not None else url
            if (record.get("request_url") != safe or record.get("body_path") != body_path
                    or record.get("body_size_bytes") != len(body) or record.get("body_sha256") != digest(body)):
                raise ValueError("raw response identity mismatch")
            response = requests.Response()
            response.status_code = record["http_status"]
            response._content = body
            response.encoding = "utf-8"
            payload = checked_response(response, allow_no_content=params is not None)
            if record.get("decoded_response_sha256") != sha256_json(payload):
                raise ValueError("decoded response identity mismatch")
            return response

    replay = Replay()
    rebuilt = preflight(source_id="census_acs5", vintage=2023, transport=replay, plan=plan, excluded=excluded)
    validate_evidence(evidence["2023"], source_id="census_acs5", vintage=2023, plan=plan, excluded=excluded)
    if (replay.index != 225 or rebuilt != evidence["2023"]
            or summary.get("snapshot_sha256") != rebuilt["snapshot_sha256"]
            or summary.get("accounting") != dict(Counter(a["classification"] for a in rebuilt["accounting"]))
            or summary.get("omission_receipt_ids") != [r["evidence_id"] for r in rebuilt["omission_evidence"]]):
        raise ValueError("evidence differs from exact captured provider responses")
    return data["provider-evidence.json"], {"schema_version": "acs_provider_evidence_review_v1",
        "archive_sha256": expected_sha256, "responses_verified": 225,
        "config_hashes": config, "evidence_sha256": digest(data["provider-evidence.json"]),
        "snapshot_sha256": rebuilt["snapshot_sha256"],
        "accounting": summary["accounting"],
        "omission_receipt_ids": summary["omission_receipt_ids"], "validated": True,
        "approval_status": "REVIEW_CANDIDATE_ONLY"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        body, report = verify_pilot_archive(args.archive, expected_sha256=PILOT_ARCHIVE_SHA256)
        args.output.mkdir(parents=True, exist_ok=False)
        (args.output / "provider-acs5-2023.json").write_bytes(body)
        report.update(repository="chineduezebuiroh/realestate-intel", run_id=38598860460,
                      artifact_id=11912706827, capture_commit="55182ef227f1c66f1da91ab102999a1b93187c0b")
        (args.output / "verification.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    except Exception:
        print("ACS5 2023 exact archive verification failed; no evidence adoption authorized")
        return 1
    print("ACS5 2023 exact archive verified; local review candidate only")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
