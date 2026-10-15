"""Bounded read-only provider evidence; no durable source writes or approvals."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from collections import Counter
from pathlib import Path
from urllib.parse import quote, quote_plus

import requests

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_monthly import API, _safe_url
from jobs.monthly_refresh.acs_backfill import scope
from sources.census_acs.artifact import PRODUCTS, VARIABLES, geography_plan, governed_config_hashes
from sources.census_acs.historical import checked_response, preflight, validate_evidence

SOURCE = "census_acs5"
VINTAGE = 2023
ENDPOINT = f"{API}/{VINTAGE}/acs/acs5"
MAX_REQUESTS = 225  # discovery, variables, all 223 configured members
MAX_SECONDS = 900
MAX_BATCH_SECONDS = 1800
MAX_OUTPUT_BYTES = 256 * 1024 * 1024
AUDIT_RESERVE = 2 * 1024 * 1024


class CaptureBudget:
    def __init__(self, years=1):
        self.started = time.monotonic()
        self.requests = 0
        self.max_requests = years * MAX_REQUESTS
        self.output_bytes = 0

    def check(self):
        if time.monotonic() - self.started >= MAX_BATCH_SECONDS:
            raise ValueError("batch time budget exhausted")

    def write(self, path: Path, data: bytes, *, audit=False):
        limit = MAX_OUTPUT_BYTES if audit else MAX_OUTPUT_BYTES - AUDIT_RESERVE
        if self.output_bytes + len(data) > limit:
            raise ValueError("batch output budget exhausted")
        path.write_bytes(data)
        self.output_bytes += len(data)


def write_json(path: Path, value: dict, budget=None, *, audit=False) -> None:
    data = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    if budget is None:
        path.write_bytes(data)
    else:
        if audit and len(data) > 256 * 1024:
            raise ValueError("audit output limit exceeded")
        budget.write(path, data, audit=audit)


def capture_identity(summary: dict) -> str:
    return "acs_provider_capture_v1__" + sha256_json({k: summary[k] for k in
        ("source_id", "vintage", "config_hashes", "snapshot_sha256", "evidence_sha256",
         "response_manifest_sha256")})


class CaptureTransport:
    """Bounded HTTP transport only; historical.py interprets provider responses.

    Never persist response URLs/headers or transport exceptions. Raw response
    bytes are retained only after checking for an echoed transport credential.
    """
    def __init__(self, session, credential: str, output: Path, *, source_id=SOURCE,
                 vintage=VINTAGE, budget=None):
        self.session, self.credential, self.output = session, credential, output
        self.source_id, self.vintage = source_id, vintage
        self.endpoint = f"{API}/{vintage}/{PRODUCTS[source_id]}"
        self.budget = budget or CaptureBudget()
        self.started = time.monotonic()
        self.records: list[dict] = []
        self.last_request = None

    def reject_credential(self, body: bytes) -> None:
        if any(value.encode() in body for value in {self.credential,
               json.dumps(self.credential)[1:-1], quote(self.credential, safe=""), quote_plus(self.credential)}):
            raise ValueError("credential echo rejected")

    def get(self, url, *, params=None, timeout):
        if url not in {API + ".json", self.endpoint + "/variables.json", self.endpoint}:
            raise ValueError("provider endpoint outside exact scope")
        self.budget.check()
        if len(self.records) >= MAX_REQUESTS or time.monotonic() - self.started >= MAX_SECONDS:
            raise ValueError("pilot request/time budget exhausted")
        if self.budget.requests >= self.budget.max_requests:
            raise ValueError("batch request budget exhausted")
        if params is not None and (url != self.endpoint or set(params) - {"get", "for", "in"}):
            raise ValueError("unexpected provider request parameters")
        safe = _safe_url(self.vintage, PRODUCTS[self.source_id], params) if params is not None else url
        self.last_request = safe
        # The existing historical preflight supplies credential-free parameters.
        # Match the existing ACS acquisition's key-only-in-transport convention.
        transport_params = {**params, "key": self.credential} if params is not None else None
        limit = 32 * 1024 * 1024 if params is None else 1024 * 1024
        self.budget.requests += 1
        with self.session.get(url, params=transport_params, timeout=min(timeout, 60),
                              allow_redirects=False, stream=True) as response:
            parts, size = [], 0
            for chunk in response.iter_content(chunk_size=65536):
                self.budget.check()
                size += len(chunk)
                if size > limit or time.monotonic() - self.started >= MAX_SECONDS:
                    raise ValueError("pilot response/time budget exhausted")
                parts.append(chunk)
            body = b"".join(parts)
            self.reject_credential(body)
            # Use requests' Response decoding via its normal json() interface;
            # no parallel Census parser or omission classification is introduced.
            captured = requests.Response()
            captured.status_code = response.status_code
            captured._content = body
            captured.encoding = "utf-8"
            name = f"responses/{len(self.records):03d}.body"
            self.budget.write(self.output / name, body)
            self.records.append({"request_url": safe, "http_status": response.status_code,
                "body_path": name, "body_size_bytes": len(body),
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "decoded_response_sha256": None})
            payload = checked_response(captured, allow_no_content=params is not None)
            self.records[-1]["decoded_response_sha256"] = sha256_json(payload)
            return captured


def capture_year(output: Path, *, source_id=SOURCE, vintage=VINTAGE, credential: str,
                 session=None, root: Path = Path("."), budget=None, expected_config=None,
                 code_commit="unknown", session_factory=requests.Session) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    (output / "responses").mkdir()
    transport = None
    budget = budget or CaptureBudget()
    summary = {"schema_version": "acs_provider_capture_summary_v1",
        "source_id": source_id, "vintage": vintage, "validated": False,
        "code_commit": code_commit,
        "production_writes": False, "credential_persisted": False}
    try:
        if not credential.strip():
            raise ValueError("provider credential unavailable")
        scope(source_id, [vintage], 1)
        budget.check()
        plan, excluded = geography_plan(root / "config/geo_manifest.generated.csv")
        if (len(plan) != 223 or len(excluded) != 10
                or set(VARIABLES) != {"B01003_001E", "B19013_001E"}):
            raise ValueError("fixed pilot governed membership changed")
        config = governed_config_hashes(root)
        if expected_config is not None and config != expected_config:
            raise ValueError("governed configuration drift")
        summary.update(requested_members=len(plan), excluded_members=len(excluded),
                       config_hashes=config)
        with session_factory() if session is None else session as client:
            transport = CaptureTransport(client, credential.strip(), output,
                source_id=source_id, vintage=vintage, budget=budget)
            value = preflight(source_id=source_id, vintage=vintage, transport=transport,
                              plan=plan, excluded=excluded)
        if len(transport.records) != MAX_REQUESTS:
            raise ValueError("pilot response inventory incomplete")
        validate_evidence(value, source_id=source_id, vintage=vintage, plan=plan, excluded=excluded)
        if governed_config_hashes(root) != config:
            raise ValueError("governed configuration drift during capture")
        transport.reject_credential(json.dumps(value).encode())
        write_json(output / "provider-evidence.json", {str(vintage): value}, budget)
        summary.update(validated=True, evidence_file="provider-evidence.json",
            evidence_sha256=sha256_file(output / "provider-evidence.json"),
            snapshot_sha256=value["snapshot_sha256"],
            accounting=dict(Counter(a["classification"] for a in value["accounting"])),
            omission_receipt_ids=[r["evidence_id"] for r in value["omission_evidence"]])
    except Exception:
        # No exception details: provider/session errors can contain key-bearing URLs.
        summary.update(failure="provider_evidence_inaccessible_or_invalid",
                       last_request=transport.last_request if transport else None)
    finally:
        records = transport.records if transport else []
        write_json(output / "response-manifest.json", {"source_id": source_id, "vintage": vintage,
            "responses": records, "raw_hash_semantics": "HTTP content bytes after content decoding",
            "decoded_hash_semantics": "existing canonical semantic JSON hash, including null for empty 204"}, budget, audit=True)
        summary["captured_responses"] = len(records)
        summary["response_manifest_sha256"] = sha256_file(output / "response-manifest.json")
        if summary["validated"]:
            summary["capture_id"] = capture_identity(summary)
        write_json(output / "summary.json", summary, budget, audit=True)
    return summary


def capture_batch(output: Path, *, source_id: str, vintages: list[int], batch_size: int,
                  credential: str, session_factory=requests.Session, root=Path(".")) -> dict:
    years = scope(source_id, vintages, batch_size)
    output.mkdir(parents=True, exist_ok=False)
    config = governed_config_hashes(root)
    code = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    budget, entries, evidence = CaptureBudget(len(years)), [], {}
    for year in years:
        directory = output / str(year)
        # Failures are explicit per year; the next year is independent. No retries
        # or partial cache reuse can turn a failed response into approved absence.
        summary = capture_year(directory, source_id=source_id, vintage=year,
            credential=credential, session_factory=session_factory, root=root, budget=budget,
            expected_config=config, code_commit=code)
        entries.append(summary)
        if summary["validated"]:
            evidence.update(json.loads((directory / "provider-evidence.json").read_text()))
    result = {"schema_version": "acs_provider_capture_batch_v1", "source_id": source_id,
        "vintages": years, "config_hashes": config, "code_commit": code,
        "hosted_run": {key: os.environ.get(key) for key in
            ("GITHUB_REPOSITORY", "GITHUB_RUN_ID", "GITHUB_RUN_ATTEMPT", "GITHUB_WORKFLOW_REF")},
        "validated": all(e["validated"] for e in entries), "entries": entries,
        "failed_vintages": [e["vintage"] for e in entries if not e["validated"]],
        "successful_vintages": [e["vintage"] for e in entries if e["validated"]],
        "production_writes": False, "http_requests": budget.requests}
    # Individual successes are durable in the artifact even if aggregation hits
    # its output limit; batch failure remains visible and must not be ignored.
    try:
        if evidence:
            write_json(output / "provider-evidence.json", evidence, budget)
    except ValueError:
        result.update(validated=False, aggregation_failure="batch_output_limit")
    result["batch_id"] = "acs_provider_capture_batch_v1__" + sha256_json({
        "source_id": source_id, "vintages": years, "config_hashes": config,
        "validated": result["validated"], "aggregation_failure": result.get("aggregation_failure"),
        "entries": [{k: e.get(k) for k in ("vintage", "validated", "capture_id", "response_manifest_sha256")} for e in entries]})
    write_json(output / "batch-summary.json", result, budget, audit=True)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-id", choices=sorted(PRODUCTS), required=True)
    parser.add_argument("--vintages", required=True)
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"\d{4}(,\d{4}){0,4}", args.vintages):
            raise ValueError("exact bounded year list required")
        summary = capture_batch(args.output, source_id=args.source_id,
            vintages=[int(y) for y in args.vintages.split(",")], batch_size=args.batch_size,
            credential=os.environ.get("CENSUS_API_KEY", ""))
    except Exception:
        print("ACS evidence batch failed before completion; details redacted")
        return 1
    print(json.dumps({k: summary[k] for k in
        ("source_id", "vintages", "validated", "successful_vintages", "failed_vintages", "production_writes")}))
    return 0 if summary["validated"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
