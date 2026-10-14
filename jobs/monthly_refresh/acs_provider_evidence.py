"""Read-only, fixed ACS5 2023 hosted evidence pilot; no durable source writes."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import Counter
from pathlib import Path
from urllib.parse import quote, quote_plus

import requests

from core.source_artifacts.hashing import sha256_file, sha256_json
from jobs.monthly_refresh.acs_monthly import API, _safe_url
from sources.census_acs.artifact import geography_plan, governed_config_hashes
from sources.census_acs.historical import checked_response, preflight, validate_evidence

SOURCE = "census_acs5"
VINTAGE = 2023
ENDPOINT = f"{API}/{VINTAGE}/acs/acs5"
MAX_REQUESTS = 225  # discovery, variables, all 223 configured members
MAX_SECONDS = 900


def write_json(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


class CaptureTransport:
    """Bounded HTTP transport only; historical.py interprets provider responses.

    Never persist response URLs/headers or transport exceptions. Raw response
    bytes are retained only after checking for an echoed transport credential.
    """
    def __init__(self, session, credential: str, output: Path):
        self.session, self.credential, self.output = session, credential, output
        self.started = time.monotonic()
        self.records: list[dict] = []
        self.last_request = None

    def reject_credential(self, body: bytes) -> None:
        if any(value.encode() in body for value in {self.credential,
               json.dumps(self.credential)[1:-1], quote(self.credential, safe=""), quote_plus(self.credential)}):
            raise ValueError("credential echo rejected")

    def get(self, url, *, params=None, timeout):
        if url not in {API + ".json", ENDPOINT + "/variables.json", ENDPOINT}:
            raise ValueError("pilot endpoint outside fixed scope")
        if len(self.records) >= MAX_REQUESTS or time.monotonic() - self.started >= MAX_SECONDS:
            raise ValueError("pilot request/time budget exhausted")
        safe = _safe_url(VINTAGE, "acs/acs5", params) if params is not None else url
        self.last_request = safe
        # The existing historical preflight supplies credential-free parameters.
        # Match the existing ACS acquisition's key-only-in-transport convention.
        transport_params = {**params, "key": self.credential} if params is not None else None
        limit = 32 * 1024 * 1024 if params is None else 1024 * 1024
        with self.session.get(url, params=transport_params, timeout=min(timeout, 60),
                              allow_redirects=False, stream=True) as response:
            parts, size = [], 0
            for chunk in response.iter_content(chunk_size=65536):
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
            (self.output / name).write_bytes(body)
            self.records.append({"request_url": safe, "http_status": response.status_code,
                "body_path": name, "body_size_bytes": len(body),
                "body_sha256": hashlib.sha256(body).hexdigest(),
                "decoded_response_sha256": None})
            payload = checked_response(captured, allow_no_content=params is not None)
            self.records[-1]["decoded_response_sha256"] = sha256_json(payload)
            return captured


def run_pilot(output: Path, *, credential: str, session=None, root: Path = Path(".")) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    (output / "responses").mkdir()
    transport = None
    summary = {"schema_version": "acs_provider_evidence_pilot_summary_v1",
        "source_id": SOURCE, "vintage": VINTAGE, "validated": False,
        "production_writes": False, "credential_persisted": False}
    try:
        if not credential.strip():
            raise ValueError("provider credential unavailable")
        plan, excluded = geography_plan(root / "config/geo_manifest.generated.csv")
        if len(plan) != 223 or len(excluded) != 10:
            raise ValueError("fixed pilot governed membership changed")
        summary.update(requested_members=len(plan), excluded_members=len(excluded),
                       config_hashes=governed_config_hashes(root))
        with requests.Session() if session is None else session as client:
            transport = CaptureTransport(client, credential.strip(), output)
            value = preflight(source_id=SOURCE, vintage=VINTAGE, transport=transport,
                              plan=plan, excluded=excluded)
        if len(transport.records) != MAX_REQUESTS:
            raise ValueError("pilot response inventory incomplete")
        validate_evidence(value, source_id=SOURCE, vintage=VINTAGE, plan=plan, excluded=excluded)
        transport.reject_credential(json.dumps(value).encode())
        write_json(output / "provider-evidence.json", {str(VINTAGE): value})
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
        write_json(output / "response-manifest.json", {"source_id": SOURCE, "vintage": VINTAGE,
            "responses": records, "raw_hash_semantics": "HTTP content bytes after content decoding",
            "decoded_hash_semantics": "existing canonical semantic JSON hash, including null for empty 204"})
        summary["captured_responses"] = len(records)
        summary["response_manifest_sha256"] = sha256_file(output / "response-manifest.json")
        write_json(output / "summary.json", summary)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        summary = run_pilot(args.output, credential=os.environ.get("CENSUS_API_KEY", ""))
    except Exception:
        print("ACS5 2023 evidence pilot failed before capture; details redacted")
        return 1
    print(json.dumps({k: summary[k] for k in
        ("source_id", "vintage", "validated", "captured_responses", "production_writes")}))
    return 0 if summary["validated"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
