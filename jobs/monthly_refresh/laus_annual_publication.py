"""Governed raw-byte capture for official BLS LAUS publications.

This C1b stop-boundary deliberately does not parse annual completion.  No
reviewed official provider bytes were available in this environment, so a parser
could not be justified.  Captures produced here are the required input to that
review rather than authority for READY_FOR_ANNUAL_DEEP.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urljoin, urlparse

import requests

from core.source_artifacts.hashing import write_canonical_json

CAPTURE_SCHEMA = "laus_annual_publication_capture_v1"
SUMMARY_SCHEMA = "laus_annual_publication_collection_summary_v1"
ALLOWED_HOSTS = frozenset({"bls.gov", "www.bls.gov"})
USER_AGENT = "realestate-intel-laus-annual-publication/1.0"
TRANSIENT_STATUS = frozenset({408, 429, *range(500, 600)})
REDIRECT_STATUS = frozenset({301, 302, 303, 307, 308})


class PublicationTransportError(RuntimeError):
    """Provider transport failed before authoritative bytes were captured."""


class PublicationParserContractUnavailable(RuntimeError):
    """No reviewed provider structure exists from which completion can be parsed."""


def validate_provider_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname not in ALLOWED_HOSTS:
        raise ValueError("LAUS annual publication URL is not authoritative BLS HTTPS")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("LAUS annual publication URL contains forbidden components")
    return url


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def fetch_publication(
    source_url: str, *, session: Any | None = None, timeout: float = 30,
    max_attempts: int = 3, max_redirects: int = 5,
    sleep: Callable[[float], None] = time.sleep,
    retrieved_at: str | None = None,
) -> tuple[bytes, dict[str, Any]]:
    """Fetch exact bytes with bounded retry and per-hop redirect validation."""
    validate_provider_url(source_url)
    if timeout <= 0 or max_attempts < 1 or max_redirects < 0:
        raise ValueError("invalid LAUS publication transport bounds")
    client = session or requests.Session()
    current = source_url; redirects: list[dict[str, Any]] = []
    for redirect_index in range(max_redirects + 1):
        response = None
        for attempt in range(max_attempts):
            try:
                response = client.get(current, headers={"User-Agent": USER_AGENT},
                                      timeout=timeout, allow_redirects=False)
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempt + 1 == max_attempts:
                    raise PublicationTransportError("BLS publication transport retries exhausted") from exc
                sleep(0.25 * (attempt + 1)); continue
            if response.status_code in TRANSIENT_STATUS:
                if attempt + 1 == max_attempts:
                    raise PublicationTransportError(
                        f"BLS publication transient HTTP {response.status_code} retries exhausted")
                sleep(0.25 * (attempt + 1)); continue
            break
        if response is None:
            raise AssertionError("unreachable")
        if response.status_code in REDIRECT_STATUS:
            location = response.headers.get("Location")
            if not location:
                raise PublicationTransportError("BLS redirect omitted Location")
            if redirect_index == max_redirects:
                raise PublicationTransportError("BLS publication redirect limit exceeded")
            target = urljoin(current, location)
            validate_provider_url(target)
            redirects.append({"from_url": current, "http_status": response.status_code,
                              "to_url": target})
            current = target; continue
        if response.status_code != 200:
            raise PublicationTransportError(f"terminal BLS publication HTTP {response.status_code}")
        validate_provider_url(current)
        body = bytes(response.content)
        digest = hashlib.sha256(body).hexdigest()
        receipt = {"schema_version": CAPTURE_SCHEMA, "source_id": "laus",
                   "source_url": source_url, "final_url": current, "http_status": 200,
                   "content_type": str(response.headers.get("Content-Type", "")),
                   "response_sha256": digest, "response_size_bytes": len(body),
                   "redirects": redirects, "retrieved_at": retrieved_at or _utc_now(),
                   "user_agent": USER_AGENT}
        return body, receipt
    raise AssertionError("unreachable")


def archive_capture(root: Path, body: bytes, receipt: dict[str, Any]) -> dict[str, str]:
    """Persist immutable raw bytes before any future semantic parsing."""
    digest = hashlib.sha256(body).hexdigest()
    if receipt.get("schema_version") != CAPTURE_SCHEMA or receipt.get("response_sha256") != digest:
        raise ValueError("LAUS publication receipt/raw-byte hash contradiction")
    raw_path = root / "raw" / f"{digest}.bin"
    receipt_semantic = {key: value for key, value in receipt.items() if key != "retrieved_at"}
    receipt_id = hashlib.sha256(json.dumps(receipt_semantic, sort_keys=True,
                                           separators=(",", ":")).encode()).hexdigest()
    receipt_path = root / "receipts" / f"{digest}__{receipt_id[:16]}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    if raw_path.exists() and raw_path.read_bytes() != body:
        raise RuntimeError("LAUS publication raw evidence hash collision")
    raw_path.write_bytes(body)
    if receipt_path.exists():
        existing = json.loads(receipt_path.read_text())
        # Retrieval time is receipt metadata; repeat retrievals retain separate
        # caller-owned summaries rather than overwriting the first raw receipt.
        old_comparable = {key: value for key, value in existing.items() if key != "retrieved_at"}
        if receipt_semantic != old_comparable:
            raise RuntimeError("LAUS publication immutable receipt conflict")
    else:
        write_canonical_json(receipt_path, receipt)
    return {"raw_path": str(raw_path), "receipt_path": str(receipt_path),
            "response_sha256": digest, "receipt_id": receipt_id}


def verify_archived_capture(raw_path: Path, receipt_path: Path) -> dict[str, Any]:
    body = raw_path.read_bytes(); receipt = json.loads(receipt_path.read_text())
    digest = hashlib.sha256(body).hexdigest()
    if receipt.get("schema_version") != CAPTURE_SCHEMA or receipt.get("response_sha256") != digest:
        raise ValueError("archived LAUS publication verification failed")
    validate_provider_url(receipt["source_url"]); validate_provider_url(receipt["final_url"])
    return {"receipt": receipt, "response_sha256": digest,
            "response_size_bytes": len(body), "archive_verified": True}


def normalized_evidence_from_capture(*_: Any, **__: Any) -> dict[str, Any]:
    """Fail closed until reviewed official bytes establish a parser contract."""
    raise PublicationParserContractUnavailable(
        "no reviewed official BLS publication bytes establish class-specific completion")


def main() -> int:
    parser = argparse.ArgumentParser(); sub = parser.add_subparsers(dest="command", required=True)
    collect = sub.add_parser("collect")
    collect.add_argument("--url", required=True); collect.add_argument("--output-root", type=Path, required=True)
    collect.add_argument("--output", type=Path, required=True); collect.add_argument("--timeout", type=float, default=30)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("--raw", type=Path, required=True); inspect.add_argument("--receipt", type=Path, required=True)
    inspect.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "collect":
        body, receipt = fetch_publication(args.url, timeout=args.timeout)
        archived = archive_capture(args.output_root, body, receipt)
        value = {"schema_version": SUMMARY_SCHEMA, "source_id": "laus", **archived,
                 "parser_contract_status": "blocked_unreviewed_provider_structure",
                 "normalized_evidence": None, "detector_evaluated": False,
                 "annual_deep_authorized": False}
    else:
        verified = verify_archived_capture(args.raw, args.receipt)
        value = {"schema_version": SUMMARY_SCHEMA, "source_id": "laus", **verified,
                 "parser_contract_status": "blocked_unreviewed_provider_structure",
                 "normalized_evidence": None, "detector_evaluated": False,
                 "annual_deep_authorized": False}
    write_canonical_json(args.output, value); print(json.dumps(value, sort_keys=True)); return 0


if __name__ == "__main__":
    raise SystemExit(main())
