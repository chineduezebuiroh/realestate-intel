"""Smoke 188: justified C1b raw capture boundary fails closed before parsing."""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path

from jobs.monthly_refresh.laus_annual_publication import (
    PublicationParserContractUnavailable, PublicationTransportError, archive_capture,
    fetch_publication, normalized_evidence_from_capture, verify_archived_capture,
)


class Response:
    def __init__(self, status, body=b"", headers=None):
        self.status_code, self.content, self.headers = status, body, headers or {}


class Session:
    def __init__(self, responses): self.responses, self.calls = list(responses), []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs)); return self.responses.pop(0)


def expect(error, operation):
    try: operation()
    except error: return
    raise AssertionError(f"expected {error.__name__}")


def main():
    url = "https://www.bls.gov/lau/notices/"
    body = b"<html><title>fixture transport only</title></html>"
    session = Session([Response(302, headers={"Location": "/lau/notices/2026.htm"}),
                       Response(200, body, {"Content-Type": "text/html; charset=utf-8"})])
    captured, receipt = fetch_publication(url, session=session, retrieved_at="2026-08-31T00:00:00Z")
    assert captured == body and receipt["final_url"].endswith("/2026.htm")
    assert receipt["response_sha256"] == hashlib.sha256(body).hexdigest()
    assert session.calls[0][1]["allow_redirects"] is False
    expect(ValueError, lambda: fetch_publication("https://example.com/x", session=Session([])))
    expect(ValueError, lambda: fetch_publication(url, session=Session([
        Response(302, headers={"Location": "https://example.com/x"})])))
    expect(PublicationTransportError, lambda: fetch_publication(url, session=Session([Response(404)])))
    retry = Session([Response(503), Response(200, body)])
    assert fetch_publication(url, session=retry, sleep=lambda _: None)[0] == body
    assert hashlib.sha256(body + b"changed").hexdigest() != receipt["response_sha256"]
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        archived = archive_capture(root, body, receipt)
        again = archive_capture(root, body, {**receipt, "retrieved_at": "2026-09-01T00:00:00Z"})
        assert archived == again
        verified = verify_archived_capture(Path(archived["raw_path"]), Path(archived["receipt_path"]))
        assert verified["archive_verified"]
        bad = json.loads(Path(archived["receipt_path"]).read_text()); bad["response_sha256"] = "0" * 64
        bad_path = root / "bad.json"; bad_path.write_text(json.dumps(bad))
        expect(ValueError, lambda: verify_archived_capture(Path(archived["raw_path"]), bad_path))
    # Synthetic transport bytes are explicitly not authoritative parser fixtures.
    expect(PublicationParserContractUnavailable,
           lambda: normalized_evidence_from_capture(body, receipt))
    print("Smoke 188 passed: BLS bytes archive deterministically; unreviewed parsing fails closed.")


if __name__ == "__main__": main()
