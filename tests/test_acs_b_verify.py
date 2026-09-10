from pathlib import Path

import pytest

from scripts.acs_b_verify import VerificationError, _external_output, _number, _request


class Response:
    def __init__(self, status=200, content=b"x", value=None, content_type="application/json"):
        self.status_code, self.content, self._value = status, content, value
        self.headers = {"Content-Type": content_type}
    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)
    def json(self):
        if isinstance(self._value, Exception):
            raise self._value
        return self._value


class Session:
    def __init__(self, response): self.response = response; self.params = None
    def get(self, _url, params, timeout): self.params = params; return self.response


CONTEXT = {
    "source_id": "census_acs1",
    "product": "acs/acs1",
    "vintage": 2024,
    "geo_id": "atlantic_city_nj_metro_area__cbsa_metro",
    "geography_level": "cbsa_metro",
    "census_code": "12100",
}


def test_request_keeps_key_transport_only_and_accepts_one_row():
    session = Session(Response(value=[["NAME", "x"], ["US", "1"]]))
    status, value, body = _request(session, "https://example.test", {"get": "NAME,x"}, "secret", CONTEXT)
    assert status == "available" and value[1] == ["US", "1"] and body
    assert session.params["key"] == "secret"


def test_request_diagnoses_empty_and_zero_row_payloads():
    with pytest.raises(VerificationError, match="empty response body"):
        _request(Session(Response(content=b"", value=None)), "x", {}, "", CONTEXT)
    assert _request(Session(Response(value=[["NAME"]])), "x", {}, "", CONTEXT)[0] == "valid_zero_rows"
    assert _request(Session(Response(status=204, content=b"", value=None)), "x", {}, "", CONTEXT)[0] == "provider_ineligible_no_content"


def test_http_200_text_failure_has_context_and_redacts_credential():
    secret = "do-not-disclose"
    response = Response(
        content=f"<html>upstream error token={secret}</html>".encode(),
        value=ValueError("not JSON"),
        content_type="text/html; charset=utf-8",
    )
    with pytest.raises(VerificationError) as caught:
        _request(
            Session(response),
            "https://example.test",
            {"get": "NAME,x", "for": "metropolitan statistical area/micropolitan statistical area:12100"},
            secret,
            CONTEXT,
        )
    message = str(caught.value)
    assert "INVALID_PROVIDER_RESPONSE" in message
    assert '"http_status": 200' in message
    assert '"content_type": "text/html; charset=utf-8"' in message
    assert f'"response_bytes": {len(response.content)}' in message
    assert '"geo_id": "atlantic_city_nj_metro_area__cbsa_metro"' in message
    assert '"census_code": "12100"' in message
    assert '"get": "NAME,x"' in message
    assert '"for": "metropolitan statistical area/micropolitan statistical area:12100"' in message
    assert '"body_preview": "<html>upstream error token=[REDACTED]</html>"' in message
    assert secret not in message
    assert '"key"' not in message


def test_missing_sentinel_and_unknown_values_fail_closed():
    assert _number("-666666666", "x") == (None, "sentinel_-666666666")
    assert _number(None, "x") == (None, "missing_null")
    with pytest.raises(VerificationError, match="unknown non-numeric"):
        _number("not-data", "x")


def test_output_must_be_outside_repository(tmp_path):
    repo = tmp_path / "repo"; repo.mkdir()
    with pytest.raises(VerificationError, match="outside"):
        _external_output(repo / "evidence", repo)
