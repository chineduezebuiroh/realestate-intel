from pathlib import Path

import pytest

from scripts.acs_b_verify import VerificationError, _external_output, _number, _request


class Response:
    def __init__(self, status=200, content=b"x", value=None):
        self.status_code, self.content, self._value = status, content, value
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


def test_request_keeps_key_transport_only_and_accepts_one_row():
    session = Session(Response(value=[["NAME", "x"], ["US", "1"]]))
    status, value, body = _request(session, "https://example.test", {"get": "NAME,x"}, "secret")
    assert status == "available" and value[1] == ["US", "1"] and body
    assert session.params["key"] == "secret"


def test_request_diagnoses_empty_and_zero_row_payloads():
    with pytest.raises(VerificationError, match="empty body"):
        _request(Session(Response(content=b"", value=None)), "x", {}, "")
    assert _request(Session(Response(value=[["NAME"]])), "x", {}, "")[0] == "valid_zero_rows"
    assert _request(Session(Response(status=204, content=b"", value=None)), "x", {}, "")[0] == "provider_ineligible_no_content"


def test_missing_sentinel_and_unknown_values_fail_closed():
    assert _number("-666666666", "x") == (None, "sentinel_-666666666")
    assert _number(None, "x") == (None, "missing_null")
    with pytest.raises(VerificationError, match="unknown non-numeric"):
        _number("not-data", "x")


def test_output_must_be_outside_repository(tmp_path):
    repo = tmp_path / "repo"; repo.mkdir()
    with pytest.raises(VerificationError, match="outside"):
        _external_output(repo / "evidence", repo)
