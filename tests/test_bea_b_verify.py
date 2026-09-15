from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bea_b_verify.py"
SPEC = importlib.util.spec_from_file_location("bea_b_verify", MODULE_PATH)
assert SPEC and SPEC.loader
verify = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify)


def test_credential_prefers_modern_and_never_mentions_value(monkeypatch):
    monkeypatch.setenv("BEA_API_KEY", "modern-secret")
    monkeypatch.setenv("BEA_API_USER_ID", "legacy-secret")
    assert verify.credential() == ("BEA_API_KEY", "modern-secret")
    assert verify.sanitized({"UserID": "modern-secret", "Year": "ALL"}) == {"Year": "ALL"}


def test_missing_credential_fails_closed(monkeypatch):
    monkeypatch.delenv("BEA_API_KEY", raising=False)
    monkeypatch.delenv("BEA_API_USER_ID", raising=False)
    with pytest.raises(verify.CredentialUnavailable, match="BEA_API_KEY is required"):
        verify.credential()


def test_governed_manifest_has_exact_bea_contract_universe():
    geos = verify.governed_geographies()
    assert sum(g["include_bea_qgdp"] == "1" for level in geos.values() for g in level) == 6
    assert sum(g["include_bea_agdp"] == "1" for level in geos.values() for g in level) == 169
    assert len([g for g in geos["county"] if g["include_bea_agdp"] == "1"]) == 163


def test_hashes_are_order_independent_and_mutation_sensitive():
    rows = [
        {"GeoFips": "24000", "TimePeriod": "2024Q1", "DataValue": "1,000", "LineCode": "1"},
        {"GeoFips": "00000", "TimePeriod": "2024Q1", "DataValue": "2,000", "LineCode": "1"},
    ]
    assert verify.digest(verify.normalized(rows)) == verify.digest(verify.normalized(list(reversed(rows))))
    proof = verify.mutation_proof(rows)
    assert proof["baseline"] != proof["synthetic_mutation"]


def test_request_plan_is_exact_and_contains_no_credential():
    plan = verify.request_plan("SQGDP9", ["00000", "24000"])
    assert plan == {
        "method": "GetData", "DataSetName": "Regional", "TableName": "SQGDP9",
        "LineCode": "1", "Year": "ALL", "GeoFips": "00000,24000",
    }
    assert not verify.SECRET_FIELDS.intersection(plan)
