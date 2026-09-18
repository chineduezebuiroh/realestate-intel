import json
from decimal import Decimal

import pytest

from scripts.nrc_b_verify import (
    COMPLETIONS, GEOGRAPHIES, METRICS, ProviderContractError, STARTS,
    canonical_hash, census_response_diagnostic, compare, month_end,
    main, parse_census_json, parse_census_response, parse_fred_csv, parse_number,
    validate_rows,
)


def row(geo="us_nation", metric=STARTS, period="2026-01", value="1500"):
    return {"geo_id": geo, "metric_id": metric, "date": month_end(period),
            "property_type_id": "all", "property_type": "all", "value": value,
            "provider": "fixture", "native_id": "x"}


def test_numeric_missing_and_no_rescaling():
    assert parse_number("1,500") == Decimal("1500")
    assert parse_number(".") is None
    assert parse_number("N/A") is None
    assert row(value=str(parse_number("1500")))["value"] == "1500"
    with pytest.raises(ValueError, match="nonnumeric"):
        parse_number("secret")


@pytest.mark.parametrize(("source", "expected"), [
    ("2024-02", "2024-02-29"), ("2025-02-01", "2025-02-28"),
    ("January 2026", "2026-01-31")])
def test_month_end(source, expected):
    assert month_end(source) == expected


def test_fred_schema_and_missing():
    rows = parse_fred_csv("DATE,HOUST\n2026-01-01,1500\n2026-02-01,.\n", "HOUST")
    assert rows == [row(value="1500", period="2026-01") | {"provider": "fred", "native_id": "HOUST"}]
    with pytest.raises(ValueError, match="schema"):
        parse_fred_csv("date,value\n2026-01-01,1\n", "HOUST")


def test_census_parser_geography_mapping_and_schema():
    header = ["cell_value", "time", "category_code", "seasonally_adj", "region_code"]
    payload = [header]
    for code in ("0", "1", "2", "3", "4"):
        payload += [["1500", "2026-01", "STARTS", "yes", code],
                    ["1400", "2026-01", "COMPLETIONS", "yes", code]]
    rows = parse_census_json(__import__("json").dumps(payload))
    assert len(rows) == 10
    assert {(x["geo_id"], x["metric_id"]) for x in rows} == {
        (geo, metric) for geo in GEOGRAPHIES.values() for metric in METRICS}
    with pytest.raises(ValueError, match="schema"):
        parse_census_json('[["cell_value"],["1"]]')


def test_census_response_rejects_non_json_with_transport_diagnostic():
    body = b"<html><title>invalid query</title>" + b"x" * 500
    metadata = {"status": 200, "content_type": "text/html",
                "requested_url": "https://example.test/request",
                "final_url": "https://example.test/error"}
    diagnostic = census_response_diagnostic(body, metadata)
    assert diagnostic["response_byte_length"] == len(body)
    assert len(diagnostic["raw_sha256"]) == 64
    assert len(diagnostic["body_prefix"].encode()) <= 256
    with pytest.raises(ProviderContractError, match=r"non-JSON.*status=200.*text/html.*invalid query"):
        parse_census_response(body, metadata)


def test_census_response_rejects_wrong_content_type_even_for_json_body():
    with pytest.raises(ProviderContractError, match="non-JSON"):
        parse_census_response(b'[["cell_value"]]',
                              {"status": 200, "content_type": "text/plain"})


def test_main_persists_and_hashes_raw_census_response_before_parse(tmp_path, monkeypatch):
    body = b"upstream error page"
    monkeypatch.setattr("scripts.nrc_b_verify._get", lambda url: (
        body, {"requested_url": url, "final_url": url, "status": 200,
               "content_type": "text/plain"}))
    workspace = tmp_path / "evidence"
    rc = main(["--legacy-db", "data/market_serving.duckdb", "--workspace",
               str(workspace), "--skip-fred"])
    assert rc == 2
    assert (workspace / "raw/census_resconst.json").read_bytes() == body
    report = json.loads((workspace / "nrc_b_verification.json").read_text())
    transport = report["providers"]["census"]["transport"]
    assert transport["response_byte_length"] == len(body)
    assert len(transport["raw_sha256"]) == 64


def test_census_selectors_are_exact_not_substring_aliases():
    header = ["cell_value", "time", "category_code", "seasonally_adj", "region_code"]
    with pytest.raises(ValueError, match="category code"):
        parse_census_json(__import__("json").dumps(
            [header, ["1", "2026-01", "HOUSING_STARTS", "yes", "0"]]))
    with pytest.raises(ValueError, match="region code"):
        parse_census_json(__import__("json").dumps(
            [header, ["1", "2026-01", "STARTS", "yes", "US"]]))


def test_hashes_deterministic_and_inventory_excludes_value():
    a = [row(value="1500"), row(metric=COMPLETIONS, value="1400")]
    b = list(reversed(a))
    assert canonical_hash(a) == canonical_hash(b)
    changed = [dict(a[0], value="1501"), a[1]]
    assert canonical_hash(a) != canonical_hash(changed)
    assert canonical_hash(a, True) == canonical_hash(changed, True)


def test_duplicate_and_unexpected_identity_fail_closed():
    with pytest.raises(ValueError, match="duplicate"):
        validate_rows([row(), row()])
    with pytest.raises(ValueError, match="applicability"):
        validate_rows([row(geo="invented")])


def test_exact_only_and_difference_classification():
    left = [row(value="1500"), row(metric=COMPLETIONS, value="1400")]
    right = [row(value="1500"), row(metric=COMPLETIONS, value="1399")]
    result = compare(left, right, "CENSUS_ONLY", "FRED_ONLY", "VALUE_DIFFERENCE")
    assert result["counts"] == {"EXACT_MATCH": 1, "VALUE_DIFFERENCE": 1}
    assert result["maximum_absolute_difference"] == "1"
