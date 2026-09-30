from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "scripts" / "bea_b_verify.py"
SPEC = importlib.util.spec_from_file_location("bea_b_verify", MODULE_PATH)
assert SPEC and SPEC.loader
verify = importlib.util.module_from_spec(SPEC); SPEC.loader.exec_module(verify)


def geo(code, geo_id, level="county", name=None):
    return {"provider_geo_fips":code,"geo_id":geo_id,"geo_name":name or geo_id,"geo_class":level,"include_bea_qgdp":"1","include_bea_agdp":"1"}


def row(code, period, value="1,000", **extra):
    return {"GeoFips":code,"TimePeriod":period,"DataValue":value,"LineCode":"1","CL_UNIT":"Millions of chained dollars",**extra}


def test_credential_prefers_modern_and_redacts_recursively(monkeypatch):
    monkeypatch.setenv("BEA_API_KEY","modern-secret"); monkeypatch.setenv("BEA_API_USER_ID","legacy-secret")
    assert verify.credential()==("BEA_API_KEY","modern-secret")
    cleaned=verify.sanitize({"UserID":"modern-secret","nested":[{"url":"x?UserID=modern-secret&Year=ALL"}]})
    assert "modern-secret" not in str(cleaned) and "UserID" not in cleaned


def test_missing_credential_fails_closed(monkeypatch):
    monkeypatch.delenv("BEA_API_KEY",raising=False); monkeypatch.delenv("BEA_API_USER_ID",raising=False)
    with pytest.raises(verify.CredentialUnavailable,match="BEA_API_KEY is required"): verify.credential()


def test_governed_manifest_has_exact_contract_universe():
    geos=verify.governed_geographies()
    assert sum(g["include_bea_qgdp"]=="1" for g in geos)==6
    assert sum(g["include_bea_agdp"]=="1" for g in geos)==169
    assert sum(g["geo_class"]=="county" and g["include_bea_agdp"]=="1" for g in geos)==163


def test_returned_geography_inventory_is_derived_from_response():
    requested=[geo("00000","nation","nation"),geo("24000","md","state"),geo("24001","county")]
    got=verify.geography_inventory([row("00000","2024"),row("99999","2024")],requested)
    assert got["requested_geography_count"]==3
    assert got["returned_distinct_provider_geofips_count"]==2
    assert got["returned_distinct_canonical_geo_id_count"]==1
    assert {x["provider_geo_fips"] for x in got["requested_but_not_returned"]}=={"24000","24001"}
    assert got["returned_but_not_requested"][0]["provider_geo_fips"]=="99999"


def test_county_reconciliation_exact_163_and_requested_missing_classification():
    requested=[geo(f"{i:05d}",f"county_{i}") for i in range(163)]
    got=verify.county_reconciliation([row("00000","2024")],requested,[])
    assert got["county_count"]==163 and len(got["records"])==163
    assert got["records"][0]["final_classification"]=="AVAILABLE_DIRECT"
    assert got["records"][1]["final_classification"]=="PROVIDER_UNAVAILABLE"


def test_county_sentinel_is_not_available_direct():
    got=verify.county_reconciliation([row("24001","2024","(NA)")],[geo("24001","county")],[])
    assert got["records"][0]["final_classification"]=="PROVIDER_SENTINEL"


def test_variable_geography_histories_and_annual_continuity():
    rows=[row("1","2020"),row("1","2021"),row("1","2022"),row("2","2021"),row("2","2023")]
    got=verify.period_inventory(rows,{"1":geo("1","a"),"2":geo("2","b")},"annual")
    by={x["provider_geo_fips"]:x for x in got["geographies"]}
    assert by["1"]["observation_count"]==3 and not by["1"]["missing_periods"]
    assert by["2"]["missing_periods"]==["2022"]
    assert got["periods_with_changing_geography_membership"]


def test_quarterly_continuity_crosses_year_boundary():
    got=verify.period_inventory([row("1","2023Q4"),row("1","2024Q1"),row("1","2024Q3")],{"1":geo("1","a")},"quarterly")
    assert got["geographies"][0]["missing_periods"]==["2024Q2"]
    assert got["earliest_period"]=="2023Q4" and got["latest_period"]=="2024Q3"


def test_parity_classifies_exact_revision_legacy_and_provider_only():
    requested=[geo("1","a")]
    live=[row("1","2023","10"),row("1","2024","12"),row("1","2025","15")]
    legacy=[{"geo_id":"a","date":"2023-12-31","value":10},{"geo_id":"a","date":"2024-12-31","value":11},{"geo_id":"a","date":"2022-12-31","value":9}]
    got=verify.parity_diagnostics(live,requested,"s","annual",legacy)
    assert got["counts"]=={"exact_match_count":1,"provider_revision_count":1,"legacy_only_count":1,"provider_only_count":1,"identity_conflict_count":0}
    assert got["revision_summary"]["earliest_revised_period"]=="2024-12-31"
    provider_only=next(x for x in got["records"] if x["classification"]=="PROVIDER_ONLY")
    assert provider_only["reason"]=="NEWER_PROVIDER_PERIOD"


def test_identity_conflict_detected_for_duplicate_provider_key():
    got=verify.parity_diagnostics([row("1","2024","1"),row("1","2024","2")],[geo("1","a")],"s","annual",[])
    assert got["counts"]["identity_conflict_count"]==1


def test_sentinel_and_annotation_inventory_before_normalization():
    got=verify.sentinel_inventory([row("1","2024","1,234"),row("1","2023",""),row("1","2022","(D)",NoteRef="1")],"s","T")
    assert got["value_category_counts"]=={"numeric":0,"comma_formatted_numeric":1,"blank":1,"nonnumeric":1}
    assert {x["token"] for x in got["observed_nonnumeric_or_blank_tokens"]}=={"","(D)"}
    assert got["observed_annotation_fields"]


def test_actual_getdata_contract_evidence_is_not_inferred_from_request():
    got=verify.getdata_contract_evidence([
        row("1","2024Q1",LineDescription="Real GDP",UNIT_MULT="6"),
        row("1","2024Q2",LineDescription="Real GDP",UNIT_MULT="6"),
    ])
    assert got["evidence_scope"]=="actual_getdata_response"
    assert got["distinct_field_values"]["LineDescription"]==["Real GDP"]
    assert got["distinct_field_values"]["UNIT_MULT"]==["6"]
    assert got["time_period_formats"]=={"quarterly_YYYYQn":2}


def test_year_all_and_batch_equivalence_comparison():
    rows=[row("1","2023"),row("1","2024")]
    assert verify.compare_responses(rows,list(reversed(rows)))["equivalent"]
    changed=[row("1","2023"),row("1","2024","2")]
    cmp=verify.compare_responses(rows,changed)
    assert not cmp["equivalent"] and len(cmp["changed_values"])==1


def test_quarterly_batch_validation_requests_years_and_filters_to_boundary_quarters(monkeypatch):
    full=[
        row("00000","2005Q1","1"),row("00000","2005Q2","2"),row("00000","2026Q1","3"),
        row("51000","2005Q1","4"),row("51000","2005Q2","5"),row("51000","2026Q1","6"),
    ]
    seen=[]
    def fake_get(_key, params):
        seen.append(params)
        codes=params["GeoFips"].split(",")
        rows=[r for r in full if r["GeoFips"] in codes]
        if params["Year"]!="ALL":
            years=set(params["Year"].split(","))
            rows=[r for r in rows if r["TimePeriod"][:4] in years]
        return b"{}", {"BEAAPI":{"Results":{"Data":rows}}}
    monkeypatch.setattr(verify,"bea_get",fake_get)
    got=verify.batch_validation("secret","SQGDP9",full,[geo("00000","nation","nation"),geo("51000","va","state")])
    assert got["deterministic_sample"]["explicit_year_parameter_values"]==["2005","2026"]
    assert got["year_all_vs_explicit_year_boundary_periods"]["reference_key_count"]==4
    assert got["year_all_vs_explicit_year_boundary_periods"]["candidate_key_count"]==4
    assert got["conclusion"]=="ONE_REQUEST_PER_PHYSICAL_SOURCE_SUPPORTED_BY_BOUNDED_SAMPLES"
    assert any(call["Year"]=="2005,2026" for call in seen)


def test_metadata_plans_distinguish_generic_and_table_specific():
    plans=verify.metadata_plan("CAGDP9")
    assert plans[0][0].startswith("generic")
    assert all(params.get("TableName")=="CAGDP9" for label,params in plans if label.startswith("table_"))
    assert all(not verify.SECRET_FIELDS.intersection(params) for _,params in plans)


def test_request_plan_exact_and_credential_free():
    plan=verify.request_plan("SQGDP9",["00000","24000"])
    assert plan=={"method":"GetData","DataSetName":"Regional","TableName":"SQGDP9","LineCode":"1","Year":"ALL","GeoFips":"00000,24000"}
    assert not verify.SECRET_FIELDS.intersection(plan)


def test_pin_semantic_and_lineage_are_separated_and_secret_free():
    config={"table":"SQGDP9","frequency":"quarterly"}; requested=[geo("00000","nation","nation")]
    repeat=[{"raw_response_sha256":"raw","normalized_governed_content_sha256":"content","canonical_key_inventory_sha256":"keys"}]
    metadata={"table":"SQGDP9"}; sentinels={"observed_nonnumeric_or_blank_tokens":[]}
    geo_inv=verify.geography_inventory([row("00000","2024Q1")],requested)
    pin=verify.pin_diagnostic("s",config,verify.request_plan("SQGDP9",["00000"]),requested,geo_inv,repeat,metadata,sentinels,"now")
    assert "retrieved_at_utc" not in pin["semantic_identity"]
    assert pin["immutable_lineage_nonsemantic"]["retrieved_at_utc"]=="now"
    assert pin["credential_participates"] is False and "UserID" not in json_string(pin)


def json_string(value):
    import json
    return json.dumps(value,sort_keys=True)


def test_hashes_are_deterministic_order_independent_and_mutation_sensitive():
    rows=[row("2","2024Q1","1,000"),row("1","2024Q1","2,000")]
    assert verify.digest(verify.normalized(rows))==verify.digest(verify.normalized(list(reversed(rows))))
    proof=verify.mutation_proof(rows)
    assert proof["distinguishes_change"] and proof["baseline"]!=proof["synthetic_mutation"]


def test_period_to_date_semantics():
    assert verify.period_to_date("2024Q1","quarterly")=="2024-03-31"
    assert verify.period_to_date("2024Q3","quarterly")=="2024-09-30"
    assert verify.period_to_date("2024","annual")=="2024-12-31"
