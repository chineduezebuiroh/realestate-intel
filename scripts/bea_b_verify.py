#!/usr/bin/env python3
"""Bounded, read-only BEA-B evidence collector.

Only first-party BEA read methods and read-only legacy DuckDB queries are used.
Sanitized diagnostics are written below artifacts/bea_verification/bea_b; this
module cannot publish candidates/pins or mutate production state.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "https://apps.bea.gov/api/data"
OUT_ROOT = ROOT / "artifacts" / "bea_verification" / "bea_b"
MANIFEST = ROOT / "config" / "geo_manifest.generated.csv"
LEGACY_DB = ROOT / "data" / "market_serving.duckdb"
PARSER_CONTRACT_VERSION = "bea-b-observation-parser-v1"
TABLES = {
    "bea_gdp_qtr": {"table": "SQGDP9", "flag": "include_bea_qgdp", "frequency": "quarterly", "metric_id": "bea_qgdp_real_total_chained2017_saar", "legacy_max": "2026Q1"},
    "bea_gdp_ann": {"table": "CAGDP9", "flag": "include_bea_agdp", "frequency": "annual", "metric_id": "bea_agdp_real_total_chained2017", "legacy_max": "2024"},
}
SECRET_FIELDS = frozenset({"UserID", "BEA_API_KEY", "BEA_API_USER_ID"})
ANNOTATION_FIELDS = ("NoteRef", "Note", "Footnote", "FootnoteRef", "Code", "Status")
NUMERIC = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$")
COMMA_NUMERIC = re.compile(r"^[+-]?(?:\d{1,3}(?:,\d{3})+)(?:\.\d*)?$")


class CredentialUnavailable(RuntimeError):
    """Credential absence without credential contents."""


def credential() -> tuple[str, str]:
    for name in ("BEA_API_KEY", "BEA_API_USER_ID"):
        value = os.environ.get(name, "").strip()
        if value:
            return name, value
    raise CredentialUnavailable("BEA_API_KEY is required for live BEA-B verification")


def sanitize(value: Any) -> Any:
    """Recursively remove secrets and credential-like URL/query fragments."""
    if isinstance(value, dict):
        return {k: sanitize(v) for k, v in value.items() if k not in SECRET_FIELDS}
    if isinstance(value, list):
        return [sanitize(v) for v in value]
    if isinstance(value, str):
        return re.sub(r"(?i)(UserID|BEA_API_KEY|BEA_API_USER_ID)=[^&\s]+", r"\1=<REDACTED>", value)
    return value


def digest(value: bytes | Any) -> str:
    payload = value if isinstance(value, bytes) else json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(payload).hexdigest()


def governed_geographies() -> list[dict[str, str]]:
    with MANIFEST.open(newline="", encoding="utf-8") as handle:
        return [{
            "geo_id": row["geo_slug"], "geo_name": row["geo_name"],
            "geo_class": row["level"], "provider_geo_fips": row["bea_geo_fips"].strip(),
            "include_bea_qgdp": row["include_bea_qgdp"], "include_bea_agdp": row["include_bea_agdp"],
        } for row in csv.DictReader(handle) if row["bea_geo_fips"].strip()]


def bea_get(key: str, params: dict[str, str]) -> tuple[bytes, dict[str, Any]]:
    full = {"UserID": key, "ResultFormat": "JSON", **params}
    request = Request(ENDPOINT, data=urlencode(full).encode(), method="POST")
    with urlopen(request, timeout=180) as response:  # noqa: S310 - fixed HTTPS endpoint
        raw = response.read()
    parsed = json.loads(raw)
    api = parsed.get("BEAAPI", {})
    if "Error" in api:
        error = api["Error"]
        raise RuntimeError(f"BEA API error code {error.get('APIErrorCode', 'unknown')}")
    return raw, parsed


def results(response: dict[str, Any]) -> dict[str, Any]:
    result = response.get("BEAAPI", {}).get("Results", {})
    return result[0] if isinstance(result, list) and result else result


def data_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    rows = results(response).get("Data", [])
    return rows if isinstance(rows, list) else []


def clean_code(value: Any) -> str:
    return str(value or "").strip()


def row_key(row: dict[str, Any]) -> tuple[str, str]:
    return clean_code(row.get("GeoFips")), clean_code(row.get("TimePeriod"))


def normalized(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ("GeoFips", "TimePeriod", "DataValue", "CL_UNIT", "UNIT_MULT", "LineCode")
    return sorted(({f: clean_code(row.get(f)) for f in fields} for row in rows), key=lambda r: (r["GeoFips"], r["TimePeriod"], r["LineCode"]))


def inventory(rows: Iterable[dict[str, Any]]) -> list[list[str]]:
    return [list(key) for key in sorted({row_key(row) for row in rows})]


def mutation_proof(rows: list[dict[str, Any]]) -> dict[str, Any]:
    baseline = normalized(rows)
    changed = json.loads(json.dumps(baseline))
    if changed:
        changed[0]["DataValue"] = "SYNTHETIC_MUTATION_FOR_HASH_PROOF_ONLY"
    return {"evidence_kind": "synthetic_local_identity_test", "baseline": digest(baseline), "synthetic_mutation": digest(changed), "distinguishes_change": digest(baseline) != digest(changed)}


def expected_periods(first: str, last: str, frequency: str) -> list[str]:
    if not first or not last:
        return []
    if frequency == "annual":
        return [str(y) for y in range(int(first), int(last) + 1)]
    fy, fq = int(first[:4]), int(first[-1]); ly, lq = int(last[:4]), int(last[-1])
    return [f"{y}Q{q}" for y in range(fy, ly + 1) for q in range(1, 5) if (y, q) >= (fy, fq) and (y, q) <= (ly, lq)]


def period_inventory(rows: list[dict[str, Any]], mapping: dict[str, dict[str, str]], frequency: str) -> dict[str, Any]:
    by_geo: dict[str, set[str]] = defaultdict(set)
    by_period: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        geo, period = row_key(row)
        by_geo[geo].add(period); by_period[period].add(geo)
    geography = []
    for code in sorted(by_geo):
        periods = sorted(by_geo[code])
        geography.append({"provider_geo_fips": code, "canonical_geo_id": mapping.get(code, {}).get("geo_id"), "observation_count": len(periods), "first_period": periods[0], "last_period": periods[-1], "missing_periods": sorted(set(expected_periods(periods[0], periods[-1], frequency)) - set(periods))})
    memberships = [{"period": p, "geography_count": len(by_period[p]), "provider_geo_fips": sorted(by_period[p])} for p in sorted(by_period)]
    signatures = {tuple(x["provider_geo_fips"]) for x in memberships}
    return {"frequency": frequency, "earliest_period": min(by_period) if by_period else None, "latest_period": max(by_period) if by_period else None, "distinct_period_count": len(by_period), "geographies": geography, "geography_counts_by_period": [{"period": x["period"], "geography_count": x["geography_count"]} for x in memberships], "periods_with_changing_geography_membership": memberships if len(signatures) > 1 else [], "all_returned_geographies_continuous": all(not x["missing_periods"] for x in geography)}


def geography_inventory(rows: list[dict[str, Any]], requested: list[dict[str, str]]) -> dict[str, Any]:
    request_map = {g["provider_geo_fips"]: g for g in requested}
    returned_codes = sorted({row_key(r)[0] for r in rows})
    counts = Counter(request_map[c]["geo_class"] if c in request_map else "other/unmapped" for c in returned_codes)
    for label in ("nation", "state", "county", "cbsa_metro", "metropolitan_division", "micropolitan", "other/unmapped"):
        counts.setdefault(label, 0)
    exact = [{**request_map[c], "canonical_geo_id": request_map[c]["geo_id"], "provider_returned": True} if c in request_map else {"provider_geo_fips": c, "canonical_geo_id": None, "geo_class": "other/unmapped", "provider_returned": True} for c in returned_codes]
    return {"requested_geography_count": len(request_map), "returned_distinct_provider_geofips_count": len(returned_codes), "returned_distinct_canonical_geo_id_count": sum(c in request_map for c in returned_codes), "returned_counts_by_canonical_geography_class": dict(sorted(counts.items())), "returned_geography_inventory": exact, "requested_but_not_returned": [request_map[c] for c in sorted(set(request_map)-set(returned_codes))], "returned_but_not_requested": [x for x in exact if x.get("canonical_geo_id") is None], "provider_to_canonical_mapping_failures": [x for x in exact if x.get("canonical_geo_id") is None]}


def sentinel_inventory(rows: list[dict[str, Any]], source_id: str, table: str) -> dict[str, Any]:
    categories = Counter(); tokens: dict[str, dict[str, Any]] = {}
    annotations = Counter()
    for row in rows:
        raw = clean_code(row.get("DataValue"))
        if not raw: category = "blank"
        elif COMMA_NUMERIC.fullmatch(raw): category = "comma_formatted_numeric"
        elif NUMERIC.fullmatch(raw): category = "numeric"
        else: category = "nonnumeric"
        categories[category] += 1
        if category in {"blank", "nonnumeric"}:
            item = tokens.setdefault(raw, {"token": raw, "count": 0, "examples": []})
            item["count"] += 1
            if len(item["examples"]) < 10: item["examples"].append({"GeoFips": row_key(row)[0], "TimePeriod": row_key(row)[1]})
        for field in ANNOTATION_FIELDS:
            value = row.get(field)
            if value not in (None, "", []): annotations[f"{field}:{json.dumps(value, sort_keys=True)}"] += 1
    return {"source_id": source_id, "table": table, "row_count": len(rows), "value_category_counts": {k: categories.get(k, 0) for k in ("numeric", "comma_formatted_numeric", "blank", "nonnumeric")}, "observed_nonnumeric_or_blank_tokens": sorted(tokens.values(), key=lambda x: x["token"]), "observed_annotation_fields": dict(sorted(annotations.items())), "sentinel_meanings": "not inferred; unknown nonnumeric tokens must fail closed"}


def getdata_contract_evidence(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Inventory contract-bearing fields exactly as returned by GetData."""
    fields = ("TableName", "LineCode", "LineDescription", "CL_UNIT", "UNIT_MULT", "Unit")
    return {
        "evidence_scope": "actual_getdata_response",
        "distinct_field_values": {
            field: sorted({clean_code(row.get(field)) for row in rows if clean_code(row.get(field))})
            for field in fields
        },
        "time_period_formats": dict(sorted(Counter(
            "quarterly_YYYYQn" if re.fullmatch(r"\d{4}Q[1-4]", row_key(row)[1])
            else "annual_YYYY" if re.fullmatch(r"\d{4}", row_key(row)[1])
            else "other"
            for row in rows
        ).items())),
    }


def period_to_date(period: str, frequency: str) -> str:
    if frequency == "annual": return f"{period}-12-31"
    year, q = int(period[:4]), int(period[-1]); return f"{year}-{q*3:02d}-{31 if q in (1,4) else 30}"


def parse_number(value: Any) -> float | None:
    text = clean_code(value)
    if NUMERIC.fullmatch(text) or COMMA_NUMERIC.fullmatch(text): return float(text.replace(",", ""))
    return None


def legacy_rows(source_id: str) -> list[dict[str, Any]]:
    import duckdb
    conn = duckdb.connect(str(LEGACY_DB), read_only=True)
    try:
        rows = conn.execute("SELECT geo_id, CAST(date AS VARCHAR), value FROM fact_timeseries WHERE source_id=? ORDER BY 1,2", [source_id]).fetchall()
    finally: conn.close()
    return [{"geo_id": g, "date": d, "value": v} for g, d, v in rows]


def parity_diagnostics(rows: list[dict[str, Any]], requested: list[dict[str, str]], source_id: str, frequency: str, legacy: list[dict[str, Any]]) -> dict[str, Any]:
    mapping = {g["provider_geo_fips"]: g for g in requested}
    provider: dict[tuple[str, str], float] = {}
    conflicts = []
    for row in rows:
        code, period = row_key(row); value = parse_number(row.get("DataValue"))
        if code not in mapping or value is None: continue
        key = (mapping[code]["geo_id"], period_to_date(period, frequency))
        if key in provider and provider[key] != value: conflicts.append({"geo_id": key[0], "date": key[1]})
        provider[key] = value
    old = {(r["geo_id"], r["date"]): float(r["value"]) for r in legacy}
    records=[]
    for key in sorted(set(provider)|set(old)):
        pv, lv = provider.get(key), old.get(key)
        if key in set((x["geo_id"], x["date"]) for x in conflicts): cls="IDENTITY_CONFLICT"
        elif pv is None: cls="LEGACY_ONLY"
        elif lv is None: cls="PROVIDER_ONLY"
        elif pv == lv: cls="EXACT_MATCH"
        else: cls="PROVIDER_REVISION"
        rec={"geo_id":key[0],"date":key[1],"classification":cls}
        if cls=="PROVIDER_REVISION": rec.update(provider_value=pv,legacy_value=lv,absolute_difference=pv-lv,relative_difference=(pv-lv)/abs(lv) if lv else None)
        if cls=="PROVIDER_ONLY": rec["reason"]="NEWER_PROVIDER_PERIOD" if key[1] > max((r["date"] for r in legacy),default="") else ("NEWLY_COVERED_GOVERNED_GEOGRAPHY" if not any(r["geo_id"]==key[0] for r in legacy) else "OTHER_PROVIDER_ONLY")
        if cls=="LEGACY_ONLY": rec["reason"]="PROVIDER_COVERAGE_OR_HISTORY_LOSS"
        records.append(rec)
    counts=Counter(r["classification"] for r in records); revised=[r for r in records if r["classification"]=="PROVIDER_REVISION"]
    return {"source_id":source_id,"counts":{f"{x.lower()}_count":counts[x] for x in ("EXACT_MATCH","PROVIDER_REVISION","LEGACY_ONLY","PROVIDER_ONLY","IDENTITY_CONFLICT")},"revision_summary":{"earliest_revised_period":min((r["date"] for r in revised),default=None),"latest_revised_period":max((r["date"] for r in revised),default=None),"counts_by_period":dict(sorted(Counter(r["date"] for r in revised).items())),"counts_by_geography_class":dict(sorted(Counter(next((g["geo_class"] for g in requested if g["geo_id"]==r["geo_id"]),"other/unmapped") for r in revised).items())),"representative_differences":revised[:20]},"records":records}


def county_reconciliation(rows: list[dict[str, Any]], requested: list[dict[str, str]], legacy: list[dict[str, Any]]) -> dict[str, Any]:
    returned: dict[str,list[dict[str, Any]]]=defaultdict(list)
    for row in rows: returned[row_key(row)[0]].append(row)
    old=Counter(r["geo_id"] for r in legacy); records=[]
    for g in sorted((x for x in requested if x["geo_class"]=="county"),key=lambda x:x["geo_id"]):
        provider_rows=returned[g["provider_geo_fips"]]
        periods=sorted({row_key(row)[1] for row in provider_rows})
        numeric_count=sum(parse_number(row.get("DataValue")) is not None for row in provider_rows)
        was_requested=True
        classification="AVAILABLE_DIRECT" if numeric_count else ("PROVIDER_SENTINEL" if periods else "PROVIDER_UNAVAILABLE")
        records.append({"canonical_geo_id":g["geo_id"],"canonical_label":g["geo_name"],"provider_geo_fips":g["provider_geo_fips"],"requested":was_requested,"provider_returned":bool(periods),"observation_count":len(periods),"first_returned_period":periods[0] if periods else None,"last_returned_period":periods[-1] if periods else None,"legacy_present":old[g["geo_id"]]>0,"legacy_observation_count":old[g["geo_id"]],"final_classification":classification})
    absent=[r for r in records if not r["legacy_present"]]
    return {"county_count":len(records),"records":records,"legacy_absent_counties":absent,"legacy_absent_live_outcomes":Counter(r["final_classification"] for r in absent),"legacy_present_now_provider_absent":[r for r in records if r["legacy_present"] and not r["provider_returned"]],"provider_returned_governed_county_absent_from_legacy":[r for r in records if r["provider_returned"] and not r["legacy_present"]]}


def compare_responses(reference: list[dict[str, Any]], candidate: list[dict[str, Any]]) -> dict[str, Any]:
    ref={row_key(r): normalized([r])[0] for r in reference}; cand={row_key(r):normalized([r])[0] for r in candidate}
    shared=set(ref)&set(cand)
    return {"reference_key_count":len(ref),"candidate_key_count":len(cand),"missing_keys":[list(x) for x in sorted(set(ref)-set(cand))],"extra_keys":[list(x) for x in sorted(set(cand)-set(ref))],"changed_values":[{"key":list(x),"reference":ref[x],"candidate":cand[x]} for x in sorted(shared) if ref[x]!=cand[x]],"equivalent":ref==cand}


def request_plan(table: str, geos: list[str], year: str="ALL") -> dict[str,str]:
    return {"method":"GetData","DataSetName":"Regional","TableName":table,"LineCode":"1","Year":year,"GeoFips":",".join(geos)}


def explicit_years_for_periods(periods: list[str]) -> list[str]:
    """Regional GetData accepts calendar years, including for quarterly tables."""
    return sorted({period[:4] for period in periods})


def metadata_plan(table: str) -> list[tuple[str,dict[str,str]]]:
    return [("generic_dataset_list",{"method":"GetDatasetList"}),("generic_parameter_list",{"method":"GetParameterList","DataSetName":"Regional"}),("generic_table_values",{"method":"GetParameterValues","DataSetName":"Regional","ParameterName":"TableName"}),("table_line_values",{"method":"GetParameterValuesFiltered","DataSetName":"Regional","TargetParameter":"LineCode","TableName":table}),("table_year_values",{"method":"GetParameterValuesFiltered","DataSetName":"Regional","TargetParameter":"Year","TableName":table,"LineCode":"1"}),("table_geography_values",{"method":"GetParameterValuesFiltered","DataSetName":"Regional","TargetParameter":"GeoFips","TableName":table,"LineCode":"1"})]


def metadata_evidence(key: str, table: str) -> dict[str,Any]:
    out={"dataset":"Regional","table":table,"layers":{}}
    for label,params in metadata_plan(table):
        raw,response=bea_get(key,params)
        out["layers"][label]={"evidence_scope":"generic_regional" if label.startswith("generic") else "table_specific","sanitized_request":sanitize(params),"raw_response_sha256":digest(raw),"sanitized_results":sanitize(results(response))}
    return out


def batch_validation(key: str, table: str, full_rows: list[dict[str,Any]], requested: list[dict[str,str]]) -> dict[str,Any]:
    codes=[g["provider_geo_fips"] for g in requested]; sample=sorted({codes[0],codes[-1]})
    baseline=[r for r in full_rows if row_key(r)[0] in sample]; periods=sorted({row_key(r)[1] for r in baseline}); selected=sorted({periods[0],periods[-1]}) if periods else []
    _,combined=bea_get(key,request_plan(table,sample)); combined_rows=data_rows(combined)
    separate=[]
    for code in sample:
        _,response=bea_get(key,request_plan(table,[code])); separate.extend(data_rows(response))
    explicit=[]; explicit_years=explicit_years_for_periods(selected)
    if explicit_years:
        _,response=bea_get(key,request_plan(table,sample,",".join(explicit_years)))
        # A quarterly explicit-year request returns every quarter in each year;
        # compare only the selected boundary periods to like-for-like reference keys.
        explicit=[r for r in data_rows(response) if row_key(r)[1] in selected]
    baseline_explicit=[r for r in baseline if row_key(r)[1] in selected]
    geo_cmp=compare_responses(baseline,separate); combined_cmp=compare_responses(baseline,combined_rows); year_cmp=compare_responses(baseline_explicit,explicit)
    safe=geo_cmp["equivalent"] and combined_cmp["equivalent"] and year_cmp["equivalent"]
    return {"deterministic_sample":{"provider_geo_fips":sample,"selected_boundary_periods":selected,"explicit_year_parameter_values":explicit_years},"full_vs_combined_sample":combined_cmp,"full_vs_separate_geography_requests":geo_cmp,"year_all_vs_explicit_year_boundary_periods":year_cmp,"conclusion":"ONE_REQUEST_PER_PHYSICAL_SOURCE_SUPPORTED_BY_BOUNDED_SAMPLES" if safe else "DETERMINISTIC_BATCHING_REQUIRED_OR_EVIDENCE_INSUFFICIENT"}


def pin_diagnostic(source_id: str, config: dict[str,str], request: dict[str,str], requested: list[dict[str,str]], geo:dict[str,Any], repeat:list[dict[str,Any]], metadata:dict[str,Any], sentinels:dict[str,Any], retrieved_at:str) -> dict[str,Any]:
    stable_raw=len({x["raw_response_sha256"] for x in repeat})==1
    return {"semantic_identity":{"schema_version":"bea-b-pin-diagnostic-v1","source_id":source_id,"dataset":"Regional","table":config["table"],"line_code":"1","frequency":config["frequency"],"sanitized_request_plan":sanitize(request),"requested_governed_membership":sorted({g["provider_geo_fips"] for g in requested}),"provider_returned_membership":sorted(x["provider_geo_fips"] for x in geo["returned_geography_inventory"]),"provider_metadata_hash":digest(metadata),"normalized_governed_content_sha256":repeat[0]["normalized_governed_content_sha256"],"canonical_key_inventory_sha256":repeat[0]["canonical_key_inventory_sha256"],"parser_contract_version":PARSER_CONTRACT_VERSION,"observed_sentinel_contract":sentinels["observed_nonnumeric_or_blank_tokens"]},"immutable_lineage_nonsemantic":{"retrieved_at_utc":retrieved_at,"transport":"HTTPS POST / read-only BEA API methods","raw_response_sha256":repeat[0]["raw_response_sha256"],"raw_hash_placement_reason":"lineage because raw envelopes can contain formatting or mutable metadata; repeat byte stability is diagnostic only","repeat_raw_byte_stable":stable_raw},"credential_participates":False}


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__); parser.add_argument("--output-dir",type=Path,default=OUT_ROOT); args=parser.parse_args()
    try: credential_name,key=credential()
    except CredentialUnavailable as exc: parser.exit(2,f"verification not run: {exc}\n")
    geos=governed_geographies(); args.output_dir.mkdir(parents=True,exist_ok=True); retrieved=datetime.now(timezone.utc).isoformat()
    summary={"evidence_kind":"live_first_party_provider_verification","credential_variable":credential_name,"endpoint":ENDPOINT,"retrieved_at_utc":retrieved,"sources":[]}
    metadata_art={}; geography_art={}; periods_art={}; parity_art={}
    for source_id,config in TABLES.items():
        requested=[g for g in geos if g[config["flag"]]=="1"]; params=request_plan(config["table"],[g["provider_geo_fips"] for g in requested]); repeats=[]; responses=[]
        for _ in range(2):
            raw,response=bea_get(key,params); rows=data_rows(response); responses.append(rows); repeats.append({"raw_response_sha256":digest(raw),"normalized_governed_content_sha256":digest(normalized(rows)),"canonical_key_inventory_sha256":digest(inventory(rows)),"row_count":len(rows),"sanitized_result_metadata":sanitize({k:v for k,v in results(response).items() if k!="Data"})})
        rows=responses[0]; mapping={g["provider_geo_fips"]:g for g in requested}; geo=geography_inventory(rows,requested); periods=period_inventory(rows,mapping,config["frequency"]); sentinels=sentinel_inventory(rows,source_id,config["table"]); legacy=legacy_rows(source_id); parity=parity_diagnostics(rows,requested,source_id,config["frequency"],legacy); meta=metadata_evidence(key,config["table"]); batching=batch_validation(key,config["table"],rows,requested); counties=county_reconciliation(rows,requested,legacy) if config["frequency"]=="annual" else None; pin=pin_diagnostic(source_id,config,params,requested,geo,repeats,meta,sentinels,retrieved)
        entry={"source_id":source_id,"metric_id":config["metric_id"],"sanitized_request":sanitize(params),"repeat_diagnostics":repeats,"repeat_raw_stable":len({x["raw_response_sha256"] for x in repeats})==1,"repeat_content_stable":len({x["normalized_governed_content_sha256"] for x in repeats})==1,"repeat_inventory_stable":len({x["canonical_key_inventory_sha256"] for x in repeats})==1,"actual_getdata_contract_evidence":getdata_contract_evidence(rows),"actual_geography_inventory":geo,"period_inventory":periods,"sentinel_inventory":sentinels,"batching_validation":batching,"legacy_parity_summary":{**parity["counts"],"revision_summary":parity["revision_summary"]},"current_provider_period":{"latest_live_period":periods["latest_period"],"legacy_max_period":config["legacy_max"],"provider_extends_legacy_max":bool(periods["latest_period"] and periods["latest_period"]>config["legacy_max"])},"county_reconciliation":counties,"provider_release_identity_conclusion":"NO_IMMUTABLE_RELEASE_IDENTITY_ASSUMED; content-addressed evidence required","same_period_revision_identity_proof":mutation_proof(rows),"pin_contract_diagnostic":pin}
        summary["sources"].append(entry); metadata_art[source_id]=meta; geography_art[source_id]={"actual_geography_inventory":geo,"county_reconciliation":counties}; periods_art[source_id]={"period_inventory":periods,"batching_validation":batching}; parity_art[source_id]=parity
    for name,payload in (("provider_metadata.json",metadata_art),("geography_reconciliation.json",geography_art),("period_inventory.json",periods_art),("legacy_parity.json",parity_art),("verification_summary.json",summary)):
        (args.output_dir/name).write_text(json.dumps(sanitize(payload),indent=2,sort_keys=True)+"\n",encoding="utf-8")
    print(args.output_dir.relative_to(ROOT)/"verification_summary.json"); return 0


if __name__=="__main__": raise SystemExit(main())
