#!/usr/bin/env python3
"""Read-only NRC-B provider and legacy verification.

This is deliberately not a production adapter.  Network bytes and the report are
written only below ``--workspace``; DuckDB is always opened read-only.
"""
from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import io
import json
import urllib.request
import urllib.error
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable
from sources.census_nrc.parser import (CENSUS_INPUTS, COMPLETIONS, GEOGRAPHIES, KEY_FIELDS,
    METRICS, SOURCE_ID, STARTS, ProviderContractError, _row, canonical_hash,
    month_end, parse_census_workbook, parse_number, validate_rows)
from zipfile import BadZipFile

import duckdb
import openpyxl
from openpyxl.utils.exceptions import InvalidFileException

LEGACY_SOURCE_ID = "census_nrc_fred"
LEGACY_R1_GEOGRAPHIES = {"us_nation":"united_states__nation",
    "us_region_northeast":"northeast_region__region",
    "us_region_midwest":"midwest_region__region", "us_region_south":"south_region__region",
    "us_region_west":"west_region__region"}
FRED_SERIES = {
    "HOUST": ("US", STARTS), "HOUSTNE": ("NE", STARTS),
    "HOUSTMW": ("MW", STARTS), "HOUSTS": ("S", STARTS),
    "HOUSTW": ("W", STARTS), "COMPUTSA": ("US", COMPLETIONS),
    "COMPUNETSA": ("NE", COMPLETIONS), "COMPUMWTSA": ("MW", COMPLETIONS),
    "COMPUSTSA": ("S", COMPLETIONS), "COMPUWTSA": ("W", COMPLETIONS),
}
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
MISSING = {"", ".", "...", "-", "--", "NA", "N/A", "(NA)", "NULL", "null", "(X)", "S", "Z"}
BODY_PREFIX_BYTES = 256


def parse_fred_csv(text: str, series: str) -> list[dict[str, Any]]:
    if series not in FRED_SERIES:
        raise ValueError(f"unexpected FRED series: {series}")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames not in (["DATE", series], ["observation_date", series]):
        raise ValueError(f"unexpected FRED schema for {series}: {reader.fieldnames}")
    date_field = reader.fieldnames[0]
    geo, metric = FRED_SERIES[series]
    return [row for item in reader
            if (row := _row(geo, metric, item[date_field],
                            None if str(item[series]).strip() in MISSING else item[series],
                            "fred", series))]


def census_response_diagnostic(body: bytes, metadata: dict[str, Any]) -> dict[str, Any]:
    return {**metadata, "response_byte_length": len(body),
            "raw_sha256": hashlib.sha256(body).hexdigest(),
            "body_prefix": body[:BODY_PREFIX_BYTES].decode("utf-8", errors="replace")}


def parse_census_response(payload: bytes, metadata: dict[str, Any], kind: str
                          ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    evidence = census_response_diagnostic(payload, metadata)
    content_type = str(metadata.get("content_type") or "").lower()
    if not (("spreadsheetml" in content_type or "application/octet-stream" in content_type)
            and payload.startswith(b"PK")):
        raise ProviderContractError("Census acquisition returned non-XLSX response: "
            f"status={metadata.get('status')} content_type={metadata.get('content_type')!r} "
            f"body_prefix={evidence['body_prefix']!r}")
    return parse_census_workbook(payload, kind)


def compare(left: Iterable[dict[str, Any]], right: Iterable[dict[str, Any]],
            left_only: str, right_only: str, unequal: str) -> dict[str, Any]:
    def keyed(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, ...], Decimal]:
        return {tuple(str(r[k]) for k in KEY_FIELDS): Decimal(str(r["value"]))
                for r in validate_rows(rows)}
    a, b = keyed(left), keyed(right)
    details, differences = [], []
    for key in sorted(a.keys() | b.keys()):
        if key not in b:
            classification = left_only
        elif key not in a:
            classification = right_only
        elif a[key] == b[key]:
            classification = "EXACT_MATCH"
        else:
            classification = unequal
            differences.append(abs(a[key] - b[key]))
        details.append({**dict(zip(KEY_FIELDS, key)), "classification": classification,
                        "left_value": str(a[key]) if key in a else None,
                        "right_value": str(b[key]) if key in b else None})
    counts = dict(sorted(Counter(x["classification"] for x in details).items()))
    series = defaultdict(Counter)
    for item in details:
        series[f'{item["metric_id"]}|{item["geo_id"]}'][item["classification"]] += 1
    differing = [d["date"] for d in details if d["classification"] == unequal]
    return {"total_keys": len(details), "counts": counts,
            "exact_match_count": counts.get("EXACT_MATCH", 0),
            f"{left_only.lower()}_count": counts.get(left_only, 0),
            f"{right_only.lower()}_count": counts.get(right_only, 0),
            f"{unequal.lower()}_count": counts.get(unequal, 0),
            "differing_or_revised_count": counts.get(unequal, 0),
            "maximum_absolute_difference": str(max(differences)) if differences else None,
            "first_differing_period": min(differing) if differing else None,
            "last_differing_period": max(differing) if differing else None,
            "series_counts": {k: dict(sorted(v.items())) for k, v in sorted(series.items())},
            "details": details}


def legacy_inventory(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    con = duckdb.connect(str(path), read_only=True)
    columns = {r[0] for r in con.execute("DESCRIBE fact_timeseries").fetchall()}
    required = {"geo_id", "metric_id", "date", "value", "source_id",
                "property_type_id", "property_type"}
    if not required.issubset(columns):
        con.close(); raise ValueError(f"legacy schema missing {sorted(required-columns)}")
    raw = con.execute("""SELECT geo_id,metric_id,date,property_type_id,property_type,
                         value,source_id FROM fact_timeseries WHERE source_id=?
                         ORDER BY geo_id,metric_id,date,property_type_id""",
                      [LEGACY_SOURCE_ID]).fetchall()
    con.close()
    rows, nulls = [], 0
    for geo, metric, period, ptid, pt, value, source in raw:
        if value is None:
            nulls += 1; continue
        if geo not in LEGACY_R1_GEOGRAPHIES or metric not in METRICS:
            raise ValueError(f"unexpected legacy identity: {geo}/{metric}")
        rows.append({"geo_id": LEGACY_R1_GEOGRAPHIES[geo], "metric_id": metric, "date": month_end(period),
                     "property_type_id": str(ptid), "property_type": str(pt),
                     "value": str(Decimal(str(value))), "provider": "legacy",
                     "native_id": str(source)})
    normalized = validate_rows(rows)
    by_pair = defaultdict(list)
    for row in normalized: by_pair[f'{row["metric_id"]}|{row["geo_id"]}'].append(row["date"])
    inventory = {"path": str(path), "opened_read_only": True, "row_count": len(raw),
                 "source_ids": sorted({str(x[6]) for x in raw}),
                 "metric_ids": sorted({str(x[1]) for x in raw}),
                 "geography_ids": sorted({str(x[0]) for x in raw}),
                 "property_type_ids": sorted({str(x[3]) for x in raw}),
                 "property_types": sorted({str(x[4]) for x in raw}),
                 "null_value_count": nulls, "duplicate_key_count": len(rows)-len(normalized),
                 "national_row_count": sum(r["geo_id"] == "united_states__nation" for r in normalized),
                 "series": {k: {"count": len(v), "first": min(v), "last": max(v)}
                            for k, v in sorted(by_pair.items())}}
    return normalized, inventory


def geography_reconciliation(path: Path) -> list[dict[str, str]]:
    """Separate required provider shape from current governed applicability."""
    expected = {
        "United States": ("US", "united_states__nation", "us_nation", "nation"),
        "Northeast": ("NE", "northeast_region__region", "us_region_northeast", "region"),
        "Midwest": ("MW", "midwest_region__region", "us_region_midwest", "region"),
        "South": ("S", "south_region__region", "us_region_south", "region"),
        "West": ("W", "west_region__region", "us_region_west", "region"),
    }
    with path.open(newline="", encoding="utf-8-sig") as handle:
        records = {row["geo_slug"]: row for row in csv.DictReader(handle)}
    result = []
    for label, (code, governed, legacy, level) in expected.items():
        record = records.get(governed)
        if record is not None and record.get("level") != level:
            raise ProviderContractError(
                f"geography manifest level mismatch for {label} identity {governed}/{level}")
        if record is None and code != "MW":
            raise ProviderContractError(
                f"geography manifest lacks governed {label} identity {governed}/{level}")
        result.append({"provider_label": label, "provider_code": code,
                       "canonical_geo_slug": governed,
                       "legacy_r1_geo_id": legacy,
                       "manifest_level": level,
                       "manifest_geo_name": record.get("geo_name", "") if record else "",
                       "classification": "GOVERNED" if record else "OUT_OF_GOVERNANCE",
                       "disposition": "INCLUDED_IN_CANONICAL_CANDIDATE" if record
                           else "EXCLUDED_FROM_CANONICAL_CANDIDATE"})
    return result


def _get(url: str) -> tuple[bytes, dict[str, Any]]:
    request = urllib.request.Request(url, headers={"User-Agent": "realestate-intel-nrc-b/0.1"})
    try:
        response = urllib.request.urlopen(request, timeout=60)
    except urllib.error.HTTPError as response:
        # HTTP errors are still provider responses. Preserve their bytes and
        # transport evidence so contract failures are diagnosable offline.
        pass
    with response:
        body = response.read()
        metadata = {"requested_url": url, "final_url": response.geturl(),
                    "status": response.status,
                    "content_type": response.headers.get("Content-Type"),
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified")}
    return body, metadata


def _provider_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates = [r["date"] for r in rows]
    series: dict[str, Any] = {}
    grouped = defaultdict(list)
    for row in rows:
        grouped[f'{row["metric_id"]}|{row["geo_id"]}'].append(row["date"])
    for key, periods in sorted(grouped.items()):
        ordered = sorted(periods)
        expected = []
        cursor = datetime.strptime(ordered[0], "%Y-%m-%d")
        last = datetime.strptime(ordered[-1], "%Y-%m-%d")
        while cursor <= last:
            expected.append(month_end(cursor))
            cursor = datetime(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
        series[key] = {"observation_count": len(ordered), "first_period": ordered[0],
                       "last_period": ordered[-1],
                       "missing_periods_within_bounds": sorted(set(expected)-set(ordered)),
                       "continuous_within_observed_bounds": set(expected) == set(ordered)}
    return {"row_count": len(rows), "metric_ids": sorted({r["metric_id"] for r in rows}),
            "geography_ids": sorted({r["geo_id"] for r in rows}),
            "first_period": min(dates), "latest_period": max(dates),
            "series": series,
            "normalized_content_sha256": canonical_hash(rows),
            "canonical_key_inventory_sha256": canonical_hash(rows, True)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only NRC-B provider verifier")
    parser.add_argument("--legacy-db", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--geo-manifest", type=Path, default=Path("config/geo_manifest.generated.csv"))
    parser.add_argument("--skip-census", action="store_true")
    parser.add_argument("--skip-fred", action="store_true")
    parser.add_argument("--offline", action="store_true",
                        help="parse workspace/raw inputs without network access")
    parser.add_argument("--census-starts-url", default=CENSUS_INPUTS["starts"][0])
    parser.add_argument("--census-completions-url", default=CENSUS_INPUTS["completions"][0])
    args = parser.parse_args(argv)
    workspace, rawdir = args.workspace, args.workspace / "raw"
    rawdir.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {
        "report_schema": "nrc_b_verification_v0_1", "status": "INCOMPLETE",
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "contract": {"physical_source_id_provisional": SOURCE_ID,
                     "unit": "thousands_of_housing_units_saar",
                     "numeric_scale_factor": 1, "metrics": list(METRICS),
                     "geographies": list(GEOGRAPHIES.values()),
                     "date_normalization": "provider observation month -> calendar month-end"},
        "providers": {}, "errors": []}
    try:
        report["geography_reconciliation"] = geography_reconciliation(args.geo_manifest)
    except Exception as exc:
        report["errors"].append({"stage": "geography", "error": f"{type(exc).__name__}: {exc}"})
    census_rows: list[dict[str, Any]] = []
    fred_rows: list[dict[str, Any]] = []
    if not args.skip_census:
        try:
            inputs, workbook_contracts = [], []
            report["providers"]["census"] = {"inputs": inputs,
                                                "workbook_contracts": workbook_contracts}
            urls = {"starts": args.census_starts_url,
                    "completions": args.census_completions_url}
            for kind, url in urls.items():
                target = rawdir / f"census_{kind}.xlsx"
                if args.offline:
                    body, transport = target.read_bytes(), {"requested_url": url,
                        "final_url": url, "status": 200,
                        "content_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "offline": True}
                else:
                    body, transport = _get(url); target.write_bytes(body)
                diagnostic = census_response_diagnostic(body, transport)
                inputs.append({"kind": kind, **diagnostic})
                parsed, contract = parse_census_response(body, transport, kind)
                census_rows.extend(parsed); workbook_contracts.append(contract)
            census_rows = validate_rows(census_rows)
            report["providers"]["census"].update(_provider_summary(census_rows))
        except Exception as exc:  # boundary records provider/fixture failure in report
            report["errors"].append({"stage": "census", "error": f"{type(exc).__name__}: {exc}"})
    if not args.skip_fred:
        fred_transport = []
        try:
            for series in FRED_SERIES:
                target = rawdir / f"fred_{series}.csv"
                if args.offline:
                    body, transport = target.read_bytes(), {"url": FRED_URL.format(series=series), "offline": True}
                else:
                    body, transport = _get(FRED_URL.format(series=series)); target.write_bytes(body)
                fred_rows.extend(parse_fred_csv(body.decode("utf-8-sig"), series))
                fred_transport.append({**transport, "series_id": series,
                                       "raw_sha256": hashlib.sha256(body).hexdigest()})
            fred_rows = validate_rows(fred_rows)
            report["providers"]["fred"] = {"inputs": fred_transport, **_provider_summary(fred_rows)}
        except Exception as exc:
            report["errors"].append({"stage": "fred", "error": f"{type(exc).__name__}: {exc}"})
    try:
        legacy_rows, inventory = legacy_inventory(args.legacy_db)
        report["legacy"] = inventory
    except Exception as exc:
        legacy_rows = []
        report["errors"].append({"stage": "legacy", "error": f"{type(exc).__name__}: {exc}"})
    if census_rows and fred_rows:
        report["census_fred_parity"] = compare(census_rows, fred_rows,
            "CENSUS_ONLY", "FRED_ONLY", "VALUE_DIFFERENCE")
    if census_rows and legacy_rows:
        report["provider_legacy_parity"] = compare(census_rows, legacy_rows,
            "PROVIDER_ONLY", "LEGACY_ONLY", "PROVIDER_REVISION")
        legacy_latest = max(r["date"] for r in legacy_rows)
        reasons = Counter()
        for item in report["provider_legacy_parity"]["details"]:
            if item["classification"] != "PROVIDER_ONLY": continue
            if item["geo_id"] == "united_states__nation": reason = "NATIONAL_ABSENT_FROM_LEGACY"
            elif item["date"] > legacy_latest: reason = "AFTER_LEGACY_SNAPSHOT"
            else: reason = "OTHER_PROVIDER_ONLY"
            item["provider_only_reason"] = reason; reasons[reason] += 1
        report["provider_legacy_parity"]["provider_only_reason_counts"] = dict(sorted(reasons.items()))
    expected_pairs = {(g, m) for g in GEOGRAPHIES.values() for m in METRICS}
    census_pairs = {(r["geo_id"], r["metric_id"]) for r in census_rows}
    report["applicability"] = {"expected_pair_count": 10,
        "observed_census_pair_count": len(census_pairs),
        "missing_census_pairs": sorted("|".join(x) for x in expected_pairs-census_pairs)}
    if not report["errors"] and census_pairs == expected_pairs and fred_rows and legacy_rows:
        report["status"] = "LIVE_VERIFICATION_COMPLETE" if not args.offline else "OFFLINE_FIXTURE_COMPLETE"
    output = workspace / "nrc_b_verification.json"
    output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"NRC-B status: {report['status']}")
    print(f"report: {output}")
    for error in report["errors"]: print(f"ERROR [{error['stage']}]: {error['error']}")
    return 0 if report["status"] != "INCOMPLETE" else 2


if __name__ == "__main__":
    raise SystemExit(main())
