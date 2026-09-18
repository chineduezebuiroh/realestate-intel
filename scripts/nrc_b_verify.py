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
import math
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import duckdb

SOURCE_ID = "census_nrc"
LEGACY_SOURCE_ID = "census_nrc_fred"
STARTS = "census_housing_starts_total_saar"
COMPLETIONS = "census_housing_completions_total_saar"
METRICS = (STARTS, COMPLETIONS)
GEOGRAPHIES = {
    "US": "us_nation",
    "NE": "us_region_northeast",
    "MW": "us_region_midwest",
    "S": "us_region_south",
    "W": "us_region_west",
}
FRED_SERIES = {
    "HOUST": ("US", STARTS), "HOUSTNE": ("NE", STARTS),
    "HOUSTMW": ("MW", STARTS), "HOUSTS": ("S", STARTS),
    "HOUSTW": ("W", STARTS), "COMPUTSA": ("US", COMPLETIONS),
    "COMPUNETSA": ("NE", COMPLETIONS), "COMPUMWTSA": ("MW", COMPLETIONS),
    "COMPUSTSA": ("S", COMPLETIONS), "COMPUWTSA": ("W", COMPLETIONS),
}
# Candidate EITS route. Its variables and selectors are intentionally validated
# against the returned metadata rather than trusted as an undocumented constant.
CENSUS_URL = (
    "https://api.census.gov/data/timeseries/eits/resconst?"
    "get=cell_value,data_type_code,time_slot_id,category_code,seasonally_adj,"
    "region_code&time=from+1959-01"
)
FRED_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}"
MISSING = {"", ".", "NA", "N/A", "NULL", "null", "(X)", "S", "Z"}
KEY_FIELDS = ("geo_id", "metric_id", "date", "property_type_id")


def month_end(value: str | date | datetime) -> str:
    """Return a canonical ISO calendar month-end from any date/month label."""
    if isinstance(value, (date, datetime)):
        year, month = value.year, value.month
    else:
        text = str(value).strip()
        for fmt in ("%Y-%m-%d", "%Y-%m", "%B %Y", "%b %Y"):
            try:
                parsed = datetime.strptime(text, fmt)
                year, month = parsed.year, parsed.month
                break
            except ValueError:
                pass
        else:
            raise ValueError(f"invalid monthly period: {value!r}")
    return f"{year:04d}-{month:02d}-{calendar.monthrange(year, month)[1]:02d}"


def parse_number(value: Any) -> Decimal | None:
    text = "" if value is None else str(value).strip()
    if text in MISSING:
        return None
    try:
        number = Decimal(text.replace(",", ""))
    except InvalidOperation as exc:
        raise ValueError(f"unexpected nonnumeric value: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"non-finite value: {value!r}")
    return number


def _row(geo_code: str, metric: str, period: Any, value: Any, provider: str,
         native_id: str) -> dict[str, Any] | None:
    if geo_code not in GEOGRAPHIES or metric not in METRICS:
        raise ValueError(f"unexpected NRC identity: {geo_code}/{metric}")
    number = parse_number(value)
    if number is None:
        return None
    return {"geo_id": GEOGRAPHIES[geo_code], "metric_id": metric,
            "date": month_end(period), "property_type_id": "all",
            "property_type": "all", "value": str(number), "provider": provider,
            "native_id": native_id}


def validate_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    result = sorted(rows, key=lambda x: tuple(str(x[k]) for k in KEY_FIELDS))
    seen: set[tuple[str, ...]] = set()
    for row in result:
        key = tuple(str(row[k]) for k in KEY_FIELDS)
        if key in seen:
            raise ValueError(f"duplicate normalized key: {key}")
        seen.add(key)
    pairs = {(r["geo_id"], r["metric_id"]) for r in result}
    allowed = {(g, m) for g in GEOGRAPHIES.values() for m in METRICS}
    extra = pairs - allowed
    if extra:
        raise ValueError(f"unexpected applicability pairs: {sorted(extra)}")
    return result


def parse_fred_csv(text: str, series: str) -> list[dict[str, Any]]:
    if series not in FRED_SERIES:
        raise ValueError(f"unexpected FRED series: {series}")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames not in (["DATE", series], ["observation_date", series]):
        raise ValueError(f"unexpected FRED schema for {series}: {reader.fieldnames}")
    date_field = reader.fieldnames[0]
    geo, metric = FRED_SERIES[series]
    rows = []
    for item in reader:
        row = _row(geo, metric, item[date_field], item[series], "fred", series)
        if row:
            rows.append(row)
    return rows


def parse_census_json(payload: bytes | str) -> list[dict[str, Any]]:
    """Parse the EITS response, failing closed on schema or selector drift.

    Census category values are accepted only when they unambiguously contain
    START and COMPLETE. Region aliases reflect the provider's documented labels;
    unknown codes are rejected rather than guessed.
    """
    data = json.loads(payload)
    if not isinstance(data, list) or len(data) < 2 or not isinstance(data[0], list):
        raise ValueError("Census response must be a header plus record arrays")
    required = {"cell_value", "time", "category_code", "seasonally_adj", "region_code"}
    header = [str(x) for x in data[0]]
    if not required.issubset(header):
        raise ValueError(f"unexpected Census schema; missing {sorted(required-set(header))}")
    records = [dict(zip(header, record, strict=True)) for record in data[1:]]
    region_alias = {
        "0": "US", "00": "US", "US": "US", "UNITED STATES": "US",
        "1": "NE", "NE": "NE", "NORTHEAST": "NE",
        "2": "MW", "MW": "MW", "MIDWEST": "MW",
        "3": "S", "S": "S", "SOUTH": "S",
        "4": "W", "W": "W", "WEST": "W",
    }
    rows = []
    for item in records:
        seasonal = str(item["seasonally_adj"]).strip().upper()
        if seasonal not in {"YES", "Y", "SA", "1"}:
            continue
        category = str(item["category_code"]).strip().upper()
        metric = COMPLETIONS if "COMPLET" in category else STARTS if "START" in category else None
        if metric is None:
            continue
        region_raw = str(item["region_code"]).strip().upper()
        if region_raw not in region_alias:
            raise ValueError(f"unexpected Census region code: {region_raw!r}")
        row = _row(region_alias[region_raw], metric, item["time"], item["cell_value"],
                   "census", category)
        if row:
            rows.append(row)
    if not rows:
        raise ValueError("Census response contained no governed SA starts/completions rows")
    return validate_rows(rows)


def canonical_hash(rows: Iterable[dict[str, Any]], inventory_only: bool = False) -> str:
    material = []
    for row in validate_rows(rows):
        fields = [str(row[k]) for k in KEY_FIELDS]
        if not inventory_only:
            fields.append(str(Decimal(str(row["value"]))))
        material.append("\x1f".join(fields))
    return hashlib.sha256(("\n".join(material) + "\n").encode()).hexdigest()


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
        if geo not in GEOGRAPHIES.values() or metric not in METRICS:
            raise ValueError(f"unexpected legacy identity: {geo}/{metric}")
        rows.append({"geo_id": geo, "metric_id": metric, "date": month_end(period),
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
                 "national_row_count": sum(r["geo_id"] == "us_nation" for r in normalized),
                 "series": {k: {"count": len(v), "first": min(v), "last": max(v)}
                            for k, v in sorted(by_pair.items())}}
    return normalized, inventory


def _get(url: str) -> tuple[bytes, dict[str, Any]]:
    request = urllib.request.Request(url, headers={"User-Agent": "realestate-intel-nrc-b/0.1"})
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read()
        metadata = {"url": response.geturl(), "status": response.status,
                    "content_type": response.headers.get("Content-Type"),
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified")}
    return body, metadata


def _provider_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    dates = [r["date"] for r in rows]
    return {"row_count": len(rows), "metric_ids": sorted({r["metric_id"] for r in rows}),
            "geography_ids": sorted({r["geo_id"] for r in rows}),
            "first_period": min(dates), "latest_period": max(dates),
            "normalized_content_sha256": canonical_hash(rows),
            "canonical_key_inventory_sha256": canonical_hash(rows, True)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only NRC-B provider verifier")
    parser.add_argument("--legacy-db", type=Path, required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--skip-census", action="store_true")
    parser.add_argument("--skip-fred", action="store_true")
    parser.add_argument("--offline", action="store_true",
                        help="parse workspace/raw inputs without network access")
    parser.add_argument("--census-url", default=CENSUS_URL)
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
    census_rows: list[dict[str, Any]] = []
    fred_rows: list[dict[str, Any]] = []
    if not args.skip_census:
        try:
            target = rawdir / "census_resconst.json"
            if args.offline:
                body, transport = target.read_bytes(), {"url": args.census_url, "offline": True}
            else:
                body, transport = _get(args.census_url); target.write_bytes(body)
            census_rows = parse_census_json(body)
            report["providers"]["census"] = {"transport": transport,
                "raw_sha256": hashlib.sha256(body).hexdigest(), **_provider_summary(census_rows)}
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
