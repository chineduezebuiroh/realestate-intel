"""Pinned, read-only Census BPS provider verification and equivalence diagnostics.

This module intentionally has no publication, catalog-write, or pointer code.
It can acquire an explicit Census ZIP or analyze an already persisted ZIP.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pandas as pd
import requests

from core.source_artifacts.hashing import sha256_file, write_canonical_json
from sources.census_bps.artifact import MASTER_DIRECTORY, REQUIRED_METRIC, load_registry

SCHEMA_VERSION = "bps_pinned_verification_v1"
TOTAL_FIELD_CANDIDATES = ("total_units", "units_total", "total_units_authorized")
COLUMN_ALIASES = {
    "period": ("period",), "year": ("year", "yr"), "month": ("month", "mo"),
    "location_type": ("location_type", "location"),
    "state_fips": ("state_fips", "state_code", "state"),
    # COUNTY_CODE is only the three-digit component.  The compiled data
    # dictionary identifies FIPS_COUNTY_5_DIGITS as the global county key.
    "county_fips": ("county_fips", "fips_county_5_digits"),
}
KEY = ["geo_id", "metric_id", "date", "property_type_id"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def release_url(release_month: str) -> str:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", release_month):
        raise ValueError("release month must be canonical YYYY-MM")
    return MASTER_DIRECTORY + f"BPS_Compiled_File_{release_month.replace('-', '')}.zip"


def acquire(url: str, output: Path, *, timeout: float = 120) -> dict[str, Any]:
    output.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with output.open("wb") as stream:
            for chunk in response.iter_content(1 << 20):
                if chunk:
                    stream.write(chunk)
        return {"url": url, "http_status": response.status_code,
                "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_type": response.headers.get("Content-Type")}


def inspect_zip(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    with zipfile.ZipFile(path) as archive:
        members = sorted(item for item in archive.namelist() if not item.endswith("/"))
        csv_members = [item for item in members if item.lower().endswith(".csv")]
        if len(csv_members) != 1:
            raise ValueError(f"expected exactly one CSV member, found {csv_members}")
        selected = csv_members[0]
        raw = archive.read(selected)
    frame = pd.read_csv(io.BytesIO(raw), dtype=str, keep_default_na=False, low_memory=False)
    frame.columns = [str(column).strip().lower() for column in frame.columns]
    if len(frame.columns) != len(set(frame.columns)):
        raise ValueError("compiled CSV contains duplicate normalized headers")
    evidence = {"zip_sha256": sha256_file(path), "zip_size_bytes": path.stat().st_size,
                "member_filenames": members, "selected_csv_filename": selected,
                "selected_csv_sha256": hashlib.sha256(raw).hexdigest(),
                "selected_csv_size_bytes": len(raw), "raw_row_count": len(frame),
                "raw_columns": list(frame.columns)}
    return frame, evidence


def _resolve_columns(columns: list[str]) -> tuple[dict[str, str], str | None]:
    resolved = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        hits = [alias for alias in aliases if alias in columns]
        if len(hits) != 1:
            raise ValueError(f"compiled schema does not resolve {canonical} exactly once: {hits}")
        resolved[canonical] = hits[0]
    total_hits = [column for column in TOTAL_FIELD_CANDIDATES if column in columns]
    if len(total_hits) > 1:
        raise ValueError(f"multiple authoritative-total candidates: {total_hits}")
    return resolved, total_hits[0] if total_hits else None


def _code(value: Any, width: int) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return str(int(float(text))).zfill(width)
    except ValueError:
        return text


def _provider_key(row: Mapping[str, Any], columns: Mapping[str, str]) -> tuple[str, str]:
    location = str(row[columns["location_type"]]).strip()
    if location == "Country": return location, "00"
    if location == "State": return location, _code(row[columns["state_fips"]], 2) or ""
    if location == "County": return location, _code(row[columns["county_fips"]], 5) or ""
    return location, ""


def verify(frame: pd.DataFrame, *, release_month: str) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], list[dict[str, Any]]]:
    columns, total_field = _resolve_columns(list(frame.columns))
    monthly = frame[frame[columns["period"]].str.strip().str.lower().eq("monthly")].copy()
    registry = load_registry()
    bindings = {(row["provider_location_type"], row["provider_identifier"]): row for row in registry}
    monthly["_provider_key"] = [_provider_key(row, columns) for _, row in monthly.iterrows()]
    monthly["_binding"] = monthly["_provider_key"].map(bindings)
    governed = monthly[monthly["_binding"].notna()].copy()
    token_counts: Counter[str] = Counter()
    examples: dict[str, dict[str, Any]] = {}
    canonical_rows = []
    per_geo: dict[str, list[tuple[pd.Timestamp, bool]]] = {}
    for _, row in governed.iterrows():
        binding = row["_binding"]
        try:
            observed = pd.Timestamp(year=int(row[columns["year"]]), month=int(row[columns["month"]]), day=1)
        except (TypeError, ValueError) as exc:
            raise ValueError("malformed governed BPS date") from exc
        raw = str(row[total_field]).strip() if total_field else "<AUTHORITATIVE_TOTAL_FIELD_ABSENT>"
        available = False
        if total_field:
            try:
                value = float(raw.replace(",", ""))
                if not pd.notna(value) or value < 0: raise ValueError
                available = True
                canonical_rows.append({"geo_id": binding["geo_id"], "metric_id": REQUIRED_METRIC,
                  "date": observed.date(), "property_type_id": "all", "value": value,
                  "source_id": "bps", "property_type": "all"})
            except ValueError:
                token_counts[raw] += 1
                examples.setdefault(raw, {key: str(row.get(value, "")) for key, value in columns.items()} | {"raw_total": raw})
        per_geo.setdefault(binding["geo_id"], []).append((observed, available))
    canonical = pd.DataFrame(canonical_rows, columns=KEY + ["value", "source_id", "property_type"])
    duplicate_row_count = 0
    duplicate_key_count = 0
    if not canonical.empty:
        duplicate_mask = canonical.duplicated(KEY, keep=False)
        duplicate_row_count = int(duplicate_mask.sum())
        duplicate_key_count = int(canonical.loc[duplicate_mask, KEY].drop_duplicates().shape[0])
        conflicts = canonical.groupby(KEY).value.nunique().reset_index(name="values").query("values > 1")
        if not conflicts.empty: raise ValueError("conflicting duplicate compiled observations")
        canonical = canonical.sort_values(KEY, kind="mergesort").drop_duplicates(KEY).reset_index(drop=True)
    coverage_rows = []
    for item in registry:
        entries = per_geo.get(item["geo_id"], [])
        coverage_rows.append({"geo_id": item["geo_id"], "provider_geography_type": item["provider_location_type"],
          "provider_identifier": item["provider_identifier"], "configured": True,
          "present_in_release": bool(entries), "first_observation": str(min((x[0] for x in entries), default=""))[:10],
          "last_observation": str(max((x[0] for x in entries), default=""))[:10], "observation_count": len(entries),
          "latest_value_available": bool(entries and max(entries)[1])})
    coverage = pd.DataFrame(coverage_rows).sort_values("geo_id").reset_index(drop=True)
    diagnostics = {"schema_version": SCHEMA_VERSION, "release_month": release_month,
      "monthly_row_count": len(monthly), "governed_raw_row_count": len(governed),
      "authoritative_total_field": total_field, "authoritative_total_field_proven": total_field == "total_units",
      "authoritative_total_field_basis": "Census Compiled Data Documentation defines TOTAL_UNITS as Total units, Estimates With Imputation; TOTAL_UNITS_REP is reported-only data",
      "identical_duplicate_row_count": duplicate_row_count,
      "identical_duplicate_key_count": duplicate_key_count,
      "identical_duplicate_excess_row_count": duplicate_row_count - duplicate_key_count,
      "nonnumeric_token_counts": dict(sorted(token_counts.items())),
      "configured_geography_count": len(registry), "present_geography_count": int(coverage.present_in_release.sum()),
      "canonical_row_count": len(canonical), "observation_min": str(canonical.date.min()) if not canonical.empty else None,
      "observation_max": str(canonical.date.max()) if not canonical.empty else None,
      "history_semantics": "complete_historical_snapshot_with_provider_variable_geography_coverage_no_synthetic_fill",
      "contract_gate": "compiled_contract_proven" if total_field == "total_units" else "blocked_authoritative_total_field_absent"}
    return canonical, coverage, diagnostics, [examples[key] for key in sorted(examples)]


def read_legacy(path: Path, *, include_provisional: bool) -> pd.DataFrame:
    connection = duckdb.connect(str(path), read_only=True)
    try:
        sources = ["census_bps", "census_bps_provisional"] if include_provisional else ["census_bps"]
        placeholders = ",".join("?" for _ in sources)
        frame = connection.execute(f"""SELECT geo_id, metric_id, date, property_type_id, value, source_id
          FROM fact_timeseries WHERE metric_id=? AND source_id IN ({placeholders})""",
          [REQUIRED_METRIC, *sources]).fetchdf()
    finally: connection.close()
    frame["date"] = pd.to_datetime(frame.date).dt.date
    frame["property_type_id"] = frame.property_type_id.fillna("all")
    if include_provisional:
        frame["priority"] = frame.source_id.map({"census_bps": 1, "census_bps_provisional": 2})
        frame = frame.sort_values(KEY + ["priority"]).drop_duplicates(KEY, keep="first").drop(columns="priority")
    return frame


def equivalence(provider: pd.DataFrame, legacy: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    merged = provider[KEY + ["value"]].merge(legacy[KEY + ["value"]], on=KEY, how="outer",
                                             suffixes=("_provider", "_legacy"), indicator=True)
    categories = []
    governed_geographies = {item["geo_id"] for item in load_registry()}
    for _, row in merged.iterrows():
        if row.geo_id not in governed_geographies:
            category = "OUT_OF_GOVERNANCE"
        elif row.metric_id != REQUIRED_METRIC:
            category = "IDENTITY_CONFLICT"
        elif row._merge == "left_only": category = "PROVIDER_ONLY"
        elif row._merge == "right_only": category = "PRIOR_ONLY"
        elif float(row.value_provider) == float(row.value_legacy): category = "EXACT_MATCH"
        else: category = "PROVIDER_REVISION"
        categories.append(category)
    merged["comparison_category"] = categories
    counts = Counter(categories)
    summary = {key.lower() + "_count": int(counts[key]) for key in
               ("EXACT_MATCH", "PROVIDER_REVISION", "PROVIDER_ONLY", "PRIOR_ONLY",
                "IDENTITY_CONFLICT", "OUT_OF_GOVERNANCE")}
    return merged.sort_values(KEY, kind="mergesort").reset_index(drop=True), summary


def run(args: argparse.Namespace) -> None:
    root = args.output; root.mkdir(parents=True, exist_ok=False)
    url = args.url or release_url(args.release_month)
    zip_path = args.zip
    http = None
    if zip_path is None:
        zip_path = root / f"BPS_Compiled_File_{args.release_month.replace('-', '')}.zip"
        http = acquire(url, zip_path)
    frame, raw = inspect_zip(zip_path)
    canonical, coverage, diagnostics, examples = verify(frame, release_month=args.release_month)
    raw.update({"release_month": args.release_month, "url": url, "retrieved_at": args.retrieved_at or utc_now(),
                "http": http, "schema_version": SCHEMA_VERSION})
    write_canonical_json(root / "raw_evidence_manifest.json", raw)
    write_canonical_json(root / "provider_contract_diagnostics.json", diagnostics)
    write_canonical_json(root / "nonnumeric_examples.json", examples)
    coverage.to_csv(root / "geography_coverage.csv", index=False)
    canonical.to_parquet(root / "canonical_provider.parquet", index=False)
    if args.legacy:
        for name, include_provisional in (("compiled", False), ("serving", True)):
            legacy = read_legacy(args.legacy, include_provisional=include_provisional)
            detail, summary = equivalence(canonical, legacy)
            detail.to_parquet(root / f"equivalence_{name}.parquet", index=False)
            write_canonical_json(root / f"equivalence_{name}.json", summary)
    print(json.dumps({"output": str(root), **diagnostics}, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--release-month", required=True)
    value.add_argument("--url")
    value.add_argument("--zip", type=Path)
    value.add_argument("--legacy", type=Path)
    value.add_argument("--output", type=Path, required=True)
    value.add_argument("--retrieved-at", help="fixed ISO timestamp for recovery/fixture determinism")
    return value


if __name__ == "__main__": run(parser().parse_args())
