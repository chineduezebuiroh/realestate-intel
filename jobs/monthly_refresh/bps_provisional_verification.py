"""Read-only verification of one coherently pinned Census BPS provisional state.

This provider adapter deliberately stops at evidence and canonical comparison.
It has no artifact publication, accepted-pointer, cohort, or database-write API.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import unquote, urlparse

import pandas as pd
import requests

from core.source_artifacts.hashing import write_canonical_json
from jobs.monthly_refresh.bps_bootstrap import equivalence, read_legacy
from sources.census_bps.artifact import CANONICAL_COLUMNS, KEY, REQUIRED_METRIC, load_registry
from sources.census_bps_provisional.ingest import (
    PROVISIONAL_COLUMNS_BY_LEVEL, discover_latest_provisional_urls,
)

SCHEMA_VERSION = "bps_provisional_verification_v1"
LEVELS = ("state", "county", "cbsa_metro")
PREFIXES = {"state": "st", "county": "co", "cbsa_metro": "cbsa"}
UNIT_FIELDS = ("units_1", "units_2", "units_3_4", "units_5plus")
STATE_PROVIDER_AGGREGATES = {
    "US": ("0", "0", "United States", "PROVIDER_NATIONAL_SUMMARY", "Country"),
    "R1": ("1", "0", "Northeast Region", "PROVIDER_REGION_SUMMARY", "Region"),
    "R2": ("2", "0", "Midwest Region", "PROVIDER_REGION_SUMMARY", "Region"),
    "R3": ("3", "0", "South Region", "PROVIDER_REGION_SUMMARY", "Region"),
    "R4": ("4", "0", "West Region", "PROVIDER_REGION_SUMMARY", "Region"),
    "D1": ("1", "1", "New England Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D2": ("1", "2", "Middle Atlantic Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D3": ("2", "3", "East North Central Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D4": ("2", "4", "West North Central Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D5": ("3", "5", "South Atlantic Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D6": ("3", "6", "East South Central Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D7": ("3", "7", "West South Central Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D8": ("4", "8", "Mountain Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
    "D9": ("4", "9", "Pacific Division", "PROVIDER_DIVISION_SUMMARY", "Division"),
}


def provisional_applicable_registry(registry: list[dict[str, str]]) -> list[dict[str, str]]:
    """Return identities physically exposed by the provisional provider family."""
    return [item for item in registry if item["provider_location_type"] in {"State", "County", "Metro"}]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def release_from_url(level: str, url: str) -> str:
    if level not in PREFIXES:
        raise ValueError(f"unknown provisional level: {level}")
    filename = Path(unquote(urlparse(url).path)).name
    match = re.fullmatch(rf"{PREFIXES[level]}(\d{{4}})c\.txt", filename, re.IGNORECASE)
    if not match:
        raise ValueError(f"unrecognized {level} provisional URL: {url}")
    yymm = match.group(1)
    month = int(yymm[2:])
    if not 1 <= month <= 12:
        raise ValueError(f"invalid provisional release identifier: {yymm}")
    return yymm


def resolve_inputs(urls: Mapping[str, str]) -> tuple[str, dict[str, str]]:
    if set(urls) != set(LEVELS):
        raise ValueError(f"provisional inputs must contain exactly {LEVELS}")
    normalized = {level: str(urls[level]) for level in LEVELS}
    releases = {level: release_from_url(level, url) for level, url in normalized.items()}
    if len(set(releases.values())) != 1:
        raise ValueError(f"mixed provisional releases fail closed: {releases}")
    return next(iter(releases.values())), normalized


def acquire(url: str, path: Path, *, timeout: float = 120) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        with path.open("wb") as stream:
            for chunk in response.iter_content(1 << 20):
                if chunk:
                    stream.write(chunk)
        return {"http_status": response.status_code, "etag": response.headers.get("ETag"),
                "last_modified": response.headers.get("Last-Modified"),
                "content_type": response.headers.get("Content-Type")}


def read_member(path: Path, level: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    expected = PROVISIONAL_COLUMNS_BY_LEVEL[level]
    # Census files have three descriptive heading lines and no machine header.
    frame = pd.read_csv(path, sep=",", header=None, skiprows=3, dtype=str,
                        keep_default_na=False, encoding="latin1", engine="python")
    frame = frame.dropna(how="all")
    if frame.shape[1] != len(expected):
        raise ValueError(f"{level} provisional layout has {frame.shape[1]} fields; expected {len(expected)}")
    frame.columns = expected
    return frame, {"sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                   "size_bytes": path.stat().st_size, "raw_row_count": len(frame),
                   "raw_columns": expected}


def _number(raw: Any) -> float:
    text = str(raw).strip()
    if not text:
        raise ValueError("<BLANK>")
    try:
        value = float(text.replace(",", ""))
    except ValueError as exc:
        raise ValueError(text) from exc
    if not pd.notna(value) or value < 0:
        raise ValueError(text)
    return value


def _identity(level: str, row: Mapping[str, Any]) -> tuple[str, str]:
    if level == "state":
        raw = str(row["state_fips"]).strip()
        if not re.fullmatch(r"\d{1,2}", raw):
            raise ValueError(f"unsafe provisional state identifier: {raw!r}")
        return "State", raw.zfill(2)
    if level == "county":
        state = str(row["state_fips"]).strip()
        county = str(row["county_fips_3"]).strip()
        if not re.fullmatch(r"\d{1,2}", state) or not re.fullmatch(r"\d{1,3}", county):
            raise ValueError(f"unsafe provisional county identifier: {state!r}/{county!r}")
        return "County", state.zfill(2) + county.zfill(3)
    raw = str(row["cbsa_code"]).strip()
    if not re.fullmatch(r"\d{1,5}", raw):
        raise ValueError(f"unsafe provisional CBSA identifier: {raw!r}")
    return "Metro", raw.zfill(5)


def _provider_summary(level: str, row: Mapping[str, Any]) -> tuple[str, str, str] | None:
    """Classify only the provider's explicit, exact state-member aggregates."""
    if level != "state":
        return None
    code = str(row.get("state_fips", "")).strip()
    if re.fullmatch(r"\d{1,2}", code):
        return None
    expected = STATE_PROVIDER_AGGREGATES.get(code)
    geography = (str(row.get("region_code", "")).strip(),
                 str(row.get("division_code", "")).strip(),
                 str(row.get("geo_name", "")).strip())
    if expected is None or geography != expected[:3]:
        raise ValueError(f"unsafe provisional state summary identity: {geography!r}")
    return expected[4], code, expected[3]


def verify(frames: Mapping[str, pd.DataFrame], *, release_id: str):
    if set(frames) != set(LEVELS):
        raise ValueError("all three provisional members are required")
    registry = load_registry()
    applicable_registry = provisional_applicable_registry(registry)
    bindings = {(row["provider_location_type"], row["provider_identifier"]): row for row in registry}
    tokens: Counter[str] = Counter()
    examples: dict[str, dict[str, str]] = {}
    raw_inventory = []
    canonical_rows = []
    dates = set()
    for level in LEVELS:
        for _, series in frames[level].iterrows():
            row = series.to_dict()
            stamp = str(row["survey_date"]).strip()
            if not re.fullmatch(r"\d{6}", stamp):
                raise ValueError(f"malformed provisional survey date: {stamp!r}")
            observed = pd.Timestamp(year=int(stamp[:4]), month=int(stamp[4:]), day=1).date()
            dates.add(observed)
            summary_identity = _provider_summary(level, row)
            if summary_identity is not None:
                raw_inventory.append({"level": level,
                    "provider_location_type": summary_identity[0],
                    "provider_identifier": summary_identity[1],
                    "geo_name": row["geo_name"],
                    "classification": summary_identity[2]})
                continue
            identity = _identity(level, row)
            raw_inventory.append({"level": level, "provider_location_type": identity[0],
                                  "provider_identifier": identity[1], "geo_name": row["geo_name"],
                                  "classification": "GOVERNED_CANDIDATE"
                                  if identity in bindings else "OUT_OF_GOVERNANCE"})
            values = []
            invalid = False
            for field in UNIT_FIELDS:
                try:
                    values.append(_number(row[field]))
                except ValueError as exc:
                    token = str(exc)
                    tokens[token] += 1
                    examples.setdefault(token, {"level": level, "field": field, "raw_value": str(row[field]),
                                                "provider_identifier": identity[1], "survey_date": stamp})
                    invalid = True
            binding = bindings.get(identity)
            if binding is not None and not invalid:
                canonical_rows.append({"geo_id": binding["geo_id"], "metric_id": REQUIRED_METRIC,
                    "date": observed, "property_type_id": "all", "value": sum(values),
                    "source_id": "bps", "property_type": "all"})
    if len(dates) != 1:
        raise ValueError(f"provisional members must represent exactly one common observation month: {sorted(map(str, dates))}")
    expected_date = pd.Timestamp(year=2000 + int(release_id[:2]), month=int(release_id[2:]), day=1).date()
    if dates != {expected_date}:
        raise ValueError(f"release {release_id} does not match survey date {dates}")
    canonical = pd.DataFrame(canonical_rows, columns=CANONICAL_COLUMNS)
    duplicate_mask = canonical.duplicated(KEY, keep=False)
    duplicate_rows = int(duplicate_mask.sum())
    duplicate_keys = int(canonical.loc[duplicate_mask, KEY].drop_duplicates().shape[0])
    if duplicate_rows:
        conflicts = canonical.groupby(KEY).value.nunique().reset_index(name="values").query("values > 1")
        if not conflicts.empty:
            raise ValueError("conflicting duplicate provisional observations")
        canonical = canonical.sort_values(KEY, kind="mergesort").drop_duplicates(KEY).reset_index(drop=True)
    else:
        canonical = canonical.sort_values(KEY, kind="mergesort").reset_index(drop=True)
    inventory = pd.DataFrame(raw_inventory).drop_duplicates().sort_values(["level", "provider_identifier"])
    governed = set(bindings)
    provisional_applicable = {
        (item["provider_location_type"], item["provider_identifier"])
        for item in applicable_registry
    }
    present = set(zip(inventory.provider_location_type, inventory.provider_identifier)) & governed
    present_applicable = present & provisional_applicable
    missing_applicable = provisional_applicable - present_applicable
    coverage = pd.DataFrame([{**item,
                              "provisional_applicable": (item["provider_location_type"], item["provider_identifier"]) in provisional_applicable,
                              "present_in_release": (item["provider_location_type"], item["provider_identifier"]) in present}
                             for item in registry]).sort_values("geo_id")
    outside = inventory[~inventory.apply(lambda r: (r.provider_location_type, r.provider_identifier) in governed, axis=1)]
    classification_counts = Counter(inventory.classification)
    present_by_type = Counter(location for location, _ in present_applicable)
    missing_inventory = [{"geo_id": item["geo_id"], "level": item["level"],
        "provider_location_type": item["provider_location_type"],
        "provider_identifier": item["provider_identifier"]}
        for item in applicable_registry
        if (item["provider_location_type"], item["provider_identifier"]) in missing_applicable]
    missing_inventory.sort(key=lambda item: item["geo_id"])
    diagnostics = {"schema_version": SCHEMA_VERSION, "provider_release_id": release_id,
        "observation_min": str(min(dates)), "observation_max": str(max(dates)),
        "current_month_only": True, "authoritative_total_semantics": "sum of estimate UNIT fields for 1, 2, 3-4, and 5+ unit structures",
        "total_encoding_consistent_across_members": True, "legitimate_numeric_zero": True,
        "nonnumeric_or_unavailable_token_counts": dict(sorted(tokens.items())),
        "configured_geography_count": len(registry),
        "provisional_applicable_geography_count": len(applicable_registry),
        "present_governed_geography_count": len(present),
        "present_provisional_applicable_geography_count": len(present_applicable),
        "present_provisional_applicable_geography_count_by_type": {
            location: present_by_type.get(location, 0) for location in ("State", "County", "Metro")},
        "missing_provisional_applicable_geography_count": len(missing_applicable),
        "missing_provisional_applicable_geographies": missing_inventory,
        "raw_provider_geography_classification_counts": {
            classification: classification_counts.get(classification, 0)
            for classification in ("PROVIDER_NATIONAL_SUMMARY", "PROVIDER_REGION_SUMMARY",
                                   "PROVIDER_DIVISION_SUMMARY", "GOVERNED_CANDIDATE",
                                   "OUT_OF_GOVERNANCE")},
        "out_of_governance_geography_count": len(outside), "canonical_row_count": len(canonical),
        "identical_duplicate_row_count": duplicate_rows, "identical_duplicate_key_count": duplicate_keys,
        "identical_duplicate_excess_row_count": duplicate_rows - duplicate_keys,
        "conflicting_duplicate_key_count": 0,
        "omission_semantics": "unresolved_do_not_interpret_as_zero_or_retraction",
        "contract_gate": "blocked_if_tokens_or_missing_stable_provisional_geography"
        if tokens or any(item["provider_location_type"] != "Metro" for item in missing_inventory)
        else "provider_layout_exact_mapping_and_stable_coverage_verified"}
    return canonical, coverage, outside.reset_index(drop=True), diagnostics, [examples[k] for k in sorted(examples)]


def run(args: argparse.Namespace) -> None:
    root: Path = args.output
    root.mkdir(parents=True, exist_ok=False)
    explicit = {level: getattr(args, f"{level}_url") for level in LEVELS}
    if any(explicit.values()) and not all(explicit.values()):
        raise ValueError("explicit provisional pin requires all three member URLs")
    urls = explicit if all(explicit.values()) else discover_latest_provisional_urls()
    release_id, urls = resolve_inputs(urls)
    frames = {}; members = {}
    for level in LEVELS:
        path = getattr(args, f"{level}_file")
        http = None
        if path is None:
            path = root / Path(unquote(urlparse(urls[level]).path)).name
            http = acquire(urls[level], path)
        frames[level], evidence = read_member(path, level)
        members[level] = {"url": urls[level], "retrieved_at": args.retrieved_at or utc_now(), "http": http, **evidence}
    canonical, coverage, outside, diagnostics, examples = verify(frames, release_id=release_id)
    write_canonical_json(root / "raw_evidence_manifest.json", {"schema_version": SCHEMA_VERSION,
                         "provider_release_id": release_id, "members": members})
    write_canonical_json(root / "provider_contract_diagnostics.json", diagnostics)
    write_canonical_json(root / "nonnumeric_examples.json", examples)
    canonical.to_parquet(root / "canonical_provider.parquet", index=False)
    coverage.to_csv(root / "geography_coverage.csv", index=False)
    outside.to_csv(root / "out_of_governance_geographies.csv", index=False)
    if args.legacy:
        legacy = read_legacy(args.legacy, include_provisional=True)
        legacy = legacy[legacy.source_id.eq("census_bps_provisional")]
        detail, summary = equivalence(canonical, legacy)
        detail.to_parquet(root / "equivalence_provisional.parquet", index=False)
        write_canonical_json(root / "equivalence_provisional.json", summary)
    print(json.dumps({"output": str(root), **diagnostics}, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    for level in LEVELS:
        value.add_argument(f"--{level.replace('_', '-')}-url")
        value.add_argument(f"--{level.replace('_', '-')}-file", type=Path)
    value.add_argument("--legacy", type=Path)
    value.add_argument("--output", type=Path, required=True)
    value.add_argument("--retrieved-at")
    return value


if __name__ == "__main__":
    run(parser().parse_args())
