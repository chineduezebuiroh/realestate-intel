#!/usr/bin/env python3
"""Bounded, read-only BEA-B provider verifier.

The command only performs BEA GET requests and writes diagnostic JSON beneath
``artifacts/bea_verification/bea_b``.  It has no imports from production
publication, database, source-set, pointer, or orchestration code.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "https://apps.bea.gov/api/data"
OUT_ROOT = ROOT / "artifacts" / "bea_verification" / "bea_b"
MANIFEST = ROOT / "config" / "geo_manifest.generated.csv"
TABLES = {
    "bea_gdp_qtr": ("SQGDP9", "include_bea_qgdp"),
    "bea_gdp_ann": ("CAGDP9", "include_bea_agdp"),
}
SECRET_FIELDS = frozenset({"UserID", "BEA_API_KEY", "BEA_API_USER_ID"})


class CredentialUnavailable(RuntimeError):
    """Raised without embedding any credential when BEA authentication is absent."""


def credential() -> tuple[str, str]:
    """Return (environment variable name, value), preferring the modern name."""
    for name in ("BEA_API_KEY", "BEA_API_USER_ID"):
        value = os.environ.get(name, "").strip()
        if value:
            return name, value
    raise CredentialUnavailable("BEA_API_KEY is required for live BEA-B verification")


def sanitized(params: dict[str, Any]) -> dict[str, Any]:
    """Return request identity with all credential-bearing fields removed."""
    return {key: value for key, value in params.items() if key not in SECRET_FIELDS}


def digest(value: bytes | Any) -> str:
    payload = value if isinstance(value, bytes) else json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def governed_geographies() -> dict[str, list[dict[str, str]]]:
    result = {"nation": [], "state": [], "county": []}
    with MANIFEST.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row["level"] in result and row["bea_geo_fips"].strip():
                result[row["level"]].append({
                    "geo_id": row["geo_slug"],
                    "geo_name": row["geo_name"],
                    "bea_geo_fips": row["bea_geo_fips"],
                    "include_bea_qgdp": row["include_bea_qgdp"],
                    "include_bea_agdp": row["include_bea_agdp"],
                })
    return result


def bea_get(key: str, params: dict[str, str]) -> tuple[bytes, dict[str, Any]]:
    full = {"UserID": key, "ResultFormat": "JSON", **params}
    request = Request(ENDPOINT, data=urlencode(full).encode(), method="POST")
    with urlopen(request, timeout=120) as response:  # noqa: S310 - fixed HTTPS origin
        raw = response.read()
    parsed = json.loads(raw)
    api = parsed.get("BEAAPI", {})
    if "Error" in api:
        error = api["Error"]
        raise RuntimeError(f"BEA API error code {error.get('APIErrorCode', 'unknown')}")
    return raw, parsed


def data_rows(response: dict[str, Any]) -> list[dict[str, Any]]:
    results = response.get("BEAAPI", {}).get("Results", {})
    if isinstance(results, list):
        results = results[0] if results else {}
    rows = results.get("Data", [])
    return rows if isinstance(rows, list) else []


def normalized(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fields = ("GeoFips", "TimePeriod", "DataValue", "CL_UNIT", "UNIT_MULT", "LineCode")
    return sorted(
        ({field: row.get(field) for field in fields} for row in rows),
        key=lambda row: (str(row["GeoFips"]), str(row["TimePeriod"]), str(row["LineCode"])),
    )


def inventory(rows: list[dict[str, Any]]) -> list[list[str]]:
    return sorted({
        (str(row.get("GeoFips", "")).strip(), str(row.get("TimePeriod", "")).strip())
        for row in rows
    })


def mutation_proof(rows: list[dict[str, Any]]) -> dict[str, str]:
    baseline = normalized(rows)
    changed = json.loads(json.dumps(baseline))
    if changed:
        changed[0]["DataValue"] = "SYNTHETIC_MUTATION_FOR_HASH_PROOF_ONLY"
    return {"baseline": digest(baseline), "synthetic_mutation": digest(changed)}


def request_plan(table: str, geos: list[str]) -> dict[str, str]:
    return {
        "method": "GetData", "DataSetName": "Regional", "TableName": table,
        "LineCode": "1", "Year": "ALL", "GeoFips": ",".join(geos),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=OUT_ROOT)
    args = parser.parse_args()
    try:
        credential_name, key = credential()
    except CredentialUnavailable as exc:
        parser.exit(2, f"verification not run: {exc}\n")

    geographies = governed_geographies()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    evidence: dict[str, Any] = {
        "evidence_kind": "live_first_party_provider_verification",
        "credential_variable": credential_name,
        "endpoint": ENDPOINT,
        "requests": [],
    }
    for source_id, (table, flag) in TABLES.items():
        targets = [g["bea_geo_fips"] for level in geographies.values() for g in level if g[flag] == "1"]
        params = request_plan(table, targets)
        repeats = []
        for _ in range(2):
            raw, response = bea_get(key, params)
            rows = data_rows(response)
            repeats.append({
                "raw_sha256": digest(raw),
                "normalized_governed_content_sha256": digest(normalized(rows)),
                "canonical_key_inventory_sha256": digest(inventory(rows)),
                "row_count": len(rows),
            })
        evidence["requests"].append({
            "source_id": source_id,
            "sanitized_request": sanitized(params),
            "repeat_diagnostics": repeats,
            "same_period_revision_identity_proof": mutation_proof(rows),
        })
    path = args.output_dir / "verification_summary.json"
    path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(path.relative_to(ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
