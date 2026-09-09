#!/usr/bin/env python3
"""Credential-safe, read-only ACS-B provider verification capture.

This is deliberately not a production adapter.  It writes an evidence bundle to
an explicitly external directory and never opens repository databases for write.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import requests

PRODUCTS = {"census_acs1": "acs/acs1", "census_acs5": "acs/acs5"}
VARIABLES = {
    "B01003_001E": "pop_total",
    "B19013_001E": "median_household_income",
}
DIVISIONS = {"11244", "15804", "23224", "31084", "35084", "35154", "36084", "41884", "42034", "47894"}
SENTINELS = {-666666666, -888888888, -999999999}


class VerificationError(RuntimeError):
    pass


def _canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _external_output(path: Path, repo: Path) -> Path:
    out = path.expanduser().resolve()
    try:
        out.relative_to(repo.resolve())
    except ValueError:
        out.mkdir(parents=True, exist_ok=False)
        return out
    raise VerificationError("--output must be outside the repository")


def _geo_params(level: str, code: str) -> dict[str, str]:
    if level == "nation":
        return {"for": "us:1"}
    if level == "state":
        return {"for": f"state:{code.zfill(2)}"}
    if level == "county" and len(code.zfill(5)) == 5:
        code = code.zfill(5)
        return {"for": f"county:{code[2:]}", "in": f"state:{code[:2]}"}
    if level == "cbsa_metro":
        return {"for": f"metropolitan statistical area/micropolitan statistical area:{code}"}
    raise VerificationError(f"unsupported canonical geography level: {level}")


def _request(session: requests.Session, url: str, params: dict[str, str], key: str) -> tuple[str, Any, bytes | None]:
    transport = dict(params)
    if key:
        transport["key"] = key
    response = session.get(url, params=transport, timeout=60)
    if response.status_code == 204:
        return "provider_ineligible_no_content", None, None
    response.raise_for_status()
    if not response.content:
        raise VerificationError(f"HTTP {response.status_code} returned an empty body")
    try:
        value = response.json()
    except ValueError as exc:
        raise VerificationError(f"HTTP {response.status_code} returned invalid JSON") from exc
    if not isinstance(value, list) or not value or not isinstance(value[0], list):
        raise VerificationError("provider response is not a Census tabular JSON payload")
    canonical = _canonical_json(value)
    if len(value) == 1:
        return "valid_zero_rows", value, canonical
    if len(value) != 2 or not isinstance(value[1], list) or len(value[0]) != len(value[1]):
        raise VerificationError("individual fact query returned malformed or multiple rows")
    return "available", value, canonical


def _number(raw: Any, identity: str) -> tuple[float | None, str]:
    if raw in (None, "", "null"):
        return None, "missing_null"
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise VerificationError(f"unknown non-numeric provider value for {identity}") from exc
    if value in SENTINELS:
        return None, f"sentinel_{int(value)}"
    return value, "valid_numeric"


def _legacy(rows: list[dict[str, Any]], repo: Path) -> dict[str, Any]:
    try:
        import duckdb
    except ImportError as exc:
        raise VerificationError("duckdb is required for read-only legacy equivalence") from exc
    current = {(r["geo_id"], r["metric_id"], r["date"], r["source_id"]): r["value"] for r in rows}
    result: dict[str, Any] = {}
    for label, relative in (("serving", "data/market_serving.duckdb"), ("public", "data/market_public.duckdb")):
        con = duckdb.connect(str(repo / relative), read_only=True)
        legacy = con.execute("""SELECT geo_id, metric_id, CAST(date AS VARCHAR), source_id, value
          FROM fact_timeseries WHERE source_id IN ('census_acs1','census_acs5')
          AND CAST(date AS VARCHAR)=?""", [rows[0]["date"]]).fetchall()
        con.close()
        old = {(a, b, c, d): e for a, b, c, d, e in legacy}
        shared = current.keys() & old.keys()
        result[label] = {
            "exact": sum(current[k] == old[k] for k in shared),
            "revised": sum(current[k] != old[k] for k in shared),
            "provider_only": len(current.keys() - old.keys()),
            "legacy_only": len(old.keys() - current.keys()),
            "legacy_rows": len(old),
        }
    return result


def run(args: argparse.Namespace) -> dict[str, Any]:
    repo = Path(args.repo).resolve()
    out = _external_output(Path(args.output), repo)
    key = os.environ.get("CENSUS_API_KEY", "").strip()
    manifest = list(csv.DictReader((repo / "config/geo_manifest.generated.csv").open(encoding="utf-8")))
    canonical = [r for r in manifest if r["include_census"].strip() == "1" and r["census_code"].strip()]
    session = requests.Session()
    facts: list[dict[str, Any]] = []
    diagnostics: dict[str, Any] = {}
    hashes: dict[str, str] = {}
    for source_id, product in PRODUCTS.items():
        product_diag = {"available_geographies": 0, "available_membership": [], "available_by_level": {}, "provider_ineligible": 0, "valid_zero_rows": 0, "sentinels": {}, "errors": []}
        for geo in canonical:
            code = geo["census_code"].strip()
            if code in DIVISIONS:
                product_diag["errors"].append({"geo_id": geo["geo_slug"], "classification": "CANONICAL_CONCEPT_MISMATCH", "disposition": "EXCLUDED_FROM_INITIAL_ACS_GOVERNED_CONTRACT"})
                continue
            params = {"get": "NAME," + ",".join(VARIABLES), **_geo_params(geo["level"].strip(), code)}
            status, payload, body = _request(session, f"https://api.census.gov/data/{args.vintage}/{product}", params, key)
            if status != "available":
                product_diag["provider_ineligible" if status == "provider_ineligible_no_content" else "valid_zero_rows"] += 1
                product_diag["errors"].append({"geo_id": geo["geo_slug"], "classification": status})
                if body is not None:
                    name = f"{source_id}__{geo['geo_slug']}.json"
                    (out / name).write_bytes(body)
                    hashes[name] = _sha(body)
                continue
            product_diag["available_geographies"] += 1
            product_diag["available_membership"].append(geo["geo_slug"])
            product_diag["available_by_level"][geo["level"]] = product_diag["available_by_level"].get(geo["level"], 0) + 1
            name = f"{source_id}__{geo['geo_slug']}.json"
            (out / name).write_bytes(body)
            hashes[name] = _sha(body)
            header, row = payload
            values = dict(zip(header, row))
            for variable, suffix in VARIABLES.items():
                value, classification = _number(values.get(variable), f"{source_id}/{geo['geo_slug']}/{variable}")
                if classification != "valid_numeric":
                    product_diag["sentinels"][classification] = product_diag["sentinels"].get(classification, 0) + 1
                    continue
                facts.append({"geo_id": geo["geo_slug"], "metric_id": f"{source_id}_{suffix}", "date": f"{args.vintage}-12-31", "property_type_id": "all", "value": value, "source_id": source_id, "property_type": "all"})
        diagnostics[source_id] = product_diag
    facts.sort(key=lambda r: (r["source_id"], r["geo_id"], r["metric_id"]))
    if not facts:
        raise VerificationError("ordinary ACS fact queries produced no observations")
    with (out / "canonical.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["geo_id", "metric_id", "date", "property_type_id", "value", "source_id", "property_type"])
        writer.writeheader(); writer.writerows(facts)
    canonical_bytes = (out / "canonical.csv").read_bytes()
    hashes["canonical.csv"] = _sha(canonical_bytes)
    revision_probe = bytearray(canonical_bytes); revision_probe[-2] ^= 1
    summary = {"schema_version": "acs_b_local_verification_v1", "vintage": args.vintage, "authentication": "environment_key" if key else "anonymous", "credential_persisted": False, "canonical_rows": len(facts), "diagnostics": diagnostics, "legacy_equivalence": _legacy(facts, repo), "same_vintage_revision_proof": {"original_sha256": _sha(canonical_bytes), "mutated_sha256": _sha(bytes(revision_probe)), "hash_changed": _sha(canonical_bytes) != _sha(bytes(revision_probe))}}
    summary_bytes = _canonical_json(summary); (out / "summary.json").write_bytes(summary_bytes); hashes["summary.json"] = _sha(summary_bytes)
    (out / "SHA256SUMS").write_text("".join(f"{digest}  {name}\n" for name, digest in sorted(hashes.items())), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default=".")
    parser.add_argument("--vintage", type=int, default=2024)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    try:
        summary = run(args)
    except (VerificationError, requests.RequestException, OSError, ValueError) as exc:
        raise SystemExit(f"ACS-B verification failed: {exc}") from exc
    print(json.dumps({"status": "complete", "output": str(Path(args.output).expanduser().resolve()), "canonical_rows": summary["canonical_rows"], "authentication": summary["authentication"]}, sort_keys=True))


if __name__ == "__main__":
    main()
