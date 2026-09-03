"""Read-only, pin-bound verification of the BPS CBSA identity contract.

This module deliberately has no discovery, publication, catalog, readiness, or
database API.  Provider names are diagnostics only; the five-digit code is the
sole crosswalk authority.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import zipfile
from collections import Counter
from pathlib import Path

import pandas as pd

from core.source_artifacts.hashing import sha256_file, write_canonical_json
from jobs.monthly_refresh.bps_bootstrap import _code, _resolve_columns
from jobs.monthly_refresh.bps_provisional_verification import read_member

SCHEMA_VERSION = "bps_cbsa_contract_verification_v1"
KEY = ["provider_cbsa_id", "observation_date"]
CONFLICT_SAMPLE_LIMIT = 25
NON_GOVERNABLE_PROVIDER_PLACEHOLDERS = {"09999"}
CONCEPT_REGISTRY = Path("config/bps_cbsa_canonical_concepts_v1.csv")


def canonical_cbsa(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    frame = frame[frame.level.str.strip().eq("cbsa_metro")].copy()
    frame["provider_cbsa_id"] = frame.census_code.str.strip().str.zfill(5)
    if (~frame.provider_cbsa_id.str.fullmatch(r"\d{5}")).any():
        raise ValueError("canonical CBSA identities require exact five-digit census_code values")
    duplicates = frame.groupby("provider_cbsa_id").geo_slug.nunique()
    if (duplicates > 1).any():
        raise ValueError(f"ambiguous canonical CBSA identifiers fail closed: {duplicates[duplicates > 1].index.tolist()}")
    result = frame[["provider_cbsa_id", "geo_slug", "geo_name"]].rename(
        columns={"geo_slug": "canonical_geo_id", "geo_name": "canonical_geo_name"}
    ).sort_values("provider_cbsa_id").reset_index(drop=True)
    concepts = pd.read_csv(CONCEPT_REGISTRY, dtype=str, keep_default_na=False).rename(
        columns={"census_code": "provider_cbsa_id"})
    if set(result.provider_cbsa_id) == set(concepts.provider_cbsa_id):
        result = result.merge(concepts[["provider_cbsa_id", "canonical_concept",
            "bps_compatibility", "reason"]], on="provider_cbsa_id", validate="one_to_one")
    return result


def exclude_provider_placeholders(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Exclude provider sentinels before canonical identity or conflict formation."""
    placeholder = frame.provider_cbsa_id.isin(NON_GOVERNABLE_PROVIDER_PLACEHOLDERS)
    return frame.loc[~placeholder].copy(), frame.loc[placeholder].copy()


def _numeric(raw: object) -> tuple[float | None, str | None]:
    text = str(raw).strip()
    try:
        value = float(text.replace(",", ""))
    except ValueError:
        return None, text or "<BLANK>"
    if not pd.notna(value) or value < 0:
        return None, text
    return value, None


def compiled_inventory(path: Path) -> tuple[pd.DataFrame, dict]:
    with zipfile.ZipFile(path) as archive:
        members = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if len(members) != 1:
            raise ValueError(f"expected exactly one compiled CSV member, found {members}")
        with archive.open(members[0]) as stream:
            header = pd.read_csv(stream, nrows=0)
        normalized = [str(value).strip().lower() for value in header.columns]
        columns, total = _resolve_columns(normalized)
        if total != "total_units" or "cbsa_code" not in normalized:
            raise ValueError("compiled CBSA verification requires authoritative total_units and cbsa_code")
        original = dict(zip(normalized, header.columns))
        # Retain the complete provider row for Metro observations.  The provider
        # schema has changed over the compiled history and preselecting guessed
        # identity columns is precisely what this diagnostic must not do.
        provider_columns = list(header.columns)
        rows, tokens, raw_count = [], Counter(), 0
        with archive.open(members[0]) as stream:
            for chunk in pd.read_csv(stream, dtype=str, keep_default_na=False,
                                     chunksize=100_000, low_memory=False):
                raw_count += len(chunk)
                chunk.columns = [str(value).strip().lower() for value in chunk.columns]
                metro = chunk[
                    chunk[columns["period"]].str.strip().str.lower().eq("monthly")
                    & chunk[columns["location_type"]].str.strip().str.lower().eq("metro")
                ]
                for _, row in metro.iterrows():
                    code = _code(row.cbsa_code, 5)
                    if code is None or not re.fullmatch(r"\d{5}", code):
                        raise ValueError(f"unsafe compiled CBSA identifier: {row.cbsa_code!r}")
                    value, token = _numeric(row[total])
                    if token is not None:
                        tokens[token] += 1
                    raw = {f"provider_raw__{name}": str(row[name]) for name in normalized}
                    fingerprint_payload = json.dumps(raw, sort_keys=True, separators=(",", ":"))
                    rows.append({"provider_cbsa_id": code,
                                 "observation_date": f"{int(row[columns['year']]):04d}-{int(row[columns['month']]):02d}-01",
                                 "total_units": value,
                                 "provider_row_fingerprint": hashlib.sha256(
                                     fingerprint_payload.encode("utf-8")).hexdigest(), **raw})
    return pd.DataFrame(rows), {"raw_row_count": raw_count, "metro_row_count": len(rows),
        "nonnumeric_token_counts": dict(sorted(tokens.items())), "authoritative_total_field": total,
        "provider_columns": provider_columns,
        "retained_provider_identity_columns": [f"provider_raw__{name}" for name in normalized]}


def provisional_inventory(path: Path) -> tuple[pd.DataFrame, dict]:
    frame, evidence = read_member(path, "cbsa_metro")
    rows, tokens = [], Counter()
    for _, row in frame.iterrows():
        code = str(row.cbsa_code).strip().zfill(5)
        if not re.fullmatch(r"\d{5}", code):
            raise ValueError(f"unsafe provisional CBSA identifier: {row.cbsa_code!r}")
        values = []
        for field in ("units_1", "units_2", "units_3_4", "units_5plus"):
            value, token = _numeric(row[field])
            if token is not None:
                tokens[token] += 1
            values.append(value)
        rows.append({"provider_cbsa_id": code, "provider_name": str(row.geo_name).strip(),
                     "observation_date": f"{row.survey_date[:4]}-{row.survey_date[4:]}-01",
                     "total_units": sum(values) if all(value is not None for value in values) else None})
    return pd.DataFrame(rows), {**evidence, "nonnumeric_token_counts": dict(sorted(tokens.items())),
        "authoritative_total_semantics": "sum of all four estimate-side unit fields"}


def diagnose(frame: pd.DataFrame, source: str) -> tuple[pd.DataFrame, dict, pd.DataFrame, pd.DataFrame]:
    duplicate = frame.duplicated(KEY, keep=False)
    value_counts = frame.loc[duplicate].groupby(KEY, dropna=False).total_units.nunique(dropna=False)
    conflict_index = value_counts[value_counts > 1].index
    conflict_mask = pd.MultiIndex.from_frame(frame[KEY]).isin(conflict_index)
    evidence_order = KEY + ["total_units"]
    if "provider_row_fingerprint" in frame:
        evidence_order.append("provider_row_fingerprint")
    conflict_rows = frame.loc[conflict_mask].sort_values(
        evidence_order, kind="mergesort", na_position="first").reset_index(drop=True)
    provider_evidence = [column for column in frame if column.startswith("provider_raw__")]
    differing_fields_by_key = {}
    for key, rows_for_key in conflict_rows.groupby(KEY, dropna=False):
        differing_fields_by_key[key] = [column for column in provider_evidence
            if rows_for_key[column].nunique(dropna=False) > 1]
    conflict_keys = (conflict_rows.groupby(KEY, as_index=False, dropna=False)
        .agg(differing_total_units=("total_units", lambda values: "|".join(
            sorted({"<NA>" if pd.isna(value) else format(float(value), ".15g") for value in values}))),
             conflicting_row_count=("total_units", "size")))
    if len(conflict_keys):
        conflict_keys["differing_provider_fields"] = conflict_keys.apply(
            lambda row: "|".join(differing_fields_by_key[
                (row.provider_cbsa_id, row.observation_date)]), axis=1)
    # Identical duplicates are safe to collapse for inventory purposes.  A
    # differing key is deliberately retained in full and never reduced to a
    # chosen observation.
    result = frame.loc[~conflict_mask].sort_values(KEY, kind="mergesort").drop_duplicates(KEY).reset_index(drop=True)
    counts = result.groupby("provider_cbsa_id").size() if not result.empty else pd.Series(dtype=int)
    by_cbsa = conflict_keys.provider_cbsa_id.value_counts().sort_index().to_dict() if len(conflict_keys) else {}
    years = conflict_keys.observation_date.str[:4].value_counts().sort_index().to_dict() if len(conflict_keys) else {}
    diagnostics = {"provider_identity_count": int(frame.provider_cbsa_id.nunique()) if not frame.empty else 0,
        "observation_min": frame.observation_date.min() if not frame.empty else None,
        "observation_max": frame.observation_date.max() if not frame.empty else None,
        "observations_per_identity_min": int(counts.min()) if len(counts) else 0,
        "observations_per_identity_max": int(counts.max()) if len(counts) else 0,
        "identical_duplicate_excess_row_count": int(sum(
            size - 1 for key, size in frame.loc[duplicate].groupby(KEY, dropna=False).size().items()
            if key not in conflict_index)),
        "conflicting_duplicate_key_count": len(conflict_keys),
        "conflicting_duplicate_row_count": len(conflict_rows),
        "affected_cbsa_identity_count": int(conflict_rows.provider_cbsa_id.nunique()),
        "conflict_observation_min": conflict_rows.observation_date.min() if len(conflict_rows) else None,
        "conflict_observation_max": conflict_rows.observation_date.max() if len(conflict_rows) else None,
        "conflicts_by_cbsa_id": by_cbsa, "conflicts_by_year": years,
        "conflict_distinguishing_provider_fields": sorted({field
            for fields in differing_fields_by_key.values() for field in fields}),
        "conflicting_rows_sample": conflict_rows.head(CONFLICT_SAMPLE_LIMIT).to_dict("records")}
    return result, diagnostics, conflict_keys, conflict_rows


def crosswalk(compiled: pd.DataFrame, provisional: pd.DataFrame, canonical: pd.DataFrame) -> pd.DataFrame:
    compiled, _ = exclude_provider_placeholders(compiled)
    provisional, _ = exclude_provider_placeholders(provisional)
    compiled_ids = set(compiled.provider_cbsa_id); provisional_ids = set(provisional.provider_cbsa_id)
    canon = canonical.set_index("provider_cbsa_id").to_dict("index")
    rows = []
    for code in sorted(compiled_ids | provisional_ids | set(canon)):
        mapped = canon.get(code)
        if mapped is None:
            status = "UNMAPPED"
        elif code in compiled_ids and code in provisional_ids:
            status = "EXACT_MATCH"
        elif code in compiled_ids:
            status = "COMPILED_ONLY"
        elif code in provisional_ids:
            status = "PROVISIONAL_ONLY"
        else:
            status = "CANONICAL_ONLY"
        rows.append({"provider_compiled_cbsa_id": code if code in compiled_ids else "",
                     "provider_provisional_cbsa_id": code if code in provisional_ids else "",
                     "canonical_geo_id": mapped["canonical_geo_id"] if mapped else "",
                     "canonical_geo_name": mapped["canonical_geo_name"] if mapped else "",
                     "mapping_status": status})
    return pd.DataFrame(rows)


def history_distribution(frame: pd.DataFrame) -> pd.DataFrame:
    """Summarize provider-observed history without manufacturing missing months."""
    if frame.empty:
        return pd.DataFrame(columns=["provider_cbsa_id", "first_observation", "last_observation", "observation_count"])
    return (frame.groupby("provider_cbsa_id", as_index=False)
        .agg(first_observation=("observation_date", "min"),
             last_observation=("observation_date", "max"),
             observation_count=("observation_date", "size"))
        .sort_values("provider_cbsa_id").reset_index(drop=True))


def governance_decision(compiled: pd.DataFrame, provisional: pd.DataFrame,
                        canonical: pd.DataFrame, *, has_tokens: bool,
                        has_conflicts: bool = False) -> tuple[str, str]:
    """Return a provider-evidence decision; inability to execute is never rejection."""
    compiled, _ = exclude_provider_placeholders(compiled)
    provisional, _ = exclude_provider_placeholders(provisional)
    compiled_ids = set(compiled.provider_cbsa_id)
    provisional_ids = set(provisional.provider_cbsa_id)
    compatible = canonical
    if "bps_compatibility" in canonical:
        compatible = canonical[canonical.bps_compatibility.eq("compatible")]
    missing = set(compatible.provider_cbsa_id) - (compiled_ids | provisional_ids)
    if has_conflicts:
        return ("BLOCK_CBSA",
                "a governable provider identity has conflicting values; no observation may be selected")
    if missing:
        return "BLOCK_CBSA", f"{len(missing)} canonical CBSA identities are absent from both physical parents"
    if has_tokens:
        return "CBSA_GOVERNANCE_DECISION_DEFERRED", "unresolved nonnumeric provider tokens require review"
    if not compiled_ids and not provisional_ids:
        return "BLOCK_CBSA", "neither physical parent contains CBSA observations"
    return ("PROMOTE_CBSA", "all admitted provider identities map by exact five-digit code to a "
            "compatible canonical concept; unsupported canonical concepts remain absent")


def verify_pin(path: Path, expected: str, label: str) -> None:
    actual = sha256_file(path)
    if actual != expected:
        raise ValueError(f"{label} does not match persisted pin: {actual}")


def run(args: argparse.Namespace) -> None:
    root = args.output; root.mkdir(parents=True, exist_ok=False)
    compiled_pin = json.loads(args.compiled_pin.read_text())
    provisional_pin = json.loads(args.provisional_pin.read_text())
    verify_pin(args.compiled_zip, compiled_pin["members"]["compiled_zip"]["sha256"], "compiled ZIP")
    verify_pin(args.provisional_cbsa, provisional_pin["members"]["cbsa_metro"]["sha256"], "provisional CBSA")
    compiled_all, compiled_raw = compiled_inventory(args.compiled_zip)
    provisional, provisional_raw = provisional_inventory(args.provisional_cbsa)
    compiled_all, compiled_placeholders = exclude_provider_placeholders(compiled_all)
    provisional, provisional_placeholders = exclude_provider_placeholders(provisional)
    compiled, compiled_diag, compiled_conflict_keys, compiled_conflict_rows = diagnose(compiled_all, "compiled")
    provisional, provisional_diag, _, _ = diagnose(provisional, "provisional")
    canonical = canonical_cbsa(args.geo_manifest)
    mapping = crosswalk(compiled_all, provisional, canonical)
    compiled_history = history_distribution(compiled)
    canonical_coverage = canonical.copy()
    canonical_coverage["compiled_present"] = canonical_coverage.provider_cbsa_id.isin(set(compiled_all.provider_cbsa_id))
    canonical_coverage["provisional_present"] = canonical_coverage.provider_cbsa_id.isin(set(provisional.provider_cbsa_id))
    canonical_coverage["mapping_status"] = canonical_coverage.apply(
        lambda row: "EXACT_MATCH" if row.compiled_present and row.provisional_present
        else "COMPILED_ONLY" if row.compiled_present else "PROVISIONAL_ONLY"
        if row.provisional_present else "UNMAPPED", axis=1)
    compiled.to_csv(root / "compiled_cbsa_inventory.csv", index=False)
    provisional.to_csv(root / "provisional_cbsa_inventory.csv", index=False)
    mapping.to_csv(root / "compiled_provisional_cbsa_crosswalk.csv", index=False)
    canonical_coverage.to_csv(root / "canonical_mapping_coverage.csv", index=False)
    compiled_history.to_csv(root / "compiled_cbsa_history_distribution.csv", index=False)
    compiled_conflict_keys.to_csv(root / "compiled_cbsa_conflicting_keys.csv", index=False)
    compiled_conflict_rows.to_csv(root / "compiled_cbsa_conflicting_rows.csv", index=False)
    statuses = mapping.mapping_status.value_counts().sort_index().to_dict()
    unresolved = int((~canonical_coverage.compiled_present & ~canonical_coverage.provisional_present).sum())
    has_tokens = bool(compiled_raw["nonnumeric_token_counts"] or provisional_raw["nonnumeric_token_counts"])
    recommendation, recommendation_basis = governance_decision(
        compiled_all, provisional, canonical, has_tokens=has_tokens,
        has_conflicts=bool(compiled_diag["conflicting_duplicate_key_count"]))
    applicability = {
        "compiled": {"nation": True, "state": True, "county": True,
                     "cbsa_metro": bool(len(compiled_all))},
        "provisional": {"nation": False, "state": True, "county": True,
                        "cbsa_metro": bool(len(provisional))},
        "logical_bps_family": {"nation": True, "state": True, "county": True,
                               "cbsa_metro": recommendation == "PROMOTE_CBSA"},
    }
    write_canonical_json(root / "applicability_matrix.json", applicability)
    write_canonical_json(root / "diagnostics.json", {"schema_version": SCHEMA_VERSION,
        "compiled_pin_id": compiled_pin["pin_id"], "provisional_pin_id": provisional_pin["pin_id"],
        "compiled": {**compiled_raw, **compiled_diag}, "provisional": {**provisional_raw, **provisional_diag},
        "canonical_identity_count": len(canonical), "crosswalk_status_counts": statuses,
        "identity_authority": "exact five-digit code only; provider names diagnostic; no fuzzy or derived matching",
        "excluded_provider_placeholders": {
            "identities": sorted(NON_GOVERNABLE_PROVIDER_PLACEHOLDERS),
            "compiled_row_count": len(compiled_placeholders),
            "provisional_row_count": len(provisional_placeholders),
            "classification": "NON_GOVERNABLE_PROVIDER_PLACEHOLDER"},
        "unresolved_identity_count": unresolved, "ambiguous_identity_count": 0,
        "applicability_matrix": applicability, "recommendation": recommendation,
        "recommendation_basis": recommendation_basis})


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--compiled-pin", type=Path, required=True)
    value.add_argument("--provisional-pin", type=Path, required=True)
    value.add_argument("--compiled-zip", type=Path, required=True)
    value.add_argument("--provisional-cbsa", type=Path, required=True)
    value.add_argument("--geo-manifest", type=Path, default=Path("config/geo_manifest.generated.csv"))
    value.add_argument("--output", type=Path, required=True)
    return value


if __name__ == "__main__":
    run(parser().parse_args())
