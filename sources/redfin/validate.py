from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from .governance import BASELINE_ID, FAMILIES, FAMILY_LEVELS, METRICS, RAW_ROOT, GovernanceError, load_baseline_manifest
from .storage import atomic_json, read_json, sha256

ALIASES = {"avg_sale_to_list": "average_sale_to_list_ratio", "sold_above_list": "share_sold_above_original_list", "off_market_in_two_weeks": "percent_off_market_in_two_weeks", "median_sale_price": "median_sale_price_nsa", "median_ppsf": "median_sale_price_per_sqft", "median_dom": "median_days_on_market_days"}
PERCENTAGES = {"average_sale_to_list_ratio": (0, 200, 50, 150), "share_sold_above_original_list": (0, 105, 0, 100), "percent_off_market_in_two_weeks": (-5, 100, 0, 100)}
KEYS = ["geo_id", "metric_id", "date", "property_type_id"]
CRITICAL = KEYS + ["value"]


def normalize_columns(columns: Iterable[str]) -> list[str]:
    values = (
        pd.Index(columns)
        .str.strip()
        .str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]+", "", regex=True)
        .str.strip("_")
    )
    return [ALIASES.get(value, value) for value in values]


def read_raw(path: Path) -> pd.DataFrame:
    frame = pd.read_csv(path, sep="\t" if ".tsv" in path.name else ",", low_memory=False)
    frame.columns = normalize_columns(frame.columns)
    return frame


def validate_percentages(frame: pd.DataFrame) -> list[str]:
    warnings = []
    for metric, (hard_low, hard_high, soft_low, soft_high) in PERCENTAGES.items():
        if metric not in frame: continue
        numeric = pd.to_numeric(frame[metric], errors="coerce")
        if (frame[metric].notna() & numeric.isna()).any(): raise GovernanceError(f"non-numeric values in {metric}")
        if ((numeric < hard_low) | (numeric > hard_high)).fillna(False).any(): raise GovernanceError(f"{metric} outside governed range {hard_low}..{hard_high}")
        count = int(((numeric < soft_low) | (numeric > soft_high)).fillna(False).sum())
        if count: warnings.append(f"{metric}: {count} tolerated source anomalies")
    return warnings


def inspect_raw(path: Path, require_governed_metrics: bool = True) -> dict:
    """Validate one raw Redfin source with bounded memory.

    Every governed raw family must have a usable Redfin identity and period
    column. Exact production-metric availability is required only for families
    currently active in the governed Redfin geography manifest.
    """
    sep = "\t" if ".tsv" in path.name else ","

    header = pd.read_csv(path, sep=sep, nrows=0)
    normalized = normalize_columns(header.columns)
    by_normalized = dict(zip(normalized, header.columns))

    date_name = (
        "period_end"
        if "period_end" in by_normalized
        else "period_begin"
        if "period_begin" in by_normalized
        else None
    )
    if not date_name:
        raise GovernanceError(f"missing period column: {path.name}")

    if "region_id" not in by_normalized and "table_id" not in by_normalized:
        raise GovernanceError(f"missing Redfin identity column: {path.name}")

    available_metrics = set(normalized)

    if require_governed_metrics:
        if (
            "inventory" not in available_metrics
            and "active_listings" not in available_metrics
        ):
            raise GovernanceError(
                f"missing inventory/active_listings: {path.name}"
            )

        # inventory may be satisfied by active_listings fallback.
        required = METRICS - {"inventory"}
        missing = required - available_metrics

        if missing:
            raise GovernanceError(
                f"missing governed metrics in {path.name}: {sorted(missing)}"
            )

    # Always inspect any governed percentage columns that the source actually
    # exposes, including inactive source families. Do not require those fields
    # merely because the raw file is preserved for future use.
    wanted = [
        date_name,
        *(metric for metric in PERCENTAGES if metric in by_normalized),
    ]
    usecols = [by_normalized[name] for name in wanted]

    minimum = maximum = None
    warnings = []

    for chunk in pd.read_csv(
        path,
        sep=sep,
        usecols=usecols,
        chunksize=100_000,
        low_memory=False,
    ):
        chunk.columns = normalize_columns(chunk.columns)

        dates = pd.to_datetime(
            chunk[date_name],
            errors="coerce",
        )

        if dates.isna().any():
            raise GovernanceError(f"invalid dates: {path.name}")

        chunk_min = dates.min().to_period("M")
        chunk_max = dates.max().to_period("M")

        minimum = (
            chunk_min
            if minimum is None
            else min(minimum, chunk_min)
        )
        maximum = (
            chunk_max
            if maximum is None
            else max(maximum, chunk_max)
        )

        warnings.extend(validate_percentages(chunk))

    if minimum is None:
        raise GovernanceError(f"empty source file: {path.name}")

    return {
        "schema": normalized,
        "minimum": str(minimum),
        "maximum": str(maximum),
        "warnings": warnings,
        "production_metric_contract_checked": require_governed_metrics,
    }


def validate_baseline(
    root: Path = RAW_ROOT,
    manifest_path: Path | None = None,
    geo_manifest: Path = Path("config/geo_manifest.generated.csv"),
) -> dict:
    manifest = load_baseline_manifest(
        manifest_path or Path("config/redfin_baseline_manifest.json")
    )
    folder = root / "baseline" / manifest["baseline_id"]

    if not folder.is_dir():
        raise GovernanceError(
            f"required governed baseline absent: {folder}"
        )

    governed = governed_geographies(geo_manifest)

    active_families = {
        family
        for family, levels in FAMILY_LEVELS.items()
        if governed["level"].str.lower().isin(levels).any()
    }

    schemas = {}
    warnings = []
    validation_scope = {}

    for item in manifest["files"]:
        path = folder / item["filename"]
        family = item["geography_family"]

        if not path.is_file() or sha256(path) != item["sha256"]:
            raise GovernanceError(
                f"baseline file/hash mismatch: {item['filename']}"
            )

        strict = family in active_families

        result = inspect_raw(
            path,
            require_governed_metrics=strict,
        )

        schemas[path.name] = result["schema"]
        warnings += result["warnings"]
        validation_scope[family] = (
            "active_production"
            if strict
            else "preserved_source_only"
        )

        if (
            result["minimum"] != item["historical_floor"]
            or result["maximum"] != BASELINE_ID
        ):
            raise GovernanceError(
                f"exact baseline coverage mismatch: {path.name}"
            )

    return {
        "baseline_id": BASELINE_ID,
        "manifest_version": manifest["manifest_version"],
        "status": "validated",
        "latest_month": BASELINE_ID,
        "active_production_families": sorted(active_families),
        "validation_scope": validation_scope,
        "schemas": schemas,
        "warnings": sorted(set(warnings)),
    }


def validate_drop(drop_id: str, root: Path = RAW_ROOT) -> dict:
    folder = root / "drops" / drop_id; meta = read_json(folder / "metadata.json")
    if meta.get("status") not in {"registered", "validated"}: raise GovernanceError("drop is not validation eligible")
    schemas, warnings, latest, families = {}, [], [], set()
    for record in meta["files"]:
        path = folder / record["filename"]
        if not path.is_file() or sha256(path) != record["sha256"]: raise GovernanceError(f"registered hash changed: {path.name}")
        family = record["geography_family"]
        governed = governed_geographies(
            Path("config/geo_manifest.generated.csv")
        )
        active_families = {
            governed_family
            for governed_family, levels in FAMILY_LEVELS.items()
            if governed["level"].str.lower().isin(levels).any()
        }
        result = inspect_raw(
            path,
            require_governed_metrics=family in active_families,
        )
        schemas[path.name] = result["schema"]
        warnings += result["warnings"]
        latest.append(result["maximum"])
        families.add(family)
    if families != set(FAMILIES): raise GovernanceError("drop must contain all seven geography families")
    if set(latest) != {drop_id}: raise GovernanceError(f"every drop family must end at {drop_id}: {latest}")
    meta.update(status="validated", validation_status="validated", schemas=schemas, latest_redfin_month=drop_id, warnings=sorted(set(warnings)))
    atomic_json(folder / "metadata.json", meta); return meta


def governed_geographies(path: Path = Path("config/geo_manifest.generated.csv")) -> pd.DataFrame:
    geo = pd.read_csv(path, dtype=str).fillna(""); geo_col = "geo_slug" if "geo_slug" in geo else "geo_id"
    if "include_redfin" in geo: geo = geo[geo["include_redfin"].str.lower().isin({"1", "true", "yes", "y"})]
    return geo.rename(columns={geo_col: "geo_id"})


def _validate_long(frame: pd.DataFrame, expected_latest: str, geo_manifest: Path) -> dict:
    missing_cols = set(CRITICAL) - set(frame.columns)
    if missing_cols or frame.empty: raise GovernanceError(f"empty candidate/serving data or missing columns: {sorted(missing_cols)}")
    if frame[CRITICAL].isna().any().any(): raise GovernanceError("null critical candidate/serving fields")
    if frame.duplicated(KEYS).any(): raise GovernanceError("duplicate canonical keys")
    if set(frame.metric_id.unique()) != METRICS: raise GovernanceError("candidate/serving metrics do not equal governed 11-metric set")
    incomplete = frame.groupby("geo_id").metric_id.nunique().ne(len(METRICS))
    if incomplete.any(): raise GovernanceError("one or more governed geographies lost governed metrics")
    numeric = pd.to_numeric(frame.value, errors="coerce")
    if numeric.isna().any(): raise GovernanceError("non-numeric candidate/serving values")
    frame = frame.copy(); frame["value"] = numeric; frame["date"] = pd.to_datetime(frame.date)
    governed = governed_geographies(geo_manifest); expected_geos = set(governed.geo_id); actual_geos = set(frame.geo_id)
    if actual_geos != expected_geos: raise GovernanceError(f"governed geography loss/addition: missing={sorted(expected_geos-actual_geos)} extra={sorted(actual_geos-expected_geos)}")
    if str(frame.date.max().to_period("M")) != expected_latest: raise GovernanceError("candidate/serving latest month mismatch")
    for family, levels in FAMILY_LEVELS.items():
        family_geos = set(governed.loc[governed.level.str.lower().isin(levels), "geo_id"])
        if not family_geos: continue
        floor = "2012-03" if family in {"zip", "neighborhood"} else "2012-01"
        observed = frame[frame.geo_id.isin(family_geos)].date.min().to_period("M")
        if str(observed) != floor: raise GovernanceError(f"candidate/serving historical floor mismatch for {family}")
    percentage_frame = pd.DataFrame({metric: pd.Series(frame.loc[frame.metric_id.eq(metric), "value"].to_numpy()) for metric in PERCENTAGES})
    warnings = validate_percentages(percentage_frame)
    return {"rows": len(frame), "geographies": len(actual_geos), "metrics": len(METRICS), "latest_month": expected_latest, "warnings": warnings}


def candidate_diagnostics(frame: pd.DataFrame, connection, expected_latest: str, geo_manifest: Path = Path("config/geo_manifest.generated.csv")) -> dict:
    old = connection.execute("SELECT geo_id,metric_id,date,property_type_id,value FROM fact_timeseries WHERE source_id='redfin'").df() if connection else pd.DataFrame(columns=KEYS+["value"])
    new = frame[KEYS+["value"]].copy(); old["date"] = pd.to_datetime(old.date); new["date"] = pd.to_datetime(new.date)
    comparison = old.merge(new, on=KEYS, how="outer", suffixes=("_old", "_new"), indicator=True)
    matched = comparison._merge.eq("both"); changed = matched & comparison.value_old.ne(comparison.value_new)
    revisions = comparison.loc[changed & (comparison.date.dt.to_period("M") < pd.Period(expected_latest, "M"))].copy()
    levels = governed_geographies(geo_manifest)[["geo_id", "level"]]
    revisions = revisions.merge(levels, on="geo_id", how="left")
    return {"old_only_keys": int(comparison._merge.eq("left_only").sum()), "new_only_keys": int(comparison._merge.eq("right_only").sum()), "matched_keys": int(matched.sum()), "changed_rows": int(changed.sum()), "historical_revisions": int(len(revisions)), "historical_revisions_by_metric": revisions.groupby("metric_id").size().to_dict(), "historical_revisions_by_geography_level": revisions.groupby("level").size().to_dict()}


def validate_candidate(candidate: Path, drop_id: str, root: Path = RAW_ROOT, geo_manifest: Path = Path("config/geo_manifest.generated.csv"), db_path: Path | None = None) -> dict:
    meta_path = root / "drops" / drop_id / "metadata.json" if drop_id != BASELINE_ID else root / "baseline" / BASELINE_ID / "candidate_metadata.json"
    meta = read_json(meta_path)
    if meta.get("status") not in {"candidate_built", "candidate_validated"} or Path(meta.get("candidate_path", "")) != candidate: raise GovernanceError("candidate metadata/build state mismatch")
    frame = pd.read_parquet(candidate); summary = _validate_long(frame, drop_id, geo_manifest)
    con = duckdb.connect(str(db_path), read_only=True) if db_path and db_path.exists() else None
    try: diagnostics = candidate_diagnostics(frame, con, drop_id, geo_manifest)
    finally:
        if con: con.close()
    report = {"status":"candidate_validated", "drop_id":drop_id, "summary":summary, "diagnostics":diagnostics}
    report_path = candidate.with_suffix(".validation.json"); atomic_json(report_path, report)
    meta.update(status="candidate_validated", candidate_validation_path=str(report_path), diagnostics=diagnostics); atomic_json(meta_path, meta)
    return report


def validate_serving(db_path: Path, expected_latest: str, geo_manifest: Path = Path("config/geo_manifest.generated.csv")) -> dict:
    con = duckdb.connect(str(db_path), read_only=True)
    try: frame = con.execute("SELECT geo_id,metric_id,date,property_type_id,value FROM fact_timeseries WHERE source_id='redfin'").df()
    finally: con.close()
    return _validate_long(frame, expected_latest, geo_manifest)


def main() -> int:
    parser=argparse.ArgumentParser(description="Validate Redfin serving rows (not raw baseline)"); parser.add_argument("--db",type=Path,required=True); parser.add_argument("--expected-latest",required=True); args=parser.parse_args()
    print(json.dumps(validate_serving(args.db,args.expected_latest),indent=2)); return 0

if __name__ == "__main__": raise SystemExit(main())
