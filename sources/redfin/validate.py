from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import duckdb
import pandas as pd

from .governance import BASELINE_ID, FAMILIES, FAMILY_LEVELS, METRICS, RAW_ROOT, GovernanceError, load_baseline_manifest, load_metric_domain_contract
from .storage import atomic_json, read_json, sha256

ALIASES = {"avg_sale_to_list": "average_sale_to_list_ratio", "sold_above_list": "share_sold_above_original_list", "off_market_in_two_weeks": "percent_off_market_in_two_weeks", "median_sale_price": "median_sale_price_nsa", "median_ppsf": "median_sale_price_per_sqft", "median_dom": "median_days_on_market_days"}
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


def latest_observation_month(path: Path, *, chunksize: int = 100_000) -> str:
    """Return a raw export's latest month without materializing the export.

    Header normalization is intentionally shared with governed validation and
    ingestion.  Only the selected period column is streamed.
    """
    sep = "\t" if ".tsv" in path.name else ","
    try:
        header = pd.read_csv(path, sep=sep, nrows=0)
        normalized = normalize_columns(header.columns)
        by_normalized = dict(zip(normalized, header.columns))
        date_name = (
            "period_end" if "period_end" in by_normalized else
            "period_begin" if "period_begin" in by_normalized else None
        )
        if not date_name:
            raise GovernanceError(f"missing period column: {path.name}")

        maximum = None
        for chunk in pd.read_csv(
            path,
            sep=sep,
            usecols=[by_normalized[date_name]],
            chunksize=chunksize,
            low_memory=False,
        ):
            chunk.columns = normalize_columns(chunk.columns)
            dates = pd.to_datetime(chunk[date_name], errors="coerce")
            if dates.isna().any():
                raise GovernanceError(f"invalid dates: {path.name}")
            if not dates.empty:
                chunk_max = dates.max().to_period("M")
                maximum = chunk_max if maximum is None else max(maximum, chunk_max)
    except GovernanceError:
        raise
    except Exception as exc:
        raise GovernanceError(f"cannot inspect endpoint: {path.name}") from exc
    if maximum is None:
        raise GovernanceError(f"empty incoming file: {path.name}")
    return str(maximum)


def validate_metric_domains(frame: pd.DataFrame, contract: dict | None = None) -> dict:
    """Aggregate business warnings separately from fail-closed source violations."""
    rules = (contract or load_metric_domain_contract())["metrics"]
    diagnostics = {}
    for metric, rule in rules.items():
        if metric not in frame:
            continue
        numeric = pd.to_numeric(frame[metric], errors="coerce")
        if (frame[metric].notna() & numeric.isna()).any():
            raise GovernanceError(f"non-numeric values in {metric}")
        populated = numeric.dropna()
        if populated.empty:
            continue
        below_expected = int((populated < rule["expected_min"]).sum())
        above_expected = int((populated > rule["expected_max"]).sum())
        below_source = int((populated < rule["source_min"]).sum())
        above_source = int((populated > rule["source_max"]).sum())
        diagnostics[metric] = {
            "actual_min": float(populated.min()), "actual_max": float(populated.max()),
            "expected_range": [rule["expected_min"], rule["expected_max"]],
            "source_range": [rule["source_min"], rule["source_max"]],
            "below_expected_rows": below_expected, "above_expected_rows": above_expected,
            "below_source_rows": below_source, "above_source_rows": above_source,
            "source_violations": below_source + above_source,
            "status": "source_domain_violation" if below_source + above_source else "warning" if below_expected + above_expected else "normal",
        }
    violations = {key: value for key, value in diagnostics.items() if value["source_violations"]}
    if violations:
        raise GovernanceError(f"source-domain violations: {json.dumps(violations, sort_keys=True)}")
    return diagnostics


def _merge_metric_domains(target: dict, addition: dict) -> None:
    for metric, result in addition.items():
        if metric not in target:
            target[metric] = dict(result)
            continue
        combined = target[metric]
        combined["actual_min"] = min(combined["actual_min"], result["actual_min"])
        combined["actual_max"] = max(combined["actual_max"], result["actual_max"])
        for key in ("below_expected_rows", "above_expected_rows", "below_source_rows", "above_source_rows", "source_violations"):
            combined[key] += result[key]
        combined["status"] = "warning" if combined["below_expected_rows"] + combined["above_expected_rows"] else "normal"


def inspect_raw(path: Path, require_governed_metrics: bool = True, domain_contract: dict | None = None) -> dict:
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
        *(metric for metric in (domain_contract or load_metric_domain_contract())["metrics"] if metric in by_normalized),
    ]
    usecols = [by_normalized[name] for name in wanted]

    minimum = maximum = None
    metric_domains = {}

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

        _merge_metric_domains(metric_domains, validate_metric_domains(chunk, domain_contract))

    if minimum is None:
        raise GovernanceError(f"empty source file: {path.name}")

    return {
        "schema": normalized,
        "minimum": str(minimum),
        "maximum": str(maximum),
        "metric_domains": metric_domains,
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
    domain_contract = load_metric_domain_contract()

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
    metric_domains = {}
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
            domain_contract=domain_contract,
        )

        schemas[path.name] = result["schema"]
        _merge_metric_domains(metric_domains, result["metric_domains"])
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

    for metric, rule in domain_contract["metrics"].items():
        result = metric_domains.get(metric)
        if result is None or abs(result["actual_min"] - rule["observed_baseline_min"]) > 1e-9 or abs(result["actual_max"] - rule["observed_baseline_max"]) > 1e-9:
            raise GovernanceError(f"baseline extrema do not reproduce governed evidence for {metric}")
    warnings = [f"{metric}: {result['below_expected_rows'] + result['above_expected_rows']} expected-range observations" for metric, result in metric_domains.items() if result["status"] == "warning"]

    return {
        "baseline_id": BASELINE_ID,
        "manifest_version": manifest["manifest_version"],
        "status": "validated",
        "latest_month": BASELINE_ID,
        "active_production_families": sorted(active_families),
        "validation_scope": validation_scope,
        "schemas": schemas,
        "warnings": sorted(warnings),
        "metric_domains": dict(sorted(metric_domains.items())),
    }


def validate_drop(drop_id: str, root: Path = RAW_ROOT) -> dict:
    folder = root / "drops" / drop_id; meta = read_json(folder / "metadata.json")
    if meta.get("status") not in {"registered", "validated"}: raise GovernanceError("drop is not validation eligible")
    schemas, latest, families, metric_domains = {}, [], set(), {}
    domain_contract = load_metric_domain_contract()
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
            domain_contract=domain_contract,
        )
        schemas[path.name] = result["schema"]
        _merge_metric_domains(metric_domains, result["metric_domains"])
        latest.append(result["maximum"])
        families.add(family)
    if families != set(FAMILIES): raise GovernanceError("drop must contain all seven geography families")
    if set(latest) != {drop_id}: raise GovernanceError(f"every drop family must end at {drop_id}: {latest}")
    warnings = [f"{metric}: {result['below_expected_rows'] + result['above_expected_rows']} expected-range observations" for metric, result in metric_domains.items() if result["status"] == "warning"]
    meta.update(status="validated", validation_status="validated", schemas=schemas, latest_redfin_month=drop_id, warnings=sorted(warnings), metric_domains=dict(sorted(metric_domains.items())))
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
    # Redfin legitimately has sparse geography/metric coverage, especially
    # for thin ZIP markets. Enforce the exact 11-metric universe globally,
    # but do not require every geography to have observed every metric.
    # Candidate comparison against trusted production below is responsible
    # for failing closed on newly lost geo/metric presence.
    numeric = pd.to_numeric(frame.value, errors="coerce")
    if numeric.isna().any(): raise GovernanceError("non-numeric candidate/serving values")
    frame = frame.copy(); frame["value"] = numeric; frame["date"] = pd.to_datetime(frame.date)
    governed = governed_geographies(geo_manifest); expected_geos = set(governed.geo_id); actual_geos = set(frame.geo_id)
    if actual_geos != expected_geos: raise GovernanceError(f"governed geography loss/addition: missing={sorted(expected_geos-actual_geos)} extra={sorted(actual_geos-expected_geos)}")
    if str(frame.date.max().to_period("M")) != expected_latest: raise GovernanceError("candidate/serving latest month mismatch")
    manifest = load_baseline_manifest()
    floors = {item["geography_family"]: item["historical_floor"] for item in manifest["files"]}
    for family, levels in FAMILY_LEVELS.items():
        family_geos = set(governed.loc[governed.level.str.lower().isin(levels), "geo_id"])
        if not family_geos: continue
        floor = floors[family]
        observed = frame[frame.geo_id.isin(family_geos)].date.min().to_period("M")
        if str(observed) != floor: raise GovernanceError(f"candidate/serving historical floor mismatch for {family}")
    rules = load_metric_domain_contract()["metrics"]
    percentage_frame = pd.DataFrame({metric: pd.Series(frame.loc[frame.metric_id.eq(metric), "value"].to_numpy()) for metric in rules})
    metric_domains = validate_metric_domains(percentage_frame)
    warnings = [f"{metric}: {result['below_expected_rows'] + result['above_expected_rows']} expected-range observations" for metric, result in metric_domains.items() if result["status"] == "warning"]

    observed_pairs = set(
        frame[["geo_id", "metric_id"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    expected_pairs = {
        (geo_id, metric_id)
        for geo_id in actual_geos
        for metric_id in METRICS
    }
    missing_pairs = expected_pairs - observed_pairs
    missing_by_metric = {}
    for _, metric_id in missing_pairs:
        missing_by_metric[metric_id] = missing_by_metric.get(metric_id, 0) + 1

    return {
        "rows": len(frame),
        "geographies": len(actual_geos),
        "metrics": len(METRICS),
        "latest_month": expected_latest,
        "warnings": warnings,
        "metric_domains": dict(sorted(metric_domains.items())),
        "sparse_geo_metric_pairs": len(missing_pairs),
        "sparse_geographies": len({geo_id for geo_id, _ in missing_pairs}),
        "sparse_geo_metric_pairs_by_metric": dict(sorted(missing_by_metric.items())),
    }


def candidate_diagnostics(frame: pd.DataFrame, connection, expected_latest: str, geo_manifest: Path = Path("config/geo_manifest.generated.csv")) -> dict:
    old = connection.execute("SELECT geo_id,metric_id,date,property_type_id,value FROM fact_timeseries WHERE source_id='redfin'").df() if connection else pd.DataFrame(columns=KEYS+["value"])
    new = frame[KEYS+["value"]].copy(); old["date"] = pd.to_datetime(old.date); new["date"] = pd.to_datetime(new.date)
    comparison = old.merge(new, on=KEYS, how="outer", suffixes=("_old", "_new"), indicator=True)
    matched = comparison._merge.eq("both"); changed = matched & comparison.value_old.ne(comparison.value_new)
    revisions = comparison.loc[changed & (comparison.date.dt.to_period("M") < pd.Period(expected_latest, "M"))].copy()
    levels = governed_geographies(geo_manifest)[["geo_id", "level"]]
    revisions = revisions.merge(levels, on="geo_id", how="left")

    old_presence = set(
        old[["geo_id", "metric_id"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )
    new_presence = set(
        new[["geo_id", "metric_id"]]
        .drop_duplicates()
        .itertuples(index=False, name=None)
    )

    lost_presence = sorted(old_presence - new_presence)
    recovered_presence = sorted(new_presence - old_presence)

    return {
        "old_only_keys": int(comparison._merge.eq("left_only").sum()),
        "new_only_keys": int(comparison._merge.eq("right_only").sum()),
        "matched_keys": int(matched.sum()),
        "changed_rows": int(changed.sum()),
        "historical_revisions": int(len(revisions)),
        "historical_revisions_by_metric": revisions.groupby("metric_id").size().to_dict(),
        "historical_revisions_by_geography_level": revisions.groupby("level").size().to_dict(),
        "lost_geo_metric_pairs": len(lost_presence),
        "recovered_geo_metric_pairs": len(recovered_presence),
        "lost_geo_metric_pair_examples": [
            {"geo_id": geo_id, "metric_id": metric_id}
            for geo_id, metric_id in lost_presence[:100]
        ],
        "recovered_geo_metric_pair_examples": [
            {"geo_id": geo_id, "metric_id": metric_id}
            for geo_id, metric_id in recovered_presence[:100]
        ],
    }


def validate_candidate(candidate: Path, drop_id: str, root: Path = RAW_ROOT, geo_manifest: Path = Path("config/geo_manifest.generated.csv"), db_path: Path | None = None) -> dict:
    meta_path = root / "drops" / drop_id / "metadata.json" if drop_id != BASELINE_ID else root / "baseline" / BASELINE_ID / "candidate_metadata.json"
    meta = read_json(meta_path)
    if meta.get("status") not in {"candidate_built", "candidate_validated"} or Path(meta.get("candidate_path", "")) != candidate: raise GovernanceError("candidate metadata/build state mismatch")
    frame = pd.read_parquet(candidate); summary = _validate_long(frame, drop_id, geo_manifest)
    con = duckdb.connect(str(db_path), read_only=True) if db_path and db_path.exists() else None
    try:
        diagnostics = candidate_diagnostics(frame, con, drop_id, geo_manifest)
    finally:
        if con:
            con.close()

    if db_path and diagnostics["lost_geo_metric_pairs"]:
        raise GovernanceError(
            "candidate loses geo/metric coverage present in trusted production: "
            f"{diagnostics['lost_geo_metric_pairs']} pairs; "
            f"examples={diagnostics['lost_geo_metric_pair_examples'][:20]}"
        )

    report = {
        "status": "candidate_validated",
        "drop_id": drop_id,
        "summary": summary,
        "diagnostics": diagnostics,
    }
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
