"""Read one explicit serving input without repeating upstream family selection."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
from io import BytesIO
from pathlib import Path
import re

import duckdb
import numpy as np
import pandas as pd

from regime._00_config_loader import RegimeConfig
from regime.canonical_metrics import resolve_canonical_metrics
from regime.derived_metrics import DERIVED_METRIC_COMPONENTS

INPUT_ADAPTER_VERSION = "macro_serving_input_v1"
LOGICAL_SOURCE_REGISTRY = Path("config/logical_source_metric_registry.csv")
ACS_METRICS = {
    "census_acs_pop_total": "population",
    "census_acs_median_household_income": "median_household_income",
}
FAMILY_PHYSICAL_SOURCES = {
    "acs": {"census_acs1", "census_acs5"},
    "bps": {"census_bps", "census_bps_provisional"},
}
COLUMNS = ["geo_id", "date", "canonical_metric_key", "value", "metric_origin"]


@dataclass(frozen=True)
class ServingInput:
    observations: pd.DataFrame
    provenance: dict


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _active(rows: pd.DataFrame) -> pd.DataFrame:
    truthy = lambda col: rows[col].astype(str).str.lower().isin({"true", "1", "yes", "y"})
    return rows[truthy("enabled") & ~truthy("diagnostic_only")]


def _acs_authority(config: RegimeConfig) -> str:
    registry_bytes = LOGICAL_SOURCE_REGISTRY.read_bytes()
    registry = pd.read_csv(BytesIO(registry_bytes), dtype=str).fillna("")
    if set(registry.columns) != {"logical_source_id", "metric_id", "family_contract_version"}:
        raise ValueError("Logical source registry schema mismatch")
    if registry.metric_id.duplicated().any() or registry.eq("").any().any():
        raise ValueError("Duplicate or incomplete logical metric ownership")
    if set(registry.metric_id) & set(config.source_metrics.metric_id):
        raise ValueError("Contradictory direct/logical metric ownership")
    acs = registry[registry.logical_source_id.eq("acs")]
    if set(acs.metric_id) != set(ACS_METRICS) or set(acs.family_contract_version) != {"census_acs_physical_source_v1"}:
        raise ValueError("Unsupported ACS logical metric ownership/contract")
    return hashlib.sha256(registry_bytes).hexdigest()


def load_serving_input(
    config: RegimeConfig,
    db_path: str | Path,
    *,
    expected_sha256: str | None = None,
    require_scoring_inputs: bool = False,
) -> ServingInput:
    """Validate input grain, resolve legacy rows, and accept logical rows once.

    Required scoring coverage is input-wide, never a geography/month Cartesian
    completeness requirement. Small legacy diagnostic fixtures may omit it.
    """
    path = Path(db_path)
    if not path.is_file():
        raise FileNotFoundError(f"Serving database not found: {path}")
    if expected_sha256 is not None and not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256):
        raise ValueError("Expected serving DB SHA256 must be 64 hexadecimal characters")
    actual_hash = sha256_file(path)
    if expected_sha256 is not None and actual_hash != expected_sha256.lower():
        raise ValueError("Serving database SHA256 mismatch")
    with duckdb.connect(str(path), read_only=True) as con:
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        if "fact_timeseries" not in tables:
            raise ValueError("Serving schema missing fact_timeseries")
        columns = {row[0] for row in con.execute("DESCRIBE fact_timeseries").fetchall()}
        required = {"geo_id", "date", "source_id", "metric_id", "value"}
        if not required.issubset(columns):
            raise ValueError(f"Serving schema missing columns: {sorted(required - columns)}")
        selected = ["geo_id", "date", "source_id", "metric_id", "value"]
        if "property_type_id" in columns:
            selected.append("property_type_id")
        facts = con.execute("SELECT " + ", ".join(selected) + " FROM fact_timeseries").fetchdf()
    if sha256_file(path) != actual_hash:
        raise ValueError("Serving database changed during input read")

    sources = set(facts.source_id)
    for logical, physical in FAMILY_PHYSICAL_SOURCES.items():
        if logical in sources and sources & physical:
            raise ValueError(f"Mixed logical/physical {logical} representation")

    wrong_acs_owner = facts.metric_id.isin(ACS_METRICS) & ~facts.source_id.eq("acs")
    if wrong_acs_owner.any():
        raise ValueError("Contradictory ACS logical metric ownership")
    logical_hash = None
    if "acs" in sources:
        logical_hash = _acs_authority(config)
        if not set(facts.loc[facts.source_id.eq("acs"), "metric_id"]).issubset(ACS_METRICS):
            raise ValueError("Unknown ACS logical metric")

    bps_definitions = config.source_metrics[config.source_metrics.source_id.eq("census_bps")]
    if "bps" in sources and not set(facts.loc[facts.source_id.eq("bps"), "metric_id"]).issubset(bps_definitions.metric_id):
        raise ValueError("Unknown BPS logical metric")

    # Retain property type until proving it can be projected onto engine grain.
    pairs = set(zip(config.source_metrics.source_id, config.source_metrics.metric_id))
    known = pd.Series([(s, m) in pairs for s, m in zip(facts.source_id, facts.metric_id)], index=facts.index)
    supported = facts[known | facts.source_id.isin(FAMILY_PHYSICAL_SOURCES)].copy()
    grain = ["geo_id", "date", "source_id", "metric_id"]
    if supported[grain].isna().any().any() or supported[grain].astype(str).eq("").any().any():
        raise ValueError("Invalid serving observation identity")
    if "property_type_id" in supported:
        if supported.property_type_id.isna().any() or supported.property_type_id.astype(str).str.strip().eq("").any():
            raise ValueError("Missing property-type identity")
    if supported.duplicated(grain, keep=False).any():
        raise ValueError("Duplicate observation or ambiguous property-type at engine grain")
    supported = supported[supported.value.notna()].copy()
    supported["date"] = pd.to_datetime(supported.date, errors="coerce")
    supported["value"] = pd.to_numeric(supported.value, errors="coerce")
    if supported.date.isna().any() or not np.isfinite(supported.value).all():
        raise ValueError("Invalid serving date/value")

    legacy = supported[~supported.source_id.isin(FAMILY_PHYSICAL_SOURCES)].merge(
        config.source_metrics[["metric_key", "source_id", "metric_id"]],
        on=["source_id", "metric_id"], how="inner", validate="many_to_one",
    )
    canonical = resolve_canonical_metrics(legacy[["geo_id", "date", "metric_key", "value"]], config)
    canonical = canonical.rename(columns={"source_metric_key": "metric_origin"})[COLUMNS]
    logical_parts = []
    acs = supported[supported.source_id.eq("acs")].copy()
    if not acs.empty:
        acs["canonical_metric_key"] = acs.metric_id.map(ACS_METRICS)
        acs["metric_origin"] = "acs:" + acs.metric_id
        logical_parts.append(acs[COLUMNS])
    bps = supported[supported.source_id.eq("bps")].merge(
        bps_definitions[["metric_id", "metric_key"]], on="metric_id", validate="many_to_one",
    )
    if not bps.empty:
        mapping = _active(config.metric_dimensions)[["metric_key", "canonical_metric_key"]].drop_duplicates()
        bps = bps.merge(mapping, on="metric_key", how="inner", validate="many_to_one")
        bps["metric_origin"] = "bps:" + bps.metric_id
        logical_parts.append(bps[COLUMNS])
    parts = [part for part in [canonical, *logical_parts] if not part.empty]
    observations = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=COLUMNS)
    if observations.duplicated(["geo_id", "date", "canonical_metric_key"]).any():
        raise ValueError("Duplicate canonical serving observations")
    if require_scoring_inputs:
        active = _active(config.metric_dimensions)
        required_rows = active[active.required.astype(str).str.lower().isin({"true", "1", "yes", "y"})]
        direct = required_rows.merge(config.source_metrics[["metric_key", "source_id"]], on="metric_key")
        required_keys = set(direct.loc[direct.source_id.ne("derived"), "canonical_metric_key"])
        for key in direct.loc[direct.source_id.eq("derived"), "canonical_metric_key"]:
            required_keys.update(DERIVED_METRIC_COMPONENTS[key])
        missing = required_keys - set(observations.canonical_metric_key)
        if missing:
            raise ValueError(f"Required scoring inputs missing: {sorted(missing)}")
    provenance = {
        "serving_db_sha256": actual_hash,
        "expected_serving_db_sha256": expected_sha256.lower() if expected_sha256 else None,
        "input_adapter_version": INPUT_ADAPTER_VERSION,
        "logical_source_metric_registry_sha256": logical_hash,
    }
    return ServingInput(observations.reset_index(drop=True), provenance)
