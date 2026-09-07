"""Canonical candidate assembly from an exact logical Source Set v2."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

from .models import CANONICAL_COLUMNS, CANONICAL_KEY
from .source_set_v2 import validate_source_set_v2
from .validation import validate_artifact

PROTECTED_DATABASES = {Path("data/market.duckdb").resolve(),
                       Path("data/market_serving.duckdb").resolve(),
                       Path("data/market_public.duckdb").resolve()}


def assemble_source_set_v2(source_set: dict[str, Any], output: Path, resolver: Any, *,
        metric_registry: Path = Path("config/source_metric_registry.csv"),
        geo_manifest: Path = Path("config/geo_manifest.generated.csv")) -> dict[str, Any]:
    """Build a new isolated DuckDB using only exact Source Set declarations."""
    validate_source_set_v2(source_set)
    if output.resolve() in PROTECTED_DATABASES:
        raise ValueError("canonical candidate cannot use a production/serving database path")
    metrics = pd.read_csv(metric_registry, dtype=str)
    owners = dict(zip(metrics.metric_id, metrics.source_id))
    geographies = set(pd.read_csv(geo_manifest, dtype=str).geo_slug)
    family_owners = {item["logical_source_id"]:{m["source_id"] for m in item["physical_sources"]}
                     for item in source_set["family_resolution"].get("families", [])}
    frames, reconciliation = [], []
    for entry in source_set["sources"]:
        directory = resolver.resolve(entry["logical_artifact_uri"])
        checked = validate_artifact(directory, expected_source_id=entry["source_id"])
        manifest = checked["manifest"]
        for manifest_key, entry_key in (("artifact_id", "artifact_id"),
                ("artifact_content_hash", "artifact_content_hash"),
                ("provider_release_id", "provider_release_id"),
                ("observation_max", "observation_max")):
            if manifest[manifest_key] != entry[entry_key]:
                raise ValueError(f"Source Set/manifest identity mismatch: {entry['source_id']} {manifest_key}")
        frame = pd.read_parquet(directory / manifest["data_filename"])
        if list(frame.columns) != CANONICAL_COLUMNS:
            raise ValueError(f"canonical schema mismatch for {entry['source_id']}")
        if set(frame.source_id) != {entry["source_id"]}:
            raise ValueError(f"source identity mismatch for {entry['source_id']}")
        if frame[CANONICAL_KEY].duplicated().any():
            raise ValueError(f"duplicate canonical key in {entry['source_id']}")
        if not set(frame.geo_id).issubset(geographies):
            raise ValueError(f"ungoverned geography in {entry['source_id']}")
        allowed_owners = {entry["source_id"], *family_owners.get(entry["source_id"], set())}
        wrong = sorted(m for m in frame.metric_id.unique() if owners.get(m) not in allowed_owners)
        if wrong: raise ValueError(f"unauthorized metric ownership for {entry['source_id']}: {wrong}")
        if frame.property_type_id.isna().any() or frame.property_type.isna().any():
            raise ValueError("null property type identity")
        dates = pd.to_datetime(frame.date, errors="coerce")
        values = pd.to_numeric(frame.value, errors="coerce")
        if dates.isna().any() or values.isna().any() or not np.isfinite(values).all():
            raise ValueError(f"invalid date/value in {entry['source_id']}")
        frames.append(frame)
        reconciliation.append({"source_id": entry["source_id"], "artifact_id": entry["artifact_id"],
                               "row_count": len(frame)})
    facts = pd.concat(frames, ignore_index=True).sort_values(CANONICAL_KEY, kind="mergesort")
    if facts[CANONICAL_KEY].duplicated().any():
        raise ValueError("cross-source canonical key collision")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists(): output.unlink()
    connection = duckdb.connect(str(output))
    try:
        connection.register("facts", facts)
        connection.execute('''CREATE TABLE fact_timeseries AS SELECT
            CAST(geo_id AS VARCHAR) geo_id, CAST(metric_id AS VARCHAR) metric_id,
            CAST(date AS DATE) date, CAST(property_type_id AS VARCHAR) property_type_id,
            CAST("value" AS DOUBLE) AS "value", CAST(source_id AS VARCHAR) source_id,
            CAST(property_type AS VARCHAR) property_type FROM facts
            ORDER BY geo_id,metric_id,date,property_type_id''')
        connection.execute("CREATE UNIQUE INDEX fact_timeseries_key ON fact_timeseries(geo_id,metric_id,date,property_type_id)")
        connection.execute("CREATE TABLE source_artifact_metadata(source_id VARCHAR, artifact_id VARCHAR, source_set_id VARCHAR)")
        for entry in source_set["sources"]:
            connection.execute("INSERT INTO source_artifact_metadata VALUES (?,?,?)",
                [entry["source_id"], entry["artifact_id"], source_set["source_set_id"]])
    finally:
        connection.close()
    return {"status": "passed", "row_count": len(facts), "source_count": len(frames),
        "geography_count": int(facts.geo_id.nunique()), "metric_count": int(facts.metric_id.nunique()),
        "first_date": str(pd.to_datetime(facts.date).min().date()),
        "last_date": str(pd.to_datetime(facts.date).max().date()), "duplicate_key_count": 0,
        "sources": [e["source_id"] for e in source_set["sources"]], "reconciliation": reconciliation}
