#!/usr/bin/env python3
"""Credentialed local BEA-C candidate verification; never publishes or promotes."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bea_monthly import candidate, discover_pin
from sources.bea.artifact import SOURCES


def legacy_parity(artifact: Path, source_id: str, database: Path) -> dict:
    if not database.is_file(): return {"status": "legacy_database_unavailable"}
    import duckdb
    current = pd.read_parquet(artifact / "data.parquet")
    connection = duckdb.connect(str(database), read_only=True)
    try:
        legacy = connection.execute("SELECT geo_id, CAST(date AS VARCHAR) date, value FROM fact_timeseries WHERE source_id=?", [source_id]).fetchdf()
    finally: connection.close()
    left = {(str(r.geo_id), str(r.date)): float(r.value) for r in current.itertuples()}
    right = {(str(r.geo_id), str(r.date)): float(r.value) for r in legacy.itertuples()}
    return {"status": "checked", "exact_match": sum(k in right and right[k] == v for k, v in left.items()),
        "provider_only": len(set(left) - set(right)), "legacy_only": len(set(right) - set(left)),
        "revised": sum(k in right and right[k] != v for k, v in left.items())}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-id", choices=sorted(SOURCES), required=True)
    parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--legacy-db", type=Path, default=Path("data/market_serving.duckdb"))
    args = parser.parse_args(); args.workspace.mkdir(parents=True, exist_ok=True)
    pin, paths, lineage = discover_pin(cycle_id=args.cycle_id, source_id=args.source_id,
        workspace=args.workspace / "acquisition")
    built = candidate(pin=pin, paths=paths, output=args.workspace / "candidate",
        cycle_id=args.cycle_id, acquisition_lineage=lineage)
    report = {"source_id": args.source_id, "pin_id": pin["pin_id"],
        "provider_release_id": pin["provider_release_id"],
        "snapshot_sha256": pin["members"]["normalized_snapshot"]["sha256"],
        "candidate_artifact_id": built["manifest"]["artifact_id"],
        "coverage": built["evidence"]["coverage"],
        "legacy_parity": legacy_parity(args.workspace / "candidate", args.source_id, args.legacy_db)}
    (args.workspace / "verification.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, sort_keys=True)); return 0


if __name__ == "__main__": raise SystemExit(main())
