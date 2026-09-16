#!/usr/bin/env python3
"""Credentialed local BEA-C pin/candidate proof; never publishes or mutates pointers."""
from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import pandas as pd

from jobs.monthly_refresh.bea_monthly import candidate, discover_pin
from sources.bea.artifact import PRODUCTS


def legacy_parity(artifact: Path, source_id: str, database: Path) -> dict:
    if not database.is_file(): return {"status": "not_run", "reason": f"legacy database unavailable: {database}"}
    import duckdb
    current = pd.read_parquet(artifact / "data.parquet")
    connection = duckdb.connect(str(database), read_only=True)
    try:
        legacy = connection.execute("SELECT geo_id, metric_id, CAST(date AS DATE) date, property_type_id, value FROM fact_timeseries WHERE source_id=?", [source_id]).fetchdf()
    finally: connection.close()
    keys = ["geo_id", "metric_id", "date", "property_type_id"]
    merged = current.merge(legacy, on=keys, how="outer", suffixes=("_candidate", "_legacy"), indicator=True)
    changed = merged[(merged["_merge"] == "both") & (merged.value_candidate != merged.value_legacy)]
    return {"status": "passed" if len(changed) == 0 and set(merged._merge) == {"both"} else "failed",
        "exact_match": int(((merged._merge == "both") & (merged.value_candidate == merged.value_legacy)).sum()),
        "candidate_only": int((merged._merge == "left_only").sum()), "legacy_only": int((merged._merge == "right_only").sum()),
        "changed": len(changed)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--legacy-db", type=Path, default=Path("data/market_serving.duckdb"))
    args = parser.parse_args(); args.output_dir.mkdir(parents=True, exist_ok=True); report = {"cycle_id": args.cycle_id, "sources": []}
    for source_id in sorted(PRODUCTS):
        root = args.output_dir / source_id
        if root.exists(): shutil.rmtree(root)
        pin, paths = discover_pin(cycle_id=args.cycle_id, source_id=source_id, workspace=root / "input")
        built = candidate(pin=pin, paths=paths, output=root / "candidate", cycle_id=args.cycle_id)
        report["sources"].append({"source_id": source_id, "pin_id": pin["pin_id"],
            "semantic_input_sha256": pin["members"]["snapshot"]["evidence"]["semantic_input_sha256"],
            "candidate_artifact_id": built["manifest"]["artifact_id"],
            "coverage": built["evidence"]["coverage"], "legacy_parity": legacy_parity(root / "candidate", source_id, args.legacy_db)})
    (args.output_dir / "verification.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output_dir / "verification.json"); return 0


if __name__ == "__main__": raise SystemExit(main())
