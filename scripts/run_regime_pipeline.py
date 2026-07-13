from __future__ import annotations
# scripts/run_regime_pipeline.py

import argparse
import json
from pathlib import Path

from regime.pipeline_runner import run_regime_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run and persist the RealEstate Intel regime pipeline."
    )

    parser.add_argument(
        "--run-id",
        required=True,
        help="Immutable identifier for this regime run.",
    )

    parser.add_argument(
        "--experiment-id",
        required=True,
        help="Logical experiment or strategy identifier.",
    )

    parser.add_argument(
        "--artifact-root",
        default="artifacts/regime/runs",
        help="Root directory for regime run artifacts.",
    )

    parser.add_argument(
        "--serving-db",
        default="data/market_serving.duckdb",
        help="Serving DuckDB used by the Feature Engine.",
    )

    parser.add_argument(
        "--validation-geo",
        action="append",
        dest="validation_geos",
        help=(
            "Validation geography. Repeat for multiple geographies. "
            "Defaults to DC and Alameda."
        ),
    )

    parser.add_argument(
        "--metadata-json",
        default=None,
        help="Optional JSON object containing additional run metadata.",
    )

    parser.add_argument(
        "--smoothing-experiment-id",
        default=None,
        help=(
            "Optional approved smoothing "
            "experiment policy to apply."
        ),
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    metadata = {}
    if args.metadata_json:
        metadata = json.loads(args.metadata_json)

        if not isinstance(metadata, dict):
            raise ValueError("--metadata-json must decode to a JSON object")

    manifest = run_regime_pipeline(
        run_id=args.run_id,
        experiment_id=args.experiment_id,
        artifact_root=Path(args.artifact_root),
        validation_geo_ids=args.validation_geos,
        serving_db_path=Path(args.serving_db),
        run_metadata=metadata,
        smoothing_experiment_id=args.smoothing_experiment_id,
    )

    print("\n[run_regime_pipeline] COMPLETE")
    print("run_id:", manifest["run_id"])
    print("experiment_id:", manifest["experiment_id"])
    print("status:", manifest["status"])
    print("artifacts:", len(manifest.get("artifacts", {})))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
