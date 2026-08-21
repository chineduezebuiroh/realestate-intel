"""Build the artifact-only Macro Regime Visualization MVP v0.2.0."""

from __future__ import annotations

import argparse
from pathlib import Path

from visualization.regime_snapshot import (
    VISUALIZATION_VERSION,
    load_county_manifest,
    render_county_site,
    render_snapshot,
)

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=f"Build county Macro Regime dashboards ({VISUALIZATION_VERSION}) from an explicit immutable run."
    )
    parser.add_argument("--run-dir", "--run", dest="run_dir", type=Path, required=True)
    parser.add_argument("--output-dir", "--output", dest="output_dir", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--geo-id", help="Render one county geography.")
    mode.add_argument("--county-manifest", type=Path, help="Render a deterministic county-only site and index.")
    parser.add_argument("--market-name", help="Required display name in single-county mode.")
    args = parser.parse_args()
    axis_registry = ROOT / "config/axis_registry.csv"
    metric_registry = ROOT / "config/metric_dimension_registry.csv"
    source_registry = ROOT / "config/source_metric_registry.csv"
    if args.geo_id:
        if not args.market_name:
            parser.error("--market-name is required with --geo-id")
        html_path, json_path, _ = render_snapshot(
            args.run_dir, args.geo_id, args.market_name, args.output_dir,
            axis_registry, metric_registry, source_registry,
        )
        print(html_path)
        print(json_path)
    else:
        counties = load_county_manifest(args.county_manifest)
        index_path, manifest_path = render_county_site(
            args.run_dir, counties, args.output_dir,
            axis_registry, metric_registry, source_registry,
        )
        print(index_path)
        print(manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
