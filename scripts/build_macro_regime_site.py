"""Build the deployable, county-only Macro Regime static site."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from visualization.regime_snapshot import render_county_site

ROOT = Path(__file__).resolve().parents[1]
DC_GEO_ID = "district_of_columbia_dc__county"


def build(run_dir: Path, output_dir: Path) -> tuple[Path, Path]:
    run_dir = run_dir.resolve()
    output_dir = output_dir.resolve()
    if not run_dir.is_dir():
        raise FileNotFoundError(f"Explicit production run does not exist: {run_dir}")
    if output_dir == run_dir or run_dir in output_dir.parents:
        raise ValueError("Deployment output must be outside the immutable production run")

    # A clean destination prevents stale counties from surviving a later publication.
    if output_dir.exists():
        shutil.rmtree(output_dir)
    counties = [{"geo_id": DC_GEO_ID, "market_name": "Washington DC"}]
    return render_county_site(
        run_dir,
        counties,
        output_dir,
        ROOT / "config/axis_registry.csv",
        ROOT / "config/metric_dimension_registry.csv",
        ROOT / "config/source_metric_registry.csv",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build the Washington DC Macro Regime v0.3 GitHub Pages site from an explicit run."
    )
    parser.add_argument("--run", type=Path, required=True, help="Explicit immutable production-run directory.")
    parser.add_argument("--output", type=Path, required=True, help="Clean deployable site destination.")
    args = parser.parse_args()
    index, manifest = build(args.run, args.output)
    print(index)
    print(manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
