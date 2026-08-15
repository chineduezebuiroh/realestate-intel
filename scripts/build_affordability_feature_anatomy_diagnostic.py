"""Build the artifact-first Affordability Phase-1 feature-anatomy review."""
from __future__ import annotations
import argparse
from pathlib import Path

from regime.diagnostics.affordability_feature_anatomy import build, load_run, write_review

DEFAULT_RUN = Path("artifacts/regime/runs/county_labor_demand_market_context_candidate_20260814")
DEFAULT_OUTPUT = Path("artifacts/regime/comparisons/affordability_feature_anatomy")


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("input_run", type=Path, nargs="?", default=DEFAULT_RUN)
    parser.add_argument("output_dir", type=Path, nargs="?", default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    write_review(build(load_run(args.input_run), root), args.output_dir)
    print(f"[affordability-phase1] input={args.input_run} output={args.output_dir} recommendation=none production=unchanged")


if __name__ == "__main__":
    main()
