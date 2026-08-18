#!/usr/bin/env python3
"""Build the artifact-first Capital Markets Phase-2 review package."""
from pathlib import Path
import argparse
from regime.diagnostics.capital_markets_feature_weight_calibration import build, load_run, write_review

DEFAULT_RUN = Path("artifacts/regime/runs/supply_s8_production_20260817")
DEFAULT_OUTPUT = Path("artifacts/regime/comparisons/capital_markets_feature_weight_calibration")

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--input",type=Path,default=DEFAULT_RUN); parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT); args=parser.parse_args()
    write_review(build(load_run(args.input),Path(".")),args.output)
    print(f"Capital Markets Phase-2 diagnostic written to {args.output}")

if __name__ == "__main__": main()
