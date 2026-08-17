#!/usr/bin/env python3
"""Build the artifact-first Supply Phase-2 feature-weight review package."""
from pathlib import Path
import argparse
from regime.diagnostics.supply_feature_weight_calibration import build, load_run, write_review

DEFAULT_RUN = Path("artifacts/regime/runs/affordability_ma12_p4_production_20260816")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--output", type=Path, default=Path("artifacts/regime/comparisons/supply_feature_weight_calibration"))
    args = parser.parse_args()
    write_review(build(load_run(args.input), Path(".")), args.output)
    print(f"Supply Phase-2 diagnostic written to {args.output}")

if __name__ == "__main__":
    main()
