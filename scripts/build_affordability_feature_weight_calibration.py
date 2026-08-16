#!/usr/bin/env python3
"""Build the artifact-first Affordability Phase-2 calibration review."""
from pathlib import Path
import argparse
from regime.diagnostics.affordability_feature_weight_calibration import build, load_run, write_review

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("artifacts/regime/runs/price_ma12_p6_production_20260815"))
    parser.add_argument("--output", type=Path, default=Path("artifacts/regime/comparisons/affordability_feature_weight_calibration"))
    args = parser.parse_args()
    write_review(build(load_run(args.input), Path(".")), args.output)
    print(f"Affordability Phase-2 diagnostic written to {args.output}")

if __name__ == "__main__":
    main()
