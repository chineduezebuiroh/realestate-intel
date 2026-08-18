#!/usr/bin/env python3
"""Build the targeted corrected-polarity spread review package."""
from pathlib import Path
import argparse
from regime.diagnostics.spread_10y_2y_feature_revalidation import load_run, build, write_review

DEFAULT_INPUT=Path("artifacts/regime/runs/capital_markets_spread_polarity_repair_20260818")
DEFAULT_OUTPUT=Path("artifacts/regime/comparisons/spread_10y_2y_feature_revalidation")

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--input",type=Path,default=DEFAULT_INPUT); parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT); args=parser.parse_args()
    tables=build(load_run(args.input),Path(".")); write_review(tables,args.output)
    print(f"Targeted spread revalidation written to {args.output}; no winner or promotion")

if __name__=="__main__": main()
