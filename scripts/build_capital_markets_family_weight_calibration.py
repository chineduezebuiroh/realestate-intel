#!/usr/bin/env python3
"""Build the diagnostic-only Capital Markets family-weight review."""
from argparse import ArgumentParser
from pathlib import Path
from regime.diagnostics.capital_markets_family_weight_calibration import AUTHORITATIVE_RUN, build, load_run, write_review

DEFAULT_RUN=AUTHORITATIVE_RUN
DEFAULT_OUTPUT=Path("artifacts/regime/comparisons/capital_markets_family_weight_calibration")

def main():
    parser=ArgumentParser(); parser.add_argument("--input",type=Path,default=DEFAULT_RUN); parser.add_argument("--output",type=Path,default=DEFAULT_OUTPUT); args=parser.parse_args()
    tables=build(load_run(args.input),Path(".")); write_review(tables,args.output)
    print(f"Diagnostic-only Capital Markets F0-F9 family-weight review written to {args.output}")

if __name__=="__main__": main()
