#!/usr/bin/env python3
"""Build the persisted-evidence-only LAUS finalist diagnostic."""
from argparse import ArgumentParser
from pathlib import Path

from regime.diagnostics.laus_finalist_stability import build_review


def main() -> None:
    parser=ArgumentParser()
    parser.add_argument("--source",type=Path,default=Path("artifacts/regime/comparisons/laus_long_weight_calibration"))
    parser.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/laus_finalist_stability"))
    args=parser.parse_args(); print(build_review(args.source,args.output))


if __name__ == "__main__": main()
