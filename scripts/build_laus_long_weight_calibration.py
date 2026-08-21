#!/usr/bin/env python3
"""Build the bounded LAUS MA x long-weighted feature architecture diagnostic review."""
from argparse import ArgumentParser
from pathlib import Path

from regime.experiments.laus_long_weight_calibration import RUN_ID, build_review


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--run", type=Path, default=Path("artifacts/regime/runs") / RUN_ID)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/regime/comparisons/laus_long_weight_calibration"),
    )
    args = parser.parse_args()
    print(build_review(args.run, args.output))


if __name__ == "__main__":
    main()
