#!/usr/bin/env python3
"""Build the bounded Structural/Cyclical Core Demand balance review."""
from argparse import ArgumentParser
from pathlib import Path

from regime.experiments.structural_cyclical_balance_calibration import RUN_ID, build_review


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "--run",
        type=Path,
        default=Path("artifacts/regime/runs") / RUN_ID,
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(
            "artifacts/regime/comparisons/structural_cyclical_balance_calibration"
        ),
    )
    args = parser.parse_args()
    print(build_review(args.run, args.output))


if __name__ == "__main__":
    main()
