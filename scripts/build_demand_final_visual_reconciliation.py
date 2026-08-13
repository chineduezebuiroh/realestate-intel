#!/usr/bin/env python3
"""Build the lightweight persisted-evidence Demand finalist reconciliation."""
from argparse import ArgumentParser
from pathlib import Path

from regime.diagnostics.demand_final_visual_reconciliation import build


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--calibration", type=Path,
        default=Path("artifacts/regime/comparisons/laus_ma_window_calibration"))
    parser.add_argument("--output", type=Path,
        default=Path("artifacts/regime/comparisons/demand_final_visual_reconciliation"))
    args = parser.parse_args(); print(build(args.calibration, args.output))


if __name__ == "__main__": main()
