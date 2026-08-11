#!/usr/bin/env python3
"""CLI for the diagnostic-only Structural turn lineage trace."""
from argparse import ArgumentParser
from pathlib import Path
from regime.experiments.structural_turn_lineage import build_review

parser = ArgumentParser()
parser.add_argument("--run-dir", type=Path, required=True)
parser.add_argument("--output-dir", type=Path, required=True)
args = parser.parse_args()
build_review(args.run_dir, args.output_dir)
