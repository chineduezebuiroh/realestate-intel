#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path
from regime.experiments.structural_turn_detector_semantics import build_review

parser = ArgumentParser(description="Build Structural turn-detector semantics evidence")
parser.add_argument("--run-dir", required=True, type=Path)
parser.add_argument("--output-dir", required=True, type=Path)
args = parser.parse_args()
build_review(args.run_dir, args.output_dir)
