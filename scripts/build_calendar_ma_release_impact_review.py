#!/usr/bin/env python3
"""CLI for the governed calendar-MA persisted-run comparison."""
from argparse import ArgumentParser
from pathlib import Path
from regime.experiments.calendar_ma_release_impact import build_review

parser=ArgumentParser()
parser.add_argument("--baseline-run",type=Path,required=True)
parser.add_argument("--candidate-run",type=Path,required=True)
parser.add_argument("--output-dir",type=Path,required=True)
args=parser.parse_args()
result=build_review(args.baseline_run,args.candidate_run,args.output_dir)
print(f"Calendar MA diagnostic review written to {result.output_dir}")
