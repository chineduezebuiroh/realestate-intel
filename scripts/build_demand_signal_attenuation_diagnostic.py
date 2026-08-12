#!/usr/bin/env python3
"""CLI for the governed Demand signal attenuation diagnostic."""
from argparse import ArgumentParser
from pathlib import Path

from regime.experiments.demand_signal_attenuation import build_review

parser=ArgumentParser()
parser.add_argument("--run-dir",type=Path,required=True)
parser.add_argument("--output-dir",type=Path,required=True)
args=parser.parse_args()
result=build_review(args.run_dir,args.output_dir)
print(f"Demand attenuation diagnostic written to {result}")
