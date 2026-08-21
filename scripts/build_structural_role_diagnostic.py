#!/usr/bin/env python3
from argparse import ArgumentParser
from pathlib import Path
from regime.experiments.structural_role_diagnostic import RUN_ID, build_review

if __name__ == "__main__":
    p=ArgumentParser(); p.add_argument("--run",type=Path,default=Path("artifacts/regime/runs")/RUN_ID); p.add_argument("--output",type=Path,default=Path("artifacts/regime/comparisons/structural_role_diagnostic")); a=p.parse_args(); print(build_review(a.run,a.output))
