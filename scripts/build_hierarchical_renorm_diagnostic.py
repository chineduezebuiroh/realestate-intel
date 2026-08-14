"""Build the diagnostic from an immutable authoritative production run."""
from __future__ import annotations
import argparse
from pathlib import Path
from regime.diagnostics.hierarchical_renorm import build_diagnostic, load_run, write_review

def main() -> None:
    parser=argparse.ArgumentParser()
    parser.add_argument("run_directory",type=Path); parser.add_argument("output_directory",type=Path)
    args=parser.parse_args()
    index=write_review(build_diagnostic(load_run(args.run_directory)),args.output_directory)
    print(f"hierarchical re-normalization review: {index}")

if __name__ == "__main__": main()
