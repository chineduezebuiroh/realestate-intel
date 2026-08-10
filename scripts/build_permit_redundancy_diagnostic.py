"""Build the governed permit correlation diagnostic from persisted aligned metrics."""
from __future__ import annotations
import argparse
from pathlib import Path
import time
import pandas as pd

from regime.diagnostics.permit_redundancy import build_permit_redundancy_evidence, write_permit_redundancy_bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("output_directory", type=Path)
    args = parser.parse_args(); started = time.perf_counter()
    aligned = pd.read_parquet(args.run_directory / "aligned_metric_scores.parquet")
    evidence = build_permit_redundancy_evidence(aligned)
    review, archive, count = write_permit_redundancy_bundle(evidence, args.output_directory)
    print(f"[permit-redundancy] authoritative runtime: {time.perf_counter()-started:.3f}s")
    print(f"[permit-redundancy] review={review} zip={archive} files={count} zip_bytes={archive.stat().st_size}")


if __name__ == "__main__":
    main()
