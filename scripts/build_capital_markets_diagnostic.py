"""Build the governed Capital Markets review from an authoritative run directory."""
from __future__ import annotations
import argparse
from pathlib import Path
import time

import pandas as pd

from regime.diagnostics.capital_markets import REVIEW_GEOGRAPHIES, build_capital_markets_evidence, write_review_bundle


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_directory", type=Path)
    parser.add_argument("output_directory", type=Path)
    args = parser.parse_args()
    started = time.perf_counter()
    names = ("normalized_features", "metric_scores", "aligned_metric_scores", "dimension_scores", "axis_scores")
    frames = {name: pd.read_parquet(args.run_directory / f"{name}.parquet") for name in names}
    native = tuple(sorted(set(frames["normalized_features"].geo_id) - set(frames["aligned_metric_scores"].geo_id)))
    if not native:
        raise ValueError("Authoritative artifacts do not expose a distinct native/source grain")
    missing = set(REVIEW_GEOGRAPHIES) - set(frames["dimension_scores"].geo_id)
    if missing:
        raise ValueError(f"Authoritative run is missing governed review geographies: {sorted(missing)}")
    evidence = build_capital_markets_evidence(**frames, native_geo_ids=native)
    review, archive, count = write_review_bundle(evidence, args.output_directory)
    print(f"[capital-markets] authoritative runtime: {time.perf_counter()-started:.3f}s")
    print(f"[capital-markets] review={review} zip={archive} files={count} zip_bytes={archive.stat().st_size}")


if __name__ == "__main__":
    main()
