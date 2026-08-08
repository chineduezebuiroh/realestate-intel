"""Build the focused Phase 4A Affordability derivation-order evidence bundle."""
from __future__ import annotations

import argparse
import html
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from regime.experiments.affordability_derivation_order import build_affordability_derivation_evidence
from regime.affordability_derivation import build_affordability_promotion_evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-metrics", required=True, type=Path,
                        help="Canonical source metric CSV or Parquet from the intended frozen Price/Affordability run")
    parser.add_argument("--source-run-id", required=True,
                        help="Immutable identity of the authoritative source run")
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    source = (pd.read_parquet(args.source_metrics) if args.source_metrics.suffix.lower() == ".parquet"
              else pd.read_csv(args.source_metrics))
    evidence = build_affordability_derivation_evidence(source)
    evidence.tables.update(build_affordability_promotion_evidence(
        source, evidence.tables["affordability_derivation_raw_chronology"],
        evidence.tables["affordability_derivation_feature_chronology"],
    ))
    args.output_dir.mkdir(parents=True, exist_ok=False)
    for name, frame in evidence.tables.items():
        frame.to_csv(args.output_dir / f"{name}.csv", index=False)
    registry = evidence.tables["affordability_derivation_policy_registry"]
    matrix = evidence.tables["affordability_derivation_decision_matrix"]
    review = f"""<!doctype html><meta charset='utf-8'><title>Affordability Derivation-Order Review</title>
<h1>Affordability Derivation-Order Review</h1>
<p><strong>Phase 4A is closed with human-selected AFF-DERIVATION-B promoted. Feature weights remain fixed at 50/20/30 pending Phase 4B.</strong></p>
<p>Source run: <code>{html.escape(args.source_run_id)}</code></p>
<h2>Exact formulas, lineage, policies, and MA12 location</h2>{registry.to_html(index=False)}
<p>The architectures are not generally equivalent. Payment is nonlinear in mortgage rate;
income forward-fill boundaries also make MA12(price)/income differ from MA12(price/income).</p>
<h2>Chronology, stability, turning points, divergence, and event review</h2>
<p>See the namespaced CSV evidence tables. Empty downstream tables mean authoritative
production scoring context was not supplied; they are not silently reconstructed.</p>
<h2>Affordability dimension and Demand-axis/regime context</h2>
<p>Context remains secondary and requires the authoritative frozen run.</p>
<h2>Decision matrix</h2>{matrix.to_html(index=False)}
"""
    (args.output_dir / "affordability_derivation_order_review.html").write_text(review, encoding="utf-8")


if __name__ == "__main__":
    main()
