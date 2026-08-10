"""Build Visualization MVP v0.1 from an immutable Regime Engine run."""

from __future__ import annotations

import argparse
from pathlib import Path

from visualization.regime_snapshot import render_snapshot

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--geo-id", required=True)
    parser.add_argument("--market-name", default="Washington, DC")
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    html_path, json_path, _ = render_snapshot(args.run_dir, args.geo_id, args.market_name, args.output_dir, ROOT / "config/axis_registry.csv")
    print(html_path)
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
