from __future__ import annotations
# forecast/cli/live_sarimax_exog.py

import argparse

from forecast.models.sarimax_exog.live_runner import run_live_latest_artifact


def main() -> int:
    ap = argparse.ArgumentParser(description="Live SARIMAX(exog) using latest design_matrix artifact for a target.")
    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--horizon", type=int, default=12)
    ap.add_argument("--batch_id", required=True)
    ap.add_argument("--runs_root", default="runs")
    ap.add_argument("--artifact_root", default="runs")
    ap.add_argument("--inactive", action="store_true", help="Write run as inactive (is_active=False)")
    ap.add_argument("--prefer_batch_id", default=None)

    args = ap.parse_args()

    run_id = run_live_latest_artifact(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=str(args.property_type_id),
        horizon=int(args.horizon),
        batch_id=args.batch_id,
        runs_root=args.runs_root,
        artifact_root=args.artifact_root,
        is_active=(not args.inactive),
        prefer_batch_id=args.prefer_batch_id,
    )
    print(f"live run_id = {run_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
