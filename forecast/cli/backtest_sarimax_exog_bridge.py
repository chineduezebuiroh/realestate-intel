from __future__ import annotations
# forecast/cli/backtest_sarimax_exog_bridge.py

import argparse
from typing import Optional, List

from forecast.models.sarimax_exog.bridge_runner import run_backtest_sarimax_exog_bridge


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="SARIMAX exog bridge backtest (Phase C).")

    ap.add_argument("--metric_id", required=True)
    ap.add_argument("--geo_id", required=True)
    ap.add_argument("--property_type_id", required=True)
    ap.add_argument("--freq", default="M")

    ap.add_argument("--horizon", type=int, required=True)
    ap.add_argument("--min_train_len", type=int, default=36)

    ap.add_argument("--selector_batch_id", required=True)
    ap.add_argument("--anchors", required=True, help="Comma-separated anchor dates YYYY-MM-DD")

    ap.add_argument("--batch_id", required=True)
    ap.add_argument("--data_asof", required=True)     # YYYY-MM-DD
    ap.add_argument("--artifact_root", required=True)

    ap.add_argument("--run_kind", default="backtest")
    ap.add_argument("--is_active", action="store_true")
    ap.add_argument("--max_exogs_for_sarimax", type=int, default=30)
    ap.add_argument("--min_non_redfin_for_sarimax", type=int, default=10)

    args = ap.parse_args(argv)

    run_backtest_sarimax_exog_bridge(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        freq=args.freq,
        horizon=int(args.horizon),
        min_train_len=int(args.min_train_len),
        selector_batch_id=args.selector_batch_id,
        anchors_csv=args.anchors,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        artifact_root=args.artifact_root,
        run_kind=args.run_kind,
        is_active=bool(args.is_active),
        max_exogs_for_sarimax=args.max_exogs_for_sarimax,
        min_non_redfin_for_sarimax=args.min_non_redfin_for_sarimax,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
