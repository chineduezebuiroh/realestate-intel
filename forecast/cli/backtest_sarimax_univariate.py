from __future__ import annotations
# forecast/cli/backtest_sarimax_univariate.py

import argparse
from typing import Optional, List

from forecast.models.sarimax_univariate.backtest_runner import run_backtest_sarimax_single


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="SARIMAX univariate backtest (DB predictions).")
    ap.add_argument("--metric_id", default="median_sale_price")
    ap.add_argument("--geo_id", default="dc_city")
    ap.add_argument("--property_type_id", default="-1")
    ap.add_argument("--horizon", type=int, default=12)

    ap.add_argument("--min_train_len", type=int, default=120)
    ap.add_argument("--anchor_step_months", type=int, default=1)
    ap.add_argument("--max_anchors", type=int, default=24)
    ap.add_argument("--latest_anchor_offset_months", type=int, default=None)

    ap.add_argument("--batch_id", type=str, default=None)
    ap.add_argument("--data_asof", type=str, default=None)  # YYYY-MM-DD
    ap.add_argument(
        "--anchors",
        type=str,
        default=None,
        help="Comma-separated anchor dates YYYY-MM-DD (optional). If provided, overrides internal anchor selection.",
    )

    args = ap.parse_args(argv)

    run_backtest_sarimax_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=int(args.horizon),
        min_train_len=int(args.min_train_len),
        anchor_step_months=int(args.anchor_step_months),
        max_anchors=int(args.max_anchors),
        latest_anchor_offset_months=args.latest_anchor_offset_months,
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        anchors_csv=args.anchors,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
