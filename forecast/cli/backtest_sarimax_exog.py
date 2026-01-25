from __future__ import annotations
# forecast/cli/backtest_sarimax_exog.py

import argparse
from typing import Optional, List

from forecast.models.sarimax_exog.backtest_runner import run_backtest_sarimax_exog_single


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="SARIMAX exog backtest (DB predictions).")
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

    ap.add_argument("--artifact_root", type=str, required=True)
    ap.add_argument("--xgb_batch_id", type=str, required=True)
    ap.add_argument("--sarimax_max_exog", type=int, default=30)
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument(
        "--anchors",
        type=str,
        default=None,
        help="Comma-separated anchor dates YYYY-MM-DD (optional). Overrides internal anchor selection.",
    )
    ap.add_argument(
        "--exog_method",
        type=str,
        default="seasonal_naive_else_last",
        choices=["seasonal_naive_else_last", "perfect_future"],
        help="How to produce FUTURE exog rows. seasonal_naive_else_last = forecasted exog; perfect_future = realized exog (cheating upper bound).",
    )


    args = ap.parse_args(argv)

    run_backtest_sarimax_exog_single(
        metric_id=args.metric_id,
        geo_id=args.geo_id,
        property_type_id=args.property_type_id,
        horizon=int(args.horizon),
        batch_id=args.batch_id,
        data_asof=args.data_asof,
        min_train_len=int(args.min_train_len),
        anchor_step_months=int(args.anchor_step_months),
        max_anchors=int(args.max_anchors),
        latest_anchor_offset_months=args.latest_anchor_offset_months,
        seed=int(args.seed),
        artifact_root=args.artifact_root,
        xgb_batch_id=args.xgb_batch_id,
        sarimax_max_exog=int(args.sarimax_max_exog),
        anchors_csv=args.anchors,
        exog_method=args.exog_method,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
