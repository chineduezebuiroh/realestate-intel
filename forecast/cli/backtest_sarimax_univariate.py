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
    ap.add_argument("--order", type=str, default=None,
                    help='ARIMA order as "p,d,q" (e.g. "0,1,0"). If omitted, use default/spec search.')
    ap.add_argument("--seasonal-order", type=str, default=None,
                    help='Seasonal order as "P,D,Q,s" (e.g. "1,1,1,12"). If omitted, use default/spec search.')
    ap.add_argument("--trend", type=str, default=None,
                    help='Trend: none|c|t|ct. If omitted, use runner default.')
    ap.add_argument("--model-version", type=str, default=None,
                    help="Optional model_version tag to write into forecast_runs.model_version")
    ap.add_argument(
        "--use-month-dummies",
        action="store_true",
        help="Include deterministic month-of-year dummy exogenous variables.",
    )

    args = ap.parse_args(argv)

    def _parse_int_tuple(s: str, n: int, name: str):
        parts = [p.strip() for p in s.split(",")]
        if len(parts) != n:
            raise ValueError(f"{name} must have {n} ints, got {len(parts)}: {s!r}")
        return tuple(int(p) for p in parts)
    
    order = _parse_int_tuple(args.order, 3, "--order") if args.order else None
    seasonal_order = _parse_int_tuple(args.seasonal_order, 4, "--seasonal-order") if args.seasonal_order else None
    
    trend = args.trend
    if trend is not None:
        trend = trend.strip().lower()
        if trend in ("none", "null", ""):
            trend = None
        elif trend not in ("c", "t", "ct"):
            raise ValueError("--trend must be one of: none|c|t|ct")

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
        order=order,
        seasonal_order=seasonal_order,
        trend=trend,
        model_version=args.model_version,
        use_month_dummies=bool(args.use_month_dummies),
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
