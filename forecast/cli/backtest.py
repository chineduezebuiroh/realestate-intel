from __future__ import annotations
# forecast/cli/backtest.py
#
# LEGACY SHIM ONLY.
# This entrypoint exists temporarily to keep older workflows working.
# Phase C canonical backtests must use:
#   - forecast/cli/backtest_xgb_forecast.py
#   - forecast/cli/backtest_xgb_selector.py
#   - forecast/cli/backtest_sarimax_univariate.py
#   - (later) forecast/cli/backtest_sarimax_exog_bridge.py / livefaithful
#
# This file intentionally does NOT attempt to construct Phase C identity keys.

import argparse


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.backtest (LEGACY)")
    p.add_argument("--legacy_ok", action="store_true", help="Allow running legacy batch backtest orchestration.")
    # passthrough args legacy may read internally
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if not args.legacy_ok:
        raise SystemExit(
            "[forecast.cli.backtest] REFUSING: this is a legacy shim.\n"
            "Use a specific Phase C CLI (e.g. forecast.cli.backtest_sarimax_univariate) instead.\n"
            "If you really intend to run legacy orchestration, pass --legacy_ok."
        )

    from forecast.run_backtest_batch import main as legacy_main
    return int(legacy_main())


if __name__ == "__main__":
    raise SystemExit(main())
