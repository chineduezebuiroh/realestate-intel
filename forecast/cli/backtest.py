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
import sys



def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.backtest (LEGACY)")
    p.add_argument("--legacy_ok", action="store_true", help="Allow running legacy batch backtest orchestration.")
    # passthrough args legacy may read internally
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    return p


def main():
    ap = argparse.ArgumentParser(
        description="Batch backtest orchestration (legacy wrapper).",
    )
    ap.add_argument(
        "--legacy_ok",
        action="store_true",
        help="Allow running legacy batch backtest orchestration (deprecated).",
    )
    ap.add_argument("--batch_id", default=None)
    ap.add_argument("--data_asof", default=None)

    args = ap.parse_args()

    if not args.legacy_ok:
        msg = """
[backtest] This entrypoint is deprecated and DISABLED by default.

Use the canonical per-model backtest CLIs instead:
  - python -m forecast.cli.backtest_sarimax_univariate  --help
  - python -m forecast.cli.backtest_sarimax_exog        --help
  - python -m forecast.cli.backtest_xgb_selector        --help
  - python -m forecast.cli.backtest_xgb_forecast        --help

If you *really* need the legacy batch orchestrator temporarily, re-run with:
  python -m forecast.cli.backtest --legacy_ok [--batch_id ...] [--data_asof YYYY-MM-DD]
""".strip()
        print(msg)
        raise SystemExit(2)

    # Escape hatch: call legacy orchestrator
    from forecast.legacy.run_backtest_batch import main as legacy_main

    # legacy_main likely parses args itself; pass through by re-invoking as module style
    # Simplest: call it directly after setting sys.argv for it.
    sys.argv = ["forecast.legacy.run_backtest_batch"] + (
        (["--batch_id", args.batch_id] if args.batch_id else [])
        + (["--data_asof", args.data_asof] if args.data_asof else [])
    )
    legacy_main()


if __name__ == "__main__":
    main()

