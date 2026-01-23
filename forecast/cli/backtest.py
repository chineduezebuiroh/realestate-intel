from __future__ import annotations
# forecast/cli/backtest.py

import argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.backtest")
    # Keep minimal: pass through to existing runner for now
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    return p

def main(argv: list[str] | None = None) -> int:
    _ = build_parser().parse_args(argv)

    # Phase C Step 0: canonical entrypoint wrapper only.
    # Defer to existing orchestration.
    from forecast.run_backtest_batch import main as legacy_main
    return int(legacy_main())  # legacy main probably reads args internally; keep as-is

if __name__ == "__main__":
    raise SystemExit(main())
