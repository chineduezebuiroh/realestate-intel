from __future__ import annotations
# forecast/cli/backtest.py

import argparse
from forecast.contracts.keys import TargetKey, SelectorBatchKey


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.backtest")
    # Keep minimal: pass through to existing runner for now
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    return p

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    
    # Phase C Step 1: establish canonical identity (non-fatal for now)
    if args.batch_id:
        # NOTE: these IDs may be numeric in your system; keep as strings
        # We'll tighten once we thread real args through.
        _ = SelectorBatchKey(
            batch_id=args.batch_id,
            target=TargetKey(
                target_metric_id="UNKNOWN",
                target_geo_id="UNKNOWN",
                target_property_type_id="UNKNOWN",
                freq="M",
            ),
        )

    # Phase C Step 0: canonical entrypoint wrapper only.
    # Defer to existing orchestration.
    from forecast.run_backtest_batch import main as legacy_main
    return int(legacy_main())  # legacy main probably reads args internally; keep as-is

if __name__ == "__main__":
    raise SystemExit(main())
