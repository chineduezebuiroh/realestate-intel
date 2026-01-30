from __future__ import annotations
# forecast/cli/live.py

import argparse
from forecast.contracts.keys import TargetKey, SelectorBatchKey


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.live")
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    p.add_argument("--run_kind", required=False)
    return p

def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    
    raise SystemExit(
        "[live] Live orchestration is intentionally disabled right now. "
        "We will wire it after backtest orchestration is stable."
    )
    

if __name__ == "__main__":
    raise SystemExit(main())
