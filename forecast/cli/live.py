from __future__ import annotations
# forecast/cli/live.py

import argparse

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("forecast.cli.live")
    p.add_argument("--batch_id", required=False)
    p.add_argument("--data_asof", required=False)
    p.add_argument("--run_kind", required=False)
    return p

def main(argv: list[str] | None = None) -> int:
    _ = build_parser().parse_args(argv)

    from forecast.run_sarimax_batch import main as legacy_main
    return int(legacy_main())

if __name__ == "__main__":
    raise SystemExit(main())
