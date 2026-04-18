from __future__ import annotations
# jobs/run_refresh_redfin.py

from jobs.common import print_context, run_module


def main() -> int:
    print_context("refresh_redfin")
    run_module("scripts.build_dims")
    run_module("sources.redfin.ingest")
    run_module("sources.redfin.transform")
    print("[job] refresh_redfin complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
