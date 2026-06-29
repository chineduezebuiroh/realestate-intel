from __future__ import annotations
# jobs/full_refresh/run_refresh_ces.py

from jobs.common import print_context, run_module

def main() -> int:
    print_context("refresh_ces")
    run_module("sources.bls_ces.ingest")
    run_module("sources.bls_ces.validate")
    print("[job] refresh_ces complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
