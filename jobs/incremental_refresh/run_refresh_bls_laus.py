from __future__ import annotations
# jobs/run_refresh_laus.py

from jobs.common import print_context, run_module

def main() -> int:
    print_context("refresh_laus")
    run_module("sources.bls_laus.ingest")
    run_module("sources.bls_laus.validate")
    print("[job] refresh_laus complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
