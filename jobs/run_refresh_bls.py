from __future__ import annotations
# jobs/run_refresh_bls.py

from jobs.common import print_context, run_module


def main() -> int:
    print_context("refresh_bls")

    run_module("sources.bls_ces.expand_spec")
    run_module("sources.bls_ces.ingest")
    run_module("sources.bls_ces.validate")

    run_module("sources.bls_laus.expand_spec")
    run_module("sources.bls_laus.ingest")
    run_module("sources.bls_laus.validate")

    print("[job] refresh_bls complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
