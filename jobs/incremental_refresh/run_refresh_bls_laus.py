from __future__ import annotations
# jobs/incremental_refresh/run_refresh_bls_laus.py

from jobs.common import print_context, run_module
from core.config import MARKET_SERVING_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(MARKET_SERVING_DB_PATH),
        "LAUS_REFRESH_MODE": "incremental",
    }

    print_context("refresh_bls_laus_incremental", env_overrides=env)

    run_module("sources.bls_laus.ingest", env_overrides=env)
    run_module("sources.bls_laus.validate", env_overrides=env)

    print("[job] refresh_bls_laus_incremental complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
