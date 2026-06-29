from __future__ import annotations
# jobs/full_refresh/run_refresh_bls_ces.py

from jobs.common import print_context, run_module
from core.config import MARKET_DB_PATH


def main() -> int:
    print_context("refresh_bls_ces_full")

    env = {
        "DUCKDB_PATH": str(MARKET_DB_PATH),
        "CES_REFRESH_MODE": "full",
    }

    run_module("sources.bls_ces.ingest", env_overrides=env)
    run_module("sources.bls_ces.validate", env_overrides=env)

    print("[job] refresh_bls_ces_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
