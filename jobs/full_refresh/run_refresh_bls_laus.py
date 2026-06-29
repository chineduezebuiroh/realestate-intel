from __future__ import annotations
# jobs/full_refresh/run_refresh_bls_laus.py

from jobs.common import print_context, run_module
from core.config import FULL_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(FULL_DB_PATH),
        "LAUS_REFRESH_MODE": "full",
    }

    print_context("refresh_bls_laus_full", env_overrides=env)

    run_module("sources.bls_laus.ingest", env_overrides=env)
    run_module("sources.bls_laus.validate", env_overrides=env)

    print("[job] refresh_bls_laus_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
