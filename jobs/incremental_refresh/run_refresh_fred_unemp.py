from __future__ import annotations
# jobs/incremental_refresh/run_refresh_fred_unemp.py

from jobs.common import print_context, run_module
from core.config import SERVING_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(SERVING_DB_PATH),
    }

    print_context("refresh_fred_unemp_incremental", env_overrides=env)
    run_module("sources.fred_unemp.ingest", env_overrides=env)

    print("[job] refresh_fred_unemp_incremental complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
