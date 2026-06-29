from __future__ import annotations
# jobs/incremental_refresh/run_refresh_bea_gdp.py

from jobs.common import print_context, run_module
from core.config import SERVING_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(SERVING_DB_PATH),
    }

    print_context("refresh_bea_gdp_incremental", env_overrides=env)
    run_module("sources.bea.ingest_gdp", env_overrides=env)
    run_module("sources.bea.transform_gdp", env_overrides=env)

    print("[job] refresh_bea_gdp_incremental complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
