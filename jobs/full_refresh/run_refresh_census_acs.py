from __future__ import annotations
# jobs/full_refresh/run_refresh_census_acs.py

from jobs.common import print_context, run_module
from core.config import FULL_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(FULL_DB_PATH),
        "CENSUS_REFRESH_MODE": "full",
    }

    print_context("refresh_census_acs_full", env_overrides=env)

    run_module("sources.census_acs.expand_spec", env_overrides=env)
    run_module("sources.census_acs.ingest", env_overrides=env)
    run_module("sources.census_acs.transform", env_overrides=env)
    run_module("sources.census_acs.validate", env_overrides=env)

    print("[job] refresh_census_acs_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
