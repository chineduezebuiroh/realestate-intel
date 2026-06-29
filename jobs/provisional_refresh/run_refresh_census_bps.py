from __future__ import annotations
# jobs/provisional_refresh/run_refresh_census_bps.py

from jobs.common import print_context, run_module, run_optional_module
from core.config import SERVING_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(SERVING_DB_PATH),
    }

    print_context("refresh_census_bps_incremental", env_overrides=env)

    run_module("sources.census_bps_provisional.ingest", env_overrides=env)
    run_module("sources.census_bps_provisional.transform", env_overrides=env)

    # Optional until serving snapshot/base DB workflow is finalized.
    run_optional_module("sources.census_bps.merge_view", env_overrides=env)

    print("[job] refresh_census_bps_incremental complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
