from __future__ import annotations
# jobs/full_refresh/run_refresh_census_bps.py

from jobs.common import print_context, run_module, run_optional_module
from core.config import FULL_DB_PATH


def main() -> int:
    env = {
        "DUCKDB_PATH": str(FULL_DB_PATH),
    }

    print_context("refresh_census_bps_full", env_overrides=env)

    run_module("sources.census_bps.ingest", env_overrides=env)
    run_module("sources.census_bps.transform", env_overrides=env)

    # Optional because this view only works when the underlying DB/state is available.
    run_optional_module("sources.census_bps.merge_view", env_overrides=env)

    print("[job] refresh_census_bps_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
