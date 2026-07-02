from __future__ import annotations
# jobs/full_refresh/run_refresh_redfin.py

from jobs.common import print_context, run_module
from core.config import FULL_DB_PATH
from pathlib import Path


RAW_REDFIN_DIR = Path("data/redfin/raw")


def main() -> int:
    if not RAW_REDFIN_DIR.exists():
        raise SystemExit(
            "[job][err] Redfin raw directory missing: data/redfin/raw. "
            "This workflow currently requires raw Redfin files to already exist in the repo/workspace. "
            "Use local refresh for now, or add a hosted raw-file acquisition step before sources.redfin.ingest."
        )
    
    env = {"DUCKDB_PATH": str(FULL_DB_PATH)}

    print_context("refresh_redfin_full", env_overrides=env)

    run_module("scripts.build_dims")

    run_module("sources.redfin.ingest", env_overrides=env)
    run_module("sources.redfin.transform", env_overrides=env)
    run_module("sources.redfin.validate", env_overrides=env)

    print("[job] refresh_redfin_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
    
