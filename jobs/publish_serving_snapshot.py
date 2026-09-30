from __future__ import annotations
# jobs/publish_serving_snapshot.py

from pathlib import Path

from jobs.common import print_context, run_module
from scripts.build_serving_snapshot import build_candidate, promote_candidate


SERVING_DB = Path("data/market_serving.duckdb")
FULL_DB = Path("data/market.duckdb")
CANDIDATE_DB = Path("data/market_serving.candidate.duckdb")


def main() -> int:
    print_context("publish_serving_snapshot")

    build_candidate(FULL_DB, CANDIDATE_DB)
    run_module("scripts.validate_serving_snapshot", {"SERVING_DUCKDB_PATH": str(CANDIDATE_DB)})
    promote_candidate(CANDIDATE_DB, SERVING_DB)
    print("[publish] serving snapshot atomically promoted locally; databases are not committed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
