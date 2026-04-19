from __future__ import annotations
# jobs/run_refresh_census.py

from jobs.common import print_context, run_module


def main() -> int:
    print_context("refresh_census")

    run_module("sources.census.expand_spec")
    run_module("sources.census.ingest")
    run_module("sources.census.transform")
    run_module("sources.census.validate")

    run_module("sources.census_bps.ingest")
    run_module("sources.census_bps.transform")

    run_module("sources.census_bps_provisional.ingest")
    run_module("sources.census_bps_provisional.transform")

    run_module("sources.census_bps.merge_view")

    run_module("sources.census_nrc_fred.ingest")
    run_module("sources.census_nrc_fred.transform")

    print("[job] refresh_census complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
