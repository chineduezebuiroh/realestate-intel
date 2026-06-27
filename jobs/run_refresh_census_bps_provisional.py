from __future__ import annotations
# jobs/run_refresh_census_bps_provisional.py

from jobs.common import print_context, run_module, run_optional_module


def main() -> int:
    print_context("refresh_census_bps_provisional")

    run_module("sources.census_bps_provisional.ingest")
    run_module("sources.census_bps_provisional.transform")

    # Optional until serving snapshot/base DB workflow is finalized.
    run_optional_module("sources.census_bps.merge_view")

    print("[job] refresh_census_bps_provisional complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
