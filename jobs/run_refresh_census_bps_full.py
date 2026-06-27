from __future__ import annotations
# jobs/run_refresh_census_bps_full.py

from jobs.common import print_context, run_module, run_optional_module


def main() -> int:
    print_context("refresh_census_bps_full")

    run_module("sources.census_bps.ingest")
    run_module("sources.census_bps.transform")

    # Optional because this view only works when the underlying DB/state is available.
    run_optional_module("sources.census_bps.merge_view")

    print("[job] refresh_census_bps_full complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
