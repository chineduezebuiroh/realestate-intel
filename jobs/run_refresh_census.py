from __future__ import annotations
# jobs/run_refresh_census.py

from jobs.common import print_context, run_module, run_optional_module


def main() -> int:
    print_context("refresh_census")

    # ACS
    run_module("sources.census.expand_spec")
    run_module("sources.census.ingest")
    run_module("sources.census.transform")
    run_module("sources.census.validate")

    # BPS compiled (required)
    run_module("sources.census_bps.ingest")
    run_module("sources.census_bps.transform")

    # BPS provisional (optional / flaky upstream)
    provisional_ok = run_optional_module("sources.census_bps_provisional.ingest")
    if provisional_ok:
        provisional_ok = run_optional_module("sources.census_bps_provisional.transform")

    # Merge view only if provisional path succeeded
    if provisional_ok:
        run_optional_module("sources.census_bps.merge_view")
    else:
        print("[job][warn] skipping sources.census_bps.merge_view because provisional data was unavailable")

    # NRC
    run_module("sources.census_nrc_fred.ingest")
    run_module("sources.census_nrc_fred.transform")

    print("[job] refresh_census complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
