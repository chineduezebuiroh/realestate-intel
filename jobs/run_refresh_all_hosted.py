from __future__ import annotations
# jobs/run_refresh_all_hosted.py

from jobs.common import print_context, run_module


def main() -> int:
    print_context("refresh_all_hosted")

    run_module("jobs.run_refresh_bls")
    run_module("jobs.run_refresh_census")
    run_module("jobs.run_refresh_macro")

    print("[job] refresh_all_hosted complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
