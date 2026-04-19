from __future__ import annotations
# jobs/run_refresh_macro.py


from jobs.common import print_context, run_module


def main() -> int:
    print_context("refresh_macro")

    run_module("sources.bea_qgdp.ingest")
    run_module("sources.bea_qgdp.transform")

    run_module("sources.fred_macro.ingest")
    run_module("sources.fred_unemp.ingest")

    print("[job] refresh_macro complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
