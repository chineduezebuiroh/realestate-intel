from __future__ import annotations
# jobs/run_refresh_redfin.py

from pathlib import Path

from jobs.common import print_context, run_module


RAW_REDFIN_DIR = Path("data/redfin/raw")


def main() -> int:
    print_context("refresh_redfin")
    run_module("scripts.build_dims")

    if not RAW_REDFIN_DIR.exists():
        raise SystemExit(
            "[job][err] Redfin raw directory missing: data/redfin/raw. "
            "This workflow currently requires raw Redfin files to already exist in the repo/workspace. "
            "Use local refresh for now, or add a hosted raw-file acquisition step before sources.redfin.ingest."
        )

    run_module("sources.redfin.ingest")
    run_module("sources.redfin.transform")
    print("[job] refresh_redfin complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
