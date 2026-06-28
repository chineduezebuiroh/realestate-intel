from __future__ import annotations
# jobs/run_build_serving_snapshot.py

from jobs.common import print_context, run_module

def main() -> int:
    print_context("build_serving_snapshot")
    run_module("scripts.build_serving_snapshot")
    print("[job] build_serving_snapshot complete")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
