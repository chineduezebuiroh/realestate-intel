from __future__ import annotations
# jobs/run_generate_bls_specs.py

from jobs.common import print_context, run_module


def main() -> int:
    print_context("generate_bls_specs")

    run_module("sources.bls_ces.expand_spec")
    run_module("sources.bls_laus.expand_spec")

    print("[job] generate_bls_specs complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
