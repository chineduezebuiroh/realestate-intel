from __future__ import annotations
# jobs/full_refresh/run_generate_bls_specs.py

from jobs.common import print_context, run_module


def run_step(module: str, failures: list[str]) -> None:
    try:
        run_module(module)
    except Exception as exc:
        print(f"[job][ERROR] {module} failed: {exc}")
        failures.append(module)


def main() -> int:
    print_context("generate_bls_specs")

    failures: list[str] = []

    run_step("sources.bls_ces.expand_spec", failures)
    run_step("sources.bls_laus.expand_spec", failures)

    if failures:
        print("[job][FAIL] generate_bls_specs completed with failures:")
        for module in failures:
            print(f"  - {module}")
        return 1

    print("[job] generate_bls_specs complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
