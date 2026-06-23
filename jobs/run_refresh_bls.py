from __future__ import annotations
# jobs/run_refresh_bls.py

from jobs.common import print_context, run_module


def run_step(module: str, failures: list[str]) -> None:
    try:
        run_module(module)
    except Exception as exc:
        print(f"[job][ERROR] {module} failed: {exc}")
        failures.append(module)


def main() -> int:
    print_context("refresh_bls")

    failures: list[str] = []

    run_step("sources.bls_ces.expand_spec", failures)
    run_step("sources.bls_ces.ingest", failures)
    run_step("sources.bls_ces.validate", failures)

    run_step("sources.bls_laus.expand_spec", failures)
    run_step("sources.bls_laus.ingest", failures)
    run_step("sources.bls_laus.validate", failures)

    if failures:
        print("[job][FAIL] refresh_bls completed with failures:")
        for module in failures:
            print(f"  - {module}")
        return 1

    print("[job] refresh_bls complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
