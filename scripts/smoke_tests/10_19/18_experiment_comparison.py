from __future__ import annotations
# scripts/smoke_tests/10_19/18_experiment_comparison.py

from regime.experiment_comparison import (
    DEFAULT_RUN_IDS,
    add_baseline_deltas,
    build_experiment_comparison,
)


def main() -> int:
    comparison = build_experiment_comparison(
        run_ids=DEFAULT_RUN_IDS,
    )

    comparison = add_baseline_deltas(comparison)

    print("[experiment_comparison] rows:", len(comparison))
    print("[experiment_comparison] runs:", comparison["run_id"].nunique())
    print("[experiment_comparison] geos:", comparison["geo_id"].nunique())

    print("\n[experiment_comparison] core results:")
    print(
        comparison[
            [
                "run_id",
                "geo_id",
                "major_transitions",
                "major_transitions_pct_vs_baseline",
                "minor_transitions",
                "minor_transitions_pct_vs_baseline",
                "recovery_hypersupply_flips",
                "recovery_hypersupply_flips_pct_vs_baseline",
                "mean_abs_supply_delta",
                "mean_abs_supply_delta_pct_vs_baseline",
                "p90_abs_supply_delta",
                "p90_abs_supply_delta_pct_vs_baseline",
                "mean_permit_abs_delta_score",
                "mean_permit_abs_delta_score_pct_vs_baseline",
                "max_permit_abs_delta_score",
                "max_permit_abs_delta_score_pct_vs_baseline",
                "mean_regime_strength",
            ]
        ].to_string(index=False)
    )

    required_runs = set(DEFAULT_RUN_IDS)
    actual_runs = set(comparison["run_id"])

    if required_runs != actual_runs:
        raise AssertionError(
            f"Expected runs {sorted(required_runs)}, "
            f"found {sorted(actual_runs)}"
        )

    print("\n[experiment_comparison] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
