from __future__ import annotations
# scripts/smoke_tests/20_29/28_smoothing_experiment_policy.py

import pandas as pd

from regime.experiments.smoothing_policy import (
    BASELINE_EXPERIMENT_ID,
    EXPECTED_CHALLENGER_IDS,
    load_smoothing_experiments,
)


def main() -> int:
    experiments = (
        load_smoothing_experiments(
            validate=True
        )
    )

    print(
        "[smoothing_policy] experiments:",
        len(experiments),
    )

    rows: list[
        dict[str, object]
    ] = []

    for (
        experiment_id,
        experiment,
    ) in experiments.items():
        for policy in (
            experiment.policies
        ):
            rows.append(
                {
                    "experiment_id": (
                        experiment_id
                    ),
                    "experiment_name": (
                        experiment
                        .experiment_name
                    ),
                    "parent_run": (
                        experiment.parent_run
                    ),
                    "metric_key": (
                        policy.metric_key
                    ),
                    "policy_role": (
                        policy.policy_role
                    ),
                    "transform_strategy": (
                        policy
                        .transform_strategy
                    ),
                    "level_window": (
                        policy.level_window
                    ),
                    "short_window": (
                        policy.short_window
                    ),
                    "short_lag_periods": (
                        policy
                        .short_lag_periods
                    ),
                    "long_window": (
                        policy.long_window
                    ),
                    "long_lag_periods": (
                        policy
                        .long_lag_periods
                    ),
                    "recompute_dependents": (
                        policy
                        .recompute_dependents
                    ),
                }
            )

    summary = pd.DataFrame(
        rows
    )

    print(
        "\n[smoothing_policy] "
        "resolved experiment matrix:"
    )

    print(
        summary.sort_values(
            [
                "experiment_id",
                "metric_key",
            ]
        ).to_string(
            index=False
        )
    )

    expected_ids = {
        BASELINE_EXPERIMENT_ID,
        *EXPECTED_CHALLENGER_IDS,
    }

    actual_ids = set(
        experiments
    )

    if actual_ids != expected_ids:
        raise AssertionError(
            "Unexpected smoothing "
            "experiment IDs. "
            f"Expected "
            f"{sorted(expected_ids)}, "
            f"found "
            f"{sorted(actual_ids)}"
        )

    baseline = experiments[
        BASELINE_EXPERIMENT_ID
    ]

    if not baseline.is_baseline:
        raise AssertionError(
            "Baseline experiment is not "
            "recognized as baseline"
        )

    if set(
        baseline.metric_keys
    ) != {
        "*",
    }:
        raise AssertionError(
            "Baseline metric policy mismatch"
        )

    momentum = experiments[
        "inventory_ma3_momentum"
    ]

    if set(
        momentum.metric_keys
    ) != {
        "active_inventory",
    }:
        raise AssertionError(
            "Momentum challenger metric "
            "policy mismatch"
        )

    momentum_policy = (
        momentum.policy_for(
            "active_inventory"
        )
    )

    if momentum_policy is None:
        raise AssertionError(
            "Momentum challenger policy "
            "could not be resolved"
        )

    if (
        momentum_policy
        .transform_strategy
        != "ma_momentum"
    ):
        raise AssertionError(
            "Momentum challenger transform "
            "strategy mismatch"
        )

    if (
        momentum_policy.level_window,
        momentum_policy.short_window,
        momentum_policy.short_lag_periods,
        momentum_policy.long_window,
        momentum_policy.long_lag_periods,
    ) != (
        3,
        3,
        3,
        3,
        12,
    ):
        raise AssertionError(
            "Momentum challenger feature "
            "contract mismatch"
        )

    deviation = experiments[
        "inventory_ma3_deviation"
    ]

    if set(
        deviation.metric_keys
    ) != {
        "active_inventory",
    }:
        raise AssertionError(
            "Deviation challenger metric "
            "policy mismatch"
        )

    deviation_policy = (
        deviation.policy_for(
            "active_inventory"
        )
    )

    if deviation_policy is None:
        raise AssertionError(
            "Deviation challenger policy "
            "could not be resolved"
        )

    if (
        deviation_policy
        .transform_strategy
        != "ma_deviation"
    ):
        raise AssertionError(
            "Deviation challenger transform "
            "strategy mismatch"
        )

    if (
        deviation_policy.level_window,
        deviation_policy.short_window,
        deviation_policy.short_lag_periods,
        deviation_policy.long_window,
        deviation_policy.long_lag_periods,
    ) != (
        3,
        3,
        0,
        3,
        12,
    ):
        raise AssertionError(
            "Deviation challenger feature "
            "contract mismatch"
        )

    if (
        momentum_policy
        .recompute_dependents
    ):
        raise AssertionError(
            "Inventory momentum challenger "
            "must not recompute dependents"
        )

    if (
        deviation_policy
        .recompute_dependents
    ):
        raise AssertionError(
            "Inventory deviation challenger "
            "must not recompute dependents"
        )

    if (
        momentum_policy
        .short_lag_periods
        == deviation_policy
        .short_lag_periods
    ):
        raise AssertionError(
            "Momentum and deviation "
            "challengers must have distinct "
            "short-feature definitions"
        )

    expected_momentum_policies = {
        "inventory_ma6_momentum_lag1": (
            6,
            6,
            1,
            6,
            12,
        ),
        "inventory_ma6_momentum_lag3": (
            6,
            6,
            3,
            6,
            12,
        ),
        "inventory_ma12_momentum_lag1": (
            12,
            12,
            1,
            12,
            12,
        ),
        "inventory_ma12_momentum_lag3": (
            12,
            12,
            3,
            12,
            12,
        ),
    }
    
    for (
        experiment_id,
        expected_contract,
    ) in expected_momentum_policies.items():
        experiment = experiments[
            experiment_id
        ]
    
        policy = experiment.policy_for(
            "active_inventory"
        )
    
        if policy is None:
            raise AssertionError(
                f"{experiment_id}: policy not found"
            )
    
        if (
            policy.transform_strategy
            != "ma_momentum"
        ):
            raise AssertionError(
                f"{experiment_id}: expected "
                "ma_momentum strategy"
            )
    
        actual_contract = (
            policy.level_window,
            policy.short_window,
            policy.short_lag_periods,
            policy.long_window,
            policy.long_lag_periods,
        )
    
        if (
            actual_contract
            != expected_contract
        ):
            raise AssertionError(
                f"{experiment_id}: expected "
                f"{expected_contract}, found "
                f"{actual_contract}"
            )
    
        if policy.recompute_dependents:
            raise AssertionError(
                f"{experiment_id}: inventory "
                "momentum must not recompute "
                "dependents"
            )

    print(
        "\n[smoothing_policy] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
