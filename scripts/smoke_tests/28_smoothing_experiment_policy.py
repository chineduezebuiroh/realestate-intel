from __future__ import annotations
# scripts/smoke_tests/28_smoothing_experiment_policy.py

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
        for policy in experiment.policies:
            rows.append(
                {
                    "experiment_id": (
                        experiment_id
                    ),
                    "experiment_name": (
                        experiment.experiment_name
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
                        policy.transform_strategy
                    ),
                    "level_window": (
                        policy.level_window
                    ),
                    (
                        "short_denominator_"
                        "window"
                    ): (
                        policy
                        .short_denominator_window
                    ),
                    "long_window": (
                        policy.long_window
                    ),
                    "long_lag_periods": (
                        policy.long_lag_periods
                    ),
                    "recompute_dependents": (
                        policy
                        .recompute_dependents
                    ),
                }
            )

    import pandas as pd

    summary = pd.DataFrame(rows)

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
            "Unexpected smoothing experiment IDs. "
            f"Expected {sorted(expected_ids)}, "
            f"found {sorted(actual_ids)}"
        )

    baseline = experiments[
        BASELINE_EXPERIMENT_ID
    ]

    if not baseline.is_baseline:
        raise AssertionError(
            "Baseline experiment is not recognized "
            "as baseline"
        )

    inventory = experiments[
        "inventory_ma3"
    ]

    if set(
        inventory.metric_keys
    ) != {
        "active_inventory"
    }:
        raise AssertionError(
            "Inventory challenger policy mismatch"
        )

    price = experiments[
        "price_family_ma3"
    ]

    if set(
        price.metric_keys
    ) != {
        "median_sale_price",
        "median_ppsf",
    }:
        raise AssertionError(
            "Price-family challenger policy mismatch"
        )

    if set(
        price.dependency_roots
    ) != {
        "median_sale_price"
    }:
        raise AssertionError(
            "Price-family dependency root mismatch"
        )

    combined = experiments[
        "inventory_price_ma3"
    ]

    if set(
        combined.metric_keys
    ) != {
        "active_inventory",
        "median_sale_price",
        "median_ppsf",
    }:
        raise AssertionError(
            "Combined challenger policy mismatch"
        )

    for experiment_id in (
        "inventory_ma3",
        "price_family_ma3",
        "inventory_price_ma3",
    ):
        for policy in (
            experiments[
                experiment_id
            ].policies
        ):
            if (
                policy.level_window,
                policy.short_denominator_window,
                policy.long_window,
                policy.long_lag_periods,
            ) != (
                3,
                3,
                3,
                12,
            ):
                raise AssertionError(
                    "Unexpected MA feature contract for "
                    f"{experiment_id}/{policy.metric_key}"
                )

    print(
        "\n[smoothing_policy] OK"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
