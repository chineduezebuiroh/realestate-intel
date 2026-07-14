from __future__ import annotations
# regime/experiments/smoothing_policy.py

from dataclasses import dataclass
from pathlib import Path

import pandas as pd


DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY = Path(
    "config/metric_smoothing_experiments.csv"
)

SUPPORTED_TRANSFORM_STRATEGIES = {
    "current",
    "ma_momentum",
    "ma_deviation",
    "ma_structural",
}

SUPPORTED_POLICY_ROLES = {
    "baseline",
    "direct",
    "dependency_root",
}

BASELINE_EXPERIMENT_ID = (
    "baseline_current"
)

EXPECTED_CHALLENGER_IDS = {
    "inventory_ma3_momentum",
    "inventory_ma3_deviation",
    "inventory_ma6_structural",
    "inventory_ma12_structural",
}


def _parse_bool(
    value: object,
    *,
    column: str,
) -> bool:
    normalized = (
        str(value)
        .strip()
        .lower()
    )

    if normalized in {
        "true",
        "1",
        "yes",
        "y",
    }:
        return True

    if normalized in {
        "false",
        "0",
        "no",
        "n",
    }:
        return False

    raise ValueError(
        f"Invalid boolean value for "
        f"{column}: {value!r}"
    )


def _parse_nonnegative_int(
    value: object,
    *,
    column: str,
) -> int:
    try:
        parsed = int(value)
    except (
        TypeError,
        ValueError,
    ) as exc:
        raise ValueError(
            f"{column} must be an integer; "
            f"received {value!r}"
        ) from exc

    if parsed < 0:
        raise ValueError(
            f"{column} must be non-negative; "
            f"received {parsed}"
        )

    return parsed


@dataclass(frozen=True)
class SmoothingMetricPolicy:
    experiment_id: str
    metric_key: str
    policy_role: str
    transform_strategy: str
    level_window: int
    short_window: int
    short_lag_periods: int
    long_window: int
    long_lag_periods: int
    recompute_dependents: bool

    @property
    def is_baseline(self) -> bool:
        return (
            self.transform_strategy
            == "current"
        )

    @property
    def is_smoothed(self) -> bool:
        return (
            self.transform_strategy
            in {
                "ma_momentum",
                "ma_deviation",
                "ma_structural",
            }
        )

    def validate(self) -> None:
        if (
            self.policy_role
            not in SUPPORTED_POLICY_ROLES
        ):
            raise ValueError(
                "Unsupported smoothing policy role "
                f"{self.policy_role!r} for "
                f"{self.experiment_id}/"
                f"{self.metric_key}"
            )

        if (
            self.transform_strategy
            not in SUPPORTED_TRANSFORM_STRATEGIES
        ):
            raise ValueError(
                "Unsupported smoothing transform "
                f"strategy "
                f"{self.transform_strategy!r} for "
                f"{self.experiment_id}/"
                f"{self.metric_key}"
            )

        if self.is_baseline:
            if self.metric_key != "*":
                raise ValueError(
                    "The baseline policy must use "
                    "metric_key='*'"
                )

            if any(
                value != 0
                for value in (
                    self.level_window,
                    self.short_window,
                    self.short_lag_periods,
                    self.long_window,
                    self.long_lag_periods,
                )
            ):
                raise ValueError(
                    "Baseline smoothing windows "
                    "and lags must all equal zero"
                )

            if self.recompute_dependents:
                raise ValueError(
                    "Baseline policy cannot "
                    "recompute dependents"
                )

            return

        if self.metric_key == "*":
            raise ValueError(
                "Challenger policies must "
                "identify a metric_key"
            )

        if self.policy_role == "baseline":
            raise ValueError(
                "A challenger metric cannot use "
                "policy_role='baseline'"
            )

        positive_fields = {
            "level_window": (
                self.level_window
            ),
            "short_window": (
                self.short_window
            ),
            "long_window": (
                self.long_window
            ),
            "long_lag_periods": (
                self.long_lag_periods
            ),
        }

        invalid_positive = {
            key: value
            for key, value
            in positive_fields.items()
            if value <= 0
        }

        if invalid_positive:
            raise ValueError(
                "Smoothed challenger windows "
                "must be positive: "
                f"{invalid_positive}"
            )

        if (
            self.transform_strategy
            == "ma_momentum"
        ):
            if (
                self.short_lag_periods
                <= 0
            ):
                raise ValueError(
                    "ma_momentum requires "
                    "short_lag_periods > 0"
                )

        if (
            self.transform_strategy
            == "ma_deviation"
        ):
            if (
                self.short_lag_periods
                != 0
            ):
                raise ValueError(
                    "ma_deviation requires "
                    "short_lag_periods = 0"
                )

        if (
            self.transform_strategy
            == "ma_structural"
        ):
            if self.short_lag_periods != 0:
                raise ValueError(
                    "ma_structural requires "
                    "short_lag_periods = 0"
                )
        
            if self.short_window <= 0:
                raise ValueError(
                    "ma_structural requires "
                    "short_window > 0"
                )
        
            if self.short_window >= self.level_window:
                raise ValueError(
                    "ma_structural requires "
                    "short_window < level_window"
                )
        
            if self.long_window != self.level_window:
                raise ValueError(
                    "ma_structural requires "
                    "long_window == level_window"
                )

        if (
            self.recompute_dependents
            and self.policy_role
            != "dependency_root"
        ):
            raise ValueError(
                "Only dependency_root policies "
                "may set "
                "recompute_dependents=true"
            )

        if (
            self.policy_role
            == "dependency_root"
            and not self.recompute_dependents
        ):
            raise ValueError(
                "dependency_root policies must "
                "set recompute_dependents=true"
            )


@dataclass(frozen=True)
class SmoothingExperiment:
    experiment_id: str
    experiment_name: str
    parent_run: str
    policies: tuple[
        SmoothingMetricPolicy,
        ...
    ]
    notes: tuple[
        str,
        ...
    ]

    @property
    def is_baseline(self) -> bool:
        return (
            self.experiment_id
            == BASELINE_EXPERIMENT_ID
        )

    @property
    def metric_keys(
        self,
    ) -> tuple[str, ...]:
        return tuple(
            policy.metric_key
            for policy in self.policies
        )

    @property
    def dependency_roots(
        self,
    ) -> tuple[str, ...]:
        return tuple(
            policy.metric_key
            for policy in self.policies
            if policy.policy_role
            == "dependency_root"
        )

    def policy_for(
        self,
        metric_key: str,
    ) -> SmoothingMetricPolicy | None:
        matches = [
            policy
            for policy in self.policies
            if policy.metric_key
            == metric_key
        ]

        if not matches:
            return None

        if len(matches) != 1:
            raise AssertionError(
                "Experiment contains duplicate "
                "metric policies: "
                f"{self.experiment_id}/"
                f"{metric_key}"
            )

        return matches[0]

    def validate(self) -> None:
        if not self.experiment_id:
            raise ValueError(
                "experiment_id cannot be blank"
            )

        if not self.experiment_name:
            raise ValueError(
                f"{self.experiment_id}: "
                "experiment_name cannot be blank"
            )

        if not self.parent_run:
            raise ValueError(
                f"{self.experiment_id}: "
                "parent_run cannot be blank"
            )

        if not self.policies:
            raise ValueError(
                f"{self.experiment_id}: "
                "no policies configured"
            )

        for policy in self.policies:
            policy.validate()

        metric_keys = [
            policy.metric_key
            for policy in self.policies
        ]

        duplicates = sorted(
            {
                metric_key
                for metric_key
                in metric_keys
                if metric_keys.count(
                    metric_key
                ) > 1
            }
        )

        if duplicates:
            raise ValueError(
                f"{self.experiment_id}: "
                "duplicate metric policies "
                f"{duplicates}"
            )

        if self.is_baseline:
            if len(self.policies) != 1:
                raise ValueError(
                    "Baseline experiment must "
                    "contain exactly one policy"
                )

            if not self.policies[
                0
            ].is_baseline:
                raise ValueError(
                    "Baseline experiment must "
                    "use current strategy"
                )

        else:
            if any(
                policy.is_baseline
                for policy
                in self.policies
            ):
                raise ValueError(
                    f"{self.experiment_id}: "
                    "challenger contains a "
                    "baseline policy"
                )


def load_smoothing_experiments(
    path: str | Path = (
        DEFAULT_SMOOTHING_EXPERIMENT_REGISTRY
    ),
    *,
    validate: bool = True,
) -> dict[
    str,
    SmoothingExperiment,
]:
    registry_path = Path(path)

    if not registry_path.exists():
        raise FileNotFoundError(
            "Smoothing experiment registry "
            "not found: "
            f"{registry_path}"
        )

    frame = pd.read_csv(
        registry_path,
        dtype=str,
    ).fillna("")

    required_columns = {
        "experiment_id",
        "experiment_name",
        "parent_run",
        "metric_key",
        "policy_role",
        "transform_strategy",
        "level_window",
        "short_window",
        "short_lag_periods",
        "long_window",
        "long_lag_periods",
        "recompute_dependents",
        "enabled",
        "notes",
    }

    missing = (
        required_columns
        - set(frame.columns)
    )

    if missing:
        raise ValueError(
            "Smoothing registry is missing "
            f"columns: {sorted(missing)}"
        )

    frame["enabled_parsed"] = frame[
        "enabled"
    ].map(
        lambda value: _parse_bool(
            value,
            column="enabled",
        )
    )

    frame = frame[
        frame["enabled_parsed"]
    ].copy()

    if frame.empty:
        raise ValueError(
            "Smoothing experiment registry "
            "contains no enabled rows"
        )

    duplicate_rows = frame.duplicated(
        subset=[
            "experiment_id",
            "metric_key",
        ],
        keep=False,
    )

    if duplicate_rows.any():
        raise ValueError(
            "Duplicate experiment/metric rows:\n"
            + frame.loc[
                duplicate_rows
            ].to_string(index=False)
        )

    experiments: dict[
        str,
        SmoothingExperiment,
    ] = {}

    for experiment_id, group in (
        frame.groupby(
            "experiment_id",
            sort=True,
        )
    ):
        experiment_names = (
            group["experiment_name"]
            .drop_duplicates()
            .tolist()
        )

        parent_runs = (
            group["parent_run"]
            .drop_duplicates()
            .tolist()
        )

        if len(experiment_names) != 1:
            raise ValueError(
                f"{experiment_id}: "
                "conflicting experiment names "
                f"{experiment_names}"
            )

        if len(parent_runs) != 1:
            raise ValueError(
                f"{experiment_id}: "
                "conflicting parent runs "
                f"{parent_runs}"
            )

        policies: list[
            SmoothingMetricPolicy
        ] = []

        for row in group.itertuples(
            index=False
        ):
            policy = (
                SmoothingMetricPolicy(
                    experiment_id=(
                        experiment_id
                    ),
                    metric_key=(
                        row.metric_key.strip()
                    ),
                    policy_role=(
                        row.policy_role.strip()
                    ),
                    transform_strategy=(
                        row
                        .transform_strategy
                        .strip()
                    ),
                    level_window=(
                        _parse_nonnegative_int(
                            row.level_window,
                            column=(
                                "level_window"
                            ),
                        )
                    ),
                    short_window=(
                        _parse_nonnegative_int(
                            row.short_window,
                            column=(
                                "short_window"
                            ),
                        )
                    ),
                    short_lag_periods=(
                        _parse_nonnegative_int(
                            row
                            .short_lag_periods,
                            column=(
                                "short_lag_periods"
                            ),
                        )
                    ),
                    long_window=(
                        _parse_nonnegative_int(
                            row.long_window,
                            column=(
                                "long_window"
                            ),
                        )
                    ),
                    long_lag_periods=(
                        _parse_nonnegative_int(
                            row
                            .long_lag_periods,
                            column=(
                                "long_lag_periods"
                            ),
                        )
                    ),
                    recompute_dependents=(
                        _parse_bool(
                            row
                            .recompute_dependents,
                            column=(
                                "recompute_"
                                "dependents"
                            ),
                        )
                    ),
                )
            )

            policies.append(policy)

        experiment = (
            SmoothingExperiment(
                experiment_id=(
                    experiment_id
                ),
                experiment_name=(
                    experiment_names[
                        0
                    ].strip()
                ),
                parent_run=(
                    parent_runs[
                        0
                    ].strip()
                ),
                policies=tuple(
                    policies
                ),
                notes=tuple(
                    note
                    for note in (
                        group["notes"]
                        .astype(str)
                        .str.strip()
                        .tolist()
                    )
                    if note
                ),
            )
        )

        if validate:
            experiment.validate()

        experiments[
            experiment_id
        ] = experiment

    if validate:
        _validate_experiment_matrix(
            experiments
        )

    return experiments


def _validate_experiment_matrix(
    experiments: dict[
        str,
        SmoothingExperiment,
    ],
) -> None:
    expected_ids = {
        BASELINE_EXPERIMENT_ID,
        *EXPECTED_CHALLENGER_IDS,
    }

    actual_ids = set(
        experiments
    )

    if actual_ids != expected_ids:
        raise ValueError(
            "Smoothing experiment matrix "
            "mismatch. "
            f"Expected {sorted(expected_ids)}, "
            f"found {sorted(actual_ids)}"
        )

    parent_runs = {
        experiment.parent_run
        for experiment
        in experiments.values()
    }

    if parent_runs != {
        "macro_regime_v1_bps120_sources"
    }:
        raise ValueError(
            "All initial smoothing "
            "experiments must use "
            "macro_regime_v1_bps120_sources "
            "as parent_run. "
            f"Found {sorted(parent_runs)}"
        )

    baseline = experiments[
        BASELINE_EXPERIMENT_ID
    ]

    if set(
        baseline.metric_keys
    ) != {
        "*",
    }:
        raise ValueError(
            "Baseline experiment must "
            "contain only metric_key='*'"
        )

    expected_policies = {
    
        "inventory_ma3_momentum": {
            "metric_key": "active_inventory",
            "transform_strategy": "ma_momentum",
            "level_window": 3,
            "short_window": 3,
            "short_lag_periods": 3,
            "long_window": 3,
            "long_lag_periods": 12,
        },
    
        "inventory_ma3_deviation": {
            "metric_key": "active_inventory",
            "transform_strategy": "ma_deviation",
            "level_window": 3,
            "short_window": 3,
            "short_lag_periods": 0,
            "long_window": 3,
            "long_lag_periods": 12,
        },
    
        "inventory_ma6_structural": {
            "metric_key": "active_inventory",
            "transform_strategy": "ma_structural",
            "level_window": 6,
            "short_window": 3,
            "short_lag_periods": 0,
            "long_window": 6,
            "long_lag_periods": 12,
        },
    
        "inventory_ma12_structural": {
            "metric_key": "active_inventory",
            "transform_strategy": "ma_structural",
            "level_window": 12,
            "short_window": 3,
            "short_lag_periods": 0,
            "long_window": 12,
            "long_lag_periods": 12,
        },
    }

    for (
        experiment_id,
        expected,
    ) in expected_policies.items():
        experiment = experiments[
            experiment_id
        ]

        if len(
            experiment.policies
        ) != 1:
            raise ValueError(
                f"{experiment_id}: "
                "expected exactly one "
                "metric policy"
            )

        policy = experiment.policies[
            0
        ]

        actual = {
            "metric_key": (
                policy.metric_key
            ),
            "transform_strategy": (
                policy.transform_strategy
            ),
            "level_window": (
                policy.level_window
            ),
            "short_window": (
                policy.short_window
            ),
            "short_lag_periods": (
                policy.short_lag_periods
            ),
            "long_window": (
                policy.long_window
            ),
            "long_lag_periods": (
                policy.long_lag_periods
            ),
        }

        if actual != expected:
            raise ValueError(
                f"{experiment_id}: "
                "experiment policy mismatch. "
                f"Expected {expected}, "
                f"found {actual}"
            )

        if (
            policy.recompute_dependents
        ):
            raise ValueError(
                f"{experiment_id}: "
                "active inventory must not "
                "recompute dependents"
            )
