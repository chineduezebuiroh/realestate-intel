from __future__ import annotations
# regime/freshness.py

from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_FRESHNESS_POLICY_PATH = Path(
    "config/derived_input_freshness_registry.csv"
)

REQUIRED_LINEAGE_COLUMNS = {
    "geo_id",
    "date",
    "derived_metric_key",
    "component_metric_key",
    "component_value",
    "component_source_date",
    "component_source_geo_id",
    "component_age_days",
    "component_age_months",
    "was_carried_forward",
}

REQUIRED_POLICY_COLUMNS = {
    "derived_metric_key",
    "component_metric_key",
    "warning_days",
    "hard_days",
    "policy_action",
    "enabled",
    "notes",
}

VALID_POLICY_ACTIONS = {
    "confidence_only",
}

STATUS_SEVERITY = {
    "fresh": 0,
    "stale_warning": 1,
    "hard_horizon_exceeded": 2,
}


def _truthy(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
        .str.strip()
        .str.lower()
        .isin({"true", "1", "yes", "y"})
    )


def load_derived_input_freshness_policy(
    path: str | Path = DEFAULT_FRESHNESS_POLICY_PATH,
) -> pd.DataFrame:
    policy_path = Path(path)

    if not policy_path.is_file():
        raise FileNotFoundError(
            f"Derived input freshness registry not found: "
            f"{policy_path}"
        )

    policy = pd.read_csv(policy_path)

    missing = REQUIRED_POLICY_COLUMNS - set(policy.columns)

    if missing:
        raise ValueError(
            "Derived input freshness registry is missing columns: "
            f"{sorted(missing)}"
        )

    policy = policy[
        _truthy(policy["enabled"])
    ].copy()

    if policy.empty:
        raise ValueError(
            "Derived input freshness registry contains no enabled rows"
        )

    policy["derived_metric_key"] = (
        policy["derived_metric_key"]
        .astype(str)
        .str.strip()
    )

    policy["component_metric_key"] = (
        policy["component_metric_key"]
        .astype(str)
        .str.strip()
    )

    policy["policy_action"] = (
        policy["policy_action"]
        .astype(str)
        .str.strip()
        .str.lower()
    )

    policy["warning_days"] = pd.to_numeric(
        policy["warning_days"],
        errors="coerce",
    )

    policy["hard_days"] = pd.to_numeric(
        policy["hard_days"],
        errors="coerce",
    )

    invalid_numeric = policy[
        policy["warning_days"].isna()
        | policy["hard_days"].isna()
    ]

    if not invalid_numeric.empty:
        raise ValueError(
            "Freshness policy contains non-numeric horizons:\n"
            + invalid_numeric.to_string(index=False)
        )

    policy["warning_days"] = (
        policy["warning_days"].astype(int)
    )

    policy["hard_days"] = (
        policy["hard_days"].astype(int)
    )

    invalid_horizons = policy[
        (policy["warning_days"] < 0)
        | (policy["hard_days"] <= 0)
        | (
            policy["warning_days"]
            >= policy["hard_days"]
        )
    ]

    if not invalid_horizons.empty:
        raise ValueError(
            "Freshness policy requires "
            "0 <= warning_days < hard_days:\n"
            + invalid_horizons.to_string(index=False)
        )

    invalid_actions = policy[
        ~policy["policy_action"].isin(
            VALID_POLICY_ACTIONS
        )
    ]

    if not invalid_actions.empty:
        raise ValueError(
            "Unsupported freshness policy actions:\n"
            + invalid_actions.to_string(index=False)
        )

    duplicate_keys = policy.duplicated(
        subset=[
            "derived_metric_key",
            "component_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise ValueError(
            "Duplicate derived input freshness policies:\n"
            + policy.loc[
                duplicate_keys
            ].to_string(index=False)
        )

    return (
        policy.sort_values(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .reset_index(drop=True)
    )


def _validate_lineage(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    missing = REQUIRED_LINEAGE_COLUMNS - set(
        lineage.columns
    )

    if missing:
        raise ValueError(
            "Derived metric lineage is missing columns: "
            f"{sorted(missing)}"
        )

    out = lineage.copy()

    out["date"] = pd.to_datetime(
        out["date"],
        errors="coerce",
    )

    out["component_source_date"] = pd.to_datetime(
        out["component_source_date"],
        errors="coerce",
    )

    out["component_age_days"] = pd.to_numeric(
        out["component_age_days"],
        errors="coerce",
    )

    out["component_age_months"] = pd.to_numeric(
        out["component_age_months"],
        errors="coerce",
    )

    invalid = out[
        out["date"].isna()
        | out["component_source_date"].isna()
        | out["component_age_days"].isna()
        | out["component_age_months"].isna()
    ]

    if not invalid.empty:
        raise ValueError(
            "Derived lineage contains invalid dates or ages:\n"
            + invalid.head(30).to_string(index=False)
        )

    negative_age = out[
        out["component_age_days"] < 0
    ]

    if not negative_age.empty:
        raise AssertionError(
            "Derived lineage contains future-dated inputs:\n"
            + negative_age.head(30).to_string(
                index=False
            )
        )

    duplicate_keys = out.duplicated(
        subset=[
            "geo_id",
            "date",
            "derived_metric_key",
            "component_metric_key",
        ],
        keep=False,
    )

    if duplicate_keys.any():
        raise AssertionError(
            "Duplicate derived lineage rows detected:\n"
            + out.loc[
                duplicate_keys
            ].head(30).to_string(index=False)
        )

    return out


def _component_status(
    age_days: pd.Series,
    warning_days: pd.Series,
    hard_days: pd.Series,
) -> pd.Series:
    status = pd.Series(
        "fresh",
        index=age_days.index,
        dtype="object",
    )

    status.loc[
        age_days > warning_days
    ] = "stale_warning"

    status.loc[
        age_days > hard_days
    ] = "hard_horizon_exceeded"

    return status


def _status_reason(row: pd.Series) -> str:
    status = row["freshness_status"]

    if status == "hard_horizon_exceeded":
        return (
            f"{row['component_metric_key']} age "
            f"{int(row['component_age_days'])} days "
            f"exceeds hard horizon "
            f"{int(row['hard_days'])} days"
        )

    if status == "stale_warning":
        return (
            f"{row['component_metric_key']} age "
            f"{int(row['component_age_days'])} days "
            f"exceeds warning horizon "
            f"{int(row['warning_days'])} days"
        )

    return (
        f"{row['component_metric_key']} age "
        f"{int(row['component_age_days'])} days "
        "is within policy horizon"
    )


def evaluate_derived_input_freshness(
    lineage: pd.DataFrame,
    *,
    policy_path: str | Path = (
        DEFAULT_FRESHNESS_POLICY_PATH
    ),
) -> dict[str, pd.DataFrame]:
    """
    Evaluate derived-input freshness without altering values.

    Returns:
      component_status:
          one row per derived observation and component

      derived_status:
          one row per derived observation, governed by its
          oldest/worst component

    Policy contract:
      - warning breach: retain value and flag stale
      - hard breach: retain value and flag exceeded
      - no output suppression
      - downstream confidence adjustment occurs in C4.5
    """
    lineage = _validate_lineage(lineage)
    policy = load_derived_input_freshness_policy(
        policy_path
    )

    component_status = lineage.merge(
        policy,
        on=[
            "derived_metric_key",
            "component_metric_key",
        ],
        how="left",
        validate="many_to_one",
    )

    missing_policy = component_status[
        component_status["warning_days"].isna()
        | component_status["hard_days"].isna()
    ]

    if not missing_policy.empty:
        missing_pairs = (
            missing_policy[
                [
                    "derived_metric_key",
                    "component_metric_key",
                ]
            ]
            .drop_duplicates()
            .sort_values(
                [
                    "derived_metric_key",
                    "component_metric_key",
                ]
            )
        )

        raise ValueError(
            "Missing derived input freshness policies:\n"
            + missing_pairs.to_string(index=False)
        )

    component_status["warning_days"] = (
        component_status["warning_days"].astype(int)
    )

    component_status["hard_days"] = (
        component_status["hard_days"].astype(int)
    )

    component_status["stale_input_flag"] = (
        component_status["component_age_days"]
        > component_status["warning_days"]
    )

    component_status["exceeded_horizon_flag"] = (
        component_status["component_age_days"]
        > component_status["hard_days"]
    )

    component_status["freshness_status"] = (
        _component_status(
            component_status["component_age_days"],
            component_status["warning_days"],
            component_status["hard_days"],
        )
    )

    component_status["freshness_severity"] = (
        component_status["freshness_status"]
        .map(STATUS_SEVERITY)
        .astype(int)
    )

    component_status["freshness_reason"] = (
        component_status.apply(
            _status_reason,
            axis=1,
        )
    )

    # Policy explicitly retains all values.
    component_status["suppress_output_flag"] = False
    component_status[
        "confidence_adjustment_required"
    ] = component_status["stale_input_flag"]

    observation_keys = [
        "geo_id",
        "date",
        "derived_metric_key",
    ]

    worst_rows = (
        component_status.sort_values(
            observation_keys
            + [
                "freshness_severity",
                "component_age_days",
                "component_metric_key",
            ]
        )
        .groupby(
            observation_keys,
            as_index=False,
            dropna=False,
        )
        .tail(1)
        [
            observation_keys
            + [
                "component_metric_key",
                "component_source_geo_id",
                "component_source_date",
                "component_age_days",
                "component_age_months",
                "warning_days",
                "hard_days",
                "freshness_status",
                "freshness_severity",
                "freshness_reason",
                "policy_action",
            ]
        ]
        .rename(
            columns={
                "component_metric_key": (
                    "governing_component_metric_key"
                ),
                "component_source_geo_id": (
                    "governing_component_source_geo_id"
                ),
                "component_source_date": (
                    "governing_component_source_date"
                ),
                "component_age_days": (
                    "governing_component_age_days"
                ),
                "component_age_months": (
                    "governing_component_age_months"
                ),
                "warning_days": (
                    "governing_warning_days"
                ),
                "hard_days": (
                    "governing_hard_days"
                ),
                "freshness_status": (
                    "derived_freshness_status"
                ),
                "freshness_severity": (
                    "derived_freshness_severity"
                ),
                "freshness_reason": (
                    "derived_freshness_reason"
                ),
                "policy_action": (
                    "derived_policy_action"
                ),
            }
        )
    )

    derived_status = (
        component_status.groupby(
            observation_keys,
            dropna=False,
        )
        .agg(
            component_count=(
                "component_metric_key",
                "nunique",
            ),
            carried_forward_component_count=(
                "was_carried_forward",
                "sum",
            ),
            stale_component_count=(
                "stale_input_flag",
                "sum",
            ),
            exceeded_component_count=(
                "exceeded_horizon_flag",
                "sum",
            ),
            oldest_component_age_days=(
                "component_age_days",
                "max",
            ),
            average_component_age_days=(
                "component_age_days",
                "mean",
            ),
        )
        .reset_index()
        .merge(
            worst_rows,
            on=observation_keys,
            how="left",
            validate="one_to_one",
        )
    )

    derived_status["stale_input_flag"] = (
        derived_status["stale_component_count"] > 0
    )

    derived_status["exceeded_horizon_flag"] = (
        derived_status["exceeded_component_count"] > 0
    )

    derived_status["suppress_output_flag"] = False

    derived_status[
        "confidence_adjustment_required"
    ] = derived_status["stale_input_flag"]

    if derived_status[
        "suppress_output_flag"
    ].any():
        raise AssertionError(
            "Freshness policy unexpectedly suppresses output"
        )

    invalid_hard = derived_status[
        derived_status["exceeded_horizon_flag"]
        & ~derived_status["stale_input_flag"]
    ]

    if not invalid_hard.empty:
        raise AssertionError(
            "Hard-horizon breach must also be stale:\n"
            + invalid_hard.head(30).to_string(
                index=False
            )
        )

    component_status = component_status.sort_values(
        observation_keys
        + ["component_metric_key"]
    ).reset_index(drop=True)

    derived_status = derived_status.sort_values(
        observation_keys
    ).reset_index(drop=True)

    return {
        "component_status": component_status,
        "derived_status": derived_status,
    }
