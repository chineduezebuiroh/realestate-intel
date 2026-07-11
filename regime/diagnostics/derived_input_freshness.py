from __future__ import annotations
# regime/diagnostics/derived_input_freshness.py

from pathlib import Path

import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)


DEFAULT_RUN_ID = "macro_regime_v1_lineage"

DEFAULT_VALIDATION_GEOS = [
    "district_of_columbia_dc__county",
    "alameda_county_ca__county",
]

EXPECTED_DERIVED_COMPONENTS = {
    "price_to_income": {
        "median_sale_price",
        "median_household_income",
    },
    "payment_burden": {
        "median_sale_price",
        "median_household_income",
        "mortgage_30y",
    },
    "permit_intensity": {
        "permit_activity",
        "population",
    },
}


def _validate_lineage(lineage: pd.DataFrame) -> pd.DataFrame:
    required = {
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

    missing = required - set(lineage.columns)

    if missing:
        raise ValueError(
            "Derived lineage artifact is missing columns: "
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

    if out["date"].isna().any():
        raise ValueError(
            "Derived lineage contains invalid derived dates"
        )

    if out["component_source_date"].isna().any():
        raise ValueError(
            "Derived lineage contains invalid component source dates"
        )

    out["component_age_days"] = pd.to_numeric(
        out["component_age_days"],
        errors="coerce",
    )

    out["component_age_months"] = pd.to_numeric(
        out["component_age_months"],
        errors="coerce",
    )

    if out["component_age_days"].isna().any():
        raise ValueError(
            "Derived lineage contains non-numeric component_age_days"
        )

    if out["component_age_months"].isna().any():
        raise ValueError(
            "Derived lineage contains non-numeric component_age_months"
        )

    negative_age = out[
        out["component_age_days"] < 0
    ]

    if not negative_age.empty:
        raise AssertionError(
            "Derived lineage contains future-dated components:\n"
            + negative_age.head(20).to_string(index=False)
        )

    recalculated_age = (
        out["date"] - out["component_source_date"]
    ).dt.days

    mismatch = out[
        recalculated_age != out["component_age_days"]
    ]

    if not mismatch.empty:
        raise AssertionError(
            "Persisted component ages do not match source dates:\n"
            + mismatch.head(20).to_string(index=False)
        )

    expected_carried_forward = (
        out["component_age_days"] > 0
    )

    carried_forward_mismatch = out[
        out["was_carried_forward"].astype(bool)
        != expected_carried_forward
    ]

    if not carried_forward_mismatch.empty:
        raise AssertionError(
            "was_carried_forward does not match component age:\n"
            + carried_forward_mismatch.head(20).to_string(
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

    actual_components = (
        out.groupby("derived_metric_key")[
            "component_metric_key"
        ]
        .agg(lambda values: set(values))
        .to_dict()
    )

    for derived_metric, expected in (
        EXPECTED_DERIVED_COMPONENTS.items()
    ):
        actual = actual_components.get(
            derived_metric,
            set(),
        )

        if actual != expected:
            raise AssertionError(
                f"Unexpected components for {derived_metric!r}. "
                f"Expected {sorted(expected)}, "
                f"found {sorted(actual)}"
            )

    return out


def _age_bucket(age_days: pd.Series) -> pd.Series:
    """
    Descriptive age buckets only.

    These are not production freshness horizons and do not imply
    suppression or confidence policy.
    """
    return pd.cut(
        age_days,
        bins=[
            -1,
            0,
            31,
            92,
            183,
            365,
            548,
            730,
            float("inf"),
        ],
        labels=[
            "current",
            "1_31_days",
            "32_92_days",
            "93_183_days",
            "184_365_days",
            "366_548_days",
            "549_730_days",
            "over_730_days",
        ],
        include_lowest=True,
    )


def _summarize_component_age(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    return (
        lineage.groupby(
            [
                "derived_metric_key",
                "component_metric_key",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            geos=("geo_id", "nunique"),
            first_derived_date=("date", "min"),
            latest_derived_date=("date", "max"),
            first_component_source_date=(
                "component_source_date",
                "min",
            ),
            latest_component_source_date=(
                "component_source_date",
                "max",
            ),
            carried_forward_rows=(
                "was_carried_forward",
                "sum",
            ),
            carried_forward_share=(
                "was_carried_forward",
                "mean",
            ),
            average_age_days=(
                "component_age_days",
                "mean",
            ),
            median_age_days=(
                "component_age_days",
                "median",
            ),
            p75_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.75),
            ),
            p90_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.90),
            ),
            p95_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.95),
            ),
            p99_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.99),
            ),
            maximum_age_days=(
                "component_age_days",
                "max",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .reset_index(drop=True)
    )


def _build_latest_component_status(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    return (
        lineage.sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
                "date",
            ]
        )
        .groupby(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ],
            as_index=False,
            dropna=False,
        )
        .tail(1)
        .sort_values(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ]
        )
        .reset_index(drop=True)
    )


def _build_derived_observation_summary(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    """
    Collapse component lineage to one row per derived observation.

    oldest_component_* identifies the weakest freshness link.
    """
    sorted_lineage = lineage.sort_values(
        [
            "geo_id",
            "date",
            "derived_metric_key",
            "component_age_days",
            "component_metric_key",
        ]
    )

    oldest_component = (
        sorted_lineage.groupby(
            [
                "geo_id",
                "date",
                "derived_metric_key",
            ],
            as_index=False,
            dropna=False,
        )
        .tail(1)
        [
            [
                "geo_id",
                "date",
                "derived_metric_key",
                "component_metric_key",
                "component_source_date",
                "component_source_geo_id",
                "component_age_days",
                "component_age_months",
            ]
        ]
        .rename(
            columns={
                "component_metric_key": (
                    "oldest_component_metric_key"
                ),
                "component_source_date": (
                    "oldest_component_source_date"
                ),
                "component_source_geo_id": (
                    "oldest_component_source_geo_id"
                ),
                "component_age_days": (
                    "oldest_component_age_days"
                ),
                "component_age_months": (
                    "oldest_component_age_months"
                ),
            }
        )
    )

    summary = (
        lineage.groupby(
            [
                "geo_id",
                "date",
                "derived_metric_key",
            ],
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
            carried_forward_component_share=(
                "was_carried_forward",
                "mean",
            ),
            average_component_age_days=(
                "component_age_days",
                "mean",
            ),
            maximum_component_age_days=(
                "component_age_days",
                "max",
            ),
        )
        .reset_index()
        .merge(
            oldest_component,
            on=[
                "geo_id",
                "date",
                "derived_metric_key",
            ],
            how="left",
            validate="one_to_one",
        )
    )

    summary["oldest_component_age_bucket"] = _age_bucket(
        summary["oldest_component_age_days"]
    )

    return summary.sort_values(
        [
            "geo_id",
            "derived_metric_key",
            "date",
        ]
    ).reset_index(drop=True)


def _build_age_distribution(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    work = lineage.copy()
    work["age_bucket"] = _age_bucket(
        work["component_age_days"]
    )

    distribution = (
        work.groupby(
            [
                "derived_metric_key",
                "component_metric_key",
                "age_bucket",
            ],
            observed=False,
            dropna=False,
        )
        .size()
        .reset_index(name="rows")
    )

    totals = (
        distribution.groupby(
            [
                "derived_metric_key",
                "component_metric_key",
            ]
        )["rows"]
        .transform("sum")
    )

    distribution["row_share"] = (
        distribution["rows"] / totals
    )

    return distribution.sort_values(
        [
            "derived_metric_key",
            "component_metric_key",
            "age_bucket",
        ]
    ).reset_index(drop=True)


def _build_carry_forward_streaks(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    """
    Measure consecutive derived observations using the same component
    source observation.

    This is different from component age. It answers:
    "For how many derived rows was this exact source observation reused?"
    """
    work = lineage.sort_values(
        [
            "geo_id",
            "derived_metric_key",
            "component_metric_key",
            "date",
        ]
    ).copy()

    grouping_columns = [
        "geo_id",
        "derived_metric_key",
        "component_metric_key",
    ]

    source_changed = (
        work.groupby(grouping_columns)[
            "component_source_date"
        ]
        .transform(
            lambda values: values.ne(values.shift())
        )
    )

    work["source_observation_sequence"] = (
        source_changed.groupby(
            [
                work[column]
                for column in grouping_columns
            ]
        )
        .cumsum()
    )

    streaks = (
        work.groupby(
            grouping_columns
            + ["source_observation_sequence"],
            dropna=False,
        )
        .agg(
            component_source_date=(
                "component_source_date",
                "first",
            ),
            component_source_geo_id=(
                "component_source_geo_id",
                "first",
            ),
            first_derived_date=("date", "min"),
            last_derived_date=("date", "max"),
            derived_observation_count=("date", "size"),
            first_age_days=(
                "component_age_days",
                "min",
            ),
            last_age_days=(
                "component_age_days",
                "max",
            ),
            maximum_age_days=(
                "component_age_days",
                "max",
            ),
        )
        .reset_index()
    )

    streaks["calendar_span_days"] = (
        streaks["last_derived_date"]
        - streaks["first_derived_date"]
    ).dt.days

    return streaks.sort_values(
        [
            "maximum_age_days",
            "derived_observation_count",
        ],
        ascending=[False, False],
    ).reset_index(drop=True)


def _build_geo_component_extremes(
    lineage: pd.DataFrame,
) -> pd.DataFrame:
    return (
        lineage.groupby(
            [
                "geo_id",
                "derived_metric_key",
                "component_metric_key",
            ],
            dropna=False,
        )
        .agg(
            rows=("date", "size"),
            carried_forward_share=(
                "was_carried_forward",
                "mean",
            ),
            average_age_days=(
                "component_age_days",
                "mean",
            ),
            p90_age_days=(
                "component_age_days",
                lambda values: values.quantile(0.90),
            ),
            maximum_age_days=(
                "component_age_days",
                "max",
            ),
            latest_derived_date=("date", "max"),
            latest_component_source_date=(
                "component_source_date",
                "max",
            ),
        )
        .reset_index()
        .sort_values(
            [
                "maximum_age_days",
                "average_age_days",
            ],
            ascending=[False, False],
        )
        .reset_index(drop=True)
    )


def build_derived_input_freshness_audit(
    run_id: str = DEFAULT_RUN_ID,
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
    geo_ids: list[str] | None = None,
    top_n: int = 50,
) -> dict[str, pd.DataFrame]:
    """
    Audit persisted derived-input lineage.

    This function is descriptive only. It does not define production
    carry-forward horizons, stale flags, suppression, or confidence
    penalties.
    """
    if top_n <= 0:
        raise ValueError("top_n must be greater than zero")

    store = RegimeArtifactStore(artifact_root)

    manifest = store.read_manifest(run_id)

    if manifest.get("status") != "complete":
        raise ValueError(
            f"Run {run_id!r} is not complete: "
            f"{manifest.get('status')!r}"
        )

    lineage = store.read_dataframe(
        run_id,
        "derived_metric_lineage",
    )

    lineage = _validate_lineage(lineage)

    component_age_summary = _summarize_component_age(
        lineage
    )

    latest_component_status = (
        _build_latest_component_status(lineage)
    )

    derived_observation_summary = (
        _build_derived_observation_summary(lineage)
    )

    age_distribution = _build_age_distribution(lineage)

    carry_forward_streaks = _build_carry_forward_streaks(
        lineage
    )

    geo_component_extremes = _build_geo_component_extremes(
        lineage
    )

    worst_component_events = (
        lineage.sort_values(
            [
                "component_age_days",
                "date",
                "geo_id",
            ],
            ascending=[False, False, True],
        )
        .head(top_n)
        .reset_index(drop=True)
    )

    worst_derived_observations = (
        derived_observation_summary.sort_values(
            [
                "oldest_component_age_days",
                "date",
                "geo_id",
            ],
            ascending=[False, False, True],
        )
        .head(top_n)
        .reset_index(drop=True)
    )

    if geo_ids is None:
        geo_ids = DEFAULT_VALIDATION_GEOS

    latest_selected_geos = latest_component_status[
        latest_component_status["geo_id"].isin(geo_ids)
    ].copy()

    oldest_selected_geos = (
        lineage[
            lineage["geo_id"].isin(geo_ids)
        ]
        .sort_values(
            [
                "component_age_days",
                "date",
            ],
            ascending=[False, False],
        )
        .head(top_n)
        .reset_index(drop=True)
    )

    # Structural assertions on the collapsed derived grain.
    expected_component_counts = {
        metric: len(components)
        for metric, components
        in EXPECTED_DERIVED_COMPONENTS.items()
    }

    expected_counts = (
        derived_observation_summary[
            "derived_metric_key"
        ].map(expected_component_counts)
    )

    invalid_component_count = (
        derived_observation_summary[
            derived_observation_summary[
                "component_count"
            ] != expected_counts
        ]
    )

    if not invalid_component_count.empty:
        raise AssertionError(
            "Derived observations have incomplete component lineage:\n"
            + invalid_component_count.head(30).to_string(
                index=False
            )
        )

    invalid_oldest_age = derived_observation_summary[
        derived_observation_summary[
            "oldest_component_age_days"
        ]
        != derived_observation_summary[
            "maximum_component_age_days"
        ]
    ]

    if not invalid_oldest_age.empty:
        raise AssertionError(
            "Oldest component metadata does not match maximum age:\n"
            + invalid_oldest_age.head(30).to_string(
                index=False
            )
        )

    return {
        "component_age_summary": (
            component_age_summary
        ),
        "latest_component_status": (
            latest_component_status
        ),
        "derived_observation_summary": (
            derived_observation_summary
        ),
        "age_distribution": age_distribution,
        "carry_forward_streaks": carry_forward_streaks,
        "geo_component_extremes": geo_component_extremes,
        "worst_component_events": (
            worst_component_events
        ),
        "worst_derived_observations": (
            worst_derived_observations
        ),
        "latest_selected_geos": (
            latest_selected_geos.reset_index(drop=True)
        ),
        "oldest_selected_geos": oldest_selected_geos,
    }
