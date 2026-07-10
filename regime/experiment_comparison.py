from __future__ import annotations
# regime/experiment_comparison.py

from pathlib import Path

import pandas as pd

from regime.artifacts import (
    DEFAULT_ARTIFACT_ROOT,
    RegimeArtifactStore,
)


DEFAULT_RUN_IDS = [
    "baseline_raw_pct_equal_supply_weights",
    "permit_ma_pct_equal_supply_weights",
    "permit_ma_pct_reweighted_supply",
]

PERMIT_METRICS = {
    "permit_activity",
    "permit_intensity",
}


def _count_recovery_hypersupply_flips(
    trajectory: pd.DataFrame,
) -> int:
    pairs = trajectory[
        trajectory["major_changed"]
    ][["previous_major_regime", "major_regime"]]

    mask = (
        (
            pairs["previous_major_regime"].eq("recovery")
            & pairs["major_regime"].eq("hypersupply")
        )
        |
        (
            pairs["previous_major_regime"].eq("hypersupply")
            & pairs["major_regime"].eq("recovery")
        )
    )

    return int(mask.sum())


def build_experiment_comparison(
    run_ids: list[str] | None = None,
    *,
    artifact_root: str | Path = DEFAULT_ARTIFACT_ROOT,
) -> pd.DataFrame:
    run_ids = run_ids or DEFAULT_RUN_IDS
    store = RegimeArtifactStore(artifact_root)

    rows: list[dict[str, object]] = []

    for run_id in run_ids:
        manifest = store.read_manifest(run_id)

        if manifest.get("status") != "complete":
            raise ValueError(
                f"Run {run_id!r} is not complete: "
                f"{manifest.get('status')!r}"
            )

        trajectory = store.read_dataframe(
            run_id,
            "historical_trajectory",
            validation=True,
        )

        metrics = store.read_dataframe(
            run_id,
            "metric_scores",
        )

        trajectory["date"] = pd.to_datetime(trajectory["date"])
        metrics["date"] = pd.to_datetime(metrics["date"])

        permit_metrics = metrics[
            metrics["canonical_metric_key"].isin(PERMIT_METRICS)
        ].copy()

        permit_metrics = permit_metrics.sort_values(
            ["geo_id", "canonical_metric_key", "date"]
        )

        permit_metrics["abs_delta_metric_score"] = (
            permit_metrics
            .groupby(
                ["geo_id", "canonical_metric_key"]
            )["metric_score"]
            .diff()
            .abs()
        )

        for geo_id, geo_trajectory in trajectory.groupby("geo_id"):
            geo_permits = permit_metrics[
                permit_metrics["geo_id"].eq(geo_id)
            ]

            supply_delta = (
                geo_trajectory["delta_supply_pressure_score"].abs()
            )

            rows.append(
                {
                    "run_id": run_id,
                    "experiment_id": manifest.get("experiment_id"),
                    "geo_id": geo_id,
                    "months": int(len(geo_trajectory)),
                    "major_transitions": int(
                        geo_trajectory["major_changed"].sum()
                    ),
                    "minor_transitions": int(
                        geo_trajectory["minor_changed"].sum()
                    ),
                    "recovery_hypersupply_flips": (
                        _count_recovery_hypersupply_flips(
                            geo_trajectory
                        )
                    ),
                    "mean_abs_supply_delta": float(
                        supply_delta.mean()
                    ),
                    "median_abs_supply_delta": float(
                        supply_delta.median()
                    ),
                    "p90_abs_supply_delta": float(
                        supply_delta.quantile(0.90)
                    ),
                    "mean_regime_strength": float(
                        geo_trajectory["regime_strength"].mean()
                    ),
                    "median_regime_strength": float(
                        geo_trajectory["regime_strength"].median()
                    ),
                    "mean_permit_abs_delta_score": float(
                        geo_permits["abs_delta_metric_score"].mean()
                    ),
                    "max_permit_abs_delta_score": float(
                        geo_permits["abs_delta_metric_score"].max()
                    ),
                }
            )

    return (
        pd.DataFrame(rows)
        .sort_values(["geo_id", "run_id"])
        .reset_index(drop=True)
    )


def add_baseline_deltas(
    comparison: pd.DataFrame,
    *,
    baseline_run_id: str = (
        "baseline_raw_pct_equal_supply_weights"
    ),
) -> pd.DataFrame:
    baseline = comparison[
        comparison["run_id"].eq(baseline_run_id)
    ].copy()

    baseline = baseline.set_index("geo_id")

    result = comparison.copy()

    measure_columns = [
        "major_transitions",
        "minor_transitions",
        "recovery_hypersupply_flips",
        "mean_abs_supply_delta",
        "median_abs_supply_delta",
        "p90_abs_supply_delta",
        "mean_permit_abs_delta_score",
        "max_permit_abs_delta_score",
    ]

    for column in measure_columns:
        baseline_map = baseline[column].to_dict()
        baseline_values = result["geo_id"].map(baseline_map)

        result[f"{column}_vs_baseline"] = (
            result[column] - baseline_values
        )

        result[f"{column}_pct_vs_baseline"] = (
            result[column] / baseline_values.replace(0, pd.NA)
        ) - 1.0

    return result
