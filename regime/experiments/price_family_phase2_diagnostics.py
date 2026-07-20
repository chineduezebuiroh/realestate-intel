from __future__ import annotations
# regime/experiments/price_family_phase2_diagnostics.py

import json
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from regime.experiments.linked_price_family_comparison import (
    CHALLENGER_ID,
    _add_change_diagnostics,
    _stability_summary,
    build_linked_price_family_comparison,
)

PRODUCTION_CANDIDATE_ID = "price_family_ma12_structural_linked"
COMPARISON_ROOT = Path("artifacts/regime/comparisons") / PRODUCTION_CANDIDATE_ID
CHRONOLOGY_DIR = COMPARISON_ROOT / "phase2_chronology"
STABILITY_DIR = COMPARISON_ROOT / "phase2_stability_seasonality"
SOURCE_PRICE_METRICS = ("median_sale_price", "median_ppsf")

PERIODS = (
    ("pre_gfc_early_history", None, "2006-12-31"),
    ("gfc_housing_correction", "2007-01-01", "2012-12-31"),
    ("recovery_expansion", "2013-01-01", "2019-12-31"),
    ("pandemic_disruption", "2020-01-01", "2021-12-31"),
    ("mortgage_rate_affordability_shock", "2022-01-01", "2024-12-31"),
    ("most_recent_available_period", None, None),
)

CHRONOLOGY_ARTIFACTS = (
    "chronology_monthly.csv",
    "chronology_period_summary.csv",
    "turning_point_lag_summary.csv",
    "affordability_shock_summary.csv",
    "chronology_flags.csv",
    "summary.json",
)

STABILITY_ARTIFACTS = (
    "feature_stability_summary.csv",
    "metric_stability_summary.csv",
    "dimension_stability_summary.csv",
    "demand_axis_stability_summary.csv",
    "seasonality_calendar_month.csv",
    "seasonality_summary.csv",
    "stability_flags.csv",
    "summary.json",
)


def _json_default(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return value.item()
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


def _comparison_result(
    comparison_result: Mapping[str, pd.DataFrame] | None,
) -> Mapping[str, pd.DataFrame]:
    if comparison_result is not None:
        return comparison_result
    return build_linked_price_family_comparison(
        challenger_id=PRODUCTION_CANDIDATE_ID
    )


def _clean_output_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    out = frame.copy()
    for column in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[column]):
            out[column] = out[column].dt.strftime("%Y-%m-%d")
    out.to_csv(path, index=False)


def _write_json(payload: dict[str, Any], path: Path) -> None:
    path.write_text(
        json.dumps(
            payload,
            indent=2,
            sort_keys=True,
            default=_json_default,
        )
        + "\n"
    )


def _sort(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    existing = [key for key in keys if key in frame.columns]
    if not existing:
        return frame.reset_index(drop=True)
    return frame.sort_values(existing, kind="mergesort").reset_index(drop=True)


def _wide_pair(
    history: pd.DataFrame,
    value: str,
    ids: list[str],
) -> pd.DataFrame:
    keep = ["run_role", "geo_id", "date", *ids, value]
    wide = (
        history[keep]
        .pivot_table(
            index=["geo_id", "date", *ids],
            columns="run_role",
            values=value,
            aggfunc="first",
        )
        .reset_index()
    )
    wide.columns.name = None
    return wide.rename(
        columns={
            "baseline": f"baseline_{value}",
            "challenger": f"challenger_{value}",
        }
    )


def _build_chronology_monthly(
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    metric = _wide_pair(
        result["metric_score_history"],
        "metric_score",
        ["canonical_metric_key"],
    ).rename(
        columns={
            "canonical_metric_key": "series_key",
        }
    )
    metric["series_type"] = "metric_score"

    dimension = _wide_pair(
        result["dimension_score_history"],
        "dimension_score",
        ["dimension"],
    ).rename(
        columns={
            "dimension": "series_key",
            "baseline_dimension_score": "baseline_metric_score",
            "challenger_dimension_score": "challenger_metric_score",
        }
    )
    dimension["series_type"] = "dimension_score"

    axis = _wide_pair(
        result["axis_score_history"],
        "axis_score",
        ["axis"],
    ).rename(
        columns={
            "axis": "series_key",
            "baseline_axis_score": "baseline_metric_score",
            "challenger_axis_score": "challenger_metric_score",
        }
    )
    axis["series_type"] = "axis_score"

    out = pd.concat(
        [metric, dimension, axis],
        ignore_index=True,
        sort=False,
    )
    out["date"] = pd.to_datetime(out["date"])
    out["difference_challenger_minus_baseline"] = (
        out["challenger_metric_score"] - out["baseline_metric_score"]
    )
    out["calendar_month"] = out["date"].dt.month
    return _sort(
        out,
        ["geo_id", "series_type", "series_key", "date"],
    )


def _period_summary(monthly: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for (geo, series_type, series_key), frame in monthly.groupby(
        ["geo_id", "series_type", "series_key"],
        dropna=False,
    ):
        frame = frame.copy()
        frame["date"] = pd.to_datetime(frame["date"])
        direct_overlap_mask = (
            frame["baseline_metric_score"].notna()
            & frame["challenger_metric_score"].notna()
        )
        latest = frame.loc[direct_overlap_mask, "date"].max()
        recent_start = latest - pd.DateOffset(months=11) if pd.notna(latest) else pd.NaT

        for period, start, end in PERIODS:
            if period == "most_recent_available_period":
                start_date = recent_start
                end_date = latest
            else:
                start_date = pd.Timestamp(start) if start else pd.Timestamp.min
                end_date = pd.Timestamp(end) if end else pd.Timestamp.max

            if pd.isna(start_date) or pd.isna(end_date):
                scoped = frame.iloc[0:0].copy()
            else:
                scoped = frame[
                    frame["date"].between(start_date, end_date)
                ]

            baseline_present = scoped["baseline_metric_score"].notna()
            challenger_present = scoped["challenger_metric_score"].notna()
            overlap = scoped[baseline_present & challenger_present]

            rows.append(
                {
                    "geo_id": geo,
                    "series_type": series_type,
                    "series_key": series_key,
                    "period": period,
                    "baseline_row_count": int(baseline_present.sum()),
                    "challenger_row_count": int(challenger_present.sum()),
                    "overlap_row_count": int(len(overlap)),
                    "available": bool(len(overlap) >= 3),
                    "first_date": overlap["date"].min() if len(overlap) else pd.NaT,
                    "last_date": overlap["date"].max() if len(overlap) else pd.NaT,
                    "baseline_mean": overlap["baseline_metric_score"].mean(),
                    "challenger_mean": overlap["challenger_metric_score"].mean(),
                    "mean_absolute_difference": (
                        overlap["challenger_metric_score"]
                        - overlap["baseline_metric_score"]
                    )
                    .abs()
                    .mean(),
                }
            )
    return _sort(
        pd.DataFrame(rows),
        ["geo_id", "series_type", "series_key", "period"],
    )


def _turns(frame: pd.DataFrame, value_col: str) -> pd.DataFrame:
    work = (
        frame[["date", value_col]]
        .dropna()
        .sort_values("date", kind="mergesort")
        .reset_index(drop=True)
    )
    values = work[value_col].to_numpy(dtype=float)
    rows: list[dict[str, Any]] = []
    for index in range(1, len(work) - 1):
        if values[index] >= values[index - 1] and values[index] > values[index + 1]:
            rows.append(
                {
                    "turn_date": work.loc[index, "date"],
                    "direction": "peak",
                    "turn_value": values[index],
                }
            )
        elif values[index] <= values[index - 1] and values[index] < values[index + 1]:
            rows.append(
                {
                    "turn_date": work.loc[index, "date"],
                    "direction": "trough",
                    "turn_value": values[index],
                }
            )
    return pd.DataFrame(rows)


def _month_delta(start: pd.Timestamp, end: pd.Timestamp) -> int:
    return (end.year - start.year) * 12 + (end.month - start.month)


def _structural_level_pairs(
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    lineage = result.get("price_family_feature_lineage", pd.DataFrame()).copy()
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_component",
        "raw_feature_value",
        "source_level_value",
    }
    columns = [
        "geo_id",
        "date",
        "series_type",
        "series_key",
        "baseline_metric_score",
        "challenger_metric_score",
    ]
    if lineage.empty or not required.issubset(lineage.columns):
        return pd.DataFrame(columns=columns)
    levels = lineage[
        lineage["feature_component"].eq("level")
        & lineage["canonical_metric_key"].isin(SOURCE_PRICE_METRICS)
    ].copy()
    levels["date"] = pd.to_datetime(levels["date"])
    levels = levels.rename(
        columns={
            "canonical_metric_key": "series_key",
            "source_level_value": "baseline_metric_score",
            "raw_feature_value": "challenger_metric_score",
        }
    )
    levels["series_type"] = "structural_price_level"
    return _sort(levels[columns], ["geo_id", "series_key", "date"])


def _turning_lag(
    monthly: pd.DataFrame,
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    pairs = [
        (
            "score_vs_baseline",
            monthly,
            "baseline_metric_score",
            "challenger_metric_score",
            ["geo_id", "series_type", "series_key"],
        )
    ]
    structural = _structural_level_pairs(result)
    if not structural.empty:
        pairs.append(
            (
                "structural_level_vs_raw_source",
                structural,
                "baseline_metric_score",
                "challenger_metric_score",
                ["geo_id", "series_type", "series_key"],
            )
        )

    for label, data, base_col, challenger_col, keys in pairs:
        for key_values, frame in data.groupby(keys, dropna=False):
            if not isinstance(key_values, tuple):
                key_values = (key_values,)
            base_turns = _turns(frame, base_col)
            challenger_turns = _turns(frame, challenger_col)
            used: set[int] = set()
            for _, base_turn in base_turns.iterrows():
                candidates = challenger_turns[
                    challenger_turns["direction"].eq(base_turn["direction"])
                ].copy()
                if not candidates.empty:
                    candidates["absolute_candidate_lag"] = candidates["turn_date"].map(
                        lambda date: abs(_month_delta(base_turn["turn_date"], date))
                    )
                    candidates = candidates[
                        ~candidates.index.isin(used)
                    ].sort_values(
                        ["absolute_candidate_lag", "turn_date"],
                        kind="mergesort",
                    )

                match = None if candidates.empty else candidates.iloc[0]
                if match is not None:
                    used.add(int(match.name))
                signed_lag = (
                    np.nan
                    if match is None
                    else _month_delta(base_turn["turn_date"], match["turn_date"])
                )
                row = {
                    key: value
                    for key, value in zip(keys, key_values, strict=True)
                }
                row.update(
                    {
                        "comparison": label,
                        "direction": base_turn["direction"],
                        "baseline_turn_date": base_turn["turn_date"],
                        "challenger_turn_date": pd.NaT if match is None else match["turn_date"],
                        "signed_lag_months": signed_lag,
                        "absolute_lag_months": abs(signed_lag) if pd.notna(signed_lag) else np.nan,
                        "matched": match is not None,
                    }
                )
                rows.append(row)

            unmatched = challenger_turns[~challenger_turns.index.isin(used)]
            for _, challenger_turn in unmatched.iterrows():
                row = {
                    key: value
                    for key, value in zip(keys, key_values, strict=True)
                }
                row.update(
                    {
                        "comparison": label,
                        "direction": challenger_turn["direction"],
                        "baseline_turn_date": pd.NaT,
                        "challenger_turn_date": challenger_turn["turn_date"],
                        "signed_lag_months": np.nan,
                        "absolute_lag_months": np.nan,
                        "matched": False,
                    }
                )
                rows.append(row)

    columns = [
        "geo_id",
        "series_type",
        "series_key",
        "comparison",
        "direction",
        "baseline_turn_date",
        "challenger_turn_date",
        "signed_lag_months",
        "absolute_lag_months",
        "matched",
        "median_absolute_lag_months_by_series",
        "maximum_absolute_lag_months_by_series",
    ]
    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=columns)
    stats = (
        out.groupby(["geo_id", "series_type", "series_key"], dropna=False)[
            "absolute_lag_months"
        ]
        .agg(
            median_absolute_lag_months_by_series="median",
            maximum_absolute_lag_months_by_series="max",
        )
        .reset_index()
    )
    out = out.merge(
        stats,
        on=["geo_id", "series_type", "series_key"],
        how="left",
        validate="many_to_one",
    )
    return _sort(
        out[columns],
        [
            "geo_id",
            "series_type",
            "series_key",
            "comparison",
            "direction",
            "baseline_turn_date",
            "challenger_turn_date",
        ],
    )


def _derived_component_history(
    result: Mapping[str, pd.DataFrame],
    component: str,
) -> pd.DataFrame:
    lineage = result.get("derived_lineage", pd.DataFrame()).copy()
    if lineage.empty:
        return pd.DataFrame(columns=["geo_id", "date", component])
    value_column = next(
        (
            column
            for column in ("component_value", "input_value", "source_value", "value")
            if column in lineage.columns
        ),
        None,
    )
    if value_column is None or "component_metric_key" not in lineage.columns:
        return pd.DataFrame(columns=["geo_id", "date", component])
    rows = lineage[lineage["component_metric_key"].eq(component)].copy()
    if rows.empty:
        return pd.DataFrame(columns=["geo_id", "date", component])
    rows["date"] = pd.to_datetime(rows["date"])
    return (
        rows[["geo_id", "date", value_column]]
        .drop_duplicates(["geo_id", "date"], keep="first")
        .rename(columns={value_column: component})
    )


def _affordability_shock_summary(
    monthly: pd.DataFrame,
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    structural = _structural_level_pairs(result)
    structural = structural[
        structural["series_key"].eq("median_sale_price")
    ][["geo_id", "date", "challenger_metric_score"]].rename(
        columns={"challenger_metric_score": "structural_median_sale_price"}
    )

    metric = monthly[
        monthly["series_type"].eq("metric_score")
        & monthly["series_key"].isin(["payment_burden", "price_to_income"])
    ][["geo_id", "date", "series_key", "challenger_metric_score"]]
    metric = metric.pivot_table(
        index=["geo_id", "date"],
        columns="series_key",
        values="challenger_metric_score",
        aggfunc="first",
    ).reset_index()
    metric.columns.name = None

    mortgage = _derived_component_history(result, "mortgage_30y")
    aligned = structural.merge(metric, on=["geo_id", "date"], how="outer")
    aligned = aligned.merge(mortgage, on=["geo_id", "date"], how="left")
    aligned = aligned[
        pd.to_datetime(aligned["date"]).between("2022-01-01", "2024-12-31")
    ].copy()

    rows: list[dict[str, Any]] = []
    for geo_id, frame in aligned.groupby("geo_id", dropna=False):
        frame = frame.sort_values("date", kind="mergesort")
        overlap = frame.dropna(
            subset=[
                "structural_median_sale_price",
                "payment_burden",
                "price_to_income",
            ]
        )
        pb_pti = frame.dropna(subset=["payment_burden", "price_to_income"])
        price_change = (
            overlap["structural_median_sale_price"].iloc[-1]
            - overlap["structural_median_sale_price"].iloc[0]
            if len(overlap) >= 2
            else np.nan
        )
        mortgage_present = frame.dropna(subset=["mortgage_30y"])
        mortgage_change = (
            mortgage_present["mortgage_30y"].iloc[-1]
            - mortgage_present["mortgage_30y"].iloc[0]
            if len(mortgage_present) >= 2
            else np.nan
        )
        payment_change = (
            overlap["payment_burden"].iloc[-1] - overlap["payment_burden"].iloc[0]
            if len(overlap) >= 2
            else np.nan
        )
        pti_change = (
            overlap["price_to_income"].iloc[-1] - overlap["price_to_income"].iloc[0]
            if len(overlap) >= 2
            else np.nan
        )
        price_pct = (
            price_change / overlap["structural_median_sale_price"].iloc[0]
            if len(overlap) >= 2 and overlap["structural_median_sale_price"].iloc[0] != 0
            else np.nan
        )
        pb_pti_distinct = (
            bool((pb_pti["payment_burden"] - pb_pti["price_to_income"]).abs().gt(1e-12).any())
            if not pb_pti.empty
            else np.nan
        )
        payment_moves_while_price_slow = (
            bool(abs(payment_change) >= 0.05 and abs(price_pct) <= 0.05)
            if pd.notna(payment_change) and pd.notna(price_pct)
            else np.nan
        )
        rows.append(
            {
                "geo_id": geo_id,
                "period": "mortgage_rate_affordability_shock",
                "first_date": overlap["date"].min() if len(overlap) else pd.NaT,
                "last_date": overlap["date"].max() if len(overlap) else pd.NaT,
                "aligned_observation_count": int(len(overlap)),
                "mortgage_observation_count": int(len(mortgage_present)),
                "structural_median_sale_price_change": price_change,
                "structural_median_sale_price_pct_change": price_pct,
                "mortgage_rate_change": mortgage_change,
                "payment_burden_change": payment_change,
                "price_to_income_change": pti_change,
                "payment_burden_changes_while_structural_price_slow": payment_moves_while_price_slow,
                "payment_burden_price_to_income_distinct_aligned": pb_pti_distinct,
            }
        )
    return _sort(pd.DataFrame(rows), ["geo_id", "period"])


def _chronology_flags(
    period: pd.DataFrame,
    lag: pd.DataFrame,
    shock: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in period[~period["available"]].itertuples(index=False):
        rows.append(
            {
                "geo_id": row.geo_id,
                "series_type": row.series_type,
                "series_key": row.series_key,
                "flag_type": "missing_segment_coverage",
                "severity": "review",
                "direction": "",
                "baseline_turn_date": "",
                "challenger_turn_date": "",
                "signed_lag_months": np.nan,
                "detail": f"{row.period} has {row.overlap_row_count} overlapping observations",
            }
        )
    for row in lag[~lag["matched"]].itertuples(index=False):
        rows.append(
            {
                "geo_id": row.geo_id,
                "series_type": row.series_type,
                "series_key": row.series_key,
                "flag_type": "unmatched_turning_point",
                "severity": "review",
                "direction": row.direction,
                "baseline_turn_date": row.baseline_turn_date,
                "challenger_turn_date": row.challenger_turn_date,
                "signed_lag_months": np.nan,
                "detail": f"{row.direction} unmatched",
            }
        )
    matched = lag[lag["absolute_lag_months"].notna()].copy()
    if not matched.empty:
        lag_group_columns = [
            "geo_id",
            "series_type",
            "series_key",
            "comparison",
            "direction",
        ]
        for group_key, group in matched.groupby(lag_group_columns, dropna=False):
            if len(group) < 5:
                continue
            cutoff = group["absolute_lag_months"].quantile(0.90)
            large = group[group["absolute_lag_months"] > cutoff]
            for row in large.itertuples(index=False):
                rows.append(
                    {
                        "geo_id": row.geo_id,
                        "series_type": row.series_type,
                        "series_key": row.series_key,
                        "flag_type": "large_lag_relative_to_observed_distribution",
                        "severity": "review",
                        "direction": row.direction,
                        "baseline_turn_date": row.baseline_turn_date,
                        "challenger_turn_date": row.challenger_turn_date,
                        "signed_lag_months": row.signed_lag_months,
                        "detail": (
                            f"absolute lag {row.absolute_lag_months} months exceeds "
                            f"group p90 {cutoff} for {group_key}"
                        ),
                    }
                )
        inversions = matched[matched["signed_lag_months"] <= -2]
        for row in inversions.itertuples(index=False):
            rows.append(
                {
                    "geo_id": row.geo_id,
                    "series_type": row.series_type,
                    "series_key": row.series_key,
                    "flag_type": "chronology_inversion_review",
                    "severity": "review",
                    "direction": row.direction,
                    "baseline_turn_date": row.baseline_turn_date,
                    "challenger_turn_date": row.challenger_turn_date,
                    "signed_lag_months": row.signed_lag_months,
                    "detail": "challenger turn materially precedes baseline/raw-source turn",
                }
            )
    for row in shock.itertuples(index=False):
        if (
            row.aligned_observation_count >= 3
            and pd.notna(row.payment_burden_change)
            and abs(row.payment_burden_change) <= 0.05
        ):
            rows.append(
                {
                    "geo_id": row.geo_id,
                    "series_type": "metric_score",
                    "series_key": "payment_burden",
                    "flag_type": "affordability_shock_materiality_review",
                    "severity": "review",
                    "direction": "",
                    "baseline_turn_date": "",
                    "challenger_turn_date": "",
                    "signed_lag_months": np.nan,
                    "detail": "2022-2024 payment_burden challenger change <= 0.05",
                }
            )
        distinct = row.payment_burden_price_to_income_distinct_aligned
        if pd.notna(distinct) and not bool(distinct):
            rows.append(
                {
                    "geo_id": row.geo_id,
                    "series_type": "metric_score",
                    "series_key": "payment_burden",
                    "flag_type": "price_to_income_payment_burden_not_distinct",
                    "severity": "review",
                    "direction": "",
                    "baseline_turn_date": "",
                    "challenger_turn_date": "",
                    "signed_lag_months": np.nan,
                    "detail": "2022-2024 aligned challenger values are identical",
                }
            )
    columns = [
        "geo_id",
        "series_type",
        "series_key",
        "flag_type",
        "severity",
        "direction",
        "baseline_turn_date",
        "challenger_turn_date",
        "signed_lag_months",
        "detail",
    ]
    if not rows:
        return pd.DataFrame(columns=columns)
    return _sort(
        pd.DataFrame(rows)[columns],
        ["geo_id", "series_type", "series_key", "flag_type", "direction", "detail"],
    )


def _summary_payload(
    *,
    rows: dict[str, int],
    flag_count: int,
    coverage: list[dict[str, Any]] | None = None,
    isolation_exact_match: bool | None = None,
    reproducibility_checked: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "candidate_id": PRODUCTION_CANDIDATE_ID,
        "legacy_candidate_id_supported": CHALLENGER_ID,
        "diagnostic_only": True,
        "flag_count": flag_count,
        "reproducibility": {
            "checked": reproducibility_checked,
            "row_level_equal": True if reproducibility_checked else None,
            "runs_compared": 2 if reproducibility_checked else None,
        },
    }
    payload.update(rows)
    if coverage is not None:
        payload["coverage"] = coverage
    if isolation_exact_match is not None:
        payload["isolation_exact_match"] = isolation_exact_match
    return payload


def build_phase2_chronology(
    output_dir: Path = CHRONOLOGY_DIR,
    *,
    comparison_result: Mapping[str, pd.DataFrame] | None = None,
    reproducibility_checked: bool = False,
) -> dict[str, pd.DataFrame]:
    _clean_output_dir(output_dir)
    result = _comparison_result(comparison_result)
    monthly = _build_chronology_monthly(result)
    period = _period_summary(monthly)
    lag = _turning_lag(monthly, result)
    shock = _affordability_shock_summary(monthly, result)
    flags = _chronology_flags(period, lag, shock)
    coverage = (
        monthly.groupby("geo_id")["date"]
        .agg(first_date="min", last_date="max")
        .reset_index()
        .to_dict("records")
    )
    summary = _summary_payload(
        rows={
            "monthly_rows": len(monthly),
            "period_rows": len(period),
            "lag_rows": len(lag),
            "affordability_shock_rows": len(shock),
        },
        flag_count=len(flags),
        coverage=coverage,
        reproducibility_checked=reproducibility_checked,
    )
    outputs = {
        "chronology_monthly": monthly,
        "chronology_period_summary": period,
        "turning_point_lag_summary": lag,
        "affordability_shock_summary": shock,
        "chronology_flags": flags,
    }
    for name, frame in outputs.items():
        _write_csv(frame, output_dir / f"{name}.csv")
    _write_json(summary, output_dir / "summary.json")
    outputs["summary"] = pd.DataFrame([summary])
    return outputs


def _stability_with_corr(
    history: pd.DataFrame,
    value: str,
    ids: list[str],
    corr: pd.DataFrame,
) -> pd.DataFrame:
    summary = _stability_summary(
        history,
        value_column=value,
        group_columns=["run_role", "geo_id", *ids],
    )
    comp = corr.rename(
        columns={
            "correlation": "baseline_challenger_correlation",
            "mean_absolute_difference": "mean_absolute_baseline_challenger_difference",
        }
    )
    return _sort(
        summary.merge(comp, on=["geo_id", *ids], how="left"),
        ["geo_id", *ids, "run_role"],
    )


def _feature_lineage_value_history(
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    lineage = result.get("price_family_feature_lineage", pd.DataFrame()).copy()
    required = {
        "geo_id",
        "date",
        "canonical_metric_key",
        "feature_component",
        "source_level_value",
        "raw_feature_value",
    }
    if lineage.empty or not required.issubset(lineage.columns):
        return pd.DataFrame(
            columns=[
                "run_role",
                "geo_id",
                "date",
                "canonical_metric_key",
                "feature_component",
                "feature_origin",
                "series_variant",
                "feature_value",
            ]
        )
    levels = lineage[
        lineage["feature_component"].eq("level")
        & lineage["canonical_metric_key"].isin(SOURCE_PRICE_METRICS)
    ].copy()
    raw = levels.assign(
        run_role="raw_source",
        feature_origin="raw_source",
        series_variant="raw_level",
        feature_value=levels["source_level_value"],
    )
    structural = levels.assign(
        run_role="structural_ma12",
        feature_origin="structural_ma12",
        series_variant="ma12_level",
        feature_value=levels["raw_feature_value"],
    )
    out = pd.concat([raw, structural], ignore_index=True, sort=False)
    out["date"] = pd.to_datetime(out["date"])
    return _add_change_diagnostics(
        out[
            [
                "run_role",
                "geo_id",
                "date",
                "canonical_metric_key",
                "feature_component",
                "feature_origin",
                "series_variant",
                "feature_value",
            ]
        ],
        value_column="feature_value",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
        ],
    )


def _safe_pair_correlation(left: pd.Series, right: pd.Series) -> float:
    pair = pd.concat(
        [
            pd.to_numeric(left, errors="coerce"),
            pd.to_numeric(right, errors="coerce"),
        ],
        axis=1,
    ).dropna()
    if len(pair) < 2:
        return np.nan
    if pair.iloc[:, 0].nunique() <= 1 or pair.iloc[:, 1].nunique() <= 1:
        return np.nan
    return float(pair.iloc[:, 0].corr(pair.iloc[:, 1]))


def _raw_structural_comparison_stats(history: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "geo_id",
        "canonical_metric_key",
        "feature_component",
        "baseline_challenger_correlation",
        "mean_absolute_baseline_challenger_difference",
        "overlap_rows",
    ]
    if history.empty:
        return pd.DataFrame(columns=columns)
    wide = history.pivot_table(
        index=["geo_id", "canonical_metric_key", "feature_component", "date"],
        columns="run_role",
        values="feature_value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None
    rows: list[dict[str, Any]] = []
    for key, group in wide.groupby(
        ["geo_id", "canonical_metric_key", "feature_component"],
        dropna=False,
    ):
        geo_id, metric, component = key
        if "raw_source" not in group.columns or "structural_ma12" not in group.columns:
            overlap = pd.DataFrame()
        else:
            overlap = group.dropna(subset=["raw_source", "structural_ma12"])
        rows.append(
            {
                "geo_id": geo_id,
                "canonical_metric_key": metric,
                "feature_component": component,
                "baseline_challenger_correlation": _safe_pair_correlation(
                    overlap.get("raw_source", pd.Series(dtype=float)),
                    overlap.get("structural_ma12", pd.Series(dtype=float)),
                ),
                "mean_absolute_baseline_challenger_difference": (
                    (overlap["structural_ma12"] - overlap["raw_source"]).abs().mean()
                    if not overlap.empty
                    else np.nan
                ),
                "overlap_rows": int(len(overlap)),
            }
        )
    return pd.DataFrame(rows, columns=columns)


def _raw_structural_feature_stability(
    history: pd.DataFrame,
) -> pd.DataFrame:
    if history.empty:
        return pd.DataFrame()
    summary = _stability_summary(
        history,
        value_column="feature_value",
        group_columns=[
            "run_role",
            "geo_id",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
        ],
    )
    comparison = _raw_structural_comparison_stats(history)
    return summary.merge(
        comparison,
        on=["geo_id", "canonical_metric_key", "feature_component"],
        how="left",
        validate="many_to_one",
    )


def _normalized_feature_stability(
    result: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    normalized = _stability_with_corr(
        result["normalized_feature_history"],
        "feature_score",
        ["canonical_metric_key", "feature_component"],
        result["feature_baseline_correlations"],
    )
    normalized["feature_origin"] = "normalized_feature"
    normalized["series_variant"] = "normalized_score"
    return normalized


def _seasonality(
    history: pd.DataFrame,
    value: str,
    ids: list[str],
    label: str,
    *,
    pair_ids: list[str] | None = None,
    comparison_pair: str = "baseline_vs_challenger",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    absolute_column = f"absolute_{value}_change_1m"
    cal = (
        history.groupby(["run_role", "geo_id", *ids, "calendar_month"], dropna=False)
        .agg(
            calendar_month_observation_count=("date", "size"),
            calendar_month_mean_absolute_change=(absolute_column, "mean"),
        )
        .reset_index()
    )
    cal["series_family"] = label
    cal["comparison_pair"] = comparison_pair
    rows: list[dict[str, Any]] = []
    for key, group in cal.groupby(["run_role", "geo_id", *ids], dropna=False):
        if not isinstance(key, tuple):
            key = (key,)
        values = group["calendar_month_mean_absolute_change"]
        min_value = values.min()
        max_value = values.max()
        spread = max_value - min_value if pd.notna(min_value) and pd.notna(max_value) else np.nan
        min_month = group.loc[values.idxmin(), "calendar_month"] if values.notna().any() else np.nan
        max_month = group.loc[values.idxmax(), "calendar_month"] if values.notna().any() else np.nan
        rows.append(
            {
                **{
                    column: value
                    for column, value in zip(["run_role", "geo_id", *ids], key, strict=True)
                },
                "series_family": label,
                "comparison_pair": comparison_pair,
                "minimum_calendar_month_mean_absolute_change": min_value,
                "maximum_calendar_month_mean_absolute_change": max_value,
                "seasonal_spread": spread,
                "minimum_month": min_month,
                "maximum_month": max_month,
            }
        )
    summary = pd.DataFrame(rows)
    pair_ids = ids if pair_ids is None else pair_ids
    baseline_role = "baseline" if summary["run_role"].eq("baseline").any() else "raw_source"
    baseline = summary[summary["run_role"].eq(baseline_role)]
    baseline = baseline.drop(columns="run_role").rename(
        columns={"seasonal_spread": "baseline_seasonal_spread"}
    )
    out = summary.merge(
        baseline[["geo_id", *pair_ids, "comparison_pair", "baseline_seasonal_spread"]],
        on=["geo_id", *pair_ids, "comparison_pair"],
        how="left",
        validate="many_to_one",
    )
    out["seasonal_spread_ratio_challenger_vs_baseline"] = np.where(
        out["baseline_seasonal_spread"].ne(0),
        out["seasonal_spread"] / out["baseline_seasonal_spread"],
        np.nan,
    )
    return (
        _sort(cal, ["series_family", "comparison_pair", "geo_id", *ids, "run_role", "calendar_month"]),
        _sort(out, ["series_family", "comparison_pair", "geo_id", *ids, "run_role"]),
    )


def build_phase2_stability_seasonality(
    output_dir: Path = STABILITY_DIR,
    *,
    comparison_result: Mapping[str, pd.DataFrame] | None = None,
    reproducibility_checked: bool = False,
) -> dict[str, pd.DataFrame]:
    _clean_output_dir(output_dir)
    result = _comparison_result(comparison_result)

    raw_feature_history = _feature_lineage_value_history(result)
    raw_feature_stability = _raw_structural_feature_stability(raw_feature_history)
    normalized_feature_stability = _normalized_feature_stability(result)
    feature = pd.concat(
        [normalized_feature_stability, raw_feature_stability],
        ignore_index=True,
        sort=False,
    )
    feature = _sort(
        feature,
        [
            "geo_id",
            "canonical_metric_key",
            "feature_component",
            "feature_origin",
            "series_variant",
            "run_role",
        ],
    )

    metric = _stability_with_corr(
        result["metric_score_history"],
        "metric_score",
        ["canonical_metric_key"],
        result["metric_baseline_correlations"],
    )
    dimension = _stability_with_corr(
        result["dimension_score_history"],
        "dimension_score",
        ["dimension"],
        result["dimension_baseline_correlations"],
    )
    axis = _stability_with_corr(
        result["axis_score_history"],
        "axis_score",
        ["axis"],
        result["axis_baseline_correlations"],
    ).merge(
        result["demand_conviction"],
        on=["run_role", "geo_id"],
        how="left",
        suffixes=("", "_conviction"),
    )

    calendar_frames: list[pd.DataFrame] = []
    seasonality_frames: list[pd.DataFrame] = []
    seasonality_inputs = [
        (
            result["normalized_feature_history"].assign(
                feature_origin="normalized_feature",
                series_variant="normalized_score",
            ),
            "feature_score",
            [
                "canonical_metric_key",
                "feature_component",
                "feature_origin",
                "series_variant",
            ],
            "normalized_feature",
        ),
        (
            raw_feature_history,
            "feature_value",
            [
                "canonical_metric_key",
                "feature_component",
                "feature_origin",
                "series_variant",
            ],
            "raw_structural_feature",
        ),
        (
            result["metric_score_history"],
            "metric_score",
            ["canonical_metric_key"],
            "metric_score",
        ),
        (
            result["dimension_score_history"],
            "dimension_score",
            ["dimension"],
            "dimension_score",
        ),
        (
            result["axis_score_history"],
            "axis_score",
            ["axis"],
            "axis_score",
        ),
    ]
    for history, value, ids, label in seasonality_inputs:
        if history.empty:
            continue
        if label == "raw_structural_feature":
            calendar, seasonality = _seasonality(
                history,
                value,
                ids,
                label,
                pair_ids=["canonical_metric_key", "feature_component"],
                comparison_pair="raw_vs_structural_ma12",
            )
        else:
            calendar, seasonality = _seasonality(history, value, ids, label)
        calendar_frames.append(calendar)
        seasonality_frames.append(seasonality)

    calendar_month = pd.concat(calendar_frames, ignore_index=True, sort=False)
    seasonality_summary = pd.concat(seasonality_frames, ignore_index=True, sort=False)

    flags: list[dict[str, Any]] = []
    bad_isolation = result["isolation_audit"][~result["isolation_audit"]["exact_match"]]
    for row in bad_isolation.itertuples(index=False):
        flags.append(
            {
                "geo_id": "ALL",
                "series_family": row.artifact_name,
                "series_key": row.comparison_scope,
                "flag_type": "phase1_isolation_contract_failed",
                "severity": "contract",
                "detail": row.error_message,
            }
        )
    flags_df = pd.DataFrame(flags) if flags else pd.DataFrame(
        columns=["geo_id", "series_family", "series_key", "flag_type", "severity", "detail"]
    )

    summary = _summary_payload(
        rows={
            "feature_rows": len(feature),
            "metric_rows": len(metric),
            "dimension_rows": len(dimension),
            "demand_axis_rows": len(axis),
            "seasonality_calendar_rows": len(calendar_month),
            "seasonality_summary_rows": len(seasonality_summary),
        },
        flag_count=len(flags_df),
        isolation_exact_match=bool(result["isolation_audit"]["exact_match"].all()),
        reproducibility_checked=reproducibility_checked,
    )

    outputs = {
        "feature_stability_summary": feature,
        "metric_stability_summary": metric,
        "dimension_stability_summary": dimension,
        "demand_axis_stability_summary": axis,
        "seasonality_calendar_month": calendar_month,
        "seasonality_summary": seasonality_summary,
        "stability_flags": flags_df,
    }
    for name, frame in outputs.items():
        _write_csv(frame, output_dir / f"{name}.csv")
    _write_json(summary, output_dir / "summary.json")
    outputs["summary"] = pd.DataFrame([summary])
    return outputs
