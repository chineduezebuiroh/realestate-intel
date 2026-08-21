#!/usr/bin/env python3
"""Build the diagnostic-only Demand chronology attenuation review.

This command has no registry or production-artifact write path.  Its output is
a disposable review package: CSV evidence and dependency-free SVG figures.
"""
from __future__ import annotations

from argparse import ArgumentParser
from pathlib import Path

import numpy as np
import pandas as pd

from regime.calendar_ma import minimum_valid_observations
from regime.diagnostics.capital_markets_ma import detect_turning_points
from regime.experiments import laus_feature_architecture as laus
from regime.experiments.demand_signal_attenuation import LABOR, cancellation
from regime.experiments.laus_ma_window_calibration import (
    GOVERNANCE, _build_chronology, _calibration_contract, _crossings,
)

SCENARIOS = {
    "A": (6, "LAUS-W-70-15-15"),
    "B": (9, "LAUS-W-70-15-15"),
    "C": (6, "LAUS-W-80-10-10"),
    "D": (9, "LAUS-W-80-10-10"),
}
MARGINS = ("A-B", "A-C", "B-D", "C-D")


def scenario_registry() -> pd.DataFrame:
    return pd.DataFrame([
        {"scenario_id": label, "ma_months": ma, "ma_window": f"MA{ma}",
         "laus_weight_policy": weights, "balance_policy": "BAL-S25-C75",
         "labor_force_membership": "LF-IN", **GOVERNANCE}
        for label, (ma, weights) in SCENARIOS.items()
    ])


def chronology_statistics(frame: pd.DataFrame) -> dict[str, float]:
    """Measure magnitude and direction changes on one ordered chronology."""
    q = frame[["date", "value"]].dropna().sort_values("date")
    value = q.value.astype(float)
    delta = value.diff().dropna()
    direction = np.sign(delta.loc[delta.ne(0)])
    reversals = int(direction.ne(direction.shift()).sum() - (not direction.empty))
    turns = detect_turning_points(q, "value")
    qualified = turns.loc[turns.get("qualified", False).eq(True)] if not turns.empty else turns
    raw = frame.get("raw_reference")
    correlation = value.corr(pd.to_numeric(raw.loc[value.index], errors="coerce")) if raw is not None else np.nan
    return {
        "observations": len(value), "standard_deviation": value.std(),
        "range": value.max() - value.min(), "mean_absolute_monthly_change": delta.abs().mean(),
        "turning_points": len(qualified),
        "zero_crossings": len(_crossings(q, "value")), "reversal_count": reversals,
        "persistence": float(1 - reversals / max(len(direction) - 1, 1)),
        "cancellation": cancellation(value)[2], "correlation_to_raw_laus": correlation,
    }


def _line_svg(series: dict[str, pd.Series], title: str, path: Path) -> None:
    """Write a compact plot on one calendar-faithful x-axis."""
    width, height, pad = 1100, 420, 48
    series = {name: values.rename_axis("date").sort_index() for name, values in series.items()}
    all_values = pd.concat(series.values()).replace([np.inf, -np.inf], np.nan).dropna()
    dates = pd.DatetimeIndex(sorted(set().union(*(pd.to_datetime(v.index) for v in series.values()))))
    start, end = dates.min(), dates.max()
    span = max((end - start).total_seconds(), 1)
    lo, hi = float(all_values.min()), float(all_values.max())
    if np.isclose(lo, hi): lo, hi = lo - 1, hi + 1
    colors = ("#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706")
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="{pad}" y="28" font-family="sans-serif" font-size="18">{title}</text>']
    for tick in pd.date_range(start, end, periods=6):
        x = pad + (tick-start).total_seconds()*(width-2*pad)/span
        parts += [f'<line x1="{x:.1f}" y1="{pad}" x2="{x:.1f}" y2="{height-pad}" stroke="#e5e7eb"/>',
                  f'<text x="{x:.1f}" y="{height-pad+16}" text-anchor="middle" font-family="sans-serif" font-size="10">{tick:%Y-%m}</text>']
    for i, (name, values) in enumerate(series.items()):
        calendar_values = values.reindex(dates)
        for _, segment in calendar_values.groupby(calendar_values.isna().cumsum()):
            q = segment.dropna()
            if q.empty:
                continue
            points = " ".join(f"{pad+(pd.Timestamp(d)-start).total_seconds()*(width-2*pad)/span:.1f},{height-pad-(v-lo)*(height-2*pad)/(hi-lo):.1f}" for d, v in q.items())
            parts.append(f'<polyline points="{points}" fill="none" stroke="{colors[i % len(colors)]}" stroke-width="1.6"/>')
        parts.append(f'<text x="{pad+150*i}" y="{height-12}" font-family="sans-serif" font-size="12" fill="{colors[i % len(colors)]}">{name}</text>')
    parts.append('</svg>')
    path.write_text("\n".join(parts), encoding="utf-8")


def _raw_ma_evidence(source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build county-first smoothing evidence and complete-panel standardized means."""
    source = source.sort_values(["geo_id", "canonical_metric_key", "date"]).copy()
    rows = []
    for (geo, metric), group in source.groupby(["geo_id", "canonical_metric_key"], sort=True):
        raw = group.drop_duplicates("date").set_index("date").raw_value.astype(float).sort_index()
        raw = raw.reindex(pd.date_range(raw.index.min(), raw.index.max(), freq="ME", name="date"))
        for stage, values in {
            "Raw": raw,
            **{
                f"MA{w}": raw.rolling(
                    w, min_periods=minimum_valid_observations(w)
                ).mean()
                for w in (3, 6, 9)
            },
        }.items():
            rows.extend({"geo_id": geo, "date": date, "metric": metric, "stage": stage, "value": value}
                        for date, value in values.items())
    by_county = pd.DataFrame(rows)

    coverage = (by_county.loc[by_county.stage.eq("Raw") & by_county.value.notna()]
                .groupby(["date", "metric"], as_index=False).geo_id.nunique()
                .rename(columns={"geo_id": "county_count"}))
    coverage["required_county_count"] = len(laus.GEOS)
    coverage["complete_seven_county_panel"] = coverage.county_count.eq(len(laus.GEOS))

    stat_rows = []
    for (geo, metric), group in by_county.groupby(["geo_id", "metric"]):
        raw = group.loc[group.stage.eq("Raw")].set_index("date").value
        raw_z = (raw - raw.mean()) / raw.std()
        raw_stats = None
        for stage, sg in group.groupby("stage", sort=False):
            q = sg.set_index("date").value.rename("value").to_frame().join(raw_z.rename("raw_reference"))
            stats = chronology_statistics(q.reset_index())
            if stage == "Raw": raw_stats = stats
            row = {"geo_id": geo, "metric": metric, "stage": stage, **stats}
            if stage != "Raw":
                for name in ("standard_deviation", "range", "mean_absolute_monthly_change", "reversal_count", "zero_crossings"):
                    base = raw_stats[name]
                    row[f"{name}_attenuation_relative_to_raw"] = (base - stats[name]) / base if base else np.nan
            stat_rows.append(row)
    county_stats = pd.DataFrame(stat_rows)

    attenuation_columns = [c for c in county_stats if c.endswith("_attenuation_relative_to_raw")]
    summaries = []
    for (metric, stage), group in county_stats.loc[county_stats.stage.ne("Raw")].groupby(["metric", "stage"]):
        for measure in attenuation_columns:
            values = group[measure].dropna()
            summaries.append({"metric": metric, "stage": stage, "measure": measure,
                              "seven_county_mean": values.mean(), "seven_county_median": values.median(),
                              "minimum": values.min(), "maximum": values.max(), "county_count": len(values)})
    summary = pd.DataFrame(summaries)

    complete = set(map(tuple, coverage.loc[coverage.complete_seven_county_panel, ["date", "metric"]].itertuples(index=False, name=None)))
    standardized = by_county.copy()
    standardized["standardized_value"] = standardized.groupby(["geo_id", "metric", "stage"]).value.transform(
        lambda x: (x - x.mean()) / x.std())
    standardized = standardized.loc[[tuple(x) in complete for x in standardized[["date", "metric"]].itertuples(index=False, name=None)]]
    pooled = (standardized.loc[standardized.standardized_value.notna()]
              .groupby(["date", "metric", "stage"], as_index=False)
              .agg(standardized_value=("standardized_value", "mean"), county_count=("geo_id", "nunique")))
    pooled = pooled.loc[pooled.county_count.eq(len(laus.GEOS))]
    return by_county, county_stats, summary, coverage, pooled


def build(run: Path, output: Path, root: Path) -> None:
    registry = scenario_registry()
    chronology, detail = _build_chronology(run.resolve(), root.resolve(), registry)
    source = laus._source(run.resolve())
    output.mkdir(parents=True, exist_ok=False)

    stage_rows = []
    by_county, county_stats, attenuation_summary, coverage, standardized = _raw_ma_evidence(source)
    by_county.to_csv(output / "raw_ma_by_county.csv", index=False)
    county_stats.to_csv(output / "raw_ma_attenuation_by_county.csv", index=False)
    attenuation_summary.to_csv(output / "raw_ma_attenuation_pooled_summary.csv", index=False)
    coverage.to_csv(output / "raw_ma_monthly_coverage.csv", index=False)
    standardized.to_csv(output / "pooled_standardized_raw_ma.csv", index=False)

    dc = by_county.loc[by_county.geo_id.eq("district_of_columbia_dc__county")]
    for metric in LABOR:
        dc_series = {stage: group.set_index("date").value for stage, group in
                     dc.loc[dc.metric.eq(metric)].groupby("stage", sort=False)}
        _line_svg(dc_series, f"Washington, DC {metric}: Raw / MA3 / MA6 / MA9",
                  output / f"01_dc_raw_ma_{metric}.svg")
        pooled_series = {stage: group.set_index("date").standardized_value for stage, group in
                         standardized.loc[standardized.metric.eq(metric)].groupby("stage", sort=False)}
        _line_svg(pooled_series, f"Seven-county standardized {metric}: Raw / MA3 / MA6 / MA9",
                  output / f"01_seven_county_standardized_raw_ma_{metric}.svg")

    # Retain the legacy stage table, but source its raw-smoothing rows from the
    # county-first evidence rather than an absolute-level cross-county pool.
    for row in county_stats.itertuples(index=False):
        stage_rows.append({"scenario": "raw_smoothing", **row._asdict()})

    if "geo_id" not in chronology:
        raise ValueError("controlled chronology lacks geo_id required for complete-panel pooling")
    chronology_coverage = chronology.groupby(["scenario_id", "date"]).geo_id.nunique()
    complete_dates = chronology_coverage.loc[chronology_coverage.eq(len(laus.GEOS))].index
    complete_chronology = chronology.set_index(["scenario_id", "date"]).loc[
        chronology.set_index(["scenario_id", "date"]).index.isin(complete_dates)].reset_index()
    pooled = complete_chronology.groupby(["scenario_id", "date"], as_index=False).mean(numeric_only=True)
    for scenario, g in pooled.groupby("scenario_id"):
        raw_ref = standardized.loc[standardized.stage.eq("Raw")].groupby("date").standardized_value.mean()
        for stage, column in (("Cyclical block","cyclical_score"), ("Structural block","structural_score"),
                              ("Core Demand","core_demand_score"), ("Demand Axis","demand_axis_score")):
            q = g.set_index("date")[[column]].rename(columns={column:"value"}).join(raw_ref.rename("raw_reference"))
            stage_rows.append({"scenario":scenario, "metric":"combined", "stage":stage,
                               **chronology_statistics(q.reset_index())})

    # Feature scores and their weighted metric scores are preserved separately.
    for scenario, g in detail.groupby("scenario_id"):
        for metric, m in g.loc[g.metric.isin(LABOR)].groupby("metric"):
            q = m.groupby("date").agg(value=("score","mean"))
            raw = standardized.loc[(standardized.metric.eq(metric)) & (standardized.stage.eq("Raw"))].set_index("date").standardized_value
            q = q.join(((raw-raw.mean())/raw.std()).rename("raw_reference"))
            stage_rows.append({"scenario":scenario, "metric":metric, "stage":"Feature score",
                               **chronology_statistics(q.reset_index())})

    feature_plot = {}
    for (scenario, metric), g in detail.loc[detail.metric.isin(LABOR)].groupby(["scenario_id", "metric"]):
        feature_plot[f"{scenario}:{metric}"] = g.groupby("date").score.mean()

    stats = pd.DataFrame(stage_rows)
    stats.to_csv(output / "stage_attenuation_statistics.csv", index=False)
    chronology.to_csv(output / "controlled_chronology.csv", index=False)
    registry.to_csv(output / "controlled_scenarios.csv", index=False)
    _line_svg(feature_plot, "LAUS feature-score chronology", output / "02_feature_score.svg")
    for number, (stage, column) in enumerate((("Cyclical block","cyclical_score"),("Structural block","structural_score"),
                                               ("Core Demand","core_demand_score"),("Demand Axis","demand_axis_score")), 3):
        _line_svg({s:g.set_index("date")[column] for s,g in pooled.groupby("scenario_id")}, stage,
                  output / f"{number:02d}_{stage.lower().replace(' ','_')}.svg")
    _line_svg({s:g.set_index("date").core_demand_score for s,g in pooled.groupby("scenario_id")},
              "A / B / C / D overlay", output / "07_abcd_overlay.svg")
    wide = pooled.pivot(index="date", columns="scenario_id", values="core_demand_score")
    differences = {name:wide[left]-wide[right] for name in MARGINS for left,right in [name.split("-")]}
    _line_svg(differences, "Controlled marginal differences", output / "08_difference_plots.svg")
    pd.DataFrame(differences).to_csv(output / "controlled_marginal_differences.csv")


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--run", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    _calibration_contract(args.root)  # explicit fail-closed registry validation
    build(args.run, args.output, args.root)


if __name__ == "__main__":
    main()
