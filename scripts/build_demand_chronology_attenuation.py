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
        "range": value.max() - value.min(), "turning_points": len(qualified),
        "zero_crossings": len(_crossings(q, "value")), "reversal_count": reversals,
        "persistence": float(1 - reversals / max(len(direction) - 1, 1)),
        "cancellation": cancellation(value)[2], "correlation_to_raw_laus": correlation,
    }


def _line_svg(series: dict[str, pd.Series], title: str, path: Path) -> None:
    """Write a compact review plot without adding a plotting dependency."""
    width, height, pad = 1100, 420, 48
    all_values = pd.concat(series.values()).replace([np.inf, -np.inf], np.nan).dropna()
    lo, hi = float(all_values.min()), float(all_values.max())
    if np.isclose(lo, hi): lo, hi = lo - 1, hi + 1
    colors = ("#2563eb", "#dc2626", "#059669", "#7c3aed", "#d97706")
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
             '<rect width="100%" height="100%" fill="white"/>',
             f'<text x="{pad}" y="28" font-family="sans-serif" font-size="18">{title}</text>']
    for i, (name, values) in enumerate(series.items()):
        q = values.dropna(); n = max(len(q) - 1, 1)
        points = " ".join(f"{pad+j*(width-2*pad)/n:.1f},{height-pad-(v-lo)*(height-2*pad)/(hi-lo):.1f}" for j, v in enumerate(q))
        parts += [f'<polyline points="{points}" fill="none" stroke="{colors[i % len(colors)]}" stroke-width="1.6"/>',
                  f'<text x="{pad+150*i}" y="{height-12}" font-family="sans-serif" font-size="12" fill="{colors[i % len(colors)]}">{name}</text>']
    parts.append('</svg>')
    path.write_text("\n".join(parts), encoding="utf-8")


def build(run: Path, output: Path, root: Path) -> None:
    registry = scenario_registry()
    chronology, detail = _build_chronology(run.resolve(), root.resolve(), registry)
    source = laus._source(run.resolve())
    output.mkdir(parents=True, exist_ok=False)

    stage_rows = []
    plot_series: dict[str, dict[str, pd.Series]] = {}
    raw_pooled = source.groupby(["date", "canonical_metric_key"], as_index=False).raw_value.mean()
    for metric in LABOR:
        raw = raw_pooled.loc[raw_pooled.canonical_metric_key.eq(metric)].set_index("date").raw_value
        stages = {"Raw LAUS": raw}
        for ma in (3, 6, 9):
            f = laus._features(source, ma)
            key = {"labor_force":"laus_labor_force_level", "employment":"laus_employment_level",
                   "laus_unemployment_rate":"laus_unemployment_rate_level"}[metric]
            stages[f"MA{ma}"] = f.loc[f.feature_key.eq(key)].groupby("date").raw_feature_value.mean()
        plot_series[metric] = stages
        raw_z = (raw - raw.mean()) / raw.std()
        for stage, values in stages.items():
            q = values.rename("value").to_frame().join(raw_z.rename("raw_reference"))
            stage_rows.append({"scenario":"raw_smoothing", "metric":metric, "stage":stage,
                               **chronology_statistics(q.reset_index())})

    pooled = chronology.groupby(["scenario_id", "date"], as_index=False).mean(numeric_only=True)
    for scenario, g in pooled.groupby("scenario_id"):
        raw_ref = raw_pooled.groupby("date").raw_value.mean()
        for stage, column in (("Cyclical block","cyclical_score"), ("Structural block","structural_score"),
                              ("Core Demand","core_demand_score"), ("Demand Axis","demand_axis_score")):
            q = g.set_index("date")[[column]].rename(columns={column:"value"}).join(raw_ref.rename("raw_reference"))
            stage_rows.append({"scenario":scenario, "metric":"combined", "stage":stage,
                               **chronology_statistics(q.reset_index())})

    # Feature scores and their weighted metric scores are preserved separately.
    for scenario, g in detail.groupby("scenario_id"):
        for metric, m in g.loc[g.metric.isin(LABOR)].groupby("metric"):
            q = m.groupby("date").agg(value=("score","mean"))
            raw = raw_pooled.loc[raw_pooled.canonical_metric_key.eq(metric)].set_index("date").raw_value
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
    for metric, values in plot_series.items():
        _line_svg(values, f"{metric}: Raw LAUS vs MA3 / MA6 / MA9", output / f"01_raw_ma_{metric}.svg")
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
