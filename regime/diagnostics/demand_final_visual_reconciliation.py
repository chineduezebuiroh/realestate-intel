"""Persisted-evidence-only reconciliation of the four governed Demand finalists."""
from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
DC = GEOS[0]
FACTORS = {
    "A": ("MA6", "LAUS-W-70-15-15"),
    "B": ("MA9", "LAUS-W-70-15-15"),
    "C": ("MA6", "LAUS-W-80-10-10"),
    "D": ("MA9", "LAUS-W-80-10-10"),
}
GOVERNANCE = {"recommendation_state": "none", "promotion_state": "current_production_unchanged",
              "human_decision": "reconciliation_pending", "automated_winner": False,
              "production_policy_changed": False}
SERIES = ("core_demand_score", "cyclical_block_score", "cyclical_contribution",
          "structural_block_score", "structural_contribution")
EFFECTS = (("A_to_B", "A", "B", "MA effect at 70/15/15"),
           ("A_to_C", "A", "C", "weight effect at MA6"),
           ("C_to_D", "C", "D", "MA effect at 80/10/10"),
           ("B_to_D", "B", "D", "weight effect at MA9"))


def resolve_four(registry: pd.DataFrame) -> pd.DataFrame:
    required = {"scenario_id", "labor_force_membership", "ma_window",
                "laus_weight_policy", "balance_policy"}
    missing = required - set(registry)
    if missing:
        raise ValueError(f"scenario registry missing fields: {sorted(missing)}")
    rows = []
    for label, (ma, weights) in FACTORS.items():
        found = registry.loc[
            registry.labor_force_membership.eq("LF-IN") & registry.ma_window.eq(ma)
            & registry.laus_weight_policy.eq(weights)
            & registry.balance_policy.eq("BAL-S25-C75")
        ]
        if len(found) != 1:
            raise ValueError(f"scenario {label} resolved to {len(found)} rows")
        rows.append({"scenario": label, **found.iloc[0][list(required)].to_dict()})
    out = pd.DataFrame(rows)
    if out.scenario_id.duplicated().any():
        raise ValueError("the four factor configurations must resolve to distinct scenarios")
    return out[["scenario", "scenario_id", "labor_force_membership", "ma_window",
                "laus_weight_policy", "balance_policy"]]


def prepare_chronology(chronology: pd.DataFrame, scenarios: pd.DataFrame) -> pd.DataFrame:
    """Extract and validate block arithmetic exactly as persisted by calibration."""
    required = {"scenario_id", "geo_id", "date", "core_demand_score",
                "structural_score", "cyclical_score"}
    missing = required - set(chronology)
    if missing:
        raise ValueError(f"calibration chronology missing fields: {sorted(missing)}")
    q = chronology.merge(scenarios[["scenario", "scenario_id"]], on="scenario_id",
                         how="inner", validate="many_to_one").copy()
    q = q.loc[q.geo_id.isin(GEOS)]
    if set(q.geo_id) != set(GEOS) or set(q.scenario) != set(FACTORS):
        raise ValueError("chronology does not contain exactly the governed scenario/geography scope")
    q["date"] = pd.to_datetime(q.date)
    if q.duplicated(["scenario", "geo_id", "date"]).any():
        raise ValueError("duplicate scenario/geography/month chronology")
    # Calibration persisted structural_score/cyclical_score as final contributions.
    q["structural_contribution"] = q.structural_score
    q["cyclical_contribution"] = q.cyclical_score
    q["structural_block_score"] = q.structural_contribution / .25
    q["cyclical_block_score"] = q.cyclical_contribution / .75
    if not np.allclose(q.core_demand_score,
                       q.structural_contribution + q.cyclical_contribution,
                       atol=1e-12, rtol=0, equal_nan=True):
        raise ValueError("persisted Core Demand does not equal its block contributions")
    control = q.pivot_table(index=["geo_id", "date"], columns="scenario",
                            values="structural_contribution")
    if control.max(axis=1).sub(control.min(axis=1)).max() > 1e-12:
        raise ValueError("unexpected Structural contribution differences across finalists")
    return q[["scenario", "scenario_id", "geo_id", "date", *SERIES]].sort_values(
        ["scenario", "geo_id", "date"]).reset_index(drop=True)


def pooled(chronology: pd.DataFrame) -> pd.DataFrame:
    counts = chronology.groupby(["scenario", "date"]).geo_id.nunique()
    if not counts.eq(len(GEOS)).all():
        raise ValueError("pooled means require all seven counties on equal footing")
    return chronology.groupby(["scenario", "scenario_id", "date"], as_index=False)[list(SERIES)].mean()


def _reversals(values: pd.Series, lag: int = 1) -> int:
    direction = np.sign(values.diff(lag)).replace(0, np.nan).dropna()
    return int(direction.ne(direction.shift()).iloc[1:].sum()) if len(direction) > 1 else 0


def statistics(values: pd.Series) -> dict[str, float]:
    x = values.dropna().astype(float)
    if x.empty:
        raise ValueError("chronology statistics require at least one available observation")
    signs = np.sign(x).replace(0, np.nan).dropna()
    crossings = int(signs.ne(signs.shift()).iloc[1:].sum()) if len(signs) > 1 else 0
    reversals = _reversals(x)
    return {"observation_count": len(x), "start_score": x.iloc[0], "end_score": x.iloc[-1],
            "std": x.std(), "range": x.max()-x.min(), "median_absolute_value": x.abs().median(),
            "mean_absolute_monthly_change": x.diff().abs().mean(), "zero_crossings": crossings,
            "reversal_1m": reversals, "reversal_3m": _reversals(x, 3),
            "same_sign_persistence": 1 - reversals / max(len(x)-2, 1),
            "largest_monthly_move": x.diff().abs().max()}


def summarize(dc: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scope, frame in (("dc", dc), ("seven_county_pooled", pool)):
        for scenario, group in frame.groupby("scenario"):
            for series in SERIES:
                rows.append({"scope": scope, "scenario": scenario, "series": series,
                             **statistics(group.sort_values("date")[series])})
    return pd.DataFrame(rows)


def recent_summary(dc: pd.DataFrame, pool: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for scope, frame in (("dc", dc), ("seven_county_pooled", pool)):
        latest = frame.date.max()
        for window, start in (("since_2022", pd.Timestamp("2022-01-31")),
                              ("latest_36_months", latest-pd.DateOffset(months=35))):
            q = frame.loc[frame.date.ge(start)]
            for scenario, series, values in ((s, v, g.sort_values("date")[v])
                for s, g in q.groupby("scenario") for v in ("core_demand_score", "cyclical_contribution")):
                rows.append({"scope": scope, "window": window, "scenario": scenario,
                             "series": series, "window_start": start, "window_end": latest,
                             **statistics(values)})
    return pd.DataFrame(rows)


def marginal_effects(summary: pd.DataFrame) -> pd.DataFrame:
    keys = ["scope", "series"]
    metrics = ["std", "range", "mean_absolute_monthly_change", "zero_crossings",
               "reversal_1m", "same_sign_persistence"]
    rows = []
    for name, left, right, description in EFFECTS:
        a = summary.loc[summary.scenario.eq(left)].set_index(keys)
        b = summary.loc[summary.scenario.eq(right)].set_index(keys)
        for key in a.index.intersection(b.index):
            for metric in metrics:
                rows.append({"comparison": name, "description": description,
                             "scope": key[0], "series": key[1], "metric": metric,
                             "left_value": a.at[key, metric], "right_value": b.at[key, metric],
                             "delta": b.at[key, metric]-a.at[key, metric]})
    out = pd.DataFrame(rows)
    # Difference-in-differences: D-A-(B-A)-(C-A) = D-B-C+A.
    wide = summary.pivot(index=keys, columns="scenario", values=metrics)
    interactions = []
    for key, row in wide.iterrows():
        for metric in metrics:
            interactions.append({"comparison": "interaction", "description": "D-B-C+A",
                "scope": key[0], "series": key[1], "metric": metric,
                "left_value": np.nan, "right_value": np.nan,
                "delta": row[(metric,"D")]-row[(metric,"B")]-row[(metric,"C")]+row[(metric,"A")]})
    return pd.concat([out, pd.DataFrame(interactions)], ignore_index=True)


def classify(effects: pd.DataFrame, material_fraction: float = .10) -> tuple[str, dict]:
    """Classify smoothing using DC Cyclical std effects with an explicit 10% threshold."""
    q = effects.loc[(effects.scope.eq("dc")) & (effects.series.eq("cyclical_contribution"))
                    & (effects.metric.eq("std"))].set_index("comparison").delta
    baseline = effects.loc[(effects.scope.eq("dc")) & effects.series.eq("cyclical_contribution")
        & effects.metric.eq("std") & effects.comparison.eq("A_to_B"), "left_value"].iloc[0]
    threshold = abs(baseline) * material_fraction
    ma = (q.A_to_B + q.C_to_D) / 2
    weight = (q.A_to_C + q.B_to_D) / 2
    interaction = q.interaction
    material = {"ma": bool(abs(ma) >= threshold), "weight": bool(abs(weight) >= threshold),
                "interaction": bool(abs(interaction) >= threshold)}
    a_persistence = effects.loc[(effects.scope.eq("dc")) & effects.series.eq("core_demand_score")
        & effects.metric.eq("same_sign_persistence") & effects.comparison.eq("A_to_B"), "left_value"].iloc[0]
    if not any(material.values()) and a_persistence >= .90: classification = "PRE_EXISTING_CHRONOLOGY"
    elif not any(material.values()): classification = "NO_MATERIAL_SMOOTHING_DIFFERENCE"
    elif sum(material.values()) > 1: classification = "MULTIPLE_CONTRIBUTING_EFFECTS"
    elif material["ma"]: classification = "MA_WINDOW_EFFECT"
    elif material["weight"]: classification = "LAUS_WEIGHT_EFFECT"
    else: classification = "MA_WEIGHT_INTERACTION"
    return classification, {"baseline_cyclical_std": float(baseline), "materiality_threshold": float(threshold),
        "average_ma_effect": float(ma), "average_weight_effect": float(weight),
        "interaction_effect": float(interaction), "scenario_a_core_persistence": float(a_persistence),
        "material_effects": material}


def _plot(frame: pd.DataFrame, value: str, title: str, destination: Path, recent=False) -> None:
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    if recent: frame = frame.loc[frame.date.ge("2022-01-31")]
    labels = {"A":"A — MA6 / 70-15-15", "B":"B — MA9 / 70-15-15",
              "C":"C — MA6 / 80-10-10", "D":"D — MA9 / 80-10-10"}
    fig, ax = plt.subplots(figsize=(12, 5))
    for scenario in FACTORS:
        q = frame.loc[frame.scenario.eq(scenario)].sort_values("date")
        ax.plot(q.date, q[value], label=labels[scenario], linewidth=1.5)
    ax.axhline(0, color="black", linewidth=.6); ax.set_title(title); ax.set_ylabel("Score")
    ax.legend(ncol=2); ax.grid(alpha=.2); fig.tight_layout(); fig.savefig(destination, dpi=140); plt.close(fig)


def build(calibration: Path, output: Path) -> Path:
    registry_path = calibration/"laus_ma_window_scenario_registry.csv"
    chronology_path = calibration/"laus_ma_window_chronology.csv"
    if not registry_path.is_file() or not chronology_path.is_file():
        raise FileNotFoundError("persisted calibration registry and chronology are required; calibration is never rerun")
    if output.exists(): raise FileExistsError(f"refusing to overwrite diagnostic output: {output}")
    scenarios = resolve_four(pd.read_csv(registry_path))
    chronology = prepare_chronology(pd.read_csv(chronology_path), scenarios)
    dc = chronology.loc[chronology.geo_id.eq(DC)].copy(); pool = pooled(chronology)
    summary = summarize(dc, pool); recent = recent_summary(dc, pool)
    effects = marginal_effects(summary); classification, evidence = classify(effects)
    output.mkdir(parents=True); visual = output/"visual_review"; visual.mkdir()
    files = ((dc,"core_demand_score","DC Core Demand","dc_core_demand_four_way.png",False),
      (dc,"cyclical_contribution","DC Cyclical final contribution","dc_cyclical_contribution_four_way.png",False),
      (dc,"structural_contribution","DC Structural final contribution","dc_structural_contribution_four_way.png",False),
      (pool,"core_demand_score","Seven-county pooled Core Demand","pooled_core_demand_four_way.png",False),
      (pool,"cyclical_contribution","Seven-county pooled Cyclical contribution","pooled_cyclical_contribution_four_way.png",False),
      (dc,"core_demand_score","DC Core Demand since 2022","dc_recent_core_demand_four_way.png",True),
      (dc,"cyclical_contribution","DC Cyclical contribution since 2022","dc_recent_cyclical_contribution_four_way.png",True))
    for frame,value,title,name,is_recent in files: _plot(frame,value,title,visual/name,is_recent)
    scenarios.to_csv(output/"demand_final_visual_reconciliation_scenarios.csv",index=False)
    chronology.to_csv(output/"demand_final_visual_reconciliation_chronology.csv",index=False)
    summary.to_csv(output/"demand_final_visual_reconciliation_summary.csv",index=False)
    effects.to_csv(output/"demand_final_visual_reconciliation_marginal_effects.csv",index=False)
    recent.to_csv(output/"demand_final_visual_reconciliation_recent.csv",index=False)
    root = {"controlled_root_cause_classification": classification, "classification_evidence": evidence,
            "scenario_ids": scenarios.set_index("scenario").scenario_id.to_dict(),
            "interpretation_guardrail": "Lower variation is not inherently better; more movement is not inherently better.",
            "scope": list(GEOS), **GOVERNANCE}
    (output/"demand_final_visual_reconciliation_root_cause.json").write_text(json.dumps(root,indent=2)+"\n")
    (output/"README.md").write_text(
        "# Final Demand visual reconciliation\n\nDiagnostic-only persisted-evidence comparison of exactly A/B/C/D. "
        "The CSVs provide DC and equal-footing seven-county summaries, four controlled marginal "
        "comparisons, and transparent `D-B-C+A` interactions. Lower variation is not interpreted "
        "as better, and more movement is not interpreted as better. Production policy remains "
        f"unchanged. Controlled classification: `{classification}`.\n")
    return output
