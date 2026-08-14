"""Bounded, diagnostic-only Structural/Cyclical Core Demand calibration.

Only the final block balance varies.  LAUS is reconstructed by the shared
calendar-MA/as-of machinery used by :mod:`laus_long_weight_calibration` and is
then held at LF-IN / MA9 / B3 (40/15/45).  Nothing in this module writes a
production registry or production run.
"""
from __future__ import annotations

from pathlib import Path
import html

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.diagnostics.laus_finalist_stability import reversal_events, reversal_summary
from regime.experiments.demand_signal_attenuation import (
    GEOS, LABOR, STRUCTURAL, _col, _contribution_layer, _load, _scope,
    cancellation, effective_contributions, recent_36,
)
from regime.experiments.laus_long_weight_calibration import (
    RUN_ID, _build as build_shared_laus_evidence, require_authoritative_run,
)
from regime.experiments.laus_ma_window_calibration import _calibration_contract
from regime.experiments.structural_cyclical_demand_architecture import _metric_weights

BALANCES = {
    "BAL-S15-C85": (.15, .85), "BAL-S20-C80": (.20, .80),
    "BAL-S25-C75": (.25, .75), "BAL-S30-C70": (.30, .70),
    "BAL-S35-C65": (.35, .65), "BAL-S40-C60": (.40, .60),
}
PERIODS = ("full_history", "2022_plus", "latest_36_months")
DC = "district_of_columbia_dc__county"
GOVERNANCE = {"recommendation_state": "none",
    "promotion_state": "current_production_unchanged",
    "human_decision": "structural_cyclical_balance_review_pending",
    "automated_winner": False, "production_policy_changed": False}
FIXED = {"labor_force_membership": "LF-IN", "ma_window": "MA9",
    "feature_policy": "B3", "level_weight": .40, "short_weight": .15,
    "long_weight": .45}
EXPORTS = (
    "scenario_registry", "chronology", "contributions", "core_statistics",
    "turn_latency", "turn_preservation", "adjacent_comparisons", "vs_s25",
    "by_county", "period_sensitivity", "demand_axis_statistics",
    "evaluation_matrix", "governance_status",
)


def scenario_registry() -> pd.DataFrame:
    rows = [{"scenario_id": sid, "balance_policy": sid,
             "structural_weight": weights[0], "cyclical_weight": weights[1],
             **FIXED, **GOVERNANCE} for sid, weights in BALANCES.items()]
    out = pd.DataFrame(rows)
    if len(out) != 6 or out.scenario_id.duplicated().any():
        raise AssertionError("exactly six unique balance scenarios required")
    if not np.allclose(out.structural_weight + out.cyclical_weight, 1):
        raise AssertionError("block weights must sum to one")
    return out


def _periods(frame):
    yield "full_history", frame
    yield "2022_plus", frame.loc[pd.to_datetime(frame.date) >= pd.Timestamp("2022-01-01")]
    yield "latest_36_months", recent_36(frame)


def _weights(base: pd.Series, sw: float, cw: float) -> pd.Series:
    out = base.loc[list(STRUCTURAL) + list(LABOR)].copy()
    out.loc[list(STRUCTURAL)] *= sw / out.loc[list(STRUCTURAL)].sum()
    out.loc[list(LABOR)] *= cw / out.loc[list(LABOR)].sum()
    return out


def reconstruct_chronology(structural: pd.DataFrame, cyclical: pd.DataFrame,
                           base_weights: pd.Series) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the production missing-metric renormalizer at each county-month."""
    panel = pd.concat([structural, cyclical], ignore_index=True)
    rows, details = [], []
    for sid, (sw, cw) in BALANCES.items():
        configured = _weights(base_weights, sw, cw)
        for (geo, date), g in panel.groupby(["geo_id", "date"], sort=True):
            calc = effective_contributions(g.score, g.metric.map(configured))
            q = g.assign(effective_metric_weight=calc.effective_feature_weight.to_numpy(),
                         contribution=calc.weighted_feature_contribution.to_numpy())
            for r in q.itertuples():
                details.append({"scenario_id": sid, "geo_id": geo, "date": date,
                    "metric": r.metric, "metric_score": r.score,
                    "effective_metric_weight": r.effective_metric_weight,
                    "weighted_contribution": r.contribution,
                    "block": "structural" if r.metric in STRUCTURAL else "cyclical"})
            s = q.loc[q.metric.isin(STRUCTURAL)]; c = q.loc[q.metric.isin(LABOR)]
            ss_raw, cs_raw = s.contribution.sum(min_count=1), c.contribution.sum(min_count=1)
            sg, _, _ = cancellation(s.contribution); cg, _, _ = cancellation(c.contribution)
            gross, net_magnitude, ci = cancellation(q.contribution)
            total = q.contribution.sum(min_count=1)
            es, ec = s.effective_metric_weight.sum(), c.effective_metric_weight.sum()
            both = pd.notna(ss_raw) and pd.notna(cs_raw)
            ss, cs = (0.0 if pd.isna(ss_raw) else ss_raw), (0.0 if pd.isna(cs_raw) else cs_raw)
            rows.append({"scenario_id": sid, "geo_id": geo, "date": date,
                "structural_block_score": (ss_raw/es if es else np.nan),
                "cyclical_block_score": (cs_raw/ec if ec else np.nan),
                "structural_weighted_contribution": ss,
                "cyclical_weighted_contribution": cs, "core_demand_score": total,
                "effective_structural_weight": es, "effective_cyclical_weight": ec,
                "availability_state": "both" if both else ("structural_only" if pd.notna(ss_raw) else "cyclical_only"),
                "sign_conflict": bool(np.sign(ss) != np.sign(cs)) if both and ss and cs else False,
                "combined_gross_contribution": gross, "cancellation_index": ci,
                "net_to_gross_ratio": net_magnitude/gross if gross else np.nan,
                "structural_share_of_gross": sg/gross if gross else np.nan,
                "cyclical_share_of_gross": cg/gross if gross else np.nan})
    return pd.DataFrame(rows), pd.DataFrame(details)


def _stats(g, value):
    q = g[["date", value]].dropna().sort_values("date"); x = q[value]; d = x.diff()
    events = reversal_events(q, value); rev = reversal_summary(events)
    turns = detect_turning_points(q, value)
    signs = np.sign(x).replace(0, np.nan).dropna()
    direction = np.sign(d).replace(0, np.nan).dropna()
    return {"observations": len(x), "standard_deviation": x.std(),
        "range": x.max()-x.min(), "mean_absolute_monthly_change": d.abs().mean(),
        **rev, "zero_crossings": int(max(0, signs.ne(signs.shift()).sum()-1)),
        "persistence": direction.eq(direction.shift()).iloc[1:].mean() if len(direction)>1 else np.nan,
        "turning_point_count": int(turns.qualified.sum()) if len(turns) else 0}


def _retention(g):
    cyc, core = "cyclical_block_score", "core_demand_score"
    a, b = _stats(g, cyc), _stats(g, core)
    dx, dy = g.sort_values("date")[[cyc, core]].diff().T.to_numpy()
    return {"cyclical_amplitude_retention": b["standard_deviation"]/a["standard_deviation"] if a["standard_deviation"] else np.nan,
        "cyclical_reversal_retention": b["total_reversal_count"]/a["total_reversal_count"] if a["total_reversal_count"] else np.nan,
        "cyclical_turning_point_retention": b["turning_point_count"]/a["turning_point_count"] if a["turning_point_count"] else np.nan,
        "direction_agreement": np.nanmean(np.sign(dx[1:]) == np.sign(dy[1:])),
        "sign_agreement": np.nanmean(np.sign(g[cyc]) == np.sign(g[core])),
        "chronology_correlation": g[cyc].corr(g[core])}


def _turn_evidence(chronology):
    latency, preservation = [], []
    for (sid, geo), g in chronology.groupby(["scenario_id", "geo_id"]):
        source = detect_turning_points(g[["date", "cyclical_block_score"]].dropna(), "cyclical_block_score")
        target = detect_turning_points(g[["date", "core_demand_score"]].dropna(), "core_demand_score")
        source = source.loc[source.qualified]; target = target.loc[target.qualified]
        matches = match_turning_points(source, target)
        for r in matches.itertuples(): latency.append({"scenario_id":sid,"geo_id":geo,
            "cyclical_turn_date":r.incumbent_date,"core_turn_date":r.challenger_date,
            "turn_type":r.turning_point_type,"matched":r.matched,"signed_latency_months":r.signed_delay_months})
        matched = matches.loc[matches.matched]; absolute = matched.signed_delay_months.abs()
        preservation.append({"scenario_id":sid,"geo_id":geo,"cyclical_turn_count":len(source),
            "matched_cyclical_turns":len(matched),"missed_cyclical_turns":len(source)-len(matched),
            "median_absolute_turn_latency":absolute.median(),
            "peak_latency":matched.loc[matched.turning_point_type.eq("peak"),"signed_delay_months"].median(),
            "trough_latency":matched.loc[matched.turning_point_type.eq("trough"),"signed_delay_months"].median(),
            "same_month_detection_share":absolute.eq(0).mean(),"plus_1_month_share":absolute.eq(1).mean(),
            "plus_2_or_more_month_share":absolute.ge(2).mean()})
    return pd.DataFrame(latency), pd.DataFrame(preservation)


def _differences(stats, pairs):
    keys = ["geo_id", "period"]
    numeric = [c for c in stats.select_dtypes("number") if c not in ()]
    rows=[]
    for left,right in pairs:
        a=stats.loc[stats.scenario_id.eq(left),keys+numeric]
        b=stats.loc[stats.scenario_id.eq(right),keys+numeric]
        q=a.merge(b,on=keys,suffixes=("_from","_to"))
        for c in numeric: q[f"change_{c}"]=q.pop(c+"_to")-q.pop(c+"_from")
        q.insert(0,"comparison_id",f"{left}__to__{right}"); q.insert(1,"from_scenario",left); q.insert(2,"to_scenario",right)
        rows.append(q)
    return pd.concat(rows,ignore_index=True)


def build_review(run: Path, output: Path, root: Path|None=None) -> Path:
    """Build all evidence, failing before output creation when the run is absent."""
    run=require_authoritative_run(run); root=(root or Path(__file__).resolve().parents[2]).resolve()
    _, _, metric, _, _ = build_shared_laus_evidence(run, root)
    laus=metric.loc[(metric.scenario_id=="MA9__B3"),["geo_id","date","metric","metric_score"]].rename(columns={"metric_score":"score"})
    persisted=_load(run,"aligned_metric_scores").rename(columns={
        _col(_load(run,"aligned_metric_scores"),"canonical_metric_key","metric_key","metric"):"metric",
        _col(_load(run,"aligned_metric_scores"),"aligned_metric_score","metric_score","score"):"score"})
    persisted["metric"]=persisted.metric.replace({"laus_labor_force":"labor_force","laus_employment":"employment"})
    persisted=_scope(persisted,"aligned_metric_scores",["geo_id","date","metric"])
    structural=persisted.loc[persisted.metric.isin(STRUCTURAL),["geo_id","date","metric","score"]]
    mr, ar = _calibration_contract(root)
    chronology, contributions = reconstruct_chronology(structural, laus, _metric_weights(mr))
    if set(chronology.geo_id)!=set(GEOS): raise ValueError("exact governed seven-county scope required")
    dims=_load(run,"dimension_scores").rename(columns={_col(_load(run,"dimension_scores"),"dimension_score","score"):"score"})
    dims=_scope(dims,"dimension_scores",["geo_id","date","dimension"])
    fixed=dims.loc[dims.dimension.str.lower().isin(["price","affordability","capital_markets"])]
    axes=[]
    for sid,g in chronology.groupby("scenario_id"):
        inp=pd.concat([g[["geo_id","date","core_demand_score"]].rename(columns={"core_demand_score":"score"}).assign(dimension="demand"),fixed])
        _,monthly=_contribution_layer(inp,ar.loc[ar.axis.str.lower().eq("demand")],"axis","dimension","score",_col(ar,"dimension_weight","weight"))
        axes.append(monthly.loc[monthly.axis.str.lower().eq("demand"),["geo_id","date","net_score"]].assign(scenario_id=sid))
    axis=pd.concat(axes).rename(columns={"net_score":"demand_axis_score"}); chronology=chronology.merge(axis)
    rows=[]
    for (sid,geo),g in chronology.groupby(["scenario_id","geo_id"]):
        for period,q in _periods(g): rows.append({"scenario_id":sid,"geo_id":geo,"period":period,
            **_stats(q,"core_demand_score"),**_retention(q),
            "structural_contribution_std":q.structural_weighted_contribution.std(),
            "cyclical_contribution_std":q.cyclical_weighted_contribution.std(),
            "structural_share_of_gross":q.structural_share_of_gross.mean(),
            "cyclical_share_of_gross":q.cyclical_share_of_gross.mean(),
            "sign_agreement_blocks":1-q.sign_conflict.mean(),"conflict_month_share":q.sign_conflict.mean(),
            "cancellation_index":q.cancellation_index.mean(),"net_to_gross_ratio":q.net_to_gross_ratio.mean()})
    stats=pd.DataFrame(rows); latency,preservation=_turn_evidence(chronology)
    stats=stats.merge(preservation,on=["scenario_id","geo_id"],how="left")
    axis_rows=[]
    for (sid,geo),g in chronology.groupby(["scenario_id","geo_id"]):
      for period,q in _periods(g):
        a=_stats(q,"demand_axis_score"); core=_stats(q,"core_demand_score")
        axis_rows.append({"scenario_id":sid,"geo_id":geo,"period":period,**a,
          "core_to_axis_amplitude_retention":a["standard_deviation"]/core["standard_deviation"] if core["standard_deviation"] else np.nan,
          "core_to_axis_direction_agreement":np.sign(q.demand_axis_score.diff()).eq(np.sign(q.core_demand_score.diff())).iloc[1:].mean(),
          "core_to_axis_reversal_retention":a["total_reversal_count"]/core["total_reversal_count"] if core["total_reversal_count"] else np.nan})
    axis_stats=pd.DataFrame(axis_rows)
    adjacent=_differences(stats,list(zip(list(BALANCES)[:-1],list(BALANCES)[1:])))
    versus=_differences(stats,[(x,"BAL-S25-C75") if list(BALANCES).index(x)<2 else ("BAL-S25-C75",x) for x in BALANCES if x!="BAL-S25-C75"])
    by_county = stats.copy()

    numeric_cols = [
        column
        for column in stats.select_dtypes(include="number").columns
        if column not in {"structural_weight", "cyclical_weight"}
    ]

    pooled = (
        stats.groupby(["scenario_id", "period"])[numeric_cols]
        .agg(["mean", "median", "min", "max"])
    )
    pooled.columns = ["_".join(x) for x in pooled.columns]
    pooled = pooled.reset_index()
    evaluation=pooled.loc[pooled.period.eq("full_history")].copy(); evaluation["automated_winner"]=False; evaluation["human_review_required"]=True
    governance=pd.DataFrame([{**GOVERNANCE,**FIXED,"authoritative_run":RUN_ID,"decision_basis":"seven_county_equal_footing","governed_county_count":7,"dc_geo_id":DC}])
    exports={"scenario_registry":scenario_registry(),"chronology":chronology,"contributions":contributions,"core_statistics":stats,
      "turn_latency":latency,"turn_preservation":preservation,"adjacent_comparisons":adjacent,"vs_s25":versus,"by_county":by_county,
      "period_sensitivity":pooled,"demand_axis_statistics":axis_stats,"evaluation_matrix":evaluation,"governance_status":governance}
    output=output.resolve()
    if output.exists(): raise FileExistsError(output)
    output.mkdir(parents=True); visual=output/"visual_review"; visual.mkdir()
    for name in EXPORTS: exports[name].to_csv(output/f"structural_cyclical_balance_{name}.csv",index=False)
    _plots(chronology,stats,preservation,visual)
    links="".join(f'<li><a href="structural_cyclical_balance_{html.escape(n)}.csv">{html.escape(n)}</a></li>' for n in EXPORTS)
    visuals="".join(f'<li><a href="visual_review/{p.name}">{p.name}</a></li>' for p in sorted(visual.glob("*.svg")))
    (output/"structural_cyclical_balance_review.html").write_text("<!doctype html><meta charset='utf-8'><h1>Structural/Cyclical balance review</h1><p>Diagnostic only; no automated winner or production change. Human balance review remains pending.</p><ul>"+links+"</ul><h2>Visuals</h2><ul>"+visuals+"</ul>")
    return output


def _plots(chronology, stats, preservation, output):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    pooled=chronology.groupby(["scenario_id","date"],as_index=False).mean(numeric_only=True)
    for scope,data in (("dc",chronology.loc[chronology.geo_id.eq(DC)]),("seven_county_equal_footing",pooled)):
      fig,axes=plt.subplots(3,2,figsize=(13,9),sharex=True)
      for sid,ax in zip(BALANCES,axes.flat):
        q=data.loc[data.scenario_id.eq(sid)]; ax.plot(q.date,q.core_demand_score); ax.set_title(sid)
      fig.tight_layout(); fig.savefig(output/f"core_demand__{scope}.svg"); plt.close(fig)
    fig,axes=plt.subplots(3,2,figsize=(13,9),sharex=True)
    for sid,ax in zip(BALANCES,axes.flat):
      q=pooled.loc[pooled.scenario_id.eq(sid)]; ax.plot(q.date,q.structural_weighted_contribution,label="Structural"); ax.plot(q.date,q.cyclical_weighted_contribution,label="Cyclical"); ax.plot(q.date,q.core_demand_score,label="Core"); ax.set_title(sid); ax.legend()
    fig.tight_layout(); fig.savefig(output/"contributions__seven_county.svg"); plt.close(fig)
    response=stats.loc[stats.period.eq("full_history")].groupby("scenario_id",as_index=False).mean(numeric_only=True)
    measures=("standard_deviation","total_reversal_count","whipsaw_2m_count","whipsaw_3m_count","persistence","median_absolute_turn_latency","cyclical_amplitude_retention","cyclical_reversal_retention","cancellation_index")
    fig,axes=plt.subplots(3,3,figsize=(14,11))
    for m,ax in zip(measures,axes.flat): ax.plot(range(15,41,5),response.set_index("scenario_id").loc[list(BALANCES),m],marker="o"); ax.set_title(m.replace("_"," "))
    fig.tight_layout(); fig.savefig(output/"response_curves.svg"); plt.close(fig)
    fig,axes=plt.subplots(1,3,figsize=(14,4)); frontier=(("whipsaw_2m_share","median_absolute_turn_latency"),("total_reversal_count","cyclical_turning_point_retention"),("structural_share_of_gross","cyclical_amplitude_retention"))
    for (x,y),ax in zip(frontier,axes): ax.scatter(response[x],response[y]); ax.set(xlabel=x,ylabel=y)
    fig.tight_layout(); fig.savefig(output/"stability_responsiveness_frontier.svg"); plt.close(fig)
    dc=stats.loc[(stats.geo_id==DC)&(stats.period=="full_history")].set_index("scenario_id"); seven=response.set_index("scenario_id")
    fig,axes=plt.subplots(1,2,figsize=(10,4))
    for m,ax in zip(("standard_deviation","cyclical_amplitude_retention"),axes): ax.plot(range(15,41,5),dc.loc[list(BALANCES),m],label="DC"); ax.plot(range(15,41,5),seven.loc[list(BALANCES),m],label="7-county mean"); ax.set_title(m); ax.legend()
    fig.tight_layout(); fig.savefig(output/"dc_vs_seven_county.svg"); plt.close(fig)
