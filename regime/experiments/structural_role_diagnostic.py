"""Diagnostic-only Structural role test: blended Demand versus Market Context.

The experiment deliberately has only three product architectures.  B and C
share one numeric chronology; C additionally preserves the governed Structural
surface as non-scoring context.  No registry is read for candidate weights and
no production artifact is written.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime.experiments.structural_cyclical_balance_calibration import (
    DC, FIXED, GEOS, RUN_ID, _metric_weights, _periods, _retention, _stats,
    _turn_evidence, reconstruct_chronology,
)
from regime.experiments.demand_signal_attenuation import (
    STRUCTURAL, _col, _contribution_layer, _load, _scope,
)
from regime.experiments.laus_long_weight_calibration import (
    _build as build_shared_laus_evidence, require_authoritative_run,
)
from regime.experiments.laus_ma_window_calibration import _calibration_contract

SCENARIOS = {
    "A_S5_BLENDED": (.05, .95, False),
    "B_LABOR_ONLY": (0.0, 1.0, False),
    "C_LABOR_ONLY_MARKET_CONTEXT": (0.0, 1.0, True),
}
AXIS_WEIGHTS = {"demand": .65, "price": .175, "affordability": .075, "capital_markets": .10}
GOVERNANCE = {"recommendation_state": "none", "promotion_state": "current_production_unchanged",
    "human_decision": "structural_role_review_pending", "automated_winner": False,
    "production_policy_changed": False}
EXPORTS = ("scenario_registry", "chronology", "core_statistics", "incremental_value",
    "turn_latency", "turn_preservation", "by_county", "period_sensitivity", "cancellation",
    "demand_axis_statistics", "market_context_profile", "information_overlap", "parity_proof",
    "evaluation_matrix", "governance_status")


def scenario_registry() -> pd.DataFrame:
    rows=[]
    for sid,(sw,lw,context) in SCENARIOS.items():
        rows.append({"scenario_id":sid,"structural_weight_inside_demand":sw,
            "labor_weight_inside_demand":lw,"market_context_retained":context,
            "market_context_scoring_weight":0.0,"demand_axis_budget":.65,
            **FIXED,**GOVERNANCE})
    return pd.DataFrame(rows)


def parity_proof() -> pd.DataFrame:
    """Algebra and explicit availability-boundary cases for arbitrary ``s``."""
    return pd.DataFrame([
      {"case":"complete_availability","inside_expression":"0.65*((1-s)*L+s*S)+O",
       "moved_expression":"0.65*(1-s)*L+0.65*s*S+O","exact_parity":True,
       "condition":"same observations and equivalent normalization; distributive law",
       "s5_labor_effective_weight":.6175,"s5_structural_effective_weight":.0325},
      {"case":"labor_only_available_equivalent_renormalization","inside_expression":"0.65*L+O",
       "moved_expression":"0.65*L+O","exact_parity":True,
       "condition":"both levels renormalize the missing Structural allocation to Labor",
       "s5_labor_effective_weight":.65,"s5_structural_effective_weight":0.0},
      {"case":"structural_only_available_equivalent_renormalization","inside_expression":"0.65*S+O",
       "moved_expression":"0.65*S+O","exact_parity":True,
       "condition":"both levels renormalize the missing Labor allocation to Structural",
       "s5_labor_effective_weight":0.0,"s5_structural_effective_weight":.65},
      {"case":"normalization_semantics_differ","inside_expression":"renorm_inside(L,S)",
       "moved_expression":"renorm_axis(0.65*(1-s)*L,0.65*s*S,O)","exact_parity":False,
       "condition":"parity can break when one level renormalizes missing components and the other preserves zero/missing weight, or availability masks differ",
       "s5_labor_effective_weight":np.nan,"s5_structural_effective_weight":np.nan},
    ])


def construct_architectures(s5: pd.DataFrame) -> pd.DataFrame:
    """Construct A/B/C from one governed S5 chronology without recomputation."""
    a=s5.copy(); a["scenario_id"]="A_S5_BLENDED"
    labor=s5.copy()
    labor["core_demand_score"]=labor["cyclical_block_score"]
    labor["structural_weighted_contribution"]=0.0
    labor["cyclical_weighted_contribution"]=labor["cyclical_block_score"]
    labor["combined_gross_contribution"]=labor.core_demand_score.abs()
    labor["cancellation_index"]=0.0
    labor["net_to_gross_ratio"]=np.where(labor.core_demand_score.notna(),1.0,np.nan)
    labor["sign_conflict"]=False
    labor["effective_structural_weight"]=0.0; labor["effective_cyclical_weight"]=1.0
    b=labor.copy(); b["scenario_id"]="B_LABOR_ONLY"
    c=labor.copy(); c["scenario_id"]="C_LABOR_ONLY_MARKET_CONTEXT"
    return pd.concat([a,b,c],ignore_index=True)


def _profile(structural: pd.DataFrame) -> pd.DataFrame:
    source_col=next((c for c in ("source_date","observation_date","vintage_date","data_as_of") if c in structural),None)
    rows=[]
    for (geo,metric),g in structural.groupby(["geo_id","metric"]):
        q=g.sort_values("date"); changed=q.score.diff().ne(0) & q.score.notna()
        ages=(pd.to_datetime(q.date)-pd.to_datetime(q[source_col])).dt.days if source_col else pd.Series(dtype=float)
        rows.append({"scenario_id":"C_LABOR_ONLY_MARKET_CONTEXT","geo_id":geo,"metric":metric,
          "observations":q.score.notna().sum(),"standard_deviation":q.score.std(),
          "update_frequency":changed.mean(),"persistence":np.sign(q.score.diff()).replace(0,np.nan).dropna().eq(np.sign(q.score.diff()).replace(0,np.nan).dropna().shift()).mean(),
          "county_coverage":q.geo_id.nunique(),"metric_available":q.score.notna().any(),
          "turning_point_suitability":"limited_slow_or_annual_context",
          "median_source_age_days":ages.median() if len(ages) else np.nan,
          "latest_source_age_days":ages.iloc[-1] if len(ages) else np.nan,
          "freshness_distribution":("min=%s;p25=%s;p75=%s;max=%s" % tuple(ages.quantile([0,.25,.75,1]).tolist())) if len(ages) else "source-date columns unavailable on normalized governed surface"})
    return pd.DataFrame(rows)


def _overlap(structural: pd.DataFrame, labor: pd.DataFrame) -> pd.DataFrame:
    labels={"population":"Labor Force","gdp":"Employment / Labor conditions","income":"Employment / Labor conditions"}
    rows=[]
    lab=labor.groupby(["geo_id","date"],as_index=False).score.mean().rename(columns={"score":"labor_score"})
    for (geo,metric),g in structural.groupby(["geo_id","metric"]):
      q=g.merge(lab,on=["geo_id","date"]).sort_values("date")
      rows.append({"geo_id":geo,"structural_metric":metric,"labor_relationship":next((v for k,v in labels.items() if k in metric.lower()),"Labor conditions"),
        "common_frequency_alignment":"month-end persisted chronology; interpret annual step series cautiously",
        "chronology_correlation":q.score.corr(q.labor_score),
        "direction_agreement":np.sign(q.score.diff()).eq(np.sign(q.labor_score.diff())).iloc[1:].mean() if len(q)>1 else np.nan,
        "interpretation":"descriptive overlap only; no feature selection or predictive claim"})
    return pd.DataFrame(rows)


def build_review(run: Path, output: Path, root: Path|None=None) -> Path:
    run=require_authoritative_run(run); root=(root or Path(__file__).resolve().parents[2]).resolve()
    _,_,metric,_,_=build_shared_laus_evidence(run,root)
    labor=metric.loc[metric.scenario_id.eq("MA9__B3"),["geo_id","date","metric","metric_score"]].rename(columns={"metric_score":"score"})
    raw=_load(run,"aligned_metric_scores")
    persisted=raw.rename(columns={_col(raw,"canonical_metric_key","metric_key","metric"):"metric",_col(raw,"aligned_metric_score","metric_score","score"):"score"})
    persisted["metric"]=persisted.metric.replace({"laus_labor_force":"labor_force","laus_employment":"employment"})
    persisted=_scope(persisted,"aligned_metric_scores",["geo_id","date","metric"])
    context_cols=[c for c in ("source_date","observation_date","vintage_date","data_as_of") if c in persisted]
    structural=persisted.loc[persisted.metric.isin(STRUCTURAL),["geo_id","date","metric","score",*context_cols]]
    mr,ar=_calibration_contract(root)
    base,_=reconstruct_chronology(structural,labor,_metric_weights(mr))
    s5=base.loc[base.scenario_id.eq("BAL-S05-C95")].drop(columns="scenario_id")
    chronology=construct_architectures(s5)
    if set(chronology.geo_id)!=set(GEOS): raise ValueError("exact governed seven-county scope required")
    dims=_scope(_load(run,"dimension_scores").rename(columns={_col(_load(run,"dimension_scores"),"dimension_score","score"):"score"}),"dimension_scores",["geo_id","date","dimension"])
    fixed=dims.loc[dims.dimension.str.lower().isin(["price","affordability","capital_markets"])]
    axes=[]
    for sid,g in chronology.groupby("scenario_id"):
      inp=pd.concat([g[["geo_id","date","core_demand_score"]].rename(columns={"core_demand_score":"score"}).assign(dimension="demand"),fixed])
      _,monthly=_contribution_layer(inp,ar.loc[ar.axis.str.lower().eq("demand")],"axis","dimension","score",_col(ar,"dimension_weight","weight"))
      axes.append(monthly.loc[monthly.axis.str.lower().eq("demand"),["geo_id","date","net_score"]].rename(columns={"net_score":"demand_axis_score"}).assign(scenario_id=sid))
    chronology=chronology.merge(pd.concat(axes),on=["scenario_id","geo_id","date"])
    stats=[]; axisstats=[]
    for (sid,geo),g in chronology.groupby(["scenario_id","geo_id"]):
      for period,q in _periods(g):
        stats.append({"scenario_id":sid,"geo_id":geo,"period":period,**_stats(q,"core_demand_score"),**_retention(q)})
        axisstats.append({"scenario_id":sid,"geo_id":geo,"period":period,**_stats(q,"demand_axis_score"),
          "direction_agreement_to_labor":np.sign(q.demand_axis_score.diff()).eq(np.sign(q.cyclical_block_score.diff())).iloc[1:].mean(),
          "chronology_correlation_to_labor":q.demand_axis_score.corr(q.cyclical_block_score)})
    stats=pd.DataFrame(stats); axisstats=pd.DataFrame(axisstats)
    latency,preservation=_turn_evidence(chronology)
    # _turn_evidence names the scenarios generically and remains shared logic.
    stats=stats.merge(preservation,on=["scenario_id","geo_id"],how="left")
    a=stats.loc[stats.scenario_id.eq("A_S5_BLENDED")]; b=stats.loc[stats.scenario_id.eq("B_LABOR_ONLY")]
    incremental=a.merge(b,on=["geo_id","period"],suffixes=("_s5","_labor"))
    for col in stats.select_dtypes("number").columns:
      if col in a.columns: incremental["delta_"+col]=incremental[col+"_s5"]-incremental[col+"_labor"]
    cancellation=chronology.groupby(["scenario_id","geo_id"],as_index=False).agg(gross_contribution=("combined_gross_contribution","mean"),net_contribution=("core_demand_score",lambda x:x.abs().mean()),cancellation_index=("cancellation_index","mean"),net_to_gross_ratio=("net_to_gross_ratio","mean"),conflict_month_share=("sign_conflict","mean"))
    numeric=[c for c in stats.select_dtypes("number")]
    pooled=stats.groupby(["scenario_id","period"])[numeric].agg(["mean","median","min","max"]); pooled.columns=["_".join(x) for x in pooled.columns]; pooled=pooled.reset_index()
    evaluation=pooled.loc[pooled.period.eq("full_history")].copy(); evaluation["automated_winner"]=False; evaluation["human_review_required"]=True
    governance=pd.DataFrame([{**GOVERNANCE,**FIXED,**{f"axis_weight_{k}":v for k,v in AXIS_WEIGHTS.items()},"authoritative_run":RUN_ID,"governed_county_count":7,"dc_geo_id":DC,
      "cbsa_gdp_roadmap":"Reassess GDP independently during CBSA calibration because higher-frequency quarterly GDP may contain usable cyclical information not present in county-level annual GDP."}])
    exports={"scenario_registry":scenario_registry(),"chronology":chronology,"core_statistics":stats,"incremental_value":incremental,
      "turn_latency":latency,"turn_preservation":preservation,"by_county":stats,"period_sensitivity":pooled,"cancellation":cancellation,
      "demand_axis_statistics":axisstats,"market_context_profile":_profile(structural),"information_overlap":_overlap(structural,labor),
      "parity_proof":parity_proof(),"evaluation_matrix":evaluation,"governance_status":governance}
    output=output.resolve()
    if output.exists(): raise FileExistsError(output)
    output.mkdir(parents=True); visual=output/"visual_review"; visual.mkdir()
    for name in EXPORTS: exports[name].to_csv(output/f"structural_role_{name}.csv",index=False)
    _plots(chronology,stats,preservation,visual)
    links="".join(f'<li><a href="structural_role_{html.escape(n)}.csv">{html.escape(n)}</a></li>' for n in EXPORTS)
    figs="".join(f'<li><a href="visual_review/{p.name}">{p.name}</a></li>' for p in sorted(visual.glob("*.svg")))
    (output/"structural_role_review.html").write_text("<!doctype html><meta charset=utf-8><h1>Structural role diagnostic</h1><p>Diagnostic only. No automated winner; production unchanged.</p><p>B and C are numerically identical. C preserves Structural only as zero-weight Market Context.</p><ul>"+links+"</ul><h2>Visuals</h2><ul>"+figs+"</ul>")
    return output


def _plots(ch,stats,preservation,out):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    ids=["A_S5_BLENDED","B_LABOR_ONLY"]
    pooled=ch.groupby(["scenario_id","date"],as_index=False).mean(numeric_only=True)
    for label,q in (("dc",ch.loc[ch.geo_id.eq(DC)]),("seven_county_equal_footing",pooled)):
      fig,ax=plt.subplots(figsize=(11,4))
      for sid in ids: z=q.loc[q.scenario_id.eq(sid)]; ax.plot(z.date,z.core_demand_score,label=sid)
      ax.legend(); fig.tight_layout(); fig.savefig(out/f"s5_vs_labor_only__{label}.svg"); plt.close(fig)
    wide=pooled.pivot(index="date",columns="scenario_id",values="core_demand_score")
    fig,ax=plt.subplots(figsize=(11,3)); ax.plot(wide.index,wide[ids[0]]-wide[ids[1]]); ax.axhline(0,color="black",lw=.5); ax.set_title("S5 blended - Labor-only"); fig.tight_layout(); fig.savefig(out/"incremental_structural_effect.svg"); plt.close(fig)
    fig,ax=plt.subplots(figsize=(11,4))
    for sid in ids: z=pooled.loc[pooled.scenario_id.eq(sid)]; ax.plot(z.date,z.demand_axis_score,label=sid)
    ax.legend(); ax.set_title("Final Demand axis"); fig.tight_layout(); fig.savefig(out/"demand_axis_comparison.svg"); plt.close(fig)
    measures=["total_reversal_count","whipsaw_2m_count","whipsaw_3m_count","persistence","cyclical_turning_point_retention"]
    q=stats.loc[stats.period.eq("full_history")].groupby("scenario_id")[measures].mean().loc[ids]
    fig,axes=plt.subplots(1,len(measures),figsize=(16,4));
    for m,ax in zip(measures,axes): ax.bar(["S5","Labor"],q[m]); ax.set_title(m.replace("_"," "))
    fig.tight_layout(); fig.savefig(out/"stability_responsiveness.svg"); plt.close(fig)
    fig,ax=plt.subplots(figsize=(11,4)); z=pooled.loc[pooled.scenario_id.eq("C_LABOR_ONLY_MARKET_CONTEXT")]; ax.plot(z.date,z.structural_block_score,label="Market Context (0% scoring)"); ax.legend(); fig.tight_layout(); fig.savefig(out/"market_context_non_scoring.svg"); plt.close(fig)
