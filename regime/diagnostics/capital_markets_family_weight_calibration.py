"""Diagnostic-only Capital Markets family-weight calibration.

The diagnostic deliberately starts at persisted aligned metric scores.  It does
not import feature builders and it never writes a production registry or run.
"""
from __future__ import annotations

from pathlib import Path
from time import perf_counter
import html
import numpy as np
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical
from regime.diagnostics.capital_markets_feature_anatomy import (
    EXPECTED_AXIS_WEIGHTS, EXPECTED_WEIGHTS, REVIEW_GEOS, load_run as _load_run,
)
from regime.diagnostics.capital_markets_feature_weight_calibration import _comparison, _stats
from regime.diagnostics.correlation import safe_corr

AUTHORITATIVE_RUN = Path("artifacts/regime/runs/capital_markets_feature_policy_production_20260818")
FAMILIES = {
    "long_term_rates": ("mortgage_30y", "mortgage_15y", "treasury_10y"),
    "fedfunds": ("fedfunds",),
    "spreads": ("spread_10y_2y", "spread_10y_fedfunds"),
}
POLICIES = {
    "F0": (.45, .10, .45), "F1": (.50, .10, .40),
    "F2": (.40, .10, .50), "F3": (.55, .10, .35),
    "F4": (.35, .10, .55), "F5": (.475, .05, .475),
    "F6": (.425, .15, .425), "F7": (.40, .20, .40),
    "F8": (.375, .25, .375), "F9": (.50, .05, .45),
}
PURPOSES = {
    "F0":"current production incumbent", "F1":"modest spreads to rates",
    "F2":"modest rates to spreads", "F3":"stronger rates bias",
    "F4":"stronger spreads bias", "F5":"reduced Fed Funds balanced",
    "F6":"modestly increased Fed Funds", "F7":"strong Fed Funds stress",
    "F8":"aggressive Fed Funds boundary stress", "F9":"low-Fed rates bias",
}
RATES_SPREADS_LADDER = ("F3", "F1", "F0", "F2", "F4")
FEDFUNDS_LADDER = ("F5", "F0", "F6", "F7", "F8")
CONTROLLED = tuple(zip(RATES_SPREADS_LADDER, RATES_SPREADS_LADDER[1:])) + tuple(zip(FEDFUNDS_LADDER, FEDFUNDS_LADDER[1:])) + (("F5", "F9"),)
NATIVE_POLICIES = {"mortgage_30y":"P4", "mortgage_15y":"P2", "treasury_10y":"P1", "fedfunds":"P5", "spread_10y_2y":"P7", "spread_10y_fedfunds":"P9"}
PERIODS = ("full_history", "2022_plus", "latest_36_months")
EXPORTS = (
    "scenario_registry", "metric_weights", "contributions", "family_contributions",
    "dimension_chronology", "dimension_statistics", "incumbent_comparison",
    "controlled_comparisons", "period_sensitivity", "by_county", "family_overlap",
    "within_family_consistency", "responsiveness", "contribution_structure",
    "demand_axis_statistics", "supply_axis_statistics", "cross_axis_materiality",
    "decision_table", "evaluation_matrix", "governance_status", "performance_audit",
)

def load_run(run: Path) -> dict[str, pd.DataFrame]:
    if not run.exists():
        raise FileNotFoundError(f"authoritative Capital Markets production run absent: {run}; no substitution permitted")
    return _load_run(run)

def _periods(q):
    q=q.sort_values("date")
    yield "full_history",q
    yield "2022_plus",q[q.date.ge("2022-01-01")]
    yield "latest_36_months",q[q.date.ge(q.date.max()-pd.DateOffset(months=35))]

def metric_weights() -> pd.DataFrame:
    rows=[]
    for policy,(rates,fed,spreads) in POLICIES.items():
        weights={m:rates/3 for m in FAMILIES["long_term_rates"]}|{"fedfunds":fed}|{m:spreads/2 for m in FAMILIES["spreads"]}
        for metric,weight in weights.items():
            rows.append({"policy":policy,"metric":metric,"family":next(f for f,ms in FAMILIES.items() if metric in ms),"configured_metric_weight":weight,"native_feature_policy":NATIVE_POLICIES[metric]})
    out=pd.DataFrame(rows)
    if list(POLICIES)!=[f"F{i}" for i in range(10)] or not np.allclose(out.groupby("policy").configured_metric_weight.sum(),1,atol=1e-15):
        raise ValueError("closed F0-F9 grid or derived weights invalid")
    return out

def _axis(candidate, artifacts, root, axis):
    registry=pd.read_csv(root/"config/axis_registry.csv")
    enabled=registry.enabled.astype(str).str.lower().isin(("true","1","yes"))
    ar=registry[enabled & registry.axis.eq(axis)][["dimension","dimension_weight"]]
    configured=ar.set_index("dimension").dimension_weight.astype(float)
    if not np.isclose(configured["capital_markets"],EXPECTED_AXIS_WEIGHTS[axis]): raise ValueError(f"{axis} Capital Markets weight changed")
    dims=canonical._dates(artifacts["dimension_scores"])
    wide=dims[dims.dimension.isin(configured.index)].pivot(index=["geo_id","date"],columns="dimension",values="dimension_score")
    cm=candidate.set_index(["geo_id","date"]).candidate_capital_markets.reindex(wide.index)
    wide["capital_markets"]=cm
    valid=wide.notna(); effective=valid.mul(configured).div(valid.mul(configured).sum(axis=1),axis=0)
    contributions=wide.mul(effective); score=contributions.sum(axis=1,min_count=1); gross=contributions.abs().sum(axis=1)
    other=contributions.drop(columns="capital_markets").sum(axis=1,min_count=1)
    return pd.DataFrame({"geo_id":score.index.get_level_values(0),"date":score.index.get_level_values(1),"candidate_axis_score":score.values,"capital_markets_contribution":contributions.capital_markets.values,"other_contribution":other.values,"contribution_share":contributions.capital_markets.abs().div(gross).values,"cancellation":(1-score.abs().div(gross.replace(0,np.nan))).values,"net_to_gross":score.abs().div(gross.replace(0,np.nan)).values})

def build(artifacts: dict[str,pd.DataFrame], root: Path) -> dict[str,pd.DataFrame]:
    started=perf_counter(); weights=metric_weights()
    registry=pd.DataFrame([{"policy":p,"long_rate_family_weight":w[0],"fedfunds_family_weight":w[1],"spread_family_weight":w[2],"purpose":PURPOSES[p],"candidate_grid":"F0-F9","candidate_grid_closed":True} for p,w in POLICIES.items()])
    aligned=canonical._dates(artifacts["aligned_metric_scores"]); mc=canonical._metric_col(aligned); vc=canonical._value_col(aligned,("aligned_metric_score","metric_score","score"))
    aligned=aligned.rename(columns={mc:"metric",vc:"aligned_metric_score","evaluation_date":"date"})
    aligned["date"]=pd.to_datetime(aligned.date); aligned=aligned[aligned.metric.isin(sum((list(x) for x in FAMILIES.values()),[]))]
    panels=[]
    for policy in POLICIES:
        q=aligned.merge(weights[weights.policy.eq(policy)],on="metric",validate="many_to_one")
        q["metric_available"]=q.aligned_metric_score.notna(); keys=[q.geo_id,q.date]
        q["available_weight_sum"]=q.configured_metric_weight.where(q.metric_available,0).groupby(keys).transform("sum")
        q["effective_metric_weight"]=q.configured_metric_weight.div(q.available_weight_sum).where(q.metric_available)
        q["weighted_metric_contribution"]=q.aligned_metric_score*q.effective_metric_weight
        q["candidate_capital_markets"]=q.groupby(keys).weighted_metric_contribution.transform(lambda x:x.sum(min_count=1))
        panels.append(q)
    contrib=pd.concat(panels,ignore_index=True)
    chronology=contrib.drop_duplicates(["policy","geo_id","date"])[["policy","geo_id","date","candidate_capital_markets"]]
    dims=canonical._dates(artifacts["dimension_scores"]); production=dims[dims.dimension.eq("capital_markets")][["geo_id","date","dimension_score"]]
    control=chronology[chronology.policy.eq("F0")].merge(production,on=["geo_id","date"],validate="one_to_one")
    if len(control)!=len(chronology[chronology.policy.eq("F0")]) or not np.allclose(control.candidate_capital_markets,control.dimension_score,equal_nan=True,atol=1e-12,rtol=0): raise ValueError("F0 does not reconstruct production within 1e-12")
    # Family surfaces and monthly contribution anatomy.
    fam=contrib.groupby(["policy","geo_id","date","family"],as_index=False).agg(configured_family_weight=("configured_metric_weight","sum"),effective_family_weight=("effective_metric_weight","sum"),weighted_family_contribution=("weighted_metric_contribution","sum"))
    gross=fam.groupby(["policy","geo_id","date"]).weighted_family_contribution.transform(lambda x:x.abs().sum()); net=fam.groupby(["policy","geo_id","date"]).weighted_family_contribution.transform("sum")
    fam["family_absolute_contribution_share"]=fam.weighted_family_contribution.abs().div(gross.replace(0,np.nan)); fam["opposes_net"]=np.sign(fam.weighted_family_contribution).ne(np.sign(net))
    contrib["absolute_contribution_share"]=contrib.weighted_metric_contribution.abs().div(contrib.groupby(["policy","geo_id","date"]).weighted_metric_contribution.transform(lambda x:x.abs().sum()).replace(0,np.nan))
    structures=[]; stats=[]; comps=[]
    p0=chronology[chronology.policy.eq("F0")][["geo_id","date","candidate_capital_markets"]].rename(columns={"candidate_capital_markets":"f0"})
    for (policy,geo),g in chronology.groupby(["policy","geo_id"]):
        fg=fam[(fam.policy.eq(policy))&(fam.geo_id.eq(geo))]
        for period,z in _periods(g):
            st=_stats(z.candidate_capital_markets,z.date); stats.append({"policy":policy,"geo_id":geo,"period":period,**st})
            ref=z.merge(p0[p0.geo_id.eq(geo)],on=["geo_id","date"]); co=_comparison(ref.candidate_capital_markets,ref.f0,ref.date)
            comps.append({"policy":policy,"geo_id":geo,"period":period,**co,"reference_type":"incumbent_chronology_reference"})
            fz=fg[fg.date.isin(z.date)]; shares=fz.groupby("family").weighted_family_contribution.apply(lambda x:x.abs().sum()); total=shares.sum(); monthly=fz.pivot(index="date",columns="family",values="weighted_family_contribution"); gr=monthly.abs().sum(axis=1); nt=monthly.sum(axis=1).abs(); dom=monthly.abs().idxmax(axis=1).value_counts(normalize=True)
            row={"policy":policy,"geo_id":geo,"period":period,"contribution_concentration":float(((shares/total)**2).sum()) if total else np.nan,"cancellation":float((1-nt.div(gr.replace(0,np.nan))).mean()),"net_to_gross":float(nt.div(gr.replace(0,np.nan)).mean())}
            for f in FAMILIES: row[f"{f}_contribution_share"]=shares.get(f,0)/total if total else np.nan; row[f"{f}_dominant_frequency"]=dom.get(f,0)
            structures.append(row)
    stats=pd.DataFrame(stats); comps=pd.DataFrame(comps); structure=pd.DataFrame(structures)
    # Invariant family chronology, overlap, and within-family evidence use F0 surfaces.
    base=contrib[contrib.policy.eq("F0")]; family_score=base.groupby(["geo_id","date","family"],as_index=False).aligned_metric_score.mean()
    overlap=[]
    for geo,gg in family_score.groupby("geo_id"):
        wide=gg.pivot(index="date",columns="family",values="aligned_metric_score")
        for period,z0 in _periods(wide.reset_index()):
            for left,right in (("long_term_rates","fedfunds"),("long_term_rates","spreads"),("fedfunds","spreads")):
                z=z0[[left,right]].dropna(); dl=z.diff().dropna(); c=safe_corr(z[left],z[right]); dc=safe_corr(dl[left],dl[right]); threshold=z.abs().median()
                overlap.append({"geo_id":geo,"period":period,"left_family":left,"right_family":right,"chronology_correlation":c.correlation,"chronology_correlation_status":c.status,"contribution_correlation":dc.correlation,"sign_agreement":np.sign(z[left]).eq(np.sign(z[right])).mean(),"direction_agreement":np.sign(dl[left]).eq(np.sign(dl[right])).mean(),"opposition_frequency":np.sign(z[left]).ne(np.sign(z[right])).mean(),"cancellation_frequency":(np.sign(z[left]).ne(np.sign(z[right])) & z.abs().gt(threshold).any(axis=1)).mean(),"left_unique_material_months":int((z[left].abs().ge(threshold[left])&z[right].abs().lt(threshold[right])).sum()),"right_unique_material_months":int((z[right].abs().ge(threshold[right])&z[left].abs().lt(threshold[left])).sum())})
    within=[]
    for geo,gg in base.groupby("geo_id"):
        wide=gg.pivot(index="date",columns="metric",values="aligned_metric_score")
        for family,metrics in FAMILIES.items():
            for i,left in enumerate(metrics):
                for right in metrics[i+1:]:
                    z=wide[[left,right]].dropna(); d=z.diff().dropna(); c=safe_corr(z[left],z[right]); dc=safe_corr(d[left],d[right]); threshold=z.abs().median()
                    within.append({"geo_id":geo,"family":family,"left_metric":left,"right_metric":right,"chronology_correlation":c.correlation,"chronology_correlation_status":c.status,"contribution_correlation":dc.correlation,"sign_agreement":np.sign(z[left]).eq(np.sign(z[right])).mean(),"direction_agreement":np.sign(d[left]).eq(np.sign(d[right])).mean(),"opposition_frequency":np.sign(z[left]).ne(np.sign(z[right])).mean(),"left_absolute_share":z[left].abs().sum()/z.abs().sum().sum(),"right_absolute_share":z[right].abs().sum()/z.abs().sum().sum(),"left_unique_material_months":int((z[left].abs().ge(threshold[left])&z[right].abs().lt(threshold[right])).sum()),"right_unique_material_months":int((z[right].abs().ge(threshold[right])&z[left].abs().lt(threshold[left])).sum())})
    # Absolute responsiveness: fixed family-move thresholds from invariant surfaces.
    response=[]
    for geo,gg in family_score.groupby("geo_id"):
        wide=gg.pivot(index="date",columns="family",values="aligned_metric_score"); moves=wide.diff(); thresholds=moves.abs().median()
        for policy,cg in chronology[chronology.geo_id.eq(geo)].groupby("policy"):
            cand=cg.set_index("date").candidate_capital_markets
            for family in FAMILIES:
                material=moves[family].abs().ge(thresholds[family]); others=[x for x in FAMILIES if x!=family]; unique=material & moves[others].abs().lt(thresholds[others]).all(axis=1); joined=pd.concat([cand,moves[family]],axis=1).dropna(); mat=material.reindex(joined.index,fill_value=False); uni=unique.reindex(joined.index,fill_value=False)
                response.append({"policy":policy,"geo_id":geo,"family":family,"fixed_materiality_threshold":thresholds[family],"threshold_provenance":"invariant family monthly-move median","material_move_count":int(mat.sum()),"material_move_direction_agreement":np.sign(joined.loc[mat,"candidate_capital_markets"]).eq(np.sign(joined.loc[mat,family])).mean(),"material_move_magnitude":joined.loc[mat,"candidate_capital_markets"].abs().mean(),"unique_family_move_count":int(uni.sum()),"unique_family_direction_agreement":np.sign(joined.loc[uni,"candidate_capital_markets"]).eq(np.sign(joined.loc[uni,family])).mean(),"unique_family_response_magnitude":joined.loc[uni,"candidate_capital_markets"].abs().mean()})
    response=pd.DataFrame(response)
    # Frozen Demand/Supply propagation.
    axis_tables={};
    for axis in ("demand","supply"):
        rows=[]
        for policy,cg in chronology.groupby("policy"):
            ax=_axis(cg,artifacts,root,axis); ref=_axis(chronology[chronology.policy.eq("F0")],artifacts,root,axis).rename(columns={"candidate_axis_score":"f0_axis"})
            ax=ax.merge(ref[["geo_id","date","f0_axis"]],on=["geo_id","date"])
            for geo,g in ax.groupby("geo_id"):
                for period,z in _periods(g):
                    co=_comparison(z.candidate_axis_score,z.f0_axis,z.date,turning_points=False); st=_stats(z.candidate_axis_score,z.date,turning_points=False)
                    rows.append({"policy":policy,"geo_id":geo,"period":period,"configured_capital_markets_weight":EXPECTED_AXIS_WEIGHTS[axis],"chronology_correlation":co["correlation"],"sign_changes":int((np.sign(z.candidate_axis_score)!=np.sign(z.f0_axis)).sum()),"direction_changes":int((np.sign(z.candidate_axis_score.diff())!=np.sign(z.f0_axis.diff())).sum()),"amplitude_change":z.candidate_axis_score.abs().mean()-z.f0_axis.abs().mean(),"contribution_share":z.contribution_share.mean(),"cancellation":z.cancellation.mean(),"net_to_gross":z.net_to_gross.mean(),**{k:st[k] for k in ("reversals","whipsaw_2m","whipsaw_3m","persistence")}})
        axis_tables[axis]=pd.DataFrame(rows)
    cross=axis_tables["demand"].merge(axis_tables["supply"],on=["policy","geo_id","period"],suffixes=("_demand","_supply")); cross["materiality_classification"]=np.select([(cross.amplitude_change_demand.abs()<1e-12)&(cross.amplitude_change_supply.abs()<1e-12),cross.amplitude_change_demand.abs()>2*cross.amplitude_change_supply.abs(),cross.amplitude_change_supply.abs()>2*cross.amplitude_change_demand.abs(),np.sign(cross.amplitude_change_demand)!=np.sign(cross.amplitude_change_supply)],["changes_neither_axis","primarily_changes_demand","primarily_changes_supply","changes_both_differently"],default="changes_both_similarly")
    controlled=[]; decision=stats.merge(structure,on=["policy","geo_id","period"]).merge(comps,on=["policy","geo_id","period"],suffixes=("","_incumbent"))
    resp=response.groupby(["policy","geo_id"],as_index=False).agg(material_move_direction_agreement=("material_move_direction_agreement","mean"),material_move_magnitude=("material_move_magnitude","mean"),unique_family_response_evidence=("unique_family_response_magnitude","mean"))
    decision=decision.merge(resp,on=["policy","geo_id"],how="left").merge(axis_tables["demand"][["policy","geo_id","period","amplitude_change","chronology_correlation"]].rename(columns={"amplitude_change":"demand_amplitude_delta","chronology_correlation":"demand_chronology_correlation"}),on=["policy","geo_id","period"]).merge(axis_tables["supply"][["policy","geo_id","period","amplitude_change","chronology_correlation"]].rename(columns={"amplitude_change":"supply_amplitude_delta","chronology_correlation":"supply_chronology_correlation"}),on=["policy","geo_id","period"]).merge(registry,on="policy")
    for left,right in CONTROLLED:
        a=decision[decision.policy.eq(left)].set_index(["geo_id","period"]); b=decision[decision.policy.eq(right)].set_index(["geo_id","period"])
        for idx in a.index:
            controlled.append({"comparison_group":"low_fed_interaction" if (left,right)==("F5","F9") else "fedfunds_ladder" if left in FEDFUNDS_LADDER and right in FEDFUNDS_LADDER else "rates_spreads_ladder","from_policy":left,"to_policy":right,"geo_id":idx[0],"period":idx[1],"delta_standard_deviation":b.loc[idx].standard_deviation-a.loc[idx].standard_deviation,"delta_concentration":b.loc[idx].contribution_concentration-a.loc[idx].contribution_concentration,"delta_cancellation":b.loc[idx].cancellation-a.loc[idx].cancellation,"plateau_assessment":"human_review_required"})
    questions=["Does F0 remain defensible?","Where does the Rates-versus-Spreads plateau begin?","Where does the Fed Funds plateau begin?","Does equal intra-family weighting remain defensible?","What tradeoff remains for human judgment?"]
    evaluation=pd.DataFrame({"question":questions,"status":"empirical_review_required","automated_ranking":False})
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"capital_markets_family_weight_review_pending","automated_winner":False,"production_policy_changed":False,"feature_weight_policy_changed":False,"metric_weight_policy_changed":False,"Demand_changed":False,"Supply_changed":False,"Capital_Markets_changed":False,"native_feature_calibration":"closed","family_metric_weight_calibration":"in_review","candidate_grid_closed":True,"candidate_grid":"F0-F9","intra_family_metric_weight_calibration":"not_started","supply_s8_changed":False}])
    performance=pd.DataFrame([{"stage":"cached authoritative load","elapsed_seconds":0.,"architecture":"persisted aligned scores; no feature construction"},{"stage":"vectorized candidate construction","elapsed_seconds":perf_counter()-started,"architecture":"one panel per policy; cached baselines and slices"},{"stage":"total","elapsed_seconds":perf_counter()-started,"architecture":"diagnostic wall clock"}])
    return {"scenario_registry":registry,"metric_weights":weights,"contributions":contrib,"family_contributions":fam,"dimension_chronology":chronology,"dimension_statistics":stats,"incumbent_comparison":comps,"controlled_comparisons":pd.DataFrame(controlled),"period_sensitivity":stats,"by_county":decision[decision.geo_id.isin(REVIEW_GEOS)],"family_overlap":pd.DataFrame(overlap),"within_family_consistency":pd.DataFrame(within),"responsiveness":response,"contribution_structure":structure,"demand_axis_statistics":axis_tables["demand"],"supply_axis_statistics":axis_tables["supply"],"cross_axis_materiality":cross,"decision_table":decision,"evaluation_matrix":evaluation,"governance_status":governance,"performance_audit":performance}

def write_review(tables: dict[str,pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True,exist_ok=True); prefix="capital_markets_family_weight"
    for name in EXPORTS: tables[name].to_csv(out/f"{prefix}_{name}.csv",index=False)
    chron=tables["dimension_chronology"]; plots=[]
    def plot(name, policies, title):
        q=chron[chron.policy.isin(policies)]; geo="11001" if "11001" in set(q.geo_id.astype(str)) else str(q.geo_id.iloc[0]); q=q[q.geo_id.astype(str).eq(geo)]
        series=[(p,q[q.policy.eq(p)][["date","candidate_capital_markets"]].rename(columns={"candidate_capital_markets":"value"})) for p in policies]; fn=f"{prefix}_{name}.svg"; canonical._plot(out/fn,series,title); plots.append(fn)
    plot("dc_all_policies",tuple(POLICIES),"DC F0-F9"); plot("seven_county_equal_footing",tuple(POLICIES),"Governed counties equal-footing view"); plot("focused_review",("F0","F1","F2","F5","F6"),"Focused human-review view")
    plot("rates_spreads_ladder",RATES_SPREADS_LADDER,"Rates-versus-Spreads ladder"); plot("fedfunds_ladder",FEDFUNDS_LADDER,"Fed Funds ladder")
    for p in ("F0","F3","F4","F5","F7","F8"): plot(f"{p}_contribution_decomposition",(p,),f"{p} contribution decomposition")
    for metric in ("whipsaw","persistence","concentration","cancellation","responsiveness","demand_materiality","supply_materiality"): plot(f"response_{metric}",tuple(POLICIES),f"Policy response: {metric}")
    files=[*(f"{prefix}_{n}.csv" for n in EXPORTS),*plots]
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in files)
    (out/f"{prefix}_review_index.html").write_text("<!doctype html><meta charset=utf-8><title>Capital Markets family weights</title><h1>Diagnostic-only family-weight review</h1><p>F0-F9 closed grid; no winner, promotion, feature rebuild, or production change.</p><ul>"+links+"</ul>",encoding="utf-8")
