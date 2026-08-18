"""Targeted, artifact-first revalidation of the corrected 10Y-minus-2Y spread.

This module is deliberately diagnostic: it reads the polarity-repair candidate,
reweights its persisted normalized features, and never writes a registry or a
production run.
"""
from __future__ import annotations

from pathlib import Path
from time import perf_counter
import html
import numpy as np
import pandas as pd

from regime.diagnostics import price_feature_anatomy as canonical
from regime.diagnostics.capital_markets_feature_anatomy import (
    EXPECTED_AXIS_WEIGHTS, EXPECTED_WEIGHTS, NATIVE_GEO, load_run as _load_run,
    resolve_contract,
)
from regime.diagnostics.capital_markets_feature_weight_calibration import (
    _comparison, _periods, _stats,
)
from regime.diagnostics.correlation import safe_corr

METRIC = "spread_10y_2y"
FEATURES = ("level", "short", "long")
POLICIES = {
    "P0": (.60, .20, .20), "P1": (.60, .15, .25), "P2": (.60, .10, .30),
    "P3": (.55, .15, .30), "P4": (.55, .10, .35), "P5": (.50, .10, .40),
    "P6": (.60, .05, .35), "P7": (.35, .10, .55), "P8": (.45, .10, .45),
    "P9": (.40, .10, .50),
}
LONG_LADDER = ("P4", "P5", "P8", "P9", "P7")
COMPARISONS = (("P0", "P1"), ("P1", "P2"), ("P2", "P6"),
               ("P1", "P3"), ("P2", "P4"), *zip(LONG_LADDER, LONG_LADDER[1:]))
FIXED_POLICIES = {"mortgage_30y":"P4", "mortgage_15y":"P2",
                  "treasury_10y":"P1", "fedfunds":"P5",
                  "spread_10y_fedfunds":"P9"}
PREFIX = "spread_10y_2y_revalidation"
EXPORTS = ("scenario_registry", "metric_chronology", "metric_statistics",
 "feature_contributions", "feature_reference_comparison",
 "corrected_raw_cycle_comparison", "responsiveness", "turning_point_comparison",
 "policy_marginal_deltas", "decision_table", "period_sensitivity",
 "capital_markets_dimension", "demand_axis", "supply_axis",
 "cross_spread_context", "evaluation_matrix", "governance_status",
 "performance_audit")


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    """Fail closed: the caller's exact corrected candidate must exist."""
    if not run.is_dir():
        raise FileNotFoundError(f"corrected authoritative polarity-repair run absent: {run}")
    if run.name != "capital_markets_spread_polarity_repair_20260818":
        raise ValueError("only capital_markets_spread_polarity_repair_20260818 is authoritative")
    return _load_run(run)


def _value(frame, choices):
    return canonical._value_col(frame, choices)


def build(artifacts: dict[str, pd.DataFrame], root: Path) -> dict[str, pd.DataFrame]:
    started = perf_counter()
    contract, _ = resolve_contract(root)
    spec = contract[contract.metric.eq(METRIC)].copy()
    if set(spec.feature_type) != set(FEATURES) or set(spec["transform"]) != {"ma_level", "ma_difference"}:
        raise ValueError("spread feature family differs from fixed MA9/difference contract")
    windows = spec.set_index("feature_type").window_lag_definition.to_dict()
    if windows != {"level":"9m", "short":"9m/lag3m", "long":"9m/lag12m"}:
        raise ValueError(f"spread windows differ from fixed construction: {windows}")
    if set(spec.score_direction) != {"positive"}:
        raise ValueError("spread normalization direction must remain positive")

    registry = pd.DataFrame([{"policy":p, "level_weight":w[0], "short_weight":w[1],
        "long_weight":w[2], "scope_metric":METRIC, "candidate_grid":"P0-P9",
        "candidate_grid_closed":True, "policy_status":
        "revalidation_required" if p == "P7" else "challenger",
        "policy_semantics":(
            "historical_60_20_20_reference" if p == "P0" else
            "corrected_persisted_run_arithmetic_baseline" if p == "P7" else
            "challenger"
        ),
        "feature_construction":"MA9; MA9-lag3(MA9); MA9-lag12(MA9)",
        "normalization_direction":"positive"} for p,w in POLICIES.items()])

    norm = canonical._dates(artifacts["normalized_features"])
    score_col = _value(norm, ("feature_score","normalized_feature_score","normalized_value"))
    fmap = spec.set_index("feature_key").feature_type
    base = norm[norm.feature_key.isin(fmap.index) & norm.geo_id.eq(NATIVE_GEO)].copy()
    base["feature_type"] = base.feature_key.map(fmap); base["normalized_feature_score"] = base[score_col]
    base["date"] = base.date.dt.to_period("M").dt.to_timestamp("M")
    if set(base.feature_type) != set(FEATURES) or base.duplicated(["date","feature_type"]).any():
        raise ValueError("persisted corrected normalized features are missing or duplicated")
    panels=[]
    for policy, weights in POLICIES.items():
        q=base[["geo_id","date","feature_key","feature_type","normalized_feature_score"]].copy()
        q["policy"]=policy; q["configured_feature_weight"]=q.feature_type.map(dict(zip(FEATURES,weights)))
        q["available_weight_sum"]=q.configured_feature_weight.where(q.normalized_feature_score.notna(),0).groupby(q.date).transform("sum")
        q["effective_feature_weight"]=q.configured_feature_weight.div(q.available_weight_sum).where(q.normalized_feature_score.notna())
        q["weighted_feature_contribution"]=q.normalized_feature_score*q.effective_feature_weight
        q["candidate_metric_score"]=q.groupby("date").weighted_feature_contribution.transform(lambda x:x.sum(min_count=1))
        panels.append(q)
    contributions=pd.concat(panels,ignore_index=True)
    chronology=contributions.drop_duplicates(["policy","date"])[["policy","geo_id","date","candidate_metric_score"]]
    prod=canonical._dates(artifacts["metric_scores"]); mc=canonical._metric_col(prod); vc=_value(prod,("metric_score","score"))
    prod=prod[(prod[mc].eq(METRIC)) & prod.geo_id.eq(NATIVE_GEO)][["date",vc]].rename(columns={vc:"persisted_corrected_metric_score"})
    check=chronology.query("policy=='P7'").merge(prod,on="date",validate="one_to_one")
    if len(check)!=len(prod) or not np.allclose(check.candidate_metric_score,check.persisted_corrected_metric_score,equal_nan=True,atol=1e-12,rtol=0):
        raise ValueError("P7 does not reconstruct persisted corrected metric score")

    # The source artifact must already be canonical 10Y-2Y. Where constituent
    # rows are available, enforce formula parity rather than trusting a label.
    source=canonical._dates(artifacts["source_metrics"]); smc=canonical._metric_col(source); svc=_value(source,("value","metric_value","raw_value"))
    source=source[source.geo_id.eq(NATIVE_GEO)].copy()
    identities=spec[["registry_metric_key","metric"]].drop_duplicates()
    spread_keys=set(identities.registry_metric_key)|{METRIC}
    raw=source[source[smc].isin(spread_keys)][["date",svc]].rename(columns={svc:"corrected_raw_spread"}).drop_duplicates("date")
    raw["date"]=raw.date.dt.to_period("M").dt.to_timestamp("M"); raw=raw.sort_values("date")
    if raw.empty: raise ValueError("canonical corrected spread chronology absent")
    keys=set(source[smc].astype(str))
    if {"treasury_10y","treasury_2y"}.issubset(keys):
        wide=source[source[smc].isin(["treasury_10y","treasury_2y"])].pivot(index="date",columns=smc,values=svc)
        expected=(wide.treasury_10y-wide.treasury_2y).rename("expected").reset_index()
        parity=raw.merge(expected,on="date")
        if not np.allclose(parity.corrected_raw_spread,parity.expected,atol=1e-12,rtol=0,equal_nan=True):
            raise ValueError("canonical raw spread is not treasury_10y - treasury_2y")
    raw["raw_movement"]=raw.corrected_raw_spread.diff()

    stats=[]; refs=[]; cycles=[]; response=[]; turning=[]
    structure=[]
    for policy,g in contributions.groupby("policy"):
        absolute=g.groupby("feature_type").weighted_feature_contribution.apply(lambda x:x.abs().sum())
        gross=g.groupby("date").weighted_feature_contribution.apply(lambda x:x.abs().sum()); net=g.groupby("date").weighted_feature_contribution.sum().abs()
        ranked=g.assign(a=g.weighted_feature_contribution.abs()).dropna(subset=["a"])
        dominant=ranked.loc[ranked.groupby("date").a.idxmax()].feature_type.value_counts(normalize=True)
        row={"policy":policy,"cancellation":1-net.sum()/gross.sum(),"net_to_gross":net.sum()/gross.sum()}
        for f in FEATURES:
            row[f"{f}_absolute_contribution_share"]=absolute.get(f,0)/absolute.sum(); row[f"{f}_dominant_frequency"]=dominant.get(f,0)
        structure.append(row)
    structure=pd.DataFrame(structure)
    for policy,g in chronology.groupby("policy"):
        joined=g.merge(raw,on="date",validate="one_to_one")
        joined["candidate_monthly_movement"]=joined.candidate_metric_score.diff()
        for period,z in _periods(joined):
            stats.append({"policy":policy,"period":period,**_stats(z.candidate_metric_score,z.date)})
            comp=_comparison(z.candidate_metric_score,z.corrected_raw_spread,z.date)
            cycles.append({"policy":policy,"period":period,"reference":"corrected 10Y-2Y raw chronology",**comp})
            turning.append({"policy":policy,"period":period,**{k:comp[k] for k in ("reference_turn_count","candidate_turn_count","turning_point_preservation","missed_reference_turns","signed_delay","absolute_delay","turning_point_status","delay_status")}})
            threshold=z.raw_movement.abs().median(); material=z[z.raw_movement.abs().ge(threshold)].dropna(subset=["raw_movement","candidate_metric_score"])
            response.append({"policy":policy,"period":period,"materiality_threshold":threshold,
                "threshold_provenance":"median absolute corrected raw movement; fixed before candidate evaluation",
                "material_raw_move_count":len(material),"unique_material_moves_retained":material.date.nunique(),
                "direction_agreement_during_material_moves":np.sign(material.candidate_monthly_movement).eq(np.sign(material.raw_movement)).mean(),
                "candidate_magnitude_during_material_moves":material.candidate_metric_score.abs().mean(),
                "turning_preservation":comp["turning_point_preservation"],"signed_delay":comp["signed_delay"],
                "absolute_delay":comp["absolute_delay"],"missed_reference_turns":comp["missed_reference_turns"]})
            for f in FEATURES:
                fg=contributions[(contributions.policy.eq(policy))&(contributions.feature_type.eq(f))][["date","normalized_feature_score"]]
                zz=z.merge(fg,on="date"); c=safe_corr(zz.candidate_metric_score,zz.normalized_feature_score)
                refs.append({"policy":policy,"period":period,"reference_feature":f,
                    "similarity_to_feature":c.correlation,"correlation_status":c.status})
    stats=pd.DataFrame(stats); cycles=pd.DataFrame(cycles); response=pd.DataFrame(response); turning=pd.DataFrame(turning)
    similarity= pd.DataFrame(refs).pivot(index=["policy","period"],columns="reference_feature",values="similarity_to_feature").rename(columns=lambda x:f"similarity_to_{x}").reset_index()
    cycle_decision=cycles[["policy","period","correlation","correlation_status","sign_agreement","direction_agreement"]].rename(columns={"correlation":"raw_correlation","correlation_status":"raw_correlation_status","sign_agreement":"raw_sign_agreement","direction_agreement":"raw_direction_agreement"})
    decision=stats.merge(response,on=["policy","period"]).merge(cycle_decision,on=["policy","period"]).merge(structure,on="policy").merge(similarity,on=["policy","period"])

    # Downstream update is algebraic and changes exactly one aligned metric.
    aligned=canonical._dates(artifacts["aligned_metric_scores"]); amc=canonical._metric_col(aligned)
    aligned=aligned[aligned[amc].eq(METRIC)].copy(); aligned["native_month"]=pd.to_datetime(aligned.metric_date).dt.to_period("M")
    native=chronology.copy(); native["native_month"]=native.date.dt.to_period("M")
    scenario=aligned[["geo_id","date","native_month"]].merge(native[["policy","native_month","candidate_metric_score"]],on="native_month")
    p7=scenario.query("policy=='P7'")[["geo_id","date","candidate_metric_score"]].rename(columns={"candidate_metric_score":"persisted_p7_corrected_baseline_metric_score"})
    scenario=scenario.merge(p7,on=["geo_id","date"],validate="many_to_one")
    scenario["metric_score_delta_from_persisted_p7"]=(scenario.candidate_metric_score-scenario.persisted_p7_corrected_baseline_metric_score).where(
        scenario.candidate_metric_score.notna() | scenario.persisted_p7_corrected_baseline_metric_score.notna(), 0.0)
    dims=canonical._dates(artifacts["dimension_scores"]); cm=dims[dims.dimension.eq("capital_markets")][["geo_id","date","dimension_score"]].rename(columns={"dimension_score":"persisted_baseline_score"})
    cmout=scenario.merge(cm,on=["geo_id","date"],validate="many_to_one"); cmout["candidate_score"]=cmout.persisted_baseline_score+EXPECTED_WEIGHTS[METRIC]*cmout.metric_score_delta_from_persisted_p7
    axes=canonical._dates(artifacts["axis_scores"]); axis_col=next(c for c in ("axis","axis_name") if c in axes); av=_value(axes,("axis_score","score"))
    axis_tables={}
    for axis in ("demand","supply"):
        base=axes[axes[axis_col].astype(str).str.lower().eq(axis)][["geo_id","date",av]].rename(columns={av:"persisted_baseline_score"})
        q=scenario.merge(base,on=["geo_id","date"],validate="many_to_one"); q["candidate_score"]=q.persisted_baseline_score+EXPECTED_AXIS_WEIGHTS[axis]*EXPECTED_WEIGHTS[METRIC]*q.metric_score_delta_from_persisted_p7; axis_tables[axis]=q

    marginal=[]; indexed=decision.set_index(["period","policy"])
    fields=("reversals","whipsaw_2m","whipsaw_3m","persistence","mean_run_length","mean_absolute_monthly_movement","standard_deviation","direction_agreement_during_material_moves","candidate_magnitude_during_material_moves","turning_preservation","absolute_delay","level_absolute_contribution_share","short_absolute_contribution_share","long_absolute_contribution_share","similarity_to_long")
    for period in ("full_history","2022_plus","latest_36_months"):
        for left,right in COMPARISONS:
            marginal.append({"period":period,"from_policy":left,"to_policy":right,**{f"delta_{f}":indexed.loc[(period,right),f]-indexed.loc[(period,left),f] for f in fields}})
    marginal=pd.DataFrame(marginal)

    # Secondary context only; never enters a score or decision rule.
    other_raw=source[source[smc].astype(str).isin({"spread_10y_fedfunds","fred_10y_fedfunds_spread"})][["date",svc]].rename(columns={svc:"other_raw"}).drop_duplicates("date")
    cross=[]
    other_scores=prod = canonical._dates(artifacts["metric_scores"]); omc=canonical._metric_col(other_scores); ovc=_value(other_scores,("metric_score","score"))
    other_scores=other_scores[(other_scores[omc].eq("spread_10y_fedfunds"))&other_scores.geo_id.eq(NATIVE_GEO)][["date",ovc]].rename(columns={ovc:"other_score"})
    raw_context=raw.merge(other_raw,on="date",how="inner"); raw_corr=safe_corr(raw_context.corrected_raw_spread,raw_context.other_raw)
    for policy,g in chronology.groupby("policy"):
        q=g.merge(other_scores,on="date",how="inner"); c=safe_corr(q.candidate_metric_score,q.other_score)
        cross.append({"policy":policy,"context_semantics":"secondary_descriptive_only_not_optimization",
            "corrected_raw_chronology_correlation":raw_corr.correlation,"raw_correlation_status":raw_corr.status,
            "metric_score_correlation":c.correlation,"metric_correlation_status":c.status,
            "sign_agreement":np.sign(q.candidate_metric_score).eq(np.sign(q.other_score)).mean(),
            "direction_agreement":np.sign(q.candidate_metric_score.diff()).eq(np.sign(q.other_score.diff())).mean()})

    questions=["Does corrected polarity materially change the supported P7 conclusion?","Does P7 still improve stability versus P9?","Does P9 now dominate P7 on the stability/responsiveness tradeoff?","Does P8 become the practical plateau?","Does P5 become sufficient after polarity correction?","Where does the corrected Long ladder plateau begin?","Does Long-majority weighting remain supported?","Is 50% Long defensible?","Is 55% Long still defensible?","Does P7 retain material raw-move responsiveness?","Does P7 materially attenuate or delay corrected spread turns?","Does P7 become excessively Long-like?","Does P6’s 5% Short boundary gain credibility under corrected chronology?","Are results robust across full history, 2022+, and latest 36 months?","Are downstream Capital Markets effects material?","Are downstream Demand effects material?","Are downstream Supply effects material?","Is one policy clearly at the beginning of the practical optimization plateau?","Does P7 survive revalidation?","If not, which policy replaces it?"]
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"spread_10y_2y_feature_policy_revalidation_pending","automated_winner":False,"production_policy_changed":False,"feature_weight_policy_changed":False,"metric_weight_policy_changed":False,"Demand_changed":False,"Supply_changed":False,"Capital_Markets_changed":False,"spread_polarity_repair":"validated","spread_10y_2y_feature_policy_status":"revalidation_required","persisted_reconstruction_anchor":"P7_corrected_persisted_run","historical_feature_policy_reference":"P0_60_20_20","other_five_capital_markets_feature_policies":"provisionally_valid","family_metric_weight_calibration":"invalidated_pending_rerun","candidate_grid_closed":True,"candidate_grid":"P0-P9"}])
    elapsed=perf_counter()-started
    performance=pd.DataFrame([{"stage":"targeted one-metric build","elapsed_seconds":elapsed,"call_count":1,"optimization_note":"persisted features and cached aligned production surfaces reused"},{"stage":"all-six-family rebuild","elapsed_seconds":0.0,"call_count":0,"optimization_note":"intentionally not performed"}])
    return {"scenario_registry":registry,"metric_chronology":chronology,"metric_statistics":stats,
        "feature_contributions":contributions,"feature_reference_comparison":pd.DataFrame(refs),
        "corrected_raw_cycle_comparison":cycles,"responsiveness":response,"turning_point_comparison":turning,
        "policy_marginal_deltas":marginal,"decision_table":decision,"period_sensitivity":decision,
        "capital_markets_dimension":cmout,"demand_axis":axis_tables["demand"],"supply_axis":axis_tables["supply"],
        "cross_spread_context":pd.DataFrame(cross),"evaluation_matrix":pd.DataFrame({"question":questions,"status":"human_review_required","automated_winner":False}),
        "governance_status":governance,"performance_audit":performance,"_raw":raw}


def write_review(tables: dict[str,pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"{PREFIX}_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    def plot(name,title,series):
        fn=f"{PREFIX}_{name}.svg"; canonical._plot(out/fn,series,title); plots.append(fn)
    raw=tables["_raw"][["date","corrected_raw_spread"]].rename(columns={"corrected_raw_spread":"value"})
    allp=[(p,chron.query("policy==@p")[["date","candidate_metric_score"]].rename(columns={"candidate_metric_score":"value"})) for p in POLICIES]
    focus=[x for x in allp if x[0] in LONG_LADDER]
    plot("corrected_raw_spread_chronology","Corrected raw spread: 10Y - 2Y",[("10Y-2Y",raw)])
    plot("policy_chronology","P0-P9 metric chronology",allp); plot("long_ladder","Controlled Long ladder",focus)
    plot("corrected_raw_cycle_overlay","Corrected raw-cycle overlay",[("raw",raw),*focus])
    plot("turning_point_overlay","Turning-point overlay",[("raw",raw),*focus])
    for p in LONG_LADDER:
        q=tables["feature_contributions"].query("policy==@p"); plot(f"{p}_contribution_decomposition",f"{p} contributions",[(f,q.query("feature_type==@f")[["date","weighted_feature_contribution"]].rename(columns={"weighted_feature_contribution":"value"})) for f in FEATURES])
    dec=tables["decision_table"].query("period=='full_history'").set_index("policy").reindex(POLICIES)
    for label,col in (("reversals","reversals"),("whipsaw","whipsaw_2m"),("persistence","persistence"),("responsiveness","direction_agreement_during_material_moves"),("long_contribution_share","long_absolute_contribution_share"),("delay","absolute_delay")):
        plot(f"response_{label}",f"Response curve: {label}",[(label,pd.DataFrame({"date":pd.date_range("2000-01-31",periods=10,freq="ME"),"value":dec[col].values}))])
    for label,key in (("capital_markets_materiality","capital_markets_dimension"),("demand_materiality","demand_axis"),("supply_materiality","supply_axis")):
        q=tables[key]; plot(label,label.replace("_"," ").title(),[(p,q.query("policy==@p")[["date","candidate_score"]].rename(columns={"candidate_score":"value"})) for p in LONG_LADDER])
    links=[*(f"{PREFIX}_{x}.csv" for x in EXPORTS),*plots]
    items="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in links)
    (out/f"{PREFIX}_review_index.html").write_text("<!doctype html><meta charset=utf-8><title>Spread revalidation</title><h1>10Y−2Y targeted feature-policy revalidation</h1><p>No winner selected; production unchanged. Cross-spread evidence is secondary context only.</p><ul>"+items+"</ul>",encoding="utf-8")
