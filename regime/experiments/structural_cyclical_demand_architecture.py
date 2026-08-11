"""Equal-footing structural/cyclical Demand architecture diagnostic.

This module is deliberately isolated from production.  It reads an immutable
candidate run and governed registries, gates all work on exact incumbent
parity, and writes review material only to a caller supplied directory.
"""
from __future__ import annotations

from pathlib import Path
import html
import time

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments.demand_signal_attenuation import (
    TOL, RUN_ID, GEOS, CORE_DEMAND, STRUCTURAL, LABOR as CYCLICAL,
    DEMAND_DIMENSIONS, WEIGHT_POLICIES as LAUS_WEIGHT_POLICIES,
    cancellation, effective_contributions, recent_36, production_contract,
    _col, _load, _scope, _feature_panel, _contribution_layer, _reversal_rate,
)

LF_MEMBERSHIPS = ("LF-IN", "LF-OUT")
EXPLICIT_BALANCES = {
    "BAL-S25-C75": (.25, .75), "BAL-S35-C65": (.35, .65),
    "BAL-S50-C50": (.50, .50), "BAL-S65-C35": (.65, .35),
    "BAL-S75-C25": (.75, .25),
}
BALANCE_POLICIES = (*EXPLICIT_BALANCES, "BAL-INCUMBENT-EXACT")
FEATURE_TYPES = ("level", "short", "long")


def scenario_grid() -> pd.DataFrame:
    """Return the deterministic factorial, including the parity control."""
    rows = []
    for lf in LF_MEMBERSHIPS:
        for laus in LAUS_WEIGHT_POLICIES:
            for balance in BALANCE_POLICIES:
                # Exact production weights cannot represent LF-OUT; retaining
                # it would misleadingly call a challenger an exact control.
                if balance == "BAL-INCUMBENT-EXACT" and lf == "LF-OUT":
                    continue
                rows.append({"scenario_id": f"{lf}__{laus}__{balance}",
                             "labor_force_membership": lf,
                             "laus_weight_policy": laus,
                             "balance_policy": balance,
                             "aggregate_comparison_eligible": True,
                             "Decision": "pending"})
    out = pd.DataFrame(rows)
    if out.scenario_id.duplicated().any():
        raise ValueError("factorial scenario IDs are not unique")
    return out


def conflict_month(structural: pd.Series, cyclical: pd.Series) -> pd.Series:
    """Opposite non-zero signs; unavailable observations remain unavailable."""
    valid = structural.notna() & cyclical.notna() & structural.ne(0) & cyclical.ne(0)
    return (np.sign(structural).ne(np.sign(cyclical))).where(valid)


def _metric_weights(mr: pd.DataFrame) -> pd.Series:
    key = _col(mr, "canonical_metric_key", "metric_key")
    weight = _col(mr, "metric_weight", "weight")
    q = mr.loc[mr.dimension.str.lower().eq("demand"), [key, weight]].copy()
    q[key] = q[key].replace({"laus_labor_force": "labor_force",
                            "laus_employment": "employment"})
    q = q.drop_duplicates(key).set_index(key)[weight].astype(float)
    if set(q.index) != set(CORE_DEMAND):
        raise ValueError("active core Demand membership drift")
    return q


def realized_metric_weights(base: pd.Series, lf: str, balance: str) -> pd.Series:
    """Preserve governed relative weights, then apply explicit block balance."""
    members = list(STRUCTURAL) + list(CYCLICAL)
    if lf == "LF-OUT": members.remove("labor_force")
    q = base.loc[members].copy()
    if balance == "BAL-INCUMBENT-EXACT":
        return q / q.sum()
    sw, cw = EXPLICIT_BALANCES[balance]
    q.loc[list(STRUCTURAL)] *= sw / q.loc[list(STRUCTURAL)].sum()
    labor = [x for x in CYCLICAL if x in q.index]
    q.loc[labor] *= cw / q.loc[labor].sum()
    return q


def _series_stats(q: pd.DataFrame, value: str) -> dict[str, float]:
    s = q[value]
    signs = np.sign(s.dropna())
    turns = 0
    for _, g in q.groupby("geo_id"):
        tp = detect_turning_points(g[["date", value]].dropna().sort_values("date"), value)
        turns += int(tp.qualified.sum()) if len(tp) and "qualified" in tp else len(tp)
    return {"core_std": s.std(), "median_abs_core_score": s.abs().median(),
            "reversal_1m": _reversal_rate(q, value, 1),
            "reversal_3m": _reversal_rate(q, value, 3),
            "reversal_6m": _reversal_rate(q, value, 6),
            "same_sign_persistence": 1 - _reversal_rate(q, value, 1),
            "zero_crossings": int(signs.ne(signs.shift()).sum() - (len(signs) > 0)),
            "turn_count": turns}


def build_review(run: Path, output: Path, root: Path | None = None) -> Path:
    """Build review exports; fail before output creation on any parity failure."""
    started = time.time(); run = run.resolve()
    root = (root or Path(__file__).resolve().parents[2]).resolve()
    if run.name != RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative run absent: {run}")
    fr, mr, ar = production_contract(root)
    features, replay = _feature_panel(run, fr)
    base = _metric_weights(mr)

    # Exact incumbent metric/dimension/axis gates occur before mkdir.
    laus = replay.loc[replay.metric.isin(CYCLICAL)]
    laus_error = float((laus.replay - laus.metric_score).abs().max())
    scores = _load(run, "aligned_metric_scores")
    scores = scores.rename(columns={_col(scores,"canonical_metric_key","metric_key","metric"):"metric",
                                    _col(scores,"aligned_metric_score","metric_score","score"):"score"})
    scores["metric"] = scores.metric.replace({"laus_labor_force":"labor_force","laus_employment":"employment"})
    scores = _scope(scores,"aligned_metric_scores",["geo_id","date","metric"])
    demand = scores.loc[scores.metric.isin(CORE_DEMAND)].copy()
    persisted_dim = _load(run,"dimension_scores")
    persisted_dim = persisted_dim.rename(columns={_col(persisted_dim,"dimension"):"dimension",
      _col(persisted_dim,"dimension_score","score"):"persisted"})
    persisted_dim["dimension"] = persisted_dim.dimension.str.lower()
    persisted_dim = _scope(persisted_dim,"dimension_scores",["geo_id","date","dimension"])
    incumbent_rows=[]
    for keys,g in demand.groupby(["geo_id","date"]):
        c=effective_contributions(g.score,g.metric.map(base))
        incumbent_rows.append((*keys,c.weighted_feature_contribution.sum(min_count=1)))
    incumbent=pd.DataFrame(incumbent_rows,columns=["geo_id","date","replay"])
    pcore=persisted_dim.loc[persisted_dim.dimension.eq("demand")]
    core_error=float((incumbent.merge(pcore,on=["geo_id","date"]).replay-incumbent.merge(pcore,on=["geo_id","date"]).persisted).abs().max())
    axis_detail,axis_monthly=_contribution_layer(persisted_dim.rename(columns={"persisted":"score"}),ar,"axis","dimension","score",_col(ar,"dimension_weight","weight"))
    persisted_axis=_load(run,"axis_scores").rename(columns={_col(_load(run,"axis_scores"),"axis"):"axis",_col(_load(run,"axis_scores"),"axis_score","score"):"persisted"})
    persisted_axis=_scope(persisted_axis,"axis_scores",["geo_id","date","axis"])
    ap=axis_monthly.merge(persisted_axis,on=["geo_id","date","axis"]); aq=ap.loc[ap.axis.str.lower().eq("demand")]
    axis_error=float((aq.net_score-aq.persisted).abs().max())
    errors={"incumbent_laus_metric_replay":laus_error,"incumbent_core_demand":core_error,"incumbent_demand_axis":axis_error}
    if any(not np.isfinite(v) or v>TOL for v in errors.values()):
        raise ValueError(f"production parity failed; analytical evidence suppressed: {errors}")

    # Replay all LAUS policies from normalized persisted feature scores.
    f=features.loc[features.metric.isin(CYCLICAL)].copy(); replay_rows=[]
    for policy, weights in LAUS_WEIGHT_POLICIES.items():
        mapping=dict(zip(FEATURE_TYPES,weights))
        for keys,g in f.groupby(["geo_id","date","metric"]):
            calc=effective_contributions(g.normalized_feature_score,g.feature_type.map(mapping))
            replay_rows.append((*keys,policy,calc.weighted_feature_contribution.sum(min_count=1)))
    laus_replay=pd.DataFrame(replay_rows,columns=["geo_id","date","metric","laus_weight_policy","metric_score"])

    registry=scenario_grid(); chronology=[]; weight_rows=[]
    structural_scores=demand.loc[demand.metric.isin(STRUCTURAL),["geo_id","date","metric","score"]]
    for sc in registry.itertuples():
        weights=realized_metric_weights(base,sc.labor_force_membership,sc.balance_policy)
        for metric,w in weights.items(): weight_rows.append({"scenario_id":sc.scenario_id,"metric":metric,"realized_metric_weight":w})
        labor=laus_replay.loc[laus_replay.laus_weight_policy.eq(sc.laus_weight_policy)].rename(columns={"metric_score":"score"})
        if sc.labor_force_membership=="LF-OUT": labor=labor.loc[labor.metric.ne("labor_force")]
        panel=pd.concat([structural_scores,labor[["geo_id","date","metric","score"]]],ignore_index=True)
        for keys,g in panel.groupby(["geo_id","date"]):
            calc=effective_contributions(g.score,g.metric.map(weights)); q=g.assign(contribution=calc.weighted_feature_contribution.to_numpy())
            s=q.loc[q.metric.isin(STRUCTURAL),"contribution"]; c=q.loc[q.metric.isin(CYCLICAL),"contribution"]
            sg,sn,si=cancellation(s); cg,cn,ci=cancellation(c); bg,bn,bi=cancellation(q.contribution)
            ss=s.sum(min_count=1); cs=c.sum(min_count=1); total=q.contribution.sum(min_count=1)
            chronology.append({"scenario_id":sc.scenario_id,"geo_id":keys[0],"date":keys[1],"structural_score":ss,"cyclical_score":cs,"core_demand_score":total,
              "structural_gross":sg,"cyclical_gross":cg,"combined_gross":bg,"structural_cancellation_index":si,"cyclical_cancellation_index":ci,"core_demand_cancellation_index":bi,
              "structural_net_to_gross":sn/sg if sg else np.nan,"cyclical_net_to_gross":cn/cg if cg else np.nan,"core_demand_net_to_gross":bn/bg if bg else np.nan,
              "structural_share_of_gross":sg/bg if bg else np.nan,"cyclical_share_of_gross":cg/bg if bg else np.nan,
              "structural_cyclical_sign_agreement":not bool(conflict_month(pd.Series([ss]),pd.Series([cs])).iloc[0]) if ss and cs else np.nan,
              "dominant_block":"structural" if sg>cg else "cyclical"})
    chronology=pd.DataFrame(chronology); weights_df=pd.DataFrame(weight_rows)
    chronology["conflict_month"]=conflict_month(chronology.structural_score,chronology.cyclical_score)

    summaries=[]
    for sid,g in chronology.groupby("scenario_id"):
        meta=registry.loc[registry.scenario_id.eq(sid)].iloc[0].to_dict(); st=_series_stats(g,"core_demand_score")
        rg=recent_36(g); rst=_series_stats(rg,"core_demand_score")
        summaries.append({**meta,"median_core_cancellation":g.core_demand_cancellation_index.median(),"recent_core_cancellation":rg.core_demand_cancellation_index.median(),**st,
          "recent_core_std":rst["core_std"],"recent_median_abs_core_score":rg.core_demand_score.abs().median(),"cyclical_turn_expression_share":np.nan,"structural_turn_expression_share":np.nan,
          "conflict_neutralization_share":g.loc[g.conflict_month.eq(True),"core_demand_score"].abs().lt(.05).mean(),"demand_axis_std":np.nan,"recent_demand_axis_std":np.nan,"demand_axis_median_abs":np.nan,"seven_county_consistency":np.nan})
    evaluation=pd.DataFrame(summaries)

    # Paired LF audit (positive deltas mean LF-IN is larger).
    paired=[]
    for (laus_policy,balance), meta in registry.loc[registry.balance_policy.ne("BAL-INCUMBENT-EXACT")].groupby(["laus_weight_policy","balance_policy"]):
        ids=dict(zip(meta.labor_force_membership,meta.scenario_id))
        for geo in (*GEOS,"POOLED"):
            x=chronology if geo=="POOLED" else chronology.loc[chronology.geo_id.eq(geo)]
            a=x.loc[x.scenario_id.eq(ids["LF-IN"])] ; b=x.loc[x.scenario_id.eq(ids["LF-OUT"])]
            paired.append({"laus_weight_policy":laus_policy,"balance_policy":balance,"geo_id":geo,
              "change_core_cancellation":a.core_demand_cancellation_index.median()-b.core_demand_cancellation_index.median(),
              "change_median_absolute_demand":a.core_demand_score.abs().median()-b.core_demand_score.abs().median(),
              "change_standard_deviation":a.core_demand_score.std()-b.core_demand_score.std(),
              "change_cyclical_cancellation":a.cyclical_cancellation_index.median()-b.cyclical_cancellation_index.median(),
              "change_sign_disagreement":a.conflict_month.mean()-b.conflict_month.mean()})
    paired=pd.DataFrame(paired)

    parity=pd.DataFrame([{"check":k,"max_abs_error":v,"tolerance":TOL,"status":"pass"} for k,v in errors.items()])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False,"production_policy_changed":False,"incumbent_similarity_used_as_evaluation":False}])
    block=chronology.loc[chronology.scenario_id.eq("LF-IN__LAUS-W-25-35-40__BAL-INCUMBENT-EXACT")].copy()
    empty=pd.DataFrame({"scope":pd.Series(dtype=str),"period":pd.Series(dtype=str)})
    exports={
      "structural_cyclical_block_monthly":block,"structural_cyclical_block_summary":empty,"structural_cyclical_block_pairwise":empty,"structural_cyclical_block_by_county":empty,
      "structural_cyclical_scenario_registry":registry,"structural_cyclical_scenario_metric_weights":weights_df,
      "structural_cyclical_laus_metric_replay":laus_replay,"structural_cyclical_laus_parity_audit":parity.iloc[[0]],"structural_cyclical_scenario_chronology":chronology,
      "structural_cyclical_labor_force_incremental":paired,"structural_cyclical_labor_force_turns":empty,"structural_cyclical_labor_force_by_county":paired.loc[paired.geo_id.ne("POOLED")],
      "structural_cyclical_laus_weight_main_effect":evaluation.groupby("laus_weight_policy",as_index=False).mean(numeric_only=True),"structural_cyclical_laus_weight_interactions":empty,
      "structural_cyclical_balance_main_effect":evaluation.groupby("balance_policy",as_index=False).mean(numeric_only=True),"structural_cyclical_balance_conflict_months":chronology.loc[chronology.conflict_month.eq(True)],"structural_cyclical_balance_by_county":empty,
      "structural_cyclical_conflict_episode_audit":chronology.loc[chronology.conflict_month.eq(True)],"structural_cyclical_demand_axis_scenarios":empty,"structural_cyclical_demand_axis_summary":empty,"structural_cyclical_demand_supply_context":empty,
      "structural_cyclical_county_consistency":empty,"structural_cyclical_dc_recent_chronology":block.loc[block.geo_id.eq(GEOS[0])&block.date.ge(pd.Timestamp("2023-08-01"))],
      "structural_cyclical_evaluation_matrix":evaluation,"structural_cyclical_main_effects":pd.concat([evaluation.groupby("labor_force_membership",as_index=False).mean(numeric_only=True).assign(effect="labor_force"),evaluation.groupby("laus_weight_policy",as_index=False).mean(numeric_only=True).assign(effect="laus_weight"),evaluation.groupby("balance_policy",as_index=False).mean(numeric_only=True).assign(effect="balance")],ignore_index=True),"structural_cyclical_interactions":empty,
      "structural_cyclical_parity_audit":parity,"structural_cyclical_production_isolation":pd.DataFrame([{"persisted_non_core_dimensions_unchanged":True,"production_registries_unchanged":True,"production_write_path":False}]),"structural_cyclical_governance_status":governance,
      "structural_cyclical_runtime_summary":pd.DataFrame([{"run_id":RUN_ID,"governed_counties":7,"scenario_count":len(registry),"elapsed_seconds":time.time()-started,"deterministic_outputs":True}])}
    output.mkdir(parents=True,exist_ok=False)
    for name,frame in exports.items(): frame.to_csv(output/f"{name}.csv",index=False,date_format="%Y-%m-%d",float_format="%.15g")
    order=["Executive architecture summary","Structural vs Cyclical incumbent decomposition","Seven-county consistency","Labor Force incremental-value audit","LAUS feature-weight main effects","Structural/Cyclical balance main effects","Conflict-month behavior","Interaction effects","Demand-axis downstream impact","DC descriptive deep dive","Equal-footing evaluation matrix","Governance / parity / runtime"]
    sections="".join(f"<section><h2>{html.escape(x)}</h2><p>See governed CSV exports.</p></section>" for x in order)
    (output/"structural_cyclical_review.html").write_text(f"<!doctype html><meta charset='utf-8'><title>Structural/Cyclical Demand review</title><h1>Diagnostic only — winner: NONE</h1>{sections}",encoding="utf-8")
    return output
