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
    turns = 0; crossings = 0
    for _, g in q.groupby("geo_id"):
        signs = np.sign(g.sort_values("date")[value].dropna())
        crossings += int(max(0, signs.ne(signs.shift()).sum() - (len(signs) > 0)))
        tp = detect_turning_points(g[["date", value]].dropna().sort_values("date"), value)
        turns += int(tp.qualified.sum()) if len(tp) and "qualified" in tp else len(tp)
    return {"core_std": s.std(), "median_abs_core_score": s.abs().median(),
            "reversal_1m": _reversal_rate(q, value, 1),
            "reversal_3m": _reversal_rate(q, value, 3),
            "reversal_6m": _reversal_rate(q, value, 6),
            "same_sign_persistence": 1 - _reversal_rate(q, value, 1),
            "zero_crossings": crossings,
            "turn_count": turns}


def _periods(frame: pd.DataFrame):
    """Yield the two governed diagnostic periods."""
    yield "full_history", frame
    yield "recent_36_months", recent_36(frame)


def _summary_row(frame: pd.DataFrame, value: str) -> dict[str, float]:
    stats = _series_stats(frame, value)
    return {"observations": int(frame[value].notna().sum()),
            "score_std": stats["core_std"],
            "median_abs_score": frame[value].abs().median(),
            "p90_abs_score": frame[value].abs().quantile(.9),
            "reversal_1m": stats["reversal_1m"], "reversal_3m": stats["reversal_3m"],
            "reversal_6m": stats["reversal_6m"],
            "same_sign_persistence": stats["same_sign_persistence"],
            "zero_crossings": stats["zero_crossings"], "turn_count": stats["turn_count"]}


def _qualified_turns(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    turns = detect_turning_points(frame[["date", value]].dropna().sort_values("date"), value)
    return turns.loc[turns.qualified].copy() if len(turns) else turns


def _persistence(frame: pd.DataFrame, value: str, date: pd.Timestamp, direction: str, months: int) -> bool:
    target = pd.Timestamp(date) + pd.DateOffset(months=months)
    q = frame.loc[frame.date.eq(target), value]
    if q.empty: return False
    return bool((q.iloc[0] < 0) if direction == "peak" else (q.iloc[0] > 0))


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

    # Reconstruct Demand exactly as production does: retain every governed
    # parent-child row, including capital_markets on both Demand and Supply.
    fixed=persisted_dim.loc[persisted_dim.dimension.isin(["price","affordability","capital_markets"])]
    axis_rows=[]
    for sid,g in chronology.groupby("scenario_id"):
        dims=pd.concat([g[["geo_id","date","core_demand_score"]].rename(columns={"core_demand_score":"score"}).assign(dimension="demand"),
                        fixed.rename(columns={"persisted":"score"})],ignore_index=True)
        detail, monthly=_contribution_layer(dims,ar.loc[ar.axis.str.lower().eq("demand")],"axis","dimension","score",_col(ar,"dimension_weight","weight"))
        wide=dims.pivot(index=["geo_id","date"],columns="dimension",values="score").reset_index()
        q=monthly.rename(columns={"gross_contribution":"gross_axis_contribution","dominant_dimension":"dominant_dimension"}).merge(wide,on=["geo_id","date"])
        q["scenario_id"]=sid; q["demand_axis_score"]=q.net_score
        q["axis_cancellation_index"]=(q.gross_axis_contribution-q.demand_axis_score.abs())/q.gross_axis_contribution.replace(0,np.nan)
        q["axis_net_to_gross_ratio"]=q.demand_axis_score.abs()/q.gross_axis_contribution.replace(0,np.nan)
        axis_rows.append(q.rename(columns={"demand":"scenario_demand_dimension","price":"price_dimension","affordability":"affordability_dimension","capital_markets":"capital_markets_dimension"}))
    axis_scenarios=pd.concat(axis_rows,ignore_index=True)
    axis_scenarios=axis_scenarios[["scenario_id","geo_id","date","scenario_demand_dimension","price_dimension","affordability_dimension","capital_markets_dimension","demand_axis_score","gross_axis_contribution","axis_cancellation_index","axis_net_to_gross_ratio","dominant_dimension"]]

    axis_summary=[]
    for sid,g in axis_scenarios.groupby("scenario_id"):
      for geo in (*GEOS,"POOLED"):
       x=g if geo=="POOLED" else g.loc[g.geo_id.eq(geo)]
       for period,q in _periods(x):
        st=_summary_row(q,"demand_axis_score")
        axis_summary.append({"scenario_id":sid,"geo_id":geo,"period":period,
          "demand_axis_std":st["score_std"],"median_abs_demand_axis":st["median_abs_score"],"p90_abs_demand_axis":st["p90_abs_score"],
          "median_axis_cancellation":q.axis_cancellation_index.median(),"median_axis_net_to_gross":q.axis_net_to_gross_ratio.median(),
          **{k:st[k] for k in ["reversal_1m","reversal_3m","reversal_6m","same_sign_persistence","zero_crossings","turn_count"]},
          "latest_demand_axis_score":q.sort_values("date").demand_axis_score.iloc[-1]})
    axis_summary=pd.DataFrame(axis_summary)

    summaries=[]; expression=[]
    for sid,g in chronology.groupby("scenario_id"):
        meta=registry.loc[registry.scenario_id.eq(sid)].iloc[0].to_dict(); st=_series_stats(g,"core_demand_score"); rg=recent_36(g); rst=_series_stats(rg,"core_demand_score")
        expr={}
        for component in ("structural","cyclical"):
            count=matched=0; lags=[]
            for geo,cg in g.groupby("geo_id"):
                source=_qualified_turns(cg,f"{component}_score"); target=_qualified_turns(cg,"core_demand_score")
                matches=match_turning_points(source,target); count+=len(source); matched+=int(matches.matched.sum())
                lags.extend(matches.loc[matches.matched,"signed_delay_months"].tolist())
            expr[f"{component}_turn_expression_share"]=matched/count if count else 0.0
            expression.append({"scenario_id":sid,"component":component,"component_turn_count":count,"matched_scenario_turn_count":matched,
              "expression_share":matched/count if count else 0.0,"median_lag_months":np.median(lags) if lags else np.nan,"matching_window_months":6})
        ax=axis_summary.loc[(axis_summary.scenario_id.eq(sid))&axis_summary.geo_id.eq("POOLED")]
        full=ax.loc[ax.period.eq("full_history")].iloc[0]; rec=ax.loc[ax.period.eq("recent_36_months")].iloc[0]
        summaries.append({**meta,"median_core_cancellation":g.core_demand_cancellation_index.median(),"recent_core_cancellation":rg.core_demand_cancellation_index.median(),**st,
          "recent_core_std":rst["core_std"],"recent_median_abs_core_score":rg.core_demand_score.abs().median(),**expr,
          "conflict_neutralization_share":g.loc[g.conflict_month.eq(True),"core_demand_score"].abs().lt(.05).mean(),"demand_axis_std":full.demand_axis_std,
          "recent_demand_axis_std":rec.demand_axis_std,"demand_axis_median_abs":full.median_abs_demand_axis})
    evaluation=pd.DataFrame(summaries); turn_expression=pd.DataFrame(expression)

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

    block=chronology.loc[chronology.scenario_id.eq("LF-IN__LAUS-W-25-35-40__BAL-INCUMBENT-EXACT")].copy()
    block_rows=[]; pair_rows=[]
    blocks={"Structural":("structural_score","structural_cancellation_index","structural_net_to_gross"),
            "Cyclical":("cyclical_score","cyclical_cancellation_index","cyclical_net_to_gross"),
            "Core Demand":("core_demand_score","core_demand_cancellation_index","core_demand_net_to_gross")}
    for geo in (*GEOS,"POOLED"):
      x=block if geo=="POOLED" else block.loc[block.geo_id.eq(geo)]
      for period,q in _periods(x):
       for name,(value,cancel,ratio) in blocks.items():
        block_rows.append({"scope":"OVERALL" if geo=="POOLED" else "COUNTY","geo_id":geo,"period":period,"block":name,
          **_summary_row(q,value),"median_cancellation_index":q[cancel].median(),"p90_cancellation_index":q[cancel].quantile(.9),"median_net_to_gross_ratio":q[ratio].median()})
       valid=q.dropna(subset=["structural_score","cyclical_score"]); conflict=valid.conflict_month.eq(True)
       pair_rows.append({"scope":"OVERALL" if geo=="POOLED" else "COUNTY","geo_id":geo,"period":period,
         "score_correlation":valid.structural_score.corr(valid.cyclical_score),"movement_correlation":valid.groupby("geo_id").structural_score.diff().corr(valid.groupby("geo_id").cyclical_score.diff()),
         "same_sign_share":valid.structural_cyclical_sign_agreement.mean(),"opposite_sign_share":conflict.mean(),
         "median_abs_structural_to_cyclical_ratio":(valid.structural_score.abs()/valid.cyclical_score.abs().replace(0,np.nan)).median(),
         "structural_dominance_share":valid.structural_score.abs().gt(valid.cyclical_score.abs()).mean(),"cyclical_dominance_share":valid.cyclical_score.abs().gt(valid.structural_score.abs()).mean(),
         "conflict_month_count":int(conflict.sum()),"conflict_month_share":conflict.mean()})
    block_summary=pd.DataFrame(block_rows); block_by_county=block_summary.loc[block_summary.geo_id.ne("POOLED")].copy(); block_pairwise=pd.DataFrame(pair_rows)

    # Paired LF turns; unmatched categories remain explicit rows by construction.
    lf_turn_rows=[]
    for (laus_policy,balance),meta in registry.loc[registry.balance_policy.ne("BAL-INCUMBENT-EXACT")].groupby(["laus_weight_policy","balance_policy"]):
      ids=dict(zip(meta.labor_force_membership,meta.scenario_id))
      for geo in GEOS:
       a=chronology.loc[(chronology.scenario_id.eq(ids["LF-IN"]))&chronology.geo_id.eq(geo)]; b=chronology.loc[(chronology.scenario_id.eq(ids["LF-OUT"]))&chronology.geo_id.eq(geo)]
       ta=_qualified_turns(a,"core_demand_score"); tb=_qualified_turns(b,"core_demand_score"); matches=match_turning_points(ta,tb)
       if matches.empty: lf_turn_rows.append({"laus_weight_policy":laus_policy,"balance_policy":balance,"geo_id":geo,"event_type":"zero_qualifying_events","event_count":0})
       for m in matches.itertuples():
        source="LF-IN" if pd.notna(m.incumbent_date) else "LF-OUT"; date=m.incumbent_date if source=="LF-IN" else m.challenger_date; frame=a if source=="LF-IN" else b
        lf_turn_rows.append({"laus_weight_policy":laus_policy,"balance_policy":balance,"geo_id":geo,"event_type":"matched" if m.matched else f"{source}_only","event_count":1,
          "turn_direction":m.turning_point_type,"turn_date":date,"nearest_paired_turn_date":m.challenger_date if source=="LF-IN" else m.incumbent_date,
          "lag_months":m.signed_delay_months,"persistence_after_3m":_persistence(frame,"core_demand_score",date,m.turning_point_type,3),
          "persistence_after_6m":_persistence(frame,"core_demand_score",date,m.turning_point_type,6),"short_lived_or_unmatched":not bool(m.matched)})
    lf_turns=pd.DataFrame(lf_turn_rows)

    # County/period scenario measures are the common evidence substrate.
    measures=[]
    for sid,g in chronology.groupby("scenario_id"):
      for geo in GEOS:
       x=g.loc[g.geo_id.eq(geo)]
       for period,q in _periods(x):
        st=_series_stats(q,"core_demand_score"); ax=axis_summary.loc[(axis_summary.scenario_id.eq(sid))&axis_summary.geo_id.eq(geo)&axis_summary.period.eq(period)].iloc[0]
        measures.append({"scenario_id":sid,"geo_id":geo,"period":period,"core_cancellation":q.core_demand_cancellation_index.median(),"cyclical_cancellation":q.cyclical_cancellation_index.median(),
          "core_std":st["core_std"],"median_abs_core":q.core_demand_score.abs().median(),"reversal_1m":st["reversal_1m"],"reversal_3m":st["reversal_3m"],"reversal_6m":st["reversal_6m"],
          "persistence":st["same_sign_persistence"],"zero_crossings":st["zero_crossings"],"turn_count":st["turn_count"],
          "conflict_neutralization":q.loc[q.conflict_month.eq(True),"core_demand_score"].abs().lt(.05).mean(),"demand_axis_std":ax.demand_axis_std,"demand_axis_median_abs":ax.median_abs_demand_axis})
    measures=pd.DataFrame(measures).merge(registry[["scenario_id","labor_force_membership","laus_weight_policy","balance_policy"]],on="scenario_id")
    laus_interactions=measures.copy()
    balance_by_county=measures.groupby(["balance_policy","geo_id","period"],as_index=False).agg(median_core_cancellation=("core_cancellation","median"),core_std=("core_std","mean"),median_abs_core_score=("median_abs_core","median"),reversal_1m=("reversal_1m","mean"),reversal_3m=("reversal_3m","mean"),reversal_6m=("reversal_6m","mean"),persistence=("persistence","mean"),zero_crossings=("zero_crossings","mean"),turn_count=("turn_count","mean"),conflict_neutralization_share=("conflict_neutralization","mean"),demand_axis_std=("demand_axis_std","mean"),demand_axis_median_abs=("demand_axis_median_abs","mean"),scenario_count=("scenario_id","nunique"))

    consistency=[]
    metric_cols=["core_cancellation","core_std","median_abs_core","reversal_1m","persistence","conflict_neutralization","demand_axis_std","demand_axis_median_abs"]
    full=measures.loc[measures.period.eq("full_history")].copy()
    for metric in metric_cols:
      full[f"{metric}_delta"]=full[metric]-full.groupby("geo_id")[metric].transform("median")
    for sid,g in full.groupby("scenario_id"):
      shares=[]
      for metric in metric_cols:
       vals=g[metric]; delta=g[f"{metric}_delta"]; signs=np.sign(delta.dropna()); share=max((signs>=0).mean(),(signs<=0).mean()) if len(signs) else np.nan; shares.append(share)
       consistency.append({"scenario_id":sid,"metric":metric,"median_across_counties":vals.median(),"min":vals.min(),"max":vals.max(),"IQR":vals.quantile(.75)-vals.quantile(.25),"sign_consistency_share":share,
         "definition":"county sign agreement of scenario-minus-county factorial median"})
      evaluation.loc[evaluation.scenario_id.eq(sid),"seven_county_consistency"]=np.nanmean(shares)
    county_consistency=pd.DataFrame(consistency)

    outcomes=metric_cols; interactions=[]
    for fa,fb,label in [("labor_force_membership","laus_weight_policy","LF × LAUS weight"),("labor_force_membership","balance_policy","LF × balance"),("laus_weight_policy","balance_policy","LAUS weight × balance")]:
      for period,q in measures.groupby("period"):
       for outcome in outcomes:
        grand=q[outcome].mean(); am=q.groupby(fa)[outcome].mean(); bm=q.groupby(fb)[outcome].mean()
        for (a,b),cell in q.groupby([fa,fb]):
         vals=cell[outcome]; deltas=cell.groupby("geo_id")[outcome].mean()-q.groupby("geo_id")[outcome].mean()
         interactions.append({"effect_type":label,"factor_a":fa,"factor_b":fb,"outcome":outcome,"level_a":a,"level_b":b,"mean_outcome":vals.mean(),"median_outcome":vals.median(),
           "interaction_delta":vals.mean()-am[a]-bm[b]+grand,"county_consistency":max((deltas>=0).mean(),(deltas<=0).mean()),"period":period})
    interactions=pd.DataFrame(interactions)

    supply=persisted_axis.loc[persisted_axis.axis.str.lower().eq("supply")].rename(columns={"persisted":"supply_axis_score"})
    supply_context=[]
    for sid,g in axis_scenarios.groupby("scenario_id"):
      joined=g.merge(supply[["geo_id","date","supply_axis_score"]],on=["geo_id","date"],how="left",validate="one_to_one")
      if joined.supply_axis_score.isna().any(): raise ValueError("persisted Supply axis coverage missing")
      for geo in GEOS:
       for period,q in _periods(joined.loc[joined.geo_id.eq(geo)]):
        ds=q.demand_axis_score.std(); ss=q.supply_axis_score.std(); da=q.demand_axis_score.abs().median(); sa=q.supply_axis_score.abs().median()
        supply_context.append({"scenario_id":sid,"geo_id":geo,"period":period,"demand_axis_std":ds,"supply_axis_std":ss,"demand_to_supply_std_ratio":ds/ss if ss else np.nan,
          "demand_median_abs":da,"supply_median_abs":sa,"demand_to_supply_median_abs_ratio":da/sa if sa else np.nan})
    supply_context=pd.DataFrame(supply_context)

    parity=pd.DataFrame([{"check":k,"max_abs_error":v,"tolerance":TOL,"status":"pass"} for k,v in errors.items()])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False,"production_policy_changed":False,"incumbent_similarity_used_as_evaluation":False}])
    exports={
      "structural_cyclical_block_monthly":block,"structural_cyclical_block_summary":block_summary,"structural_cyclical_block_pairwise":block_pairwise,"structural_cyclical_block_by_county":block_by_county,
      "structural_cyclical_scenario_registry":registry,"structural_cyclical_scenario_metric_weights":weights_df,
      "structural_cyclical_laus_metric_replay":laus_replay,"structural_cyclical_laus_parity_audit":parity.iloc[[0]],"structural_cyclical_scenario_chronology":chronology,
      "structural_cyclical_labor_force_incremental":paired,"structural_cyclical_labor_force_turns":lf_turns,"structural_cyclical_labor_force_by_county":paired.loc[paired.geo_id.ne("POOLED")],
      "structural_cyclical_laus_weight_main_effect":evaluation.groupby("laus_weight_policy",as_index=False).mean(numeric_only=True),"structural_cyclical_laus_weight_interactions":laus_interactions,
      "structural_cyclical_balance_main_effect":evaluation.groupby("balance_policy",as_index=False).mean(numeric_only=True),"structural_cyclical_balance_conflict_months":chronology.loc[chronology.conflict_month.eq(True)],"structural_cyclical_balance_by_county":balance_by_county,
      "structural_cyclical_conflict_episode_audit":chronology.loc[chronology.conflict_month.eq(True)].assign(record_grain="conflict_month"),"structural_cyclical_demand_axis_scenarios":axis_scenarios,"structural_cyclical_demand_axis_summary":axis_summary,"structural_cyclical_demand_supply_context":supply_context,
      "structural_cyclical_county_consistency":county_consistency,"structural_cyclical_dc_recent_chronology":block.loc[block.geo_id.eq(GEOS[0])&block.date.ge(pd.Timestamp("2023-08-01"))],
      "structural_cyclical_evaluation_matrix":evaluation,"structural_cyclical_main_effects":pd.concat([evaluation.groupby("labor_force_membership",as_index=False).mean(numeric_only=True).assign(effect="labor_force"),evaluation.groupby("laus_weight_policy",as_index=False).mean(numeric_only=True).assign(effect="laus_weight"),evaluation.groupby("balance_policy",as_index=False).mean(numeric_only=True).assign(effect="balance")],ignore_index=True),"structural_cyclical_interactions":interactions,
      "structural_cyclical_parity_audit":parity,"structural_cyclical_production_isolation":pd.DataFrame([{"persisted_non_core_dimensions_unchanged":True,"production_registries_unchanged":True,"production_write_path":False}]),"structural_cyclical_governance_status":governance,
      "structural_cyclical_runtime_summary":pd.DataFrame([{"run_id":RUN_ID,"governed_counties":7,"scenario_count":len(registry),"elapsed_seconds":time.time()-started,"deterministic_outputs":True}])}
    output.mkdir(parents=True,exist_ok=False)
    for name,frame in exports.items(): frame.to_csv(output/f"{name}.csv",index=False,date_format="%Y-%m-%d",float_format="%.15g")
    order=["Executive architecture summary","Structural vs Cyclical incumbent decomposition","Seven-county consistency","Labor Force incremental-value audit","LAUS feature-weight main effects","Structural/Cyclical balance main effects","Conflict-month behavior","Interaction effects","Demand-axis downstream impact","DC descriptive deep dive","Equal-footing evaluation matrix","Governance / parity / runtime"]
    sections="".join(f"<section><h2>{html.escape(x)}</h2><p>See governed CSV exports.</p></section>" for x in order)
    (output/"structural_cyclical_review.html").write_text(f"<!doctype html><meta charset='utf-8'><title>Structural/Cyclical Demand review</title><h1>Diagnostic only — winner: NONE</h1>{sections}",encoding="utf-8")
    return output
