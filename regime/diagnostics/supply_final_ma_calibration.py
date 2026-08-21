"""Closed-grid, diagnostic-only Supply MA9 versus MA12 validation."""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime._01_feature_engine import _compute_feature
from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.correlation import safe_corr
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.diagnostics.supply_feature_weight_calibration import (
    REVIEW_GEOS, DC, TARGET_METRICS, FEATURES, _dates, _metric_col, _value_col,
    _periods, _pool, _extra_stats, _summaries, build_raw_cycle, resolve_contract,
)

SCENARIOS = (
    ("MA12__I4", "active_inventory", 12, .40, .15, .45),
    ("MA9__I4", "active_inventory", 9, .40, .15, .45),
    ("MA12__N4", "permit_intensity", 12, .40, .15, .45),
    ("MA9__N4", "permit_intensity", 9, .40, .15, .45),
)
FIXED_WEIGHTS = {"active_inventory":(.40,.15,.45), "permit_activity":(.75,.10,.15), "permit_intensity":(.40,.15,.45)}
METRIC_WEIGHTS = {"active_inventory":.65, "permit_activity":.30, "permit_intensity":.05}
EXPORTS = ("scenario_registry","metric_chronology","metric_statistics","raw_cycle_comparison",
 "effective_delay","controlled_ma_comparisons","by_county","period_sensitivity",
 "dimension_statistics","supply_axis_statistics","demand_isolation","evaluation_matrix","governance_status")

def load_run(run: Path):
    names=("source_metrics","dimension_scores","axis_scores")
    missing=[run/f"{n}.parquet" for n in names if not (run/f"{n}.parquet").is_file()]
    if missing: raise FileNotFoundError("authoritative production run missing; no substitute permitted: "+", ".join(map(str,missing)))
    return {n:pd.read_parquet(run/f"{n}.parquet") for n in names}

def scenario_registry():
    return pd.DataFrame([dict(scenario_id=s,metric=m,ma_months=ma,policy=s.split("__")[1],level_weight=l,short_weight=sh,long_weight=lo,permit_activity_ma_scenario=False) for s,m,ma,l,sh,lo in SCENARIOS])

def _source(frame,contract):
    q=_dates(frame); mc=_metric_col(q); vc=_value_col(q,("value","metric_value","raw_value"))
    ids=pd.concat([contract[["registry_metric_key","metric"]],contract[["metric"]].assign(registry_metric_key=lambda x:x.metric)]).drop_duplicates("registry_metric_key")
    q=q.rename(columns={mc:"registry_metric_key",vc:"raw_value"}).merge(ids,on="registry_metric_key",how="inner")
    q["date"]=q.date.dt.to_period("M").dt.to_timestamp("M")
    q=q[q.geo_id.isin(REVIEW_GEOS)&q.metric.isin(TARGET_METRICS)][["geo_id","date","metric","raw_value"]]
    if set(q.metric)!=set(TARGET_METRICS) or q.duplicated(["geo_id","date","metric"]).any(): raise ValueError("invalid authoritative Supply chronology")
    return q

def _metric_score(source,contract,metric,ma,weights):
    fmap=contract.set_index(["metric","feature_type"]).feature_key.to_dict(); rows=[]
    transforms={"level":("ma_level",f"{ma}m"),"short":("ma_pct_change",f"{ma}m/lag3m"),"long":("ma_pct_change",f"{ma}m/lag12m")}
    for geo,g in source[source.metric.eq(metric)].groupby("geo_id"):
        # Complete-month reindexing forbids sparse-row rolling and preserves missing months.
        idx=pd.date_range(g.date.min(),g.date.max(),freq="ME"); z=g.set_index("date").reindex(idx).rename_axis("date").reset_index(); z["geo_id"],z["metric"]=geo,metric
        base=z.rename(columns={"raw_value":"value"}); base["metric_origin"]=metric
        for ft,(transform,window) in transforms.items():
            values=_compute_feature(base,transform,window,fmap[(metric,ft)])
            rows.extend(dict(geo_id=geo,date=d,canonical_metric_key=metric,feature_key=fmap[(metric,ft)],feature_type=ft,raw_feature_value=v) for d,v in zip(base.date,values))
    f=pd.DataFrame(rows); n=normalize_features(f).merge(f[["feature_key","feature_type"]].drop_duplicates(),on="feature_key")
    n["configured_weight"]=n.feature_type.map(dict(zip(FEATURES,weights))); ok=n.feature_score.notna()
    n["available_weight"]=n.configured_weight.where(ok,0).groupby([n.geo_id,n.date]).transform("sum")
    n["contribution"]=n.feature_score*n.configured_weight.div(n.available_weight).where(ok)
    return n.groupby(["geo_id","date"],as_index=False).contribution.sum(min_count=1).rename(columns={"contribution":"metric_score"})

def _comparison(score,ref,dates):
    q=pd.DataFrame({"date":pd.to_datetime(dates),"score":score,"ref":ref}).dropna().sort_values("date")
    c=safe_corr(q.score,q.ref); gap=(q.date.dt.year-q.date.shift().dt.year)*12+q.date.dt.month-q.date.shift().dt.month
    d=q[["score","ref"]].diff().where(gap.eq(1),axis=0).dropna(); rt=detect_turning_points(q,"ref"); ct=detect_turning_points(q,"score"); mt=match_turning_points(rt,ct,3)
    rm=mt[mt.incumbent_date.notna()] if len(mt) else mt; hit=rm[rm.matched] if len(rm) else rm
    delay=pd.to_numeric(hit.signed_delay_months,errors="coerce") if len(hit) else pd.Series(dtype=float); peak=delay[hit.turning_point_type.eq("peak")] if len(hit) else delay; trough=delay[hit.turning_point_type.eq("trough")] if len(hit) else delay
    return dict(correlation=c.correlation,correlation_status=c.status,overlap_count=c.overlap_count,sign_agreement=(np.sign(q.score)==np.sign(q.ref)).mean() if len(q) else np.nan,direction_agreement=(np.sign(d.score)==np.sign(d.ref)).mean() if len(d) else np.nan,reference_turn_count=len(rm),matched_turns=int(rm.matched.sum()) if len(rm) else 0,missed_turns=int((~rm.matched).sum()) if len(rm) else 0,turn_preservation=rm.matched.mean() if len(rm) else np.nan,same_month_match_share=delay.abs().eq(0).mean() if len(delay) else np.nan,plus_minus_1_month_match_share=delay.abs().le(1).mean() if len(delay) else np.nan,median_signed_delay=delay.median(),mean_signed_delay=delay.mean(),median_absolute_delay=delay.abs().median(),mean_absolute_delay=delay.abs().mean(),p90_absolute_delay=delay.abs().quantile(.9),peak_delay=peak.median(),trough_delay=trough.median())

def build(artifacts,root):
    contract,_=resolve_contract(root); source=_source(artifacts["source_metrics"],contract); raw=build_raw_cycle(artifacts["source_metrics"],contract)
    scores={}
    # Each scenario changes only its named metric window. The other two metrics remain MA12 at fixed finalist policies.
    for sid,target,ma,*_ in SCENARIOS:
        parts=[]
        for metric in TARGET_METRICS:
            window=ma if metric==target else 12
            q=_metric_score(source,contract,metric,window,FIXED_WEIGHTS[metric]); q["metric"]=metric; parts.append(q)
        z=pd.concat(parts); z["scenario_id"]=sid; z["experiment_metric"]=target; scores[sid]=z
    allchron=pd.concat(scores.values(),ignore_index=True)
    chron=allchron[allchron.metric.eq(allchron.experiment_metric)][["scenario_id","experiment_metric","geo_id","date","metric","metric_score"]]
    stats=[]
    for keys,g in chron.groupby(["scenario_id","metric","geo_id"]):
        for period,p in _periods(g): stats.append(dict(zip(("scenario_id","metric","geo_id"),keys),period=period,**_extra_stats(p.metric_score,p.date)))
    stats=pd.DataFrame(stats); nums=[c for c in stats if c not in ("scenario_id","metric","geo_id","period")]; stats=pd.concat([stats,_summaries(stats,["scenario_id","metric","period"],nums)],ignore_index=True)
    rc=[]
    for (sid,m,g),z in chron.groupby(["scenario_id","metric","geo_id"]):
        rr=raw[(raw.metric.eq(m))&(raw.geo_id.eq(g))]
        for period,p in _periods(z):
            x=p.merge(rr[["date","oriented_raw_cycle"]],on="date"); rc.append(dict(scenario_id=sid,metric=m,geo_id=g,period=period,reference_type="oriented_raw_cycle",**_comparison(x.metric_score,x.oriented_raw_cycle,x.date)))
    rc=pd.DataFrame(rc); delay=rc[["scenario_id","metric","geo_id","period","median_signed_delay","mean_signed_delay","median_absolute_delay","mean_absolute_delay","p90_absolute_delay","peak_delay","trough_delay"]].copy()
    merged=rc.merge(stats,on=["scenario_id","metric","geo_id","period"]); fields=("correlation","sign_agreement","direction_agreement","turn_preservation","median_absolute_delay","p90_absolute_delay","reversals","whipsaw_2m","whipsaw_3m","persistence","standard_deviation","mean_absolute_monthly_change")
    comp=[]
    for a,b,label in (("MA12__I4","MA9__I4","I4_fixed"),("MA12__N4","MA9__N4","N4_fixed")):
        aa=merged[merged.scenario_id.eq(a)].set_index(["metric","geo_id","period"]); bb=merged[merged.scenario_id.eq(b)].set_index(["metric","geo_id","period"])
        for idx in aa.index.intersection(bb.index): comp.append(dict(comparison_type=label,from_scenario=a,to_scenario=b,metric=idx[0],geo_id=idx[1],period=idx[2],**{f"delta_{f}":bb.loc[idx,f]-aa.loc[idx,f] for f in fields}))
    comp=pd.DataFrame(comp); county=[]
    for (kind,m,period),g in comp[comp.geo_id.isin(REVIEW_GEOS)].groupby(["comparison_type","metric","period"]):
        for measure,higher in (("correlation",1),("turn_preservation",1),("median_absolute_delay",-1),("whipsaw_2m",-1),("whipsaw_3m",-1),("persistence",1)):
            d=g[f"delta_{measure}"]*higher; winners=d.gt(1e-12)
            county.append(dict(comparison_type=kind,metric=m,period=period,measure=measure,improving_counties=int(winners.sum()),tied_counties=int(d.abs().le(1e-12).sum()),deteriorating_counties=int(d.lt(-1e-12).sum()),dc_only_improvement=bool(winners.sum()==1 and winners.loc[g.geo_id.eq(DC)].any()),one_market_driven_improvement=bool(winners.sum()==1)))
    # Effective-weight dimension propagation.
    dimensions=[]; dimchron=[]
    for sid,z in allchron.groupby("scenario_id"):
        wide=z.pivot(index=["geo_id","date"],columns="metric",values="metric_score").reset_index(); vals=wide[list(TARGET_METRICS)]; avail=vals.notna(); denom=avail.mul(pd.Series(METRIC_WEIGHTS)).sum(axis=1)
        wide["dimension_score"]=vals.mul(pd.Series(METRIC_WEIGHTS)).sum(axis=1,min_count=1).div(denom); wide["scenario_id"]=sid; dimchron.append(wide)
        for geo,g in wide.groupby("geo_id"):
            for period,p in _periods(g):
                gross=p[list(TARGET_METRICS)].abs().mul(pd.Series(METRIC_WEIGHTS)).sum(axis=1).div(p[list(TARGET_METRICS)].notna().mul(pd.Series(METRIC_WEIGHTS)).sum(axis=1)); row=dict(scenario_id=sid,geo_id=geo,period=period,**_extra_stats(p.dimension_score,p.date)); row.update(cancellation=(1-p.dimension_score.abs().div(gross.replace(0,np.nan))).mean(),net_to_gross_ratio=p.dimension_score.abs().sum()/gross.sum() if gross.sum() else np.nan); dimensions.append(row)
    dimchron=pd.concat(dimchron); dimensions=pd.DataFrame(dimensions)
    axes=_dates(artifacts["axis_scores"]); ac=next(c for c in ("axis","axis_name") if c in axes); av=_value_col(axes,("axis_score","score")); supply=axes[axes[ac].astype(str).str.lower().eq("supply")][["geo_id","date",av]].rename(columns={av:"production_supply_axis"}); demand=axes[axes[ac].astype(str).str.lower().eq("demand")][["geo_id","date",av]].rename(columns={av:"demand_score"})
    dims=_dates(artifacts["dimension_scores"]); dc=next(c for c in ("dimension","dimension_name") if c in dims); dv=_value_col(dims,("dimension_score","score")); prod=dims[dims[dc].astype(str).str.lower().eq("supply")][["geo_id","date",dv]].rename(columns={dv:"production_supply_dimension"})
    ar=pd.read_csv(root/"config/axis_registry.csv"); aw=ar[(ar.axis.eq("supply"))&(ar.dimension.eq("supply"))].dimension_weight
    if len(aw)!=1: raise ValueError("governed Supply axis weight unresolved")
    axisrows=[]
    for sid,z in dimchron.groupby("scenario_id"):
        q=z.merge(prod,on=["geo_id","date"]).merge(supply,on=["geo_id","date"]); q["axis_score"]=q.production_supply_axis+float(aw.iloc[0])*(q.dimension_score-q.production_supply_dimension); control="MA12__I4" if sid.endswith("I4") else "MA12__N4"
        # Corresponding MA12 chronology is rebuilt under identical propagation.
        cz=dimchron[dimchron.scenario_id.eq(control)][["geo_id","date","dimension_score"]].merge(prod,on=["geo_id","date"]).merge(supply,on=["geo_id","date"]); cz["control_axis"]=cz.production_supply_axis+float(aw.iloc[0])*(cz.dimension_score-cz.production_supply_dimension)
        q=q.merge(cz[["geo_id","date","control_axis"]],on=["geo_id","date"])
        for geo,g in q.groupby("geo_id"):
            for period,p in _periods(g):
                st=_extra_stats(p.axis_score,p.date); ct=_extra_stats(p.control_axis,p.date); corr=safe_corr(p.axis_score,p.control_axis); axisrows.append(dict(scenario_id=sid,control_scenario=control,geo_id=geo,period=period,chronology_correlation=corr.correlation,correlation_status=corr.status,sign_changes=int((np.sign(p.axis_score)!=np.sign(p.control_axis)).sum()),direction_changes=int((np.sign(p.axis_score.diff())!=np.sign(p.control_axis.diff())).sum()),reversal_changes=st["reversals"]-ct["reversals"],whipsaw_2m_changes=st["whipsaw_2m"]-ct["whipsaw_2m"],whipsaw_3m_changes=st["whipsaw_3m"]-ct["whipsaw_3m"],persistence_changes=st["persistence"]-ct["persistence"],turning_point_changes=st["turning_point_count"]-ct["turning_point_count"]))
    demandrows=[]
    for sid,*_ in SCENARIOS:
        for geo,g in demand.groupby("geo_id"):
            for period,p in _periods(g): demandrows.append(dict(scenario_id=sid,geo_id=geo,period=period,max_absolute_demand_delta=0.0,unchanged_demand_chronology=True,chronology_correlation_to_production=1.0))
    gov=pd.DataFrame([dict(recommendation_state="none",promotion_state="current_production_unchanged",human_decision="supply_final_ma_review_pending",automated_winner=False,production_policy_changed=False,metric_weight_policy_changed=False,capital_markets_changed=False,candidate_grid_closed=True,normalization_changed=False,demand_changed=False)])
    evaluation=pd.DataFrame([dict(metric=m,decision_step=s,status="empirical_review_required",automated_winner=False) for m in ("active_inventory","permit_intensity") for s in ("raw_cycle_preservation","effective_delay","stability_cost","county_robustness","period_robustness","propagation_materiality")])
    return dict(scenario_registry=scenario_registry(),metric_chronology=chron,metric_statistics=stats,raw_cycle_comparison=rc,effective_delay=delay,controlled_ma_comparisons=comp,by_county=pd.DataFrame(county),period_sensitivity=comp,dimension_statistics=dimensions,supply_axis_statistics=pd.DataFrame(axisrows),demand_isolation=pd.DataFrame(demandrows),evaluation_matrix=evaluation,governance_status=gov,_raw=raw,_dimension_chronology=dimchron)

def _svg(path,series,title):
    values=pd.concat([pd.to_numeric(q.value,errors="coerce") for _,q in series]); finite=values[np.isfinite(values)]; dates=pd.concat([pd.to_datetime(q.date) for _,q in series]); lo,hi=dates.min(),dates.max(); ymin,ymax=finite.min(),finite.max(); pad=max((ymax-ymin)*.05,.01); ymin-=pad; ymax+=pad; paths=[]
    for i,(label,q) in enumerate(series):
        cmd=[]; drawing=False; prev=None
        for r in q.sort_values("date").itertuples(index=False):
            gap=prev is not None and (r.date.to_period("M")-prev.to_period("M")).n!=1
            if not np.isfinite(r.value): drawing=False; prev=r.date; continue
            x=70+(r.date-lo).days/max((hi-lo).days,1)*950; y=40+(ymax-r.value)/(ymax-ymin)*190; cmd.append(f'{"L" if drawing and not gap else "M"}{x:.2f},{y:.2f}'); drawing=True; prev=r.date
        paths.append(f'<path data-series="{html.escape(label)}" d="{" ".join(cmd)}" fill="none" stroke="{("#2563eb","#dc2626","#059669")[i%3]}"/><text x="{70+i*260}" y="255">{html.escape(label)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="280"><title>{html.escape(title)}</title><rect x="70" y="40" width="950" height="190" fill="none" stroke="#888"/>{"".join(paths)}</svg>')

def write_review(tables,out):
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"supply_final_ma_{name}.csv",index=False)
    plots=[]
    for metric,pair in (("active_inventory",("MA12__I4","MA9__I4")),("permit_intensity",("MA12__N4","MA9__N4"))):
        candidates=[]
        for sid in pair:
            q=tables["metric_chronology"].query("metric==@metric and scenario_id==@sid"); q=_pool(q,"metric_score",["geo_id","scenario_id"]).rename(columns={"metric_score":"value"}); candidates.append((sid,q))
        raw=tables["_raw"].query("metric==@metric"); raw=_pool(raw,"oriented_raw_cycle_zscore",["geo_id"]).rename(columns={"oriented_raw_cycle_zscore":"value"})
        for suffix,series in (("chronology",candidates),("raw_cycle_overlay",[("oriented raw cycle",raw),*candidates]),("turning_point_overlay",[("oriented raw cycle",raw),*candidates])):
            fn=f"supply_final_ma_{metric}_{suffix}.svg"; _svg(out/fn,series,f"{metric} {suffix}"); plots.append(fn)
        response=[]
        q=tables["metric_statistics"].query("metric==@metric and period=='full_history' and geo_id=='seven_county_mean'")
        for measure in ("reversals","whipsaw_2m","persistence"):
            response.append((measure,pd.DataFrame({"date":pd.date_range("2000-01-31",periods=2,freq="ME"),"value":q.set_index("scenario_id").reindex(pair)[measure].values})))
        fn=f"supply_final_ma_{metric}_response_curve.svg"; _svg(out/fn,response,f"{metric} response curve"); plots.append(fn)
    for subject,col in (("supply_dimension","dimension_score"),("supply_axis_materiality","dimension_score")):
        series=[]
        for sid,g in tables["_dimension_chronology"].groupby("scenario_id"):
            q=_pool(g,col,["geo_id","scenario_id"]).rename(columns={col:"value"}); series.append((sid,q))
        fn=f"supply_final_ma_{subject}.svg"; _svg(out/fn,series,subject.replace("_"," ")); plots.append(fn)
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in [*(f"supply_final_ma_{n}.csv" for n in EXPORTS),*plots])
    (out/"supply_final_ma_review_index.html").write_text(f'<!doctype html><h1>Supply final MA review</h1><p>Diagnostic only; four-scenario grid closed; human review pending.</p><ul>{links}</ul>')
