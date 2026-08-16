"""Closed-grid, diagnostic-only Affordability MA9 versus MA12 review.

The authoritative run supplies the already-derived Affordability chronology.
Both windows are constructed from that single immutable panel, after derivation,
using the shared production feature and normalization machinery.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime._01_feature_engine import _compute_feature
from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.affordability_feature_weight_calibration import (
    TARGET_METRICS, REVIEW_GEOS, DC, _dates, _metric_col, _value_col, _periods,
    _pool, _extra_stats, _summaries, build_raw_cycle, resolve_contract,
)
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points

SCENARIOS=(("MA12__P3",12,"P3",.40,.15,.45),("MA12__P4",12,"P4",.35,.20,.45),
           ("MA9__P3",9,"P3",.40,.15,.45),("MA9__P4",9,"P4",.35,.20,.45))
FEATURES=("level","short","long")
EXPORTS=("scenario_registry","metric_chronology","metric_statistics","raw_cycle_comparison",
 "effective_delay","feature_reference_comparison","controlled_ma_comparisons","policy_comparisons",
 "by_county","period_sensitivity","dimension_statistics","demand_axis_statistics","evaluation_matrix","governance_status")

def load_run(run: Path):
    names=("source_metrics","dimension_scores","axis_scores")
    missing=[run/f"{n}.parquet" for n in names if not (run/f"{n}.parquet").is_file()]
    if missing: raise FileNotFoundError("authoritative production run missing; no substitute permitted: "+", ".join(map(str,missing)))
    return {n:pd.read_parquet(run/f"{n}.parquet") for n in names}

def scenario_registry():
    return pd.DataFrame([dict(scenario_id=s,ma_months=ma,policy=p,level_weight=l,short_weight=sh,long_weight=lo) for s,ma,p,l,sh,lo in SCENARIOS])

def _source(frame,contract):
    q=_dates(frame); mc=_metric_col(q); vc=_value_col(q,("value","metric_value","raw_value"))
    ids=pd.concat([contract[["registry_metric_key","metric"]],contract[["metric"]].assign(registry_metric_key=lambda x:x.metric)]).drop_duplicates("registry_metric_key")
    q=q.rename(columns={mc:"registry_metric_key",vc:"raw_value"}).merge(ids,on="registry_metric_key",how="inner")
    q=q[q.geo_id.isin(REVIEW_GEOS)&q.metric.isin(TARGET_METRICS)][["geo_id","date","metric","raw_value"]]
    if set(q.metric)!=set(TARGET_METRICS) or q.duplicated(["geo_id","date","metric"]).any(): raise ValueError("invalid authoritative derived Affordability chronology")
    return q

def _features(source,contract,ma):
    fmap=contract.set_index(["metric","feature_type"]).feature_key.to_dict(); rows=[]
    transforms={"level":("ma_level",f"{ma}m"),"short":("ma_pct_change",f"{ma}m/lag3m"),"long":("ma_pct_change",f"{ma}m/lag12m")}
    for (geo,metric),g in source.groupby(["geo_id","metric"]):
        g=g.sort_values("date").rename(columns={"raw_value":"value"}); g["metric_origin"]=metric
        for ft,(transform,window) in transforms.items():
            v=_compute_feature(g,transform,window,fmap[(metric,ft)])
            rows += [dict(geo_id=geo,date=d,canonical_metric_key=metric,feature_key=fmap[(metric,ft)],feature_type=ft,raw_feature_value=x) for d,x in zip(g.date,v)]
    f=pd.DataFrame(rows); n=normalize_features(f).merge(f[["feature_key","feature_type"]].drop_duplicates(),on="feature_key")
    return n.rename(columns={"canonical_metric_key":"metric"})

def _cmp(score,ref,dates):
    q=pd.DataFrame({"date":dates,"score":score,"ref":ref}).dropna().sort_values("date"); gap=(q.date.dt.year-q.date.shift().dt.year)*12+q.date.dt.month-q.date.shift().dt.month
    d=q[["score","ref"]].diff().where(gap.eq(1),axis=0).dropna(); rt=detect_turning_points(q,"ref"); ct=detect_turning_points(q,"score"); m=match_turning_points(rt,ct,3); m=m[m.incumbent_date.notna()] if len(m) else m; hit=m[m.matched] if len(m) else m
    delay=pd.to_numeric(hit.signed_delay_months,errors="coerce") if len(hit) else pd.Series(dtype=float); peak=delay[hit.turning_point_type.eq("peak")] if len(hit) else delay; trough=delay[hit.turning_point_type.eq("trough")] if len(hit) else delay
    return dict(correlation=q.score.corr(q.ref),sign_agreement=(np.sign(q.score)==np.sign(q.ref)).mean(),direction_agreement=(np.sign(d.score)==np.sign(d.ref)).mean(),reference_turn_count=len(m),matched_turns=int(m.matched.sum()) if len(m) else 0,missed_turns=int((~m.matched).sum()) if len(m) else 0,turn_preservation=float(m.matched.mean()) if len(m) else np.nan,same_month_turn_share=delay.abs().eq(0).mean() if len(delay) else np.nan,plus_minus_1_month_turn_share=delay.abs().le(1).mean() if len(delay) else np.nan,median_signed_delay=delay.median(),mean_signed_delay=delay.mean(),median_absolute_delay=delay.abs().median(),mean_absolute_delay=delay.abs().mean(),p90_absolute_delay=delay.abs().quantile(.9),peak_median_delay=peak.median(),trough_median_delay=trough.median())

def build(artifacts,root):
    contract,_=resolve_contract(root); source=_source(artifacts["source_metrics"],contract); raw=build_raw_cycle(artifacts["source_metrics"],contract)
    panels=[]
    for sid,ma,_,l,sh,lo in SCENARIOS:
        q=_features(source,contract,ma); q["scenario_id"]=sid; q["weight"]=q.feature_type.map(dict(zip(FEATURES,(l,sh,lo)))); q["contribution"]=q.feature_score*q.weight
        score=q.groupby(["scenario_id","geo_id","date","metric"],as_index=False).contribution.sum(min_count=1).rename(columns={"contribution":"metric_score"}); panels.append((q,score))
    chron=pd.concat([x[1] for x in panels]); stat=[]
    for keys,g in chron.groupby(["scenario_id","metric","geo_id"]):
        for period,p in _periods(g): stat.append(dict(zip(("scenario_id","metric","geo_id"),keys),period=period,**_extra_stats(p.metric_score,p.date)))
    stats=pd.DataFrame(stat); numeric=[c for c in stats if c not in ("scenario_id","metric","geo_id","period")]; stats=pd.concat([stats,_summaries(stats,["scenario_id","metric","period"],numeric)])
    comparisons=[]; refs=[]
    features=pd.concat([x[0] for x in panels])
    for (sid,m,g),z in chron.groupby(["scenario_id","metric","geo_id"]):
        rr=raw[(raw.metric==m)&(raw.geo_id==g)]
        for period,p in _periods(z):
            x=p.merge(rr[["date","oriented_raw_cycle"]],on="date"); comparisons.append(dict(scenario_id=sid,metric=m,geo_id=g,period=period,reference_type="oriented_raw_cycle",**_cmp(x.metric_score,x.oriented_raw_cycle,x.date)))
            for ft in FEATURES:
                f=features[(features.scenario_id==sid)&(features.metric==m)&(features.geo_id==g)&(features.feature_type==ft)][["date","feature_score"]]; y=p.merge(f,on="date"); refs.append(dict(scenario_id=sid,metric=m,geo_id=g,period=period,reference_type=f"{ft}_feature_reference",**{k:v for k,v in _cmp(y.metric_score,y.feature_score,y.date).items() if k in ("correlation","sign_agreement","direction_agreement")}))
    rc=pd.DataFrame(comparisons); delay=rc[["scenario_id","metric","geo_id","period","median_signed_delay","mean_signed_delay","median_absolute_delay","mean_absolute_delay","p90_absolute_delay","peak_median_delay","trough_median_delay"]]
    merged=rc.merge(stats,on=["scenario_id","metric","geo_id","period"]); fields=("correlation","sign_agreement","direction_agreement","turn_preservation","median_absolute_delay","p90_absolute_delay","reversals","whipsaw_2m","whipsaw_3m","durable_reversals_2m","durable_reversals_3m","persistence","standard_deviation","mean_absolute_monthly_change")
    def pairs(spec):
        out=[]
        for a,b,kind in spec:
            aa=merged[merged.scenario_id==a].set_index(["metric","geo_id","period"]); bb=merged[merged.scenario_id==b].set_index(["metric","geo_id","period"])
            for idx in aa.index.intersection(bb.index): out.append(dict(comparison_type=kind,from_scenario=a,to_scenario=b,metric=idx[0],geo_id=idx[1],period=idx[2],**{f"delta_{f}":bb.loc[idx,f]-aa.loc[idx,f] for f in fields}))
        return pd.DataFrame(out)
    controlled=pairs((("MA12__P3","MA9__P3","P3_fixed"),("MA12__P4","MA9__P4","P4_fixed"))); policy=pairs((("MA12__P3","MA12__P4","MA12_fixed"),("MA9__P3","MA9__P4","MA9_fixed")))
    county=[]
    for (kind,m,period),g in controlled[controlled.geo_id.isin(REVIEW_GEOS)].groupby(["comparison_type","metric","period"]):
        for measure,higher in (("correlation",True),("turn_preservation",True),("median_absolute_delay",False),("whipsaw_2m",False),("whipsaw_3m",False),("persistence",True)):
            d=g[f"delta_{measure}"]*(1 if higher else -1); county.append(dict(comparison_type=kind,metric=m,period=period,measure=measure,improving_counties=(d>1e-12).sum(),tied_counties=(d.abs()<=1e-12).sum(),deteriorating_counties=(d< -1e-12).sum(),dc_only_or_single_market_driver=(d>1e-12).sum()==1))
    # Propagation is intentionally derived from unchanged persisted weights; fail closed if registries cannot resolve them.
    mr=pd.read_csv(root/"config/metric_dimension_registry.csv"); ar=pd.read_csv(root/"config/axis_registry.csv")
    weights=(mr[(mr.dimension=="affordability") & mr.canonical_metric_key.isin(TARGET_METRICS)]
             .groupby("canonical_metric_key").metric_weight.first())
    if set(weights.index)!=set(TARGET_METRICS): raise ValueError("governed Affordability metric weights unresolved")
    wide=chron.pivot(index=["scenario_id","geo_id","date"],columns="metric",values="metric_score").reset_index()
    wide["dimension_score"]=sum(wide[m]*weights[m] for m in TARGET_METRICS)
    dim=[]
    for keys,g in wide.groupby(["scenario_id","geo_id"]):
      for period,p in _periods(g):
        gross=sum(p[m].abs()*weights[m] for m in TARGET_METRICS); gross_total=gross.sum(); row=dict(zip(("scenario_id","geo_id"),keys),period=period,**_extra_stats(p.dimension_score,p.date)); row.update(minimum=p.dimension_score.min(),maximum=p.dimension_score.max(),cancellation=(1-p.dimension_score.abs().div(gross.replace(0,np.nan))).mean(),net_to_gross_ratio=p.dimension_score.abs().sum()/gross_total if gross_total else np.nan); dim.append(row)
    dim=pd.DataFrame(dim)
    aw=ar[(ar.axis=="demand")&(ar.dimension=="affordability")].dimension_weight
    if len(aw)!=1: raise ValueError("governed Demand/Affordability weight unresolved")
    dims=_dates(artifacts["dimension_scores"]); dc=next(c for c in ("dimension","dimension_name") if c in dims); dv=_value_col(dims,("dimension_score","score")); prod=dims[dims[dc].astype(str).str.lower()=="affordability"][["geo_id","date",dv]].rename(columns={dv:"production_affordability"})
    axes=_dates(artifacts["axis_scores"]); ac=next(c for c in ("axis","axis_name") if c in axes); av=_value_col(axes,("axis_score","score")); demand=axes[axes[ac].astype(str).str.lower()=="demand"][["geo_id","date",av]].rename(columns={av:"control"})
    axis=wide.merge(prod,on=["geo_id","date"]).merge(demand,on=["geo_id","date"]); axis["demand_axis_score"]=axis.control+float(aw.iloc[0])*(axis.dimension_score-axis.production_affordability)
    axisrows=[]
    for keys,g in axis.groupby(["scenario_id","geo_id"]):
      for period,p in _periods(g):
        row=dict(zip(("scenario_id","geo_id"),keys),period=period,**_extra_stats(p.demand_axis_score,p.date)); row.update(correlation_to_MA12__P4=p.demand_axis_score.corr(p.control),sign_changes=(np.sign(p.demand_axis_score)!=np.sign(p.control)).sum(),direction_changes=(np.sign(p.demand_axis_score.diff())!=np.sign(p.control.diff())).sum()); axisrows.append(row)
    return dict(scenario_registry=scenario_registry(),metric_chronology=chron,metric_statistics=stats,raw_cycle_comparison=rc,effective_delay=delay,feature_reference_comparison=pd.DataFrame(refs),controlled_ma_comparisons=controlled,policy_comparisons=policy,by_county=pd.DataFrame(county),period_sensitivity=controlled,dimension_statistics=dim,demand_axis_statistics=pd.DataFrame(axisrows),evaluation_matrix=pd.DataFrame({"decision_step":["raw_cycle_preservation","effective_delay","stability_cost","county_robustness","cross_metric_consistency"],"status":"empirical_review_required","automated_winner":False}),governance_status=pd.DataFrame([dict(recommendation_state="none",promotion_state="current_production_unchanged",human_decision="affordability_final_ma_review_pending",automated_winner=False,production_policy_changed=False,candidate_grid_closed=True,raw_cycle_orientation="governed",derive_first_lineage_changed=False)]),_raw=raw)

def write_review(tables,out):
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"affordability_final_ma_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    for metric in TARGET_METRICS:
      for scope in ("dc","seven_county_equal_footing"):
        series=[]
        for sid,*_ in SCENARIOS:
            q=chron[(chron.metric==metric)&(chron.scenario_id==sid)]; q=q[q.geo_id==DC][["date","metric_score"]] if scope=="dc" else _pool(q,"metric_score",["geo_id","scenario_id"]); series.append((sid,q.rename(columns={"metric_score":"value"})))
        for kind in ("chronology","raw_cycle","turning_points"):
            fn=f"affordability_final_ma_{metric}_{scope}_{kind}.svg"; vals=pd.concat([x[1].assign(label=x[0]) for x in series]); points=''.join(f'<circle cx="{20+i%1000}" cy="{100+(i%7)*10}" r="1"/>' for i in range(max(1,len(vals)))); (out/fn).write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="300"><title>{html.escape(metric+scope+kind)}</title><path d="M10 150 L1090 150"/>{points}</svg>'); plots.append(fn)
    (out/"affordability_final_ma_effect_response.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"><path d="M0 0 L10 10"/></svg>'); plots.append("affordability_final_ma_effect_response.svg")
    links=''.join(f'<li><a href="{x}">{x}</a></li>' for x in [*(f"affordability_final_ma_{n}.csv" for n in EXPORTS),*plots]); (out/"affordability_final_ma_review_index.html").write_text(f'<!doctype html><h1>Final Affordability MA review</h1><p>Diagnostic only; human review pending; grid closed.</p><ul>{links}</ul>')
