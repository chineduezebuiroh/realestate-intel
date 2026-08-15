"""Closed-grid, diagnostic-only Price MA9 versus MA12 finalist review.

Feature construction and normalization deliberately call the production shared
machinery.  No registry is mutated and no result is an automated selection.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime._01_feature_engine import _compute_feature
from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.capital_markets_ma import (
    TURN_FIXED_PROMINENCE, TURN_PROMINENCE_MULTIPLIER,
    detect_turning_points, match_turning_points,
)
from regime.diagnostics.price_feature_anatomy import DC, REVIEW_GEOS, TARGET_METRICS, _dates, _metric_col, _periods, _pool, _value_col, resolve_contract
from regime.diagnostics.price_feature_weight_calibration import _extra_stats, _summaries, build_raw_cycle

SCENARIOS = (
    ("MA12__P4", 12, "P4", .35, .15, .50),
    ("MA12__P6", 12, "P6", .35, .20, .45),
    ("MA9__P4", 9, "P4", .35, .15, .50),
    ("MA9__P6", 9, "P6", .35, .20, .45),
)
FEATURES = ("level", "short", "long")
MATCH_MONTHS = 3
PERSISTENCE = 2
PRICE_WEIGHTS = {"median_sale_price": .5, "median_ppsf": .5}
DEMAND_WEIGHTS = {"labor_demand": .650, "price": .175, "affordability": .075, "capital_markets": .100}
EXPORTS = (
    "scenario_registry", "metric_chronology", "metric_statistics",
    "raw_cycle_comparison", "effective_delay", "long_reference_comparison",
    "controlled_ma_comparisons", "policy_comparisons", "by_county",
    "period_sensitivity", "price_dimension_statistics",
    "demand_axis_statistics", "evaluation_matrix", "governance_status",
)


def scenario_registry() -> pd.DataFrame:
    return pd.DataFrame([{"scenario_id": s, "ma_months": ma, "policy": p,
        "level_weight": l, "short_weight": sh, "long_weight": lo,
        "detector_persistence": PERSISTENCE} for s,ma,p,l,sh,lo in SCENARIOS])


def load_run(run: Path) -> dict[str, pd.DataFrame]:
    required=("source_metrics", "dimension_scores", "axis_scores")
    missing=[run/f"{x}.parquet" for x in required if not (run/f"{x}.parquet").is_file()]
    if missing: raise FileNotFoundError("authoritative post-ADR-008 run missing; no substitute permitted: "+", ".join(map(str,missing)))
    return {x:pd.read_parquet(run/f"{x}.parquet") for x in required}


def _source(artifacts, contract):
    raw=_dates(artifacts["source_metrics"]); mc=_metric_col(raw); vc=_value_col(raw,("value","metric_value","raw_value"))
    identities=pd.concat([contract[["registry_metric_key","metric"]],contract[["metric"]].assign(registry_metric_key=lambda x:x.metric)]).drop_duplicates("registry_metric_key")
    q=raw.rename(columns={mc:"registry_metric_key",vc:"value"}).merge(identities,on="registry_metric_key",how="inner")
    q=q[q.geo_id.isin(REVIEW_GEOS)&q.metric.isin(TARGET_METRICS)].copy()
    if set(q.metric.unique()) != set(TARGET_METRICS): raise ValueError("authoritative source does not contain both Price metrics")
    if q.duplicated(["geo_id","date","metric"]).any(): raise ValueError("duplicate Price source month")
    if "metric_origin" not in q: q["metric_origin"]=q.registry_metric_key
    return q[["geo_id","date","metric","value","metric_origin"]]


def _features(source, contract, ma):
    fmap=contract.set_index(["metric","feature_type"]).feature_key.to_dict(); rows=[]
    transforms={"level":("ma_level",f"{ma}m"),"short":("ma_pct_change",f"{ma}m/lag3m"),"long":("ma_pct_change",f"{ma}m/lag12m")}
    for (geo,metric),g in source.groupby(["geo_id","metric"],sort=True):
        g=g.sort_values("date").copy()
        for ft,(transform,window) in transforms.items():
            values=_compute_feature(g,transform,window,fmap[(metric,ft)])
            rows.extend({"geo_id":geo,"date":date,"canonical_metric_key":metric,
                "feature_key":fmap[(metric,ft)],"feature_type":ft,"raw_feature_value":value}
                for date,value in zip(g.date,values))
    frame=pd.DataFrame(rows)
    normalized=normalize_features(frame).merge(
        frame[["feature_key","feature_type"]].drop_duplicates(),
        on="feature_key", how="left", validate="many_to_one")
    return normalized[["geo_id","date","canonical_metric_key","feature_key","feature_type","raw_feature_value","feature_score"]].rename(columns={"canonical_metric_key":"metric"})


def _turns(score, reference, dates):
    q=pd.DataFrame({"date":pd.to_datetime(dates),"score":score,"reference":reference}).dropna().sort_values("date")
    rt=detect_turning_points(q,"reference",persistence=PERSISTENCE,fixed_prominence=TURN_FIXED_PROMINENCE,prominence_multiplier=TURN_PROMINENCE_MULTIPLIER)
    ct=detect_turning_points(q,"score",persistence=PERSISTENCE,fixed_prominence=TURN_FIXED_PROMINENCE,prominence_multiplier=TURN_PROMINENCE_MULTIPLIER)
    matches=match_turning_points(rt,ct,MATCH_MONTHS); refs=matches[matches.incumbent_date.notna()] if len(matches) else matches; hits=refs[refs.matched] if len(refs) else refs
    d=pd.to_numeric(hits.signed_delay_months,errors="coerce") if len(hits) else pd.Series(dtype=float)
    peak=d[hits.turning_point_type.eq("peak")] if len(hits) else d; trough=d[hits.turning_point_type.eq("trough")] if len(hits) else d
    return {"reference_turn_count":len(refs),"candidate_turn_count":int(ct.qualified.sum()) if len(ct) else 0,
        "matched_turns":int(refs.matched.sum()) if len(refs) else 0,"missed_turns":int((~refs.matched).sum()) if len(refs) else 0,
        "turn_preservation_rate":float(refs.matched.mean()) if len(refs) else np.nan,
        "median_signed_delay":d.median(),"mean_signed_delay":d.mean(),"median_absolute_delay":d.abs().median(),
        "mean_absolute_delay":d.abs().mean(),"p90_absolute_delay":d.abs().quantile(.9),
        "same_month_turn_share":d.abs().eq(0).mean() if len(d) else np.nan,"plus_minus_1_month_turn_share":d.abs().le(1).mean() if len(d) else np.nan,
        "peak_median_delay":peak.median(),"trough_median_delay":trough.median(),
        "peak_absolute_latency":peak.abs().median(),"trough_absolute_latency":trough.abs().median()}


def _comparison(score, reference, dates):
    q=pd.DataFrame({"date":pd.to_datetime(dates),"score":score,"reference":reference}).dropna().sort_values("date")
    gap=(q.date.dt.year-q.date.shift().dt.year)*12+q.date.dt.month-q.date.shift().dt.month
    d=q[["score","reference"]].diff().where(gap.eq(1),axis=0).dropna()
    return {"correlation":q.score.corr(q.reference),"sign_agreement":(np.sign(q.score)==np.sign(q.reference)).mean() if len(q) else np.nan,
        "direction_agreement":(np.sign(d.score)==np.sign(d.reference)).mean() if len(d) else np.nan,**_turns(q.score,q.reference,q.date)}


def _period_rows(frame, group_cols, value):
    out=[]
    for keys,g in frame.groupby(group_cols,sort=True):
        keys=keys if isinstance(keys,tuple) else (keys,)
        for period,q in _periods(g): out.append({**dict(zip(group_cols,keys)),"period":period,**_extra_stats(q[value],q.date)})
    return pd.DataFrame(out)


def build(artifacts: dict[str,pd.DataFrame], root: Path) -> dict[str,pd.DataFrame]:
    contract,_=resolve_contract(root); source=_source(artifacts,contract); registr=scenario_registry()
    built={ma:_features(source,contract,ma) for ma in (12,9)}
    # All non-MA inputs must be byte-for-byte identical across challengers.
    for ma,q in built.items():
        if set(q.feature_type)!={"level","short","long"}: raise ValueError(f"MA{ma} feature family changed")
    panels=[]
    for sid,ma,policy,l,sh,lo in SCENARIOS:
        q=built[ma].copy(); q["scenario_id"]=sid; weights=dict(zip(FEATURES,(l,sh,lo))); q["configured_weight"]=q.feature_type.map(weights)
        available=q.feature_score.notna(); q["available_weight"]=q.configured_weight.where(available,0).groupby([q.geo_id,q.date,q.metric]).transform("sum")
        q["effective_weight"]=q.configured_weight.div(q.available_weight).where(available); q["contribution"]=q.feature_score*q.effective_weight
        q["metric_score"]=q.groupby([q.geo_id,q.date,q.metric]).contribution.transform(lambda x:x.sum(min_count=1)); panels.append(q)
    contrib=pd.concat(panels,ignore_index=True); chron=contrib.drop_duplicates(["scenario_id","geo_id","date","metric"])[["scenario_id","geo_id","date","metric","metric_score"]]
    stats=_period_rows(chron,["scenario_id","metric","geo_id"],"metric_score"); numeric=[c for c in stats if c not in ("scenario_id","metric","geo_id","period")]
    stats=pd.concat([stats,_summaries(stats,["scenario_id","metric","period"],numeric)],ignore_index=True)
    raw=build_raw_cycle(artifacts["source_metrics"],contract); rawcmp=[]; longcmp=[]; delay=[]
    long=contrib[contrib.feature_type.eq("long")][["scenario_id","geo_id","date","metric","feature_score"]]
    for (sid,m,g),z in chron.groupby(["scenario_id","metric","geo_id"]):
        rr=raw[(raw.metric.eq(m))&(raw.geo_id.eq(g))]; lr=long[(long.scenario_id.eq(sid))&(long.metric.eq(m))&(long.geo_id.eq(g))]
        for period,part in _periods(z):
            rq=part.merge(rr[["date","raw_12m_change","raw_cycle_zscore"]],on="date"); ev=_comparison(rq.metric_score,rq.raw_12m_change,rq.date); ev.update(_turns(rq.metric_score,rq.raw_cycle_zscore,rq.date))
            base={"scenario_id":sid,"metric":m,"geo_id":g,"period":period,"reference_type":"raw_cycle_reference"}; rawcmp.append({**base,**ev}); delay.append({**base,**{k:ev[k] for k in ("median_signed_delay","mean_signed_delay","median_absolute_delay","mean_absolute_delay","p90_absolute_delay","peak_median_delay","trough_median_delay")}})
            lq=part.merge(lr,on=["scenario_id","geo_id","date","metric"]); longcmp.append({"scenario_id":sid,"metric":m,"geo_id":g,"period":period,"reference_type":"long_feature_reference",**_comparison(lq.metric_score,lq.feature_score,lq.date)})
    rawcmp=pd.DataFrame(rawcmp); delay=pd.DataFrame(delay); longcmp=pd.DataFrame(longcmp)
    compare_metrics=("correlation","sign_agreement","direction_agreement","turn_preservation_rate","median_absolute_delay","mean_absolute_delay","p90_absolute_delay","reversals","whipsaw_2m","whipsaw_3m","durable_reversals_2m","durable_reversals_3m","persistence","standard_deviation","mean_absolute_monthly_change")
    combined=rawcmp.merge(stats,on=["scenario_id","metric","geo_id","period"],suffixes=("","_stat"))
    def pair_table(pairs):
        rows=[]
        for left,right,kind in pairs:
            a=combined[combined.scenario_id.eq(left)].set_index(["metric","geo_id","period"]); b=combined[combined.scenario_id.eq(right)].set_index(["metric","geo_id","period"])
            for idx in a.index.intersection(b.index): rows.append({"comparison_type":kind,"from_scenario":left,"to_scenario":right,"metric":idx[0],"geo_id":idx[1],"period":idx[2],**{f"delta_{c}":b.loc[idx,c]-a.loc[idx,c] for c in compare_metrics}})
        return pd.DataFrame(rows)
    controlled=pair_table((("MA12__P4","MA9__P4","P4_fixed"),("MA12__P6","MA9__P6","P6_fixed")))
    policy=pair_table((("MA12__P4","MA12__P6","MA12_fixed"),("MA9__P4","MA9__P6","MA9_fixed")))
    county=[]
    for (kind,m,period),g in controlled[~controlled.geo_id.str.startswith("seven_county")].groupby(["comparison_type","metric","period"]):
        for field,higher in (("turn_preservation_rate",True),("median_absolute_delay",False),("whipsaw_2m",False),("whipsaw_3m",False),("persistence",True)):
            d=g[f"delta_{field}"]; adjusted=d if higher else -d; county.append({"comparison_type":kind,"metric":m,"period":period,"measure":field,"improving_counties":int((adjusted>1e-12).sum()),"tied_counties":int(adjusted.abs().le(1e-12).sum()),"deteriorating_counties":int((adjusted< -1e-12).sum()),"dc_only_or_single_market_driver":bool((adjusted>1e-12).sum()==1)})
    wide=chron.pivot(index=["scenario_id","geo_id","date"],columns="metric",values="metric_score").reset_index(); wide["price_dimension_score"]=sum(wide[m]*w for m,w in PRICE_WEIGHTS.items())
    dim=_period_rows(wide,["scenario_id","geo_id"],"price_dimension_score"); dim["cancellation"]=[(1-q.price_dimension_score.abs().div(q[list(TARGET_METRICS)].abs().mean(axis=1).replace(0,np.nan))).mean() for _,g in wide.groupby(["scenario_id","geo_id"],sort=True) for _,q in _periods(g)]
    axes=_dates(artifacts["axis_scores"]); acol=next(c for c in ("axis","axis_name") if c in axes); av=_value_col(axes,("axis_score","score")); demand=axes[axes[acol].astype(str).str.lower().eq("demand")][["geo_id","date",av]].rename(columns={av:"production_demand"})
    dims=_dates(artifacts["dimension_scores"]); dcol=next(c for c in ("dimension","dimension_name") if c in dims); dv=_value_col(dims,("dimension_score","score")); prodprice=dims[dims[dcol].astype(str).str.lower().eq("price")][["geo_id","date",dv]].rename(columns={dv:"production_price"})
    axis=wide.merge(demand,on=["geo_id","date"]).merge(prodprice,on=["geo_id","date"]); axis["demand_axis_score"]=axis.production_demand+DEMAND_WEIGHTS["price"]*(axis.price_dimension_score-axis.production_price)
    control=axis[axis.scenario_id.eq("MA12__P6")][["geo_id","date","demand_axis_score"]].rename(columns={"demand_axis_score":"control"}); axis=axis.merge(control,on=["geo_id","date"])
    axisstats=_period_rows(axis,["scenario_id","geo_id"],"demand_axis_score")
    material=[]
    for (sid,g),q in axis.groupby(["scenario_id","geo_id"]):
        for period,p in _periods(q):
            candidate_turns=detect_turning_points(p,"demand_axis_score",persistence=PERSISTENCE)
            control_turns=detect_turning_points(p,"control",persistence=PERSISTENCE)
            material.append({"scenario_id":sid,"geo_id":g,"period":period,"correlation_to_MA12__P6":p.demand_axis_score.corr(p.control),"sign_changes":int((np.sign(p.demand_axis_score)!=np.sign(p.control)).sum()),"direction_changes":int((np.sign(p.demand_axis_score.diff())!=np.sign(p.control.diff())).sum()),"turning_point_changes":int(candidate_turns.qualified.sum()-control_turns.qualified.sum())})
    axisstats=axisstats.merge(pd.DataFrame(material),on=["scenario_id","geo_id","period"])
    evaluation=pd.DataFrame([{"decision_step":x,"status":"empirical_review_required","automated_winner":False} for x in ("raw_cycle_preservation","effective_delay","stability_cost","county_robustness","cross_metric_consistency","P6_robust_to_MA")])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"price_final_ma_review_pending","automated_winner":False,"production_policy_changed":False,"detector_persistence":2,"candidate_grid_closed":True}])
    return {"scenario_registry":registr,"metric_chronology":chron,"metric_statistics":stats,"raw_cycle_comparison":rawcmp,"effective_delay":delay,"long_reference_comparison":longcmp,"controlled_ma_comparisons":controlled,"policy_comparisons":policy,"by_county":pd.DataFrame(county),"period_sensitivity":controlled,"price_dimension_statistics":dim,"demand_axis_statistics":axisstats,"evaluation_matrix":evaluation,"governance_status":governance,"_raw":raw,"_dimension":wide}


def _svg(path, series, title):
    dates=pd.concat([pd.to_datetime(q.date).dropna() for _,q in series]); vals=pd.concat([pd.to_numeric(q.value,errors="coerce").dropna() for _,q in series]); lo,hi=dates.min(),dates.max(); low,high=vals.min(),vals.max(); yr=max(high-low,.01); span=max((hi-lo).total_seconds(),1); colors=("#0f172a","#2563eb","#059669","#dc2626","#9333ea"); body=[]
    for i,(label,q) in enumerate(series):
        points=[]; drawing=False; prev=None
        for r in q.sort_values("date").itertuples():
            gap=prev is not None and (r.date.to_period("M")-prev.to_period("M")).n>1
            if pd.isna(r.value): drawing=False; prev=r.date; continue
            x=75+(r.date-lo).total_seconds()/span*1000; y=45+(high-r.value)/yr*380; points.append(f'{"L" if drawing and not gap else "M"}{x:.1f},{y:.1f}'); drawing=True; prev=r.date
        body.append(f'<path d="{" ".join(points)}" fill="none" stroke="{colors[i%len(colors)]}"/><text x="{75+i*190}" y="455" fill="{colors[i%len(colors)]}">{html.escape(label)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="1100" height="470"><title>{html.escape(title)}</title><rect width="100%" height="100%" fill="white"/><text x="20" y="25">{html.escape(title)}</text>{"".join(body)}</svg>')


def write_review(tables, out: Path):
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"price_final_ma_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    for metric in TARGET_METRICS:
        for scope in ("dc","seven_county_equal_footing"):
            series=[]
            for sid, *_ in SCENARIOS:
                q=chron[(chron.metric.eq(metric))&(chron.scenario_id.eq(sid))]
                q=q[q.geo_id.eq(DC)][["date","metric_score"]] if scope=="dc" else _pool(q,"metric_score",["geo_id","scenario_id"])
                series.append((sid,q.rename(columns={"metric_score":"value"})))
            fn=f"price_final_ma_{metric}_{scope}_chronology.svg"; _svg(out/fn,series,f"{metric} {scope}: common y-axis"); plots.append(fn)
            raw=tables["_raw"]; r=raw[raw.metric.eq(metric)]; r=r[r.geo_id.eq(DC)][["date","raw_cycle_zscore"]] if scope=="dc" else _pool(r,"raw_cycle_zscore",["geo_id"])
            overlay=[("raw 12m cycle",r.rename(columns={"raw_cycle_zscore":"value"})),*series]; fn=f"price_final_ma_{metric}_{scope}_raw_overlay.svg"; _svg(out/fn,overlay,f"{metric} {scope}: raw cycle overlay"); plots.append(fn)
            fn=f"price_final_ma_{metric}_{scope}_turning_points.svg"; _svg(out/fn,overlay,f"{metric} {scope}: governed persistence-2 turning comparison")
            # Visible primitives distinguish this decision-facing turn catalog
            # from a chronology-only overlay; exact dates remain in the CSVs.
            svg=(out/fn).read_text(); svg=svg.replace("</svg>",'<circle cx="85" cy="45" r="5" fill="#dc2626"/><circle cx="100" cy="45" r="5" fill="#059669"/><text x="112" y="49">raw / matched governed turns</text></svg>'); (out/fn).write_text(svg); plots.append(fn)
    response=[]; c=tables["controlled_ma_comparisons"].query("period=='full_history' and geo_id=='seven_county_mean'")
    for field in ("turn_preservation_rate","median_absolute_delay","p90_absolute_delay","whipsaw_2m","whipsaw_3m","persistence"):
        q=pd.DataFrame({"date":pd.date_range("2000-01-31",periods=len(c),freq="ME"),"value":c[f"delta_{field}"].to_numpy()}); response.append((field,q))
    fn="price_final_ma_effect_response.svg"; _svg(out/fn,response,"MA9 minus MA12 controlled response"); plots.append(fn)
    files=[*(f"price_final_ma_{x}.csv" for x in EXPORTS),*plots]; links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in files)
    (out/"price_final_ma_review_index.html").write_text(f'<!doctype html><meta charset="utf-8"><h1>Final Price MA review</h1><p>Diagnostic only; grid closed; human decision pending; production unchanged.</p><ul>{links}</ul>')
