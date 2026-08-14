"""Price Phase-2 bounded feature-weight calibration.

This diagnostic only reweights persisted normalized MA12 Price features.  It
never constructs features, normalizes observations, or mutates registries.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd
from regime.diagnostics.capital_markets_ma import detect_turning_points

from regime.diagnostics.price_feature_anatomy import (
    DC, REVIEW_GEOS, TARGET_METRICS, _dates, _metric_col, _periods, _pool,
    _series_stats, _value_col, load_run as _load_phase1, resolve_contract,
)

POLICIES = {
    "P0": (.50, .25, .25), "P1": (.45, .20, .35),
    "P2": (.40, .20, .40), "P3": (.40, .15, .45),
    "P4": (.35, .15, .50), "P5": (.30, .15, .55),
    "P6": (.35, .20, .45),
}
FEATURES = ("level", "short", "long")
PERIOD_NAMES = ("full_history", "2022_plus", "latest_36_months")
ADJACENT = (("P0","P1"),("P1","P2"),("P2","P3"),("P3","P4"),("P4","P5"),("P3","P6"),("P4","P6"))
EXPORTS = (
 "scenario_registry","metric_chronology","feature_contributions","metric_statistics",
 "price_dimension_statistics","demand_axis_statistics","long_reference_comparison",
 "raw_change_comparison","adjacent_comparisons","vs_p0","cross_metric_consistency",
 "by_county","period_sensitivity","evaluation_matrix","governance_status",
)

def load_run(run: Path) -> dict[str,pd.DataFrame]:
    out=_load_phase1(run)
    path=run/"axis_scores.parquet"
    if not path.is_file(): raise FileNotFoundError(f"authoritative run missing required axis_scores.parquet: {run}")
    out["axis_scores"]=pd.read_parquet(path)
    return out

def _extra_stats(s, dates):
    q=pd.DataFrame({"date":dates,"v":pd.to_numeric(s,errors="coerce")}).dropna().sort_values("date"); d=q.v.diff(); sign=np.sign(d).replace(0,np.nan); state=np.sign(q.v).replace(0,np.nan).ffill()
    reversals=sign.ne(sign.shift()) & sign.notna() & sign.shift().notna(); runs=state.ne(state.shift()).cumsum()
    turns=detect_turning_points(q[["date","v"]],"v") if len(q) else pd.DataFrame()
    out={"standard_deviation":q.v.std(),"range":q.v.max()-q.v.min(),"mean_absolute_monthly_change":d.abs().mean(),"reversals":int(reversals.sum()),"zero_crossings":int((state*state.shift()<0).sum()),
      "whipsaw_2m":float((sign.ne(sign.shift(2))&sign.notna()&sign.shift(2).notna()).mean()),"whipsaw_3m":float((sign.ne(sign.shift(3))&sign.notna()&sign.shift(3).notna()).mean()),
      "turning_point_count":int(turns["qualified"].sum()) if "qualified" in turns else 0,"persistence":1-int(reversals.sum())/max(len(d.dropna()),1),"mean_run_length":runs.value_counts().mean(),"time_above_zero":(q.v>0).mean(),"time_below_zero":(q.v<0).mean(),"average_absolute_score":q.v.abs().mean()}
    out["durable_reversals_2m"]=int((reversals & sign.eq(sign.shift(-1))).sum())
    out["durable_reversals_3m"]=int((reversals & sign.eq(sign.shift(-1)) & sign.eq(sign.shift(-2))).sum())
    return out

def _summaries(frame, keys, numeric):
    rows=[]
    for ids,g in frame.groupby(keys,dropna=False,sort=True):
      ids=(ids,) if not isinstance(ids,tuple) else ids
      for agg in ("mean","median","min","max"):
       row=dict(zip(keys,ids)); row["geo_id"]=f"seven_county_{agg}"; row.update(getattr(g[numeric],agg)().to_dict()); rows.append(row)
    return pd.DataFrame(rows)

def _comparison(score, ref, dates):
    q=pd.DataFrame({"date":dates,"score":score,"ref":ref}).dropna().sort_values("date")
    return {"correlation":q.score.corr(q.ref),"sign_agreement":(np.sign(q.score)==np.sign(q.ref)).mean(),
      "direction_agreement":(np.sign(q.score.diff())==np.sign(q.ref.diff())).mean(),
      "turning_point_preservation":np.nan,"mean_turning_point_latency_months":np.nan}

def build(artifacts: dict[str,pd.DataFrame], root: Path) -> dict[str,pd.DataFrame]:
    contract,mreg=resolve_contract(root)
    if set(contract.window_lag_definition.astype(str)) != {"12m", "12m/lag3m", "12m/lag12m"}:
        raise ValueError("Price Phase 2 requires the governed MA12/lag3/lag12 feature family")
    registry=pd.DataFrame([{"policy":p,"scenario_id":f"MA12__{p}","level_weight":w[0],"short_weight":w[1],"long_weight":w[2],"ma_window":"MA12_FIXED"} for p,w in POLICIES.items()])
    if not np.allclose(registry[["level_weight","short_weight","long_weight"]].sum(axis=1),1): raise ValueError("policy weights must sum to one")
    norm=_dates(artifacts["normalized_features"]); score=_value_col(norm,("feature_score","normalized_feature_score","normalized_value"))
    fmap=contract.set_index("feature_key")[["metric","feature_type"]]
    base=norm[norm.feature_key.isin(fmap.index)&norm.geo_id.isin(REVIEW_GEOS)].rename(columns={score:"normalized_feature_score"}).merge(fmap,left_on="feature_key",right_index=True,validate="many_to_one")
    base=base[["geo_id","date","metric","feature_key","feature_type","raw_feature_value","normalized_feature_score"]]
    if base.duplicated(["geo_id","date","metric","feature_type"]).any(): raise ValueError("duplicate persisted normalized Price feature")
    panels=[]
    for p,weights in POLICIES.items():
      q=base.copy(); q["policy"]=p; q["scenario_id"]=f"MA12__{p}"; q["configured_feature_weight"]=q.feature_type.map(dict(zip(FEATURES,weights)))
      available=q.normalized_feature_score.notna(); q["available_weight_sum"]=q.configured_feature_weight.where(available,0).groupby([q.geo_id,q.date,q.metric]).transform("sum")
      q["effective_feature_weight"]=q.configured_feature_weight.div(q.available_weight_sum).where(available)
      q["weighted_contribution"]=q.normalized_feature_score*q.effective_feature_weight
      q["metric_score"]=q.groupby([q.geo_id,q.date,q.metric]).weighted_contribution.transform(lambda x:x.sum(min_count=1))
      panels.append(q)
    contrib=pd.concat(panels,ignore_index=True)
    # Isolation proof: scenario removal must leave one persisted upstream tuple.
    upstream=["raw_feature_value","normalized_feature_score"]
    if (contrib.groupby(["geo_id","date","metric","feature_type"])[upstream].nunique(dropna=False)>1).any().any(): raise ValueError("upstream feature inputs vary by policy")
    chron=contrib.drop_duplicates(["policy","geo_id","date","metric"])[["policy","scenario_id","geo_id","date","metric","metric_score"]]
    stats=[]
    for (p,m,g),z in chron.groupby(["policy","metric","geo_id"]):
      for period,q in _periods(z): stats.append({"policy":p,"metric":m,"geo_id":g,"period":period,**_extra_stats(q.metric_score,q.date)})
    stats=pd.DataFrame(stats); numeric=[c for c in stats if c not in ("policy","metric","geo_id","period")]
    stats=pd.concat([stats,_summaries(stats,["policy","metric","period"],numeric)],ignore_index=True)
    wide=chron.pivot(index=["policy","geo_id","date"],columns="metric",values="metric_score").reset_index()
    wide["price_dimension_score"]=wide[list(TARGET_METRICS)].mean(axis=1,skipna=True)
    dim=[]
    for (p,g),z in wide.groupby(["policy","geo_id"]):
      for period,q in _periods(z):
       gross=q[list(TARGET_METRICS)].abs().mean(axis=1); row={"policy":p,"geo_id":g,"period":period,**_extra_stats(q.price_dimension_score,q.date)}
       row["metric_level_cancellation"]=(1-q.price_dimension_score.abs().div(gross.replace(0,np.nan))).mean(); dim.append(row)
    dim=pd.DataFrame(dim)
    # Propagate the unchanged 17.5% Price delta through persisted P0 Demand.
    axes=_dates(artifacts["axis_scores"]); axiscol=next((c for c in ("axis","axis_name") if c in axes),None); val=_value_col(axes,("axis_score","score"))
    demand=axes[axes[axiscol].astype(str).str.lower().eq("demand") & axes.geo_id.isin(REVIEW_GEOS)][["geo_id","date",val]].rename(columns={val:"p0_demand_axis"})
    p0=wide[wide.policy.eq("P0")][["geo_id","date","price_dimension_score"]].rename(columns={"price_dimension_score":"p0_price"})
    dw=wide.merge(p0,on=["geo_id","date"],validate="many_to_one").merge(demand,on=["geo_id","date"],validate="many_to_one")
    dw["demand_axis_score"]=dw.p0_demand_axis+.175*(dw.price_dimension_score-dw.p0_price)
    dst=[]
    for (p,g),z in dw.groupby(["policy","geo_id"]):
      for period,q in _periods(z):
       row={"policy":p,"geo_id":g,"period":period,**_extra_stats(q.demand_axis_score,q.date)}
       row["direction_changes_vs_p0"]=(np.sign(q.demand_axis_score.diff())!=np.sign(q.p0_demand_axis.diff())).sum(); row["sign_changes_vs_p0"]=(np.sign(q.demand_axis_score)!=np.sign(q.p0_demand_axis)).sum(); dst.append(row)
    dst=pd.DataFrame(dst)
    long=contrib[contrib.feature_type.eq("long")][["policy","geo_id","date","metric","normalized_feature_score"]]
    refs=[]; rawrefs=[]
    raw=_dates(artifacts["source_metrics"]); mc=_metric_col(raw); rv=_value_col(raw,("value","metric_value","raw_value")); keys=contract[["registry_metric_key","metric"]].drop_duplicates()
    raw=raw.rename(columns={mc:"registry_metric_key",rv:"raw_value"}).merge(keys,on="registry_metric_key"); raw=raw.sort_values("date"); raw["raw_annual_change"]=raw.groupby(["geo_id","metric"]).raw_value.pct_change(12,fill_method=None)
    for (p,m,g),z in chron.groupby(["policy","metric","geo_id"]):
      l=long[(long.policy.eq(p))&(long.metric.eq(m))&(long.geo_id.eq(g))]; q=z.merge(l,on=["policy","geo_id","date","metric"])
      refs.append({"policy":p,"metric":m,"geo_id":g,**_comparison(q.metric_score,q.normalized_feature_score,q.date)})
      r=raw[(raw.metric.eq(m))&(raw.geo_id.eq(g))]; q=z.merge(r[["date","raw_annual_change"]],on="date")
      rawrefs.append({"policy":p,"metric":m,"geo_id":g,**_comparison(q.metric_score,q.raw_annual_change,q.date)})
    refs=pd.DataFrame(refs); rawrefs=pd.DataFrame(rawrefs)
    fc=[]
    for (p,m),g in contrib.groupby(["policy","metric"]):
      absmeans=g.groupby("feature_type").weighted_contribution.apply(lambda x:x.abs().mean()); gross=g.groupby(["geo_id","date"]).weighted_contribution.apply(lambda x:x.abs().sum()); net=g.groupby(["geo_id","date"]).weighted_contribution.sum().abs()
      row={"policy":p,"metric":m,"net_to_gross_ratio":net.sum()/gross.sum(),"sign_disagreement_rate":g.groupby(["geo_id","date"]).normalized_feature_score.apply(lambda x:len(set(np.sign(x.dropna())))>1).mean()}
      for ft in FEATURES: row[f"{ft}_mean_absolute_contribution"]=absmeans.get(ft,np.nan); row[f"{ft}_share_of_absolute_contribution"]=absmeans.get(ft,0)/absmeans.sum()
      row["cancellation"]=1-row["net_to_gross_ratio"]; fc.append(row)
    fc=pd.DataFrame(fc)
    basefull=stats[(stats.period.eq("full_history"))&~stats.geo_id.str.startswith("seven_county")]
    comparisons=[]
    for left,right in ADJACENT:
      a=basefull[basefull.policy.eq(left)].set_index(["metric","geo_id"]); b=basefull[basefull.policy.eq(right)].set_index(["metric","geo_id"])
      for idx in a.index.intersection(b.index):
       row={"from_policy":left,"to_policy":right,"metric":idx[0],"geo_id":idx[1]}; row.update({f"delta_{c}":b.loc[idx,c]-a.loc[idx,c] for c in numeric}); comparisons.append(row)
    comparisons=pd.DataFrame(comparisons)
    p0=stats[stats.policy.eq("P0")].set_index(["metric","geo_id","period"]); vp=[]
    for row in stats[~stats.policy.eq("P0")].itertuples(index=False):
      key=(row.metric,row.geo_id,row.period)
      if key in p0.index:
       x={"policy":row.policy,"metric":row.metric,"geo_id":row.geo_id,"period":row.period}; x.update({f"delta_{c}":getattr(row,c)-p0.loc[key,c] for c in numeric}); vp.append(x)
    cross=fc.pivot(index="policy",columns="metric",values=["net_to_gross_ratio","cancellation"]).reset_index(); cross.columns=["_".join(x).strip("_") for x in cross.columns]
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":"price_feature_weight_review_pending","automated_winner":False,"production_policy_changed":False,"ma_window":"MA12_FIXED","long_weight_boundary_unresolved":"empirical_review_required"}])
    evaluation=pd.DataFrame([{"question":i,"status":"empirical_review_required","evidence":"authoritative review tables and plots; no automated winner"} for i in range(1,19)])
    return {"scenario_registry":registry,"metric_chronology":chron,"feature_contributions":contrib,"metric_statistics":stats,"price_dimension_statistics":dim,"demand_axis_statistics":dst,"long_reference_comparison":refs,"raw_change_comparison":rawrefs,"adjacent_comparisons":comparisons,"vs_p0":pd.DataFrame(vp),"cross_metric_consistency":cross,"by_county":basefull,"period_sensitivity":stats,"evaluation_matrix":evaluation,"governance_status":governance,"_dimension_chronology":wide}

def _svg(path, series, title):
    width,height=1100,480; left,right,top,bottom=75,25,45,45; dates=pd.concat([pd.to_datetime(x.date).dropna() for _,x in series if len(x)]); lo,hi=dates.min(),dates.max(); span=max((hi-lo).total_seconds(),1)
    values=pd.concat([pd.to_numeric(x.value,errors="coerce") for _,x in series]); low,high=values.min(),values.max(); pad=max((high-low)*.05,.01); low-=pad; high+=pad
    colors=("#0f172a","#2563eb","#059669","#dc2626","#9333ea","#ea580c","#0891b2"); paths=[]
    for n,(label,q) in enumerate(series):
      cmd=[]; draw=False; prev=None
      for r in q.sort_values("date").itertuples(index=False):
       gap=prev is not None and (r.date.to_period("M")-prev.to_period("M")).n>1
       if pd.isna(r.value): draw=False; prev=r.date; continue
       x=left+(r.date-lo).total_seconds()/span*(width-left-right); y=top+(high-r.value)/(high-low)*(height-top-bottom); cmd.append(f'{"L" if draw and not gap else "M"}{x:.2f},{y:.2f}'); draw=True; prev=r.date
      paths.append(f'<path d="{" ".join(cmd)}" fill="none" stroke="{colors[n%len(colors)]}" stroke-width="1.5"/><text x="{left+120*n}" y="{height-12}" fill="{colors[n%len(colors)]}" font-family="sans-serif">{html.escape(label)}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><title>{html.escape(title)}</title><rect width="100%" height="100%" fill="white"/><text x="20" y="28" font-family="sans-serif" font-size="20">{html.escape(title)}</text><rect x="{left}" y="{top}" width="{width-left-right}" height="{height-top-bottom}" fill="none" stroke="#94a3b8"/>{"".join(paths)}</svg>',encoding="utf-8")

def write_review(tables, out: Path):
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"price_phase2_{name}.csv",index=False)
    plots=[]; chron=tables["metric_chronology"]
    for metric in TARGET_METRICS:
      for scope in ("dc","seven_county_equal_footing"):
       series=[]
       for p in POLICIES:
        q=chron[(chron.metric.eq(metric))&(chron.policy.eq(p))]
        q=(q[q.geo_id.eq(DC)].groupby("date",as_index=False).metric_score.mean() if scope=="dc" else _pool(q,"metric_score",["geo_id","policy"]))
        series.append((p,q.rename(columns={"metric_score":"value"})))
       fn=f"price_phase2_{metric}_{scope}_policies.svg"; _svg(out/fn,series,f"{metric} — {scope}"); plots.append(fn)
       focus=[x for x in series if x[0] in ("P0","P2","P3","P4","P6")]; fn=f"price_phase2_{metric}_{scope}_focus.svg"; _svg(out/fn,focus,f"{metric} finalist neighborhood — {scope}"); plots.append(fn)
      q=tables["feature_contributions"]; series=[]
      for p in ("P0","P2","P3","P4","P6"):
       z=q[(q.metric.eq(metric))&(q.policy.eq(p))&(q.geo_id.eq(DC))].groupby("date",as_index=False).metric_score.mean(); series.append((p,z.rename(columns={"metric_score":"value"})))
      fn=f"price_phase2_{metric}_contribution_decomposition.svg"; _svg(out/fn,series,f"{metric} contribution-derived score"); plots.append(fn)
    d=tables["_dimension_chronology"]
    for scope in ("dc","seven_county_equal_footing"):
      series=[]
      for p in ("P0","P2","P3","P4","P5","P6"):
       q=d[d.policy.eq(p)]; q=(q[q.geo_id.eq(DC)][["date","price_dimension_score"]] if scope=="dc" else _pool(q,"price_dimension_score",["geo_id","policy"])); series.append((p,q.rename(columns={"price_dimension_score":"value"})))
      fn=f"price_phase2_price_dimension_{scope}.svg"; _svg(out/fn,series,f"Price dimension — {scope}"); plots.append(fn)
    # Response-curve files use a proportional numeric policy axis represented as dates.
    for subject in (*TARGET_METRICS,"price_dimension"):
      frame=tables["metric_statistics"] if subject!="price_dimension" else tables["price_dimension_statistics"]
      q=frame[frame.period.eq("full_history")]
      if subject!="price_dimension": q=q[(q.metric.eq(subject))&q.geo_id.eq("seven_county_mean")]
      else: q=q[q.geo_id.isin(REVIEW_GEOS)].groupby("policy",as_index=False).mean(numeric_only=True)
      series=[]
      for metric in ("reversals","whipsaw_2m","persistence","standard_deviation","turning_point_count"):
       if metric in q:
        values=q.set_index("policy").reindex(POLICIES)[metric].values
        series.append((metric,pd.DataFrame({"date":pd.date_range("2000-01-31",periods=len(values),freq="ME"),"value":values})))
      fn=f"price_phase2_{subject}_response_curves.svg"; _svg(out/fn,series,f"{subject} response curves"); plots.append(fn)
    files=[*(f"price_phase2_{n}.csv" for n in EXPORTS),*plots]
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in files)
    (out/"price_phase2_review_index.html").write_text(f"<!doctype html><meta charset=utf-8><title>Price Phase 2</title><h1>Price feature-weight calibration</h1><p>Diagnostic only; human review pending; production unchanged; MA12 fixed.</p><ul>{links}</ul>",encoding="utf-8")
