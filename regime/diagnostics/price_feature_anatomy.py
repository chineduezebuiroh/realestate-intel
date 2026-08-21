"""Registry-driven metric-dimension Phase-1 anatomy diagnostic.

Price was the first consumer.  The implementation is deliberately dimension
parameterized so subsequent calibration campaigns reuse the same chronology,
reconstruction, statistics, rendering, and governance machinery.
"""
from __future__ import annotations

from pathlib import Path
import html
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments.demand_labor_finalist import reversal_events

TARGET_METRICS = ("median_sale_price", "median_ppsf")
REVIEW_GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
DC = REVIEW_GEOS[0]
OUTPUTS = (
 "production_contract","raw_chronology","feature_anatomy","normalized_features",
 "feature_contributions","aligned_metric_scores","feature_statistics","metric_statistics",
 "price_dimension_statistics","raw_feature_relationship","seasonality_noise",
 "monthly_coverage","evaluation_matrix","governance_status",
)

def _bool(s: pd.Series) -> pd.Series:
    return s.astype(str).str.lower().isin(("true","1","yes"))

def resolve_contract(root: Path, dimension: str = "price") -> tuple[pd.DataFrame, pd.DataFrame]:
    """Resolve rather than assume each governed dimension feature contract."""
    fr=pd.read_csv(root/"config/feature_registry.csv")
    mr=pd.read_csv(root/"config/metric_dimension_registry.csv")
    sr=pd.read_csv(root/"config/source_metric_registry.csv")
    nr=pd.read_csv(root/"config/normalization_registry.csv")
    price=mr[_bool(mr.enabled)&~_bool(mr.diagnostic_only)&mr.dimension.eq(dimension)]
    if price.empty or price.canonical_metric_key.duplicated().any():
        raise ValueError(f"governed {dimension} membership is empty or ambiguous")
    rows=[]
    for m in price.itertuples(index=False):
        fs=fr[fr.metric_key.eq(m.metric_key)].copy()
        fs["feature_type"]=fs.feature_type.replace({"short_term_change":"short","long_term_change":"long"})
        if set(fs.feature_type)!={"level","short","long"} or len(fs)!=3:
            raise ValueError(f"{m.canonical_metric_key} does not own exactly Level/Short/Long")
        source=sr[sr.metric_key.eq(m.metric_key)]
        if len(source)!=1: raise ValueError("ambiguous source family")
        for f in fs.itertuples(index=False):
            enabled=nr[_bool(nr.enabled)]
            # Match production precedence: a feature override wins, followed by
            # source family.  Older BPS registry rows use ``census_bps`` while
            # the normalization family is named ``bps``; the governed metric
            # key prefix supplies that registry alias without dimension logic.
            candidates=(
                enabled[enabled.policy_scope.eq("feature_key") & enabled.policy_key.eq(f.feature_key)],
                enabled[enabled.policy_scope.eq("source_family") & enabled.policy_key.eq(source.source_id.iloc[0])],
                enabled[enabled.policy_scope.eq("source_family") & enabled.policy_key.eq(str(m.metric_key).split("_")[0])],
                enabled[enabled.policy_scope.eq("global") & enabled.policy_key.eq("*")],
            )
            policy=next((candidate for candidate in candidates if len(candidate)==1),None)
            if policy is None: raise ValueError(f"ambiguous normalization policy for {f.feature_key}")
            rows.append({"metric":m.canonical_metric_key,"registry_metric_key":m.metric_key,
              "source_family":source.source_id.iloc[0],"feature_type":f.feature_type,"feature_key":f.feature_key,
              "transform":f.transform,"window_lag_definition":f.feature_window,
              "configured_feature_weight":float(f.feature_weight),"normalization_policy":policy.normalization_method.iloc[0],
              "normalization_lookback":int(policy.lookback_periods.iloc[0]),"minimum_periods":int(policy.min_periods.iloc[0]),
              "score_direction":policy.score_direction.iloc[0],"metric_weight":float(m.metric_weight)})
    out=pd.DataFrame(rows).sort_values(["metric","feature_type"]).reset_index(drop=True)
    return out, price

def _read(run: Path, name: str) -> pd.DataFrame:
    path=run/f"{name}.parquet"
    if not path.is_file(): raise FileNotFoundError(f"authoritative run missing required {name}.parquet: {run}")
    return pd.read_parquet(path)

def load_run(run: Path) -> dict[str,pd.DataFrame]:
    if not run.is_dir(): raise FileNotFoundError(f"authoritative production run unavailable; no substitution: {run}")
    return {n:_read(run,n) for n in ("source_metrics","features","normalized_features","metric_scores","aligned_metric_scores","dimension_scores")}

def _dates(frame: pd.DataFrame) -> pd.DataFrame:
    q=frame.copy(); col=next((x for x in ("date","evaluation_date","metric_date") if x in q),None)
    if not col: raise ValueError("artifact lacks date identity")
    q=q.rename(columns={col:"date"}); q.date=pd.to_datetime(q.date,errors="raise")
    return q

def _metric_col(q): return next((c for c in ("canonical_metric_key","metric_key","metric") if c in q),None)
def _value_col(q, names):
    c=next((c for c in names if c in q),None)
    if not c: raise ValueError(f"artifact lacks value column from {names}")
    return c

def _calendar(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    chunks=[]
    for ids,g in frame.groupby(keys,sort=True):
        idx=pd.date_range(g.date.min(),g.date.max(),freq="ME")
        z=g.set_index("date").reindex(idx).rename_axis("date").reset_index()
        ids=(ids,) if not isinstance(ids,tuple) else ids
        for k,v in zip(keys,ids): z[k]=v
        chunks.append(z)
    return pd.concat(chunks,ignore_index=True) if chunks else frame

def _series_stats(s: pd.Series, dates: pd.Series) -> dict[str,float]:
    q=pd.DataFrame({"date":dates,"v":pd.to_numeric(s,errors="coerce")}).dropna().sort_values("date")
    if q.empty: return {k:np.nan for k in ("standard_deviation","range","mean_absolute_monthly_change","reversals","zero_crossings","turning_point_count","persistence","mean_run_length","time_above_zero","time_below_zero","average_absolute_score","whipsaw_2m","whipsaw_3m")}
    d=q.v.diff(); signs=np.sign(q.v).replace(0,np.nan).ffill()
    events=reversal_events(q.assign(geo_id="series",move=d),"move",(1,2,3))
    one=events[events.horizon_months.eq(1)]; reversals=int(one.reversed.sum()); zero=int((signs*signs.shift()<0).sum())
    runs=(signs.ne(signs.shift())).cumsum(); turns=detect_turning_points(q[["date","v"]],"v")
    return {"standard_deviation":q.v.std(),"range":q.v.max()-q.v.min(),"mean_absolute_monthly_change":d.abs().mean(),
      "reversals":reversals,"zero_crossings":zero,"turning_point_count":int(turns.qualified.sum()) if len(turns) else 0,
      "persistence":1-reversals/max(len(d.dropna()),1),"mean_run_length":runs.value_counts().mean(),
      "time_above_zero":(q.v>0).mean(),"time_below_zero":(q.v<0).mean(),"average_absolute_score":q.v.abs().mean(),
      "whipsaw_2m":events.loc[events.horizon_months.eq(2),"reversed"].mean(),
      "whipsaw_3m":events.loc[events.horizon_months.eq(3),"reversed"].mean()}

def _periods(g):
    end=g.date.max()
    return (("full_history",g),("2022_plus",g[g.date.ge("2022-01-01")]),("latest_36_months",g[g.date.ge(end-pd.DateOffset(months=35))]))

def build(artifacts: dict[str,pd.DataFrame], root: Path, dimension: str = "price",
          native_geographies: tuple[str, ...] | None = None,
          evaluation_geographies: tuple[str, ...] | None = None,
          contract_override: tuple[pd.DataFrame, pd.DataFrame] | None = None) -> dict[str,pd.DataFrame]:
    """Build anatomy at explicit native and evaluation geography boundaries.

    Defaults preserve the original seven-county contract. National-source
    dimensions must opt in explicitly, so county safeguards remain unchanged.
    """
    native_geographies = native_geographies or REVIEW_GEOS
    evaluation_geographies = evaluation_geographies or REVIEW_GEOS
    contract,mreg=contract_override or resolve_contract(root, dimension); target_metrics=tuple(sorted(contract.metric.unique())); fmap=contract.set_index("feature_key")[["metric","feature_type","configured_feature_weight"]]
    raw=_dates(artifacts["source_metrics"]); mc=_metric_col(raw); val=_value_col(raw,("value","metric_value","raw_value"))
    # Persisted source artifacts may identify the same governed observation by
    # either its registry key or its canonical metric.  Resolve both forms at
    # the artifact boundary and immediately expose only diagnostic ``metric``.
    registry_identities=contract[["registry_metric_key","metric"]].drop_duplicates()
    canonical_identities=(contract[["metric"]]
        .assign(registry_metric_key=lambda q:q["metric"])
        [["registry_metric_key","metric"]])
    identities=pd.concat([registry_identities,canonical_identities],ignore_index=True).drop_duplicates()
    ambiguous=identities.groupby("registry_metric_key").metric.nunique()
    ambiguous=ambiguous[ambiguous.gt(1)]
    if not ambiguous.empty:
        raise ValueError(f"ambiguous raw source metric identities: {sorted(ambiguous.index)}")
    raw=(raw.rename(columns={mc:"registry_metric_key",val:"raw_value"})
        .merge(identities,on="registry_metric_key",how="inner",validate="many_to_one"))
    raw=raw[raw.geo_id.isin(native_geographies)][["geo_id","date","metric","raw_value"]]
    missing=set(target_metrics).difference(raw.metric.unique())
    if missing: raise ValueError(f"governed raw metrics missing after identity resolution: {sorted(missing)}")
    if raw.duplicated(["geo_id","date","metric"]).any(): raise ValueError("duplicate raw monthly chronology")
    raw=_calendar(raw,["geo_id","metric"]).sort_values(["metric","geo_id","date"])
    features=_dates(artifacts["features"]); features=features[features.feature_key.isin(fmap.index)&features.geo_id.isin(native_geographies)]
    features=features.merge(fmap,left_on="feature_key",right_index=True,validate="many_to_one")
    norm=_dates(artifacts["normalized_features"]); score=_value_col(norm,("feature_score","normalized_feature_score","normalized_value"))
    norm=norm[norm.feature_key.isin(fmap.index)&norm.geo_id.isin(native_geographies)].rename(columns={score:"normalized_feature_score"})
    norm=norm.merge(fmap,left_on="feature_key",right_index=True,validate="many_to_one")
    lineage=set(norm.feature_key.unique())
    if lineage!=set(contract.feature_key): raise ValueError("registry and persisted feature lineage disagree")
    anatomy=features[["geo_id","date","metric","feature_type","feature_key","raw_feature_value"]].merge(raw,on=["geo_id","date","metric"],how="left",validate="many_to_one")
    panel=norm[["geo_id","date","metric","feature_type","feature_key","normalized_feature_score","configured_feature_weight"]].copy()
    available=panel.normalized_feature_score.notna(); totals=panel.configured_feature_weight.where(available,0).groupby([panel.geo_id,panel.date,panel.metric]).transform("sum")
    panel["effective_feature_weight"]=panel.configured_feature_weight.div(totals).where(available)
    panel["weighted_feature_contribution"]=panel.normalized_feature_score*panel.effective_feature_weight
    # Feature arithmetic lives at the native scoring identity.  It must be
    # reconciled to metric_scores, not to the later as-of alignment surface.
    metric=_dates(artifacts["metric_scores"]); mc=_metric_col(metric); sv=_value_col(metric,("metric_score","score"))
    metric=metric.rename(columns={mc:"metric",sv:"production_metric_score"}); metric=metric[metric.metric.isin(target_metrics)&metric.geo_id.isin(native_geographies)]
    panel=panel.merge(metric[["geo_id","date","metric","production_metric_score"]],on=["geo_id","date","metric"],how="left",validate="many_to_one")
    panel["native_metric_date"]=panel["date"]
    panel["native_metric_score"]=panel["production_metric_score"]
    replay=panel.groupby(["geo_id","date","metric"]).weighted_feature_contribution.sum(min_count=1)
    actual=metric.set_index(["geo_id","date","metric"]).production_metric_score.reindex(replay.index)
    if not np.allclose(replay,actual,equal_nan=True,atol=1e-12): raise ValueError("feature contributions do not reconstruct production metric scores")
    # Dimensions consume the common month-end, backward-as-of metric surface.
    # Keep its evaluation and native metric identities distinct in evidence.
    aligned=artifacts["aligned_metric_scores"].copy(); mc=_metric_col(aligned); sv=_value_col(aligned,("metric_score","aligned_metric_score","score"))
    eval_col="evaluation_date" if "evaluation_date" in aligned else "date"
    aligned=aligned.rename(columns={mc:"metric",sv:"aligned_metric_score",eval_col:"date"})
    aligned["date"]=pd.to_datetime(aligned["date"],errors="raise")
    if "metric_date" not in aligned: aligned["metric_date"]=aligned["date"]
    aligned["metric_date"]=pd.to_datetime(aligned["metric_date"],errors="raise")
    aligned=aligned[aligned.metric.isin(target_metrics)&aligned.geo_id.isin(evaluation_geographies)]
    dimension_score=f"{dimension}_dimension_score"
    dims=_dates(artifacts["dimension_scores"]); dims=dims[dims.dimension.eq(dimension)&dims.geo_id.isin(evaluation_geographies)].rename(columns={"dimension_score":dimension_score})
    wide=aligned.pivot(index=["geo_id","date"],columns="metric",values="aligned_metric_score")
    weights=mreg.set_index("canonical_metric_key").metric_weight
    valid=wide.notna(); denom=valid.mul(weights).sum(axis=1); reconstructed=wide.mul(weights).sum(axis=1,min_count=1).div(denom)
    observed=dims.set_index(["geo_id","date"])[dimension_score].reindex(reconstructed.index)
    if not np.allclose(reconstructed,observed,equal_nan=True,atol=1e-12): raise ValueError(f"metric weights do not reconstruct {dimension} dimension")
    statistics=[]
    for (metric_name,ft,geo),g in panel.groupby(["metric","feature_type","geo_id"]):
      for period,q in _periods(g): statistics.append({"metric":metric_name,"feature_type":ft,"geo_id":geo,"period":period,**_series_stats(q.normalized_feature_score,q.date)})
    fs=pd.DataFrame(statistics); summaries=[]
    numeric=[c for c in fs if c not in ("metric","feature_type","geo_id","period")]
    for keys,g in fs.groupby(["metric","feature_type","period"]):
      for agg in ("mean","median","min","max"):
       row={"metric":keys[0],"feature_type":keys[1],"period":keys[2],"geo_id":f"seven_county_{agg}"}
       row.update(getattr(g[numeric],agg)().to_dict()); summaries.append(row)
    fs=pd.concat([fs,pd.DataFrame(summaries)],ignore_index=True)
    ms=[]
    for (m,geo),g in metric.groupby(["metric","geo_id"]):
      for period,q in _periods(g): ms.append({"metric":m,"geo_id":geo,"period":period,**_series_stats(q.production_metric_score,q.date)})
    ds=[]
    joined=wide.reset_index().merge(dims[["geo_id","date",dimension_score]],on=["geo_id","date"])
    for geo,g in joined.groupby("geo_id"):
      for period,q in _periods(g):
       st=_series_stats(q[dimension_score],q.date); gross=q[list(target_metrics)].abs().mul(weights).sum(axis=1); net=q[dimension_score].abs()
       ds.append({"geo_id":geo,"period":period,**st,"mean_cancellation_ratio":(1-net.div(gross.replace(0,np.nan))).mean(),
        "both_metrics_positive_rate":q[list(target_metrics)].gt(0).all(axis=1).mean()})
    rel=[]
    merged=anatomy.merge(panel[["geo_id","date","metric","feature_type","normalized_feature_score"]],on=["geo_id","date","metric","feature_type"])
    for (m,ft,geo),g in merged.groupby(["metric","feature_type","geo_id"]):
      q=g.sort_values("date"); feat=q.raw_feature_value; r=q.raw_value
      raw_turns=detect_turning_points(pd.DataFrame({"date":q.date,"v":r}).dropna(),"v"); feature_turns=detect_turning_points(pd.DataFrame({"date":q.date,"v":feat}).dropna(),"v")
      matched=match_turning_points(raw_turns,feature_turns) if len(raw_turns) and len(feature_turns) else pd.DataFrame()
      lag_col=next((c for c in ("lag_months","signed_lag_months","absolute_lag_months") if c in matched),None)
      rel.append({"metric":m,"feature_type":ft,"geo_id":geo,"correlation_to_raw":feat.corr(r),"correlation_to_raw_monthly_change":feat.corr(r.diff()),
       "correlation_to_raw_12_month_change":feat.corr(r.pct_change(12,fill_method=None)),"direction_agreement":(np.sign(feat)==np.sign(r.diff())).mean(),
       "feature_turning_points":_series_stats(feat,q.date)["turning_point_count"],"raw_turning_points":_series_stats(r,q.date)["turning_point_count"],
       "matched_turning_points":len(matched),"median_turning_point_lag_months":pd.to_numeric(matched[lag_col],errors="coerce").median() if lag_col else np.nan})
    season=[]
    for (m,ft,geo),g in panel.groupby(["metric","feature_type","geo_id"]):
      q=g.sort_values("date"); stats=_series_stats(q.normalized_feature_score,q.date); bymonth=q.assign(month=q.date.dt.month).groupby("month").normalized_feature_score.mean()
      season.append({"metric":m,"feature_type":ft,"geo_id":geo,"reversal_frequency":1-stats["persistence"],"whipsaw_2m_rate":stats["whipsaw_2m"],"whipsaw_3m_rate":stats["whipsaw_3m"],
       "calendar_month_effect_range":bymonth.max()-bymonth.min(),"strongest_calendar_month":bymonth.abs().idxmax() if len(bymonth) else np.nan})
    coverage=raw.groupby(["metric","date"]).raw_value.agg(available_count=lambda x:x.notna().sum(),total_count="size").reset_index(); coverage["coverage_rate"]=coverage.available_count/len(native_geographies)
    evaluation=pd.DataFrame([{"question":i,"status":"empirical_review_required","evidence":"review linked plots and tables; no automated recommendation"} for i in range(1,13)])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged","human_decision":f"{dimension}_feature_anatomy_review_pending","automated_winner":False,"production_policy_changed":False}])
    return {"production_contract":contract,"raw_chronology":raw,"feature_anatomy":anatomy,"normalized_features":panel[["geo_id","date","metric","feature_type","feature_key","normalized_feature_score"]],
      "feature_contributions":panel,"feature_statistics":fs,"metric_statistics":pd.DataFrame(ms),f"{dimension}_dimension_statistics":pd.DataFrame(ds),
      "raw_feature_relationship":pd.DataFrame(rel),"seasonality_noise":pd.DataFrame(season),"monthly_coverage":coverage,"evaluation_matrix":evaluation,"governance_status":governance,
      "aligned_metric_scores":aligned[["geo_id","date","metric","metric_date","aligned_metric_score"]].rename(columns={"date":"evaluation_date","metric_date":"native_metric_date"}),
      "_dimension":joined,"_aligned_metrics":aligned[["geo_id","date","metric","metric_date","aligned_metric_score"]],
      "_metadata":{"dimension":dimension,"target_metrics":target_metrics,"dimension_score":dimension_score}}

def _pool(frame,value,groups):
    q=frame.copy(); q[value]=q.groupby(groups)[value].transform(lambda x:(x-x.mean())/x.std() if x.std() else np.nan)
    return q.groupby(["date"]+[g for g in groups if g!="geo_id"],as_index=False)[value].mean()

def _plot(path: Path, panels, title, ylim=None):
    # Native SVG keeps the diagnostic dependency-light while producing real
    # plotted paths.  A new M command after each missing observation breaks the
    # line; epoch-time scaling makes the horizontal axis calendar-proportional.
    width=1100; panel_h=190; left=95; right=25; top=55; inner=width-left-right
    # pandas is deprecating dtype inference when concat receives empty/all-NA
    # entries.  Such entries cannot affect the plotted extent, so exclude them.
    date_parts=[pd.to_datetime(s.date).dropna() for _,s in panels]
    date_parts=[s for s in date_parts if not s.empty]
    if not date_parts: raise ValueError("plot has no finite calendar dates")
    all_dates=pd.concat(date_parts,ignore_index=True)
    lo_date,hi_date=all_dates.min(),all_dates.max(); span=max((hi_date-lo_date).total_seconds(),1)
    body=[]
    for i,(label,series) in enumerate(panels):
      ytop=top+i*panel_h; q=series[["date","value"]].sort_values("date").copy(); q.date=pd.to_datetime(q.date); q.value=pd.to_numeric(q.value,errors="coerce")
      low,high=ylim if ylim else (q.value.min(),q.value.max());
      if not np.isfinite(low) or not np.isfinite(high): low,high=-1,1
      if high==low: low,high=low-1,high+1
      body.append(f'<rect x="{left}" y="{ytop}" width="{inner}" height="{panel_h-35}" fill="none" stroke="#cbd5e1"/><text x="12" y="{ytop+20}" font-family="sans-serif" font-size="14">{html.escape(label)}</text>')
      if low<=0<=high:
       zy=ytop+(high/(high-low))*(panel_h-35); body.append(f'<line x1="{left}" x2="{width-right}" y1="{zy:.2f}" y2="{zy:.2f}" stroke="#64748b" stroke-width="0.7"/>')
      commands=[]; drawing=False; previous=None
      for row in q.itertuples(index=False):
       # A calendar gap also breaks the path even when sparse input omitted the
       # missing row before this renderer.
       gap=previous is not None and (row.date.to_period("M")-previous.to_period("M")).n>1
       if pd.isna(row.value): drawing=False; previous=row.date; continue
       x=left+((row.date-lo_date).total_seconds()/span)*inner; y=ytop+(high-row.value)/(high-low)*(panel_h-35)
       commands.append(f'{"L" if drawing and not gap else "M"}{x:.2f},{y:.2f}'); drawing=True; previous=row.date
      body.append(f'<path d="{" ".join(commands)}" fill="none" stroke="#2563eb" stroke-width="1.5"/>')
    height=top+len(panels)*panel_h
    svg=f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" role="img"><title>{html.escape(title)}</title><rect width="100%" height="100%" fill="white"/><text x="20" y="30" font-family="sans-serif" font-size="20">{html.escape(title)}</text>{"".join(body)}</svg>'
    path.write_text(svg,encoding="utf-8")

def write_review(tables: dict[str,pd.DataFrame], out: Path, dimension: str = "price") -> None:
    metadata=tables.get("_metadata", {"dimension":dimension,"target_metrics":TARGET_METRICS,"dimension_score":"price_dimension_score"})
    dimension=metadata["dimension"]; target_metrics=metadata["target_metrics"]; dimension_score=metadata["dimension_score"]
    prefix=f"{dimension}_phase1"
    outputs=tuple(f"{dimension}_dimension_statistics" if n=="price_dimension_statistics" else n for n in OUTPUTS)
    out.mkdir(parents=True,exist_ok=True)
    for name in outputs: tables[name].to_csv(out/f"{prefix}_{name}.csv",index=False)
    plots=[]
    for metric in target_metrics:
      for scope in ("dc","seven_county_standardized"):
       a=tables["feature_anatomy"].query("metric==@metric"); n=tables["normalized_features"].query("metric==@metric"); c=tables["feature_contributions"].query("metric==@metric")
       def series(frame,value,ft=None):
        q=frame if ft is None else frame[frame.feature_type.eq(ft)]
        if scope=="dc": q=q[q.geo_id.eq(DC)].groupby("date",as_index=False)[value].mean()
        else: q=_pool(q,value,["geo_id"])
        return q.rename(columns={value:"value"})
       raw=tables["raw_chronology"].query("metric==@metric")
       rawseries=(raw[raw.geo_id.eq(DC)].rename(columns={"raw_value":"value"}) if scope=="dc" else _pool(raw,"raw_value",["geo_id"]).rename(columns={"raw_value":"value"}))
       families=[("raw_features",[("Raw",rawseries)]+[(x.title(),series(a,"raw_feature_value",x)) for x in ("level","short","long")],None),
        ("normalized",[(x.title(),series(n,"normalized_feature_score",x)) for x in ("level","short","long")],(-1,1)),
        ("contributions",[(x.title(),series(c,"weighted_feature_contribution",x)) for x in ("level","short","long")]+[("Metric",series(c,"production_metric_score"))],(-1,1))]
       for kind,panels,ylim in families:
        fn=f"{prefix}_{metric}_{scope}_{kind}.svg"; _plot(out/fn,panels,f"{metric} — {scope.replace('_',' ')} — {kind}",ylim); plots.append(fn)
    d=tables["_dimension"]
    for scope in ("dc","seven_county_standardized"):
      panels=[]
      for col in (*target_metrics,dimension_score):
       q=d[["geo_id","date",col]].dropna()
       q=(q[q.geo_id.eq(DC)].rename(columns={col:"value"}) if scope=="dc" else _pool(q,col,["geo_id"]).rename(columns={col:"value"}))
       panels.append((col,q))
      fn=f"{prefix}_{dimension}_family_{scope}_comparison.svg"; _plot(out/fn,panels,f"{dimension.title()} family — {scope.replace('_',' ')}",(-1,1)); plots.append(fn)
    links=[f'<li><a href="{html.escape(p)}">{html.escape(p)}</a></li>' for p in [*(f"{prefix}_{n}.csv" for n in outputs),*plots]]
    (out/f"{prefix}_review_index.html").write_text(f"<!doctype html><meta charset=utf-8><title>{dimension.title()} Phase 1</title><h1>{dimension.title()} Feature Anatomy — Phase 1</h1><p>Diagnostic only; human review pending; production unchanged.</p><ul>"+"".join(links)+"</ul>",encoding="utf-8")
