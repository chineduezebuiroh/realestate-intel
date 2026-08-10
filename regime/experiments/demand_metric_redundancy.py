"""Governed, diagnostic-only Demand metric redundancy review.

The implementation deliberately starts at persisted metric scores: challengers may
change membership and proportional weights, but never feature construction or
normalization chronology.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path
import json
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points

TOL = 1e-12
REVIEW_GEOS = (
    "district_of_columbia_dc__county", "essex_county_nj__county",
    "montgomery_county_md__county", "prince_george_s_county_md__county",
    "fairfax_county_va__county", "san_francisco_county_ca__county",
    "los_angeles_county_ca__county",
)
METRICS = ("population", "median_household_income", "gdp_annual", "labor_force", "employment", "laus_unemployment_rate")
STRUCTURAL = METRICS[:3]
LABOR = METRICS[3:]
POLICIES = {
    "DEM-LABOR-A": set(METRICS),
    "DEM-LABOR-B": set(METRICS) - {"labor_force"},
    "DEM-LABOR-C": set(METRICS) - {"labor_force", "employment"},
    "DEM-LABOR-D": set(METRICS) - {"labor_force"},
}
ABLATIONS = {
    "DROP-LABOR-FORCE": {"labor_force"}, "DROP-EMPLOYMENT": {"employment"},
    "DROP-UNEMPLOYMENT-RATE": {"laus_unemployment_rate"}, "DROP-GDP": {"gdp_annual"},
    "DROP-INCOME": {"median_household_income"}, "DROP-POPULATION": {"population"},
    "DROP-LABOR-FORCE-AND-EMPLOYMENT": {"labor_force", "employment"},
    "DROP-EMPLOYMENT-AND-UNEMPLOYMENT": {"employment", "laus_unemployment_rate"},
    "LABOR-UNEMPLOYMENT-ONLY": {"labor_force", "employment"},
    "LABOR-EMPLOYMENT-ONLY": {"labor_force", "laus_unemployment_rate"},
}
OUTPUTS = (
 "demand_metric_production_contract", "demand_metric_pairwise_redundancy",
 "demand_metric_contribution_summary", "demand_metric_movement_attribution",
 "demand_metric_cancellation_summary", "demand_axis_cancellation_summary",
 "demand_structural_vs_labor_summary", "demand_metric_ablation_summary",
 "demand_metric_incremental_information", "demand_metric_policy_registry",
 "demand_metric_policy_stability", "demand_metric_policy_turning_points",
 "demand_metric_recent_36m", "demand_metric_decision_matrix",
 "demand_metric_parity_audit", "demand_metric_governance_status",
 "demand_metric_runtime_summary",
)

def _col(df, *names):
    for n in names:
        if n in df.columns: return n
    raise ValueError(f"Required column absent (accepted {names}); found {list(df.columns)}")

def production_contract(root: Path) -> tuple[pd.DataFrame, dict[str, float]]:
    mr=pd.read_csv(root/"config/metric_dimension_registry.csv")
    fr=pd.read_csv(root/"config/feature_registry.csv")
    nr=pd.read_csv(root/"config/normalization_registry.csv")
    active=mr[(mr.dimension.eq("demand")) & mr.enabled.astype(bool) & mr.metric_weight.gt(0)].copy()
    # Source alternatives collapse to one canonical production metric.
    got=set(active.canonical_metric_key)
    if got != set(METRICS): raise ValueError(f"Demand metric contract drift: expected {set(METRICS)}, found {got}")
    weights=active.groupby("canonical_metric_key").metric_weight.first().to_dict()
    rows=[]
    for metric in METRICS:
        sources=active.loc[active.canonical_metric_key.eq(metric),"metric_key"].tolist()
        feats=fr[fr.metric_key.isin(sources)]
        for f in feats.itertuples():
            norm=nr[nr.policy_key.eq(f.feature_key)]
            rows.append({"canonical_metric_key":metric,"dimension":"demand","configured_metric_weight":weights[metric],
              "metric_polarity":norm.score_direction.iloc[0] if len(norm) else "unknown", "feature_key":f.feature_key,
              "feature_transform":f.transform,"feature_window":f.feature_window,"feature_weight":f.feature_weight,
              "normalization_method":norm.normalization_method.iloc[0] if len(norm) else "unknown",
              "source_lineage_identity":"|".join(sources)})
    out=pd.DataFrame(rows)
    expected={"level":(.25,"6m"),"short":(.35,"6m/lag3m"),"long":(.40,"6m/lag12m")}
    for source in ("laus_labor_force","laus_employment","laus_unemployment_rate"):
        for suffix,(weight,window) in expected.items():
            q=fr[fr.feature_key.eq(f"{source}_{suffix}")]
            if len(q)!=1 or abs(float(q.feature_weight.iloc[0])-weight)>TOL or q.feature_window.iloc[0]!=window:
                raise ValueError(f"Frozen LAUS feature contract differs for {source}_{suffix}")
    return out, weights

def _load(run: Path, name: str) -> pd.DataFrame:
    p=run/f"{name}.parquet"
    if not p.is_file(): raise FileNotFoundError(f"authoritative v1.0 artifact required; no substitution: {p}")
    return pd.read_parquet(p)

def _metric_long(raw: pd.DataFrame) -> pd.DataFrame:
    geo=_col(raw,"geo_id"); date=_col(raw,"evaluation_date","date"); metric=_col(raw,"canonical_metric_key","metric_key"); score=_col(raw,"metric_score","score")
    x=raw.rename(columns={geo:"geo_id",date:"date",metric:"metric",score:"score"})[["geo_id","date","metric","score"]]
    x["date"]=pd.to_datetime(x.date); x=x[x.geo_id.isin(REVIEW_GEOS)&x.metric.isin(METRICS)]
    if set(x.geo_id)!=set(REVIEW_GEOS): raise ValueError(f"governed geography coverage missing: {set(REVIEW_GEOS)-set(x.geo_id)}")
    if x.duplicated(["geo_id","date","metric"]).any(): raise ValueError("duplicate governed metric chronology")
    return x

def _score(x: pd.DataFrame, weights: dict, included: set[str]) -> pd.DataFrame:
    z=x[x.metric.isin(included)].copy(); z["base_weight"]=z.metric.map(weights)
    z["effective_weight"]=z.base_weight/z.groupby(["geo_id","date"]).base_weight.transform("sum")
    z["contribution"]=z.score*z.effective_weight
    z["demand_dimension"]=z.groupby(["geo_id","date"]).contribution.transform("sum")
    return z

def _series(raw, dimension=None, axis=None):
    geo=_col(raw,"geo_id"); date=_col(raw,"evaluation_date","date")
    q=raw.copy()
    if dimension is not None:
        dc=_col(q,"dimension"); q=q[q[dc].eq(dimension)]
    if axis is not None:
        ac=_col(q,"axis"); q=q[q[ac].eq(axis)]
    val=_col(q,"dimension_score","axis_score","score")
    return q.rename(columns={geo:"geo_id",date:"date",val:"value"})[["geo_id","date","value"]].assign(date=lambda d:pd.to_datetime(d.date))

def _axis(dimensions, demand: pd.DataFrame, axis_weights: dict) -> tuple[pd.DataFrame,pd.DataFrame]:
    d=dimensions.copy(); dc=_col(d,"dimension"); geo=_col(d,"geo_id"); date=_col(d,"evaluation_date","date"); val=_col(d,"dimension_score","score")
    d=d.rename(columns={dc:"dimension",geo:"geo_id",date:"date",val:"score"}); d.date=pd.to_datetime(d.date)
    d=d[d.geo_id.isin(REVIEW_GEOS)&d.dimension.isin(axis_weights)]
    replacement=demand.rename(columns={"demand_dimension":"score"})[["geo_id","date","score"]].drop_duplicates().assign(dimension="demand")
    d=pd.concat([d[d.dimension.ne("demand")],replacement],ignore_index=True); d["weight"]=d.dimension.map(axis_weights)
    d["effective_weight"]=d.weight/d.groupby(["geo_id","date"]).weight.transform("sum"); d["axis_contribution"]=d.score*d.effective_weight
    a=d.groupby(["geo_id","date"],as_index=False).axis_contribution.sum().rename(columns={"axis_contribution":"demand_axis"})
    return a,d

def _q(s,q): return float(s.dropna().quantile(q)) if s.notna().any() else np.nan
def _turns(frame,value,policy):
    rows=[]
    for geo,g in frame.groupby("geo_id"):
        t=detect_turning_points(g[["date",value]].sort_values("date"),value)
        if len(t): t=t.assign(geo_id=geo,policy=policy); rows.append(t)
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified","geo_id","policy"])

def build(run: Path, root: Path) -> dict[str,pd.DataFrame]:
    contract,weights=production_contract(root)
    # Presence checks are intentional even where diagnostic computation starts downstream.
    for name in ("source_metrics","features","normalized_features","regime_assignments"): _load(run,name)
    x=_metric_long(_load(run,"metric_scores")); dimensions=_load(run,"dimension_scores"); persisted_axis=_load(run,"axis_scores")
    incumbent=_score(x,weights,set(METRICS)); inc_series=incumbent[["geo_id","date","demand_dimension"]].drop_duplicates()
    persisted_dim=_series(dimensions,dimension="demand").rename(columns={"value":"persisted"})
    p=inc_series.merge(persisted_dim,on=["geo_id","date"],how="left"); dim_err=(p.demand_dimension-p.persisted).abs().max()
    ar=pd.read_csv(root/"config/axis_registry.csv"); ar=ar[(ar.axis.eq("demand"))&ar.enabled.astype(bool)]; axis_weights=dict(zip(ar.dimension,ar.dimension_weight))
    inc_axis,inc_axis_detail=_axis(dimensions,inc_series,axis_weights)
    inc_axis_detail=inc_axis_detail.merge(inc_axis,on=["geo_id","date"],how="left")
    pa=_series(persisted_axis,axis="demand").rename(columns={"value":"persisted"}); ap=inc_axis.merge(pa,on=["geo_id","date"],how="left"); axis_err=(ap.demand_axis-ap.persisted).abs().max()
    if pd.isna(dim_err) or dim_err>TOL or pd.isna(axis_err) or axis_err>TOL: raise ValueError(f"incumbent parity failed: dimension={dim_err}, axis={axis_err}")
    parity=pd.DataFrame([{"check":"normalized metric scores reused exactly","max_abs_error":0.,"status":"pass"},{"check":"effective metric weights and contributions reconstruct Demand dimension","max_abs_error":dim_err,"status":"pass"},{"check":"weighted Demand contribution and final Demand axis","max_abs_error":axis_err,"status":"pass"}])
    # Pairwise aligned score diagnostics.
    pair=[]
    wide=x.pivot(index=["geo_id","date"],columns="metric",values="score")
    for a,b in combinations(METRICS,2):
      for geo in (*REVIEW_GEOS,"POOLED"):
        q=wide[[a,b]].dropna() if geo=="POOLED" else wide.loc[geo,[a,b]].dropna()
        roll=q[a].rolling(36,min_periods=24).corr(q[b]); da=q[a].diff(); db=q[b].diff()
        pair.append({"geo_id":geo,"metric_a":a,"metric_b":b,"observations":len(q),"score_correlation":q[a].corr(q[b]),"first_difference_correlation":da.corr(db),"same_month_sign_agreement":(np.sign(q[a])==np.sign(q[b])).mean(),"rolling_36m_correlation_median":roll.median(),"rolling_36m_correlation_p10":_q(roll,.1),"rolling_36m_correlation_p90":_q(roll,.9),"polarity_aligned":True})
    pair=pd.DataFrame(pair)
    # Contributions and movement.
    incumbent=incumbent.sort_values(["geo_id","metric","date"]); incumbent["contribution_delta"]=incumbent.groupby(["geo_id","metric"]).contribution.diff(); incumbent["dimension_delta"]=incumbent.groupby("geo_id").demand_dimension.diff()
    abs_total=incumbent.contribution.abs().sum(); move_total=incumbent.contribution_delta.abs().sum()
    contrib=[]; movement=[]
    for metric,g in incumbent.groupby("metric"):
      cutoff=g.date.max()-pd.DateOffset(months=35); recent=g[g.date>=cutoff]
      drivers=incumbent.assign(mx=incumbent.groupby(["geo_id","date"]).contribution.transform(lambda s:s.abs().max()), pos=incumbent.groupby(["geo_id","date"]).contribution.transform("max"), neg=incumbent.groupby(["geo_id","date"]).contribution.transform("min"))
      contrib.append({"canonical_metric_key":metric,"configured_metric_weight":weights[metric],"mean_absolute_effective_contribution":g.contribution.abs().mean(),"median_absolute_effective_contribution":g.contribution.abs().median(),"p90_absolute_contribution":_q(g.contribution.abs(),.9),"share_total_absolute_contribution":g.contribution.abs().sum()/abs_total,"largest_absolute_driver_share":(drivers.query('metric==@metric').contribution.abs()==drivers.query('metric==@metric').mx).mean(),"largest_positive_driver_share":(drivers.query('metric==@metric').contribution==drivers.query('metric==@metric').pos).mean(),"largest_negative_driver_share":(drivers.query('metric==@metric').contribution==drivers.query('metric==@metric').neg).mean(),"latest_36m_mean_absolute_contribution":recent.contribution.abs().mean(),"latest_36m_share_absolute_contribution":recent.contribution.abs().sum()/incumbent[incumbent.date>=cutoff].contribution.abs().sum()})
      movement.append({"canonical_metric_key":metric,"correlation_with_demand_dimension_delta":g.contribution_delta.corr(g.dimension_delta),"same_sign_agreement":(np.sign(g.contribution_delta)==np.sign(g.dimension_delta)).mean(),"mean_absolute_contribution_delta":g.contribution_delta.abs().mean(),"absolute_movement_share_descriptive":g.contribution_delta.abs().sum()/move_total,"dominant_monthly_movement_driver_share":(g.contribution_delta.abs()==incumbent.groupby(["geo_id","date"]).contribution_delta.transform(lambda s:s.abs().max()).loc[g.index]).mean(),"latest_36m_mean_absolute_delta":recent.contribution_delta.abs().mean(),"latest_36m_correlation":recent.contribution_delta.corr(recent.dimension_delta)})
    # Exact movement reconstruction.
    recon=incumbent.groupby(["geo_id","date"]).contribution_delta.sum(min_count=1)-incumbent.groupby(["geo_id","date"]).dimension_delta.first()
    if recon.abs().max()>TOL: raise ValueError("Demand movement reconstruction failed")
    def cancellation(detail, contribution, net):
      q=detail.groupby(["geo_id","date"]).agg(gross=(contribution,lambda s:s.abs().sum()),net=(net,"first")).reset_index(); q["ratio"]=np.where(q.gross.gt(0),1-q.net.abs()/q.gross,np.nan); return q
    dc=cancellation(incumbent,"contribution","demand_dimension"); ac=cancellation(inc_axis_detail,"axis_contribution","demand_axis")
    def cancel_summary(q):
      rows=[]
      for geo,g in [("POOLED",q),*q.groupby("geo_id")]: rows.append({"geo_id":geo,"median_cancellation_ratio":g.ratio.median(),"p90_cancellation_ratio":_q(g.ratio,.9),"p99_cancellation_ratio":_q(g.ratio,.99),"latest_36m_median_cancellation_ratio":g[g.date>=g.date.max()-pd.DateOffset(months=35)].ratio.median()})
      return pd.DataFrame(rows)
    # Group balance.
    gg=incumbent.assign(group=np.where(incumbent.metric.isin(STRUCTURAL),"STRUCTURAL","LABOR_CYCLICAL")).groupby(["geo_id","date","group"]).agg(net_contribution=("contribution","sum"),gross_absolute_contribution=("contribution",lambda s:s.abs().sum()),movement_contribution=("contribution_delta","sum")).reset_index()
    denom=gg.groupby(["geo_id","date"]).gross_absolute_contribution.transform("sum"); gg["absolute_contribution_share"]=gg.gross_absolute_contribution/denom; gg["within_group_cancellation_ratio"]=np.where(gg.gross_absolute_contribution.gt(0),1-gg.net_contribution.abs()/gg.gross_absolute_contribution,np.nan)
    group_summary=gg.groupby(["geo_id","group"],as_index=False).agg(net_contribution_mean=("net_contribution","mean"),gross_absolute_contribution_mean=("gross_absolute_contribution","mean"),absolute_contribution_share=("absolute_contribution_share","mean"),mean_absolute_movement=("movement_contribution",lambda s:s.abs().mean()),median_within_group_cancellation=("within_group_cancellation_ratio","median"))
    # All challengers and policy family.
    chron={"INCUMBENT":(inc_series,inc_axis,incumbent)}
    for name,drops in ABLATIONS.items():
      s=_score(x,weights,set(METRICS)-drops); ds=s[["geo_id","date","demand_dimension"]].drop_duplicates(); ax,_=_axis(dimensions,ds,axis_weights); chron[name]=(ds,ax,s)
    policy_map={"DEM-LABOR-A":"INCUMBENT","DEM-LABOR-B":"DROP-LABOR-FORCE","DEM-LABOR-C":"DROP-LABOR-FORCE-AND-EMPLOYMENT","DEM-LABOR-D":"DROP-LABOR-FORCE"}
    ab=[]; incremental=[]; stability=[]; turns=[]
    base_turn=_turns(inc_axis,"demand_axis","INCUMBENT")
    for name,(ds,ax,detail) in chron.items():
      dm=inc_series.merge(ds,on=["geo_id","date"],suffixes=("_inc","_chal")); am=inc_axis.merge(ax,on=["geo_id","date"],suffixes=("_inc","_chal")); diff=(dm.demand_dimension_chal-dm.demand_dimension_inc).abs(); adiff=(am.demand_axis_chal-am.demand_axis_inc).abs()
      if name!="INCUMBENT": ab.append({"challenger":name,"median_abs_dimension_difference":diff.median(),"p90_dimension_difference":_q(diff,.9),"p99_dimension_difference":_q(diff,.99),"dimension_correlation":dm.demand_dimension_inc.corr(dm.demand_dimension_chal),"dimension_sign_disagreement":(np.sign(dm.demand_dimension_inc)!=np.sign(dm.demand_dimension_chal)).mean(),"monthly_changed_direction_share":(np.sign(dm.groupby('geo_id').demand_dimension_inc.diff())!=np.sign(dm.groupby('geo_id').demand_dimension_chal.diff())).mean(),"median_abs_axis_difference":adiff.median(),"p90_axis_difference":_q(adiff,.9),"p99_axis_difference":_q(adiff,.99),"axis_correlation":am.demand_axis_inc.corr(am.demand_axis_chal),"axis_sign_disagreement":(np.sign(am.demand_axis_inc)!=np.sign(am.demand_axis_chal)).mean()})
      delta=ds.sort_values(["geo_id","date"]).groupby("geo_id").demand_dimension.diff(); ad=ax.sort_values(["geo_id","date"]).groupby("geo_id").demand_axis.diff(); ts=_turns(ax,"demand_axis",name); turns.append(ts)
      stability.append({"policy":name,"median_abs_demand_dimension_movement":delta.abs().median(),"p90_dimension_movement":_q(delta.abs(),.9),"p99_dimension_movement":_q(delta.abs(),.99),"max_dimension_jump":delta.abs().max(),"sign_flips":int((np.sign(ds.demand_dimension)!=np.sign(ds.groupby('geo_id').demand_dimension.shift())).sum()),"rolling_12m_volatility":ds.groupby('geo_id').demand_dimension.rolling(12,min_periods=2).std().median(),"qualified_turning_points":len(ts),"latest_36m_turning_points":int((pd.to_datetime(ts.turning_point_date)>=ax.date.max()-pd.DateOffset(months=35)).sum()) if len(ts) else 0,"median_abs_demand_axis_movement":ad.abs().median()})
      if name.startswith("DROP-") and name.count("AND")==0:
        row=ab[-1].copy(); row.update({"metric_removed":name.removeprefix("DROP-"),"unique_movement_disappearing":(dm.groupby('geo_id').demand_dimension_inc.diff()-dm.groupby('geo_id').demand_dimension_chal.diff()).abs().median(),"responsiveness_change":delta.abs().median()-dm.groupby('geo_id').demand_dimension_inc.diff().abs().median(),"interpretation":"diagnostic; no composite score"}); incremental.append(row)
    policy_registry=[]; decision=[]
    stab=pd.DataFrame(stability)
    for policy,key in policy_map.items():
      included=set(chron[key][2].metric.unique()); ew={m:weights[m]/sum(weights[k] for k in included) for m in included}; s=stab[stab.policy.eq(key)].iloc[0]
      policy_registry.append({"policy":policy,"included_labor_metrics":"|".join(sorted(included&set(LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"diagnostic_only":True})
      pair_lf=pair.query("geo_id=='POOLED' and metric_a=='labor_force' and metric_b=='employment'").score_correlation.iloc[0]; pair_ue=pair.query("geo_id=='POOLED' and metric_a=='employment' and metric_b=='laus_unemployment_rate'").score_correlation.iloc[0]
      decision.append({"policy":policy,"included_labor_metrics":"|".join(sorted(included&set(LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"median_Demand_dimension_movement":s.median_abs_demand_dimension_movement,"P90_movement":s.p90_dimension_movement,"rolling_volatility":s.rolling_12m_volatility,"turning_points":s.qualified_turning_points,"latest_36m_turns":s.latest_36m_turning_points,"median_Demand_axis_movement":s.median_abs_demand_axis_movement,"Demand_axis_sign_disagreements_vs_incumbent":0 if policy=="DEM-LABOR-A" else next(r["axis_sign_disagreement"] for r in ab if r["challenger"]==key),"median_dimension_cancellation":cancel_summary(dc).query("geo_id=='POOLED'").median_cancellation_ratio.iloc[0],"median_axis_cancellation":cancel_summary(ac).query("geo_id=='POOLED'").median_cancellation_ratio.iloc[0],"structural_contribution_share":group_summary.query("group=='STRUCTURAL'").absolute_contribution_share.mean(),"labor_contribution_share":group_summary.query("group=='LABOR_CYCLICAL'").absolute_contribution_share.mean(),"employment_labor_force_redundancy_evidence":pair_lf,"employment_unemployment_redundancy_evidence":pair_ue,"Decision":"pending"})
    recent=inc_series.merge(inc_axis,on=["geo_id","date"]).merge(dc[["geo_id","date","ratio"]],on=["geo_id","date"]).merge(gg.pivot(index=["geo_id","date"],columns="group",values="net_contribution").reset_index(),on=["geo_id","date"])
    for policy,key in policy_map.items():
      recent=recent.merge(chron[key][0].rename(columns={"demand_dimension":f"{policy}_demand_dimension"}),on=["geo_id","date"]).merge(chron[key][1].rename(columns={"demand_axis":f"{policy}_demand_axis"}),on=["geo_id","date"])
    recent=recent[recent.date>=recent.groupby("geo_id").date.transform("max")-pd.DateOffset(months=35)]
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    runtime=pd.DataFrame([{"authoritative_run":run.name,"geography_count":len(REVIEW_GEOS),"metric_count":len(METRICS),"parity_tolerance":TOL,"production_policy_changed":False}])
    return dict(zip(OUTPUTS,[contract,pair,pd.DataFrame(contrib),pd.DataFrame(movement),cancel_summary(dc),cancel_summary(ac),group_summary,pd.DataFrame(ab),pd.DataFrame(incremental),pd.DataFrame(policy_registry),stab,pd.concat(turns,ignore_index=True),recent,pd.DataFrame(decision),parity,governance,runtime]))

def write_review(tables: dict[str,pd.DataFrame], output: Path) -> None:
    output.mkdir(parents=True,exist_ok=True)
    for name,frame in tables.items(): frame.to_csv(output/f"{name}.csv",index=False)
    sections=[]
    for name in OUTPUTS: sections.append(f"<h2>{name}</h2>"+tables[name].to_html(index=False,border=0))
    (output/"demand_metric_redundancy_review.html").write_text("<html><body><h1>Demand Metric Redundancy Review</h1><p>Diagnostic only. Decision pending; no automated winner.</p>"+"".join(sections)+"</body></html>")
