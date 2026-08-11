"""Two-stage, diagnostic-only calibration of the governed LAUS feature family.

The module deliberately delegates smoothing, normalization, Demand scoring and
turn detection to existing repository implementations.  It never writes config
or production artifacts and never selects a policy.
"""
from __future__ import annotations

from pathlib import Path
import time
import numpy as np
import pandas as pd

from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments import demand_metric_redundancy as demand
from regime.experiments.demand_labor_finalist import reversal_events
from regime.smoothing_features import build_smoothed_metric_features_wide
from regime.smoothing_policy import SmoothingMetricPolicy

TOL = 1e-12
GEOS = demand.REVIEW_GEOS
LAUS = ("laus_labor_force", "laus_employment", "laus_unemployment_rate")
CANONICAL = {"laus_labor_force":"labor_force", "laus_employment":"employment",
             "laus_unemployment_rate":"laus_unemployment_rate"}
MA_POLICIES = {f"LAUS-MA{x}": x for x in (3, 6, 9, 12)}
WEIGHT_POLICIES = {
    "LAUS-W-25-35-40": (.25, .35, .40), "LAUS-W-40-30-30": (.40, .30, .30),
    "LAUS-W-50-25-25": (.50, .25, .25), "LAUS-W-60-20-20": (.60, .20, .20),
    "LAUS-W-70-15-15": (.70, .15, .15),
}
PRODUCTION_WEIGHTS = WEIGHT_POLICIES["LAUS-W-25-35-40"]


def production_contract(root: Path) -> pd.DataFrame:
    """Fail closed unless the registry describes exactly the governed contract."""
    demand.production_contract(root)
    fr = pd.read_csv(root / "config/feature_registry.csv")
    rows = fr.loc[fr["metric_key"].isin(LAUS)].copy()
    if set(rows.metric_key) != set(LAUS) or len(rows) != 9:
        raise ValueError("LAUS production contract must contain exactly three metrics and nine features")
    expected = {"level": ("ma_level", .25, "6m"),
                "short_term_change": ("ma_pct_change", .35, "6m/lag3m"),
                "long_term_change": ("ma_pct_change", .40, "6m/lag12m")}
    for row in rows.itertuples():
        transform, weight, window = expected[row.feature_type]
        if row.transform != transform or row.feature_window != window or not np.isclose(row.feature_weight, weight, atol=0, rtol=0):
            raise ValueError(f"material LAUS production contract drift: {row.feature_key}")
    return rows


def policy_registry(stage: str, selected_ma: int | None = None) -> pd.DataFrame:
    if stage == "ma":
        return pd.DataFrame([{"policy": p, "ma_horizon": ma, "level_weight": .25,
            "short_weight": .35, "long_weight": .40, "short_lag": 3, "long_lag": 12,
            "only_policy_difference": "MA smoothing horizon", "decision": "pending"}
            for p, ma in MA_POLICIES.items()])
    if selected_ma not in (3, 6, 9, 12):
        raise ValueError("Stage B requires explicit --selected-ma in {3,6,9,12}")
    return pd.DataFrame([{"policy": p, "selected_ma": selected_ma, "level_weight": w[0],
        "short_weight": w[1], "long_weight": w[2], "short_lag": 3, "long_lag": 12,
        "within_family_incumbent": p == "LAUS-W-25-35-40",
        "production_reference": selected_ma == 6 and p == "LAUS-W-25-35-40",
        "only_policy_difference": "feature weights", "decision": "pending"}
        for p, w in WEIGHT_POLICIES.items()])


def _source(run: Path) -> pd.DataFrame:
    raw = demand._load(run, "source_metrics")
    geo=demand._col(raw,"geo_id"); date=demand._col(raw,"date","evaluation_date")
    metric=demand._col(raw,"canonical_metric_key","metric_key"); value=demand._col(raw,"value","raw_value")
    out=raw.rename(columns={geo:"geo_id",date:"date",metric:"canonical_metric_key",value:"raw_value"})
    out=out.loc[out["geo_id"].isin(GEOS) & out["canonical_metric_key"].isin(LAUS),
                ["geo_id","date","canonical_metric_key","raw_value"]].copy()
    out["date"]=pd.to_datetime(out.date)
    if set(out.geo_id) != set(GEOS): raise ValueError(f"missing governed counties: {set(GEOS)-set(out.geo_id)}")
    if out.geo_id.str.contains("cbsa|metro",case=False,regex=True).any(): raise ValueError("CBSA/metro leakage")
    return out


def _features(source: pd.DataFrame, ma: int) -> pd.DataFrame:
    chunks=[]
    for metric in LAUS:
        policy=SmoothingMetricPolicy(f"laus_ma{ma}",metric,"direct","ma_momentum",ma,ma,3,ma,12,False)
        wide=build_smoothed_metric_features_wide(source.loc[source.canonical_metric_key.eq(metric)],policy=policy,value_column="raw_value")
        for suffix,col in (("level","smoothed_level_value"),("short","smoothed_short_value"),("long","smoothed_long_value")):
            q=wide[["geo_id","date","canonical_metric_key",col]].rename(columns={col:"raw_feature_value"})
            q["feature_key"]=f"{metric}_{suffix}"; q["feature_type"]=suffix; chunks.append(q)
    return pd.concat(chunks,ignore_index=True)


def _chronology(source: pd.DataFrame, policy: str, ma: int, weights: tuple[float,float,float]) -> pd.DataFrame:
    features=_features(source,ma)
    normalized=normalize_features(features[["geo_id","date","canonical_metric_key","feature_key","raw_feature_value"]])
    raw=features.pivot(index=["geo_id","date","canonical_metric_key"],columns="feature_type",values="raw_feature_value")
    normalized["feature_type"]=normalized.feature_key.str.rsplit("_",n=1).str[-1]
    norm=normalized.pivot(index=["geo_id","date","canonical_metric_key"],columns="feature_type",values="feature_score")
    out=raw.join(norm,lsuffix="_raw",rsuffix="_score").reset_index()
    available=out[["level_score","short_score","long_score"]].notna()
    denom=sum(available[f"{f}_score"]*w for f,w in zip(("level","short","long"),weights))
    for f,w in zip(("level","short","long"),weights):
        out[f"configured_{f}_weight"]=w
        out[f"effective_{f}_weight"]=np.where(available[f"{f}_score"],w/denom.replace(0,np.nan),0.)
        out[f"{f}_contribution"]=out[f"{f}_score"].fillna(0)*out[f"effective_{f}_weight"]
    out["metric_score"]=out[[f"{f}_contribution" for f in ("level","short","long")]].sum(axis=1).where(denom.gt(0))
    out["metric"]=out.canonical_metric_key.map(CANONICAL); out["policy"]=policy
    return out


def _stability(frame: pd.DataFrame, series: str, value: str, groups: list[str]) -> pd.DataFrame:
    rows=[]
    for keys,g in frame.sort_values("date").groupby(groups):
        keys=(keys,) if not isinstance(keys,tuple) else keys; x=g[value]; delta=x.diff(); signs=np.sign(delta.where(delta.abs()>TOL)).dropna()
        turns=detect_turning_points(g[["date",value]].dropna(),value); qualified=turns.loc[turns["qualified"].eq(True)] if len(turns) else turns
        recent_cut=pd.to_datetime(g.date).max()-pd.DateOffset(months=35)
        row=dict(zip(groups,keys)); row.update({"series":series,"median_abs_mom":delta.abs().median(),"p90_abs_mom":delta.abs().quantile(.9),
          "p99_abs_mom":delta.abs().quantile(.99),"max_jump":delta.abs().max(),"rolling_12m_volatility":x.rolling(12,min_periods=2).std().median(),
          "sign_flips":int(signs.ne(signs.shift()).sum()-1 if len(signs) else 0),"qualified_turns":len(qualified),
          "latest_36m_turns":int((pd.to_datetime(qualified.turning_point_date)>=recent_cut).sum()) if len(qualified) else 0})
        rows.append(row)
    return pd.DataFrame(rows)


def _turns(frame: pd.DataFrame, value: str, series: str) -> pd.DataFrame:
    rows=[]
    for (policy,geo),g in frame.groupby(["policy","geo_id"]):
        found=detect_turning_points(g[["date",value]].dropna(),value)
        if len(found): rows.append(found.loc[found["qualified"].eq(True)].assign(policy=policy,geo_id=geo,series=series))
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified","policy","geo_id","series"])


def _downstream(run: Path, chron: pd.DataFrame, weights: dict[str,float]):
    persisted=demand._metric_long(demand._load(run,"aligned_metric_scores"))
    replacement=chron[["policy","geo_id","date","metric","metric_score"]].rename(columns={"metric_score":"score"})
    dimensions=demand._load(run,"dimension_scores"); axes=demand._load(run,"axis_scores")
    ar=pd.read_csv("config/axis_registry.csv"); ar=ar.loc[ar["axis"].eq("demand") & ar["enabled"].eq(True)]
    axis_weights=dict(zip(ar.dimension,ar.dimension_weight)); dim=[]; axis=[]; detail=[]
    for policy in chron.policy.unique():
        x=persisted.loc[~persisted.metric.isin(CANONICAL.values())].copy()
        x=pd.concat([x,replacement.loc[replacement.policy.eq(policy),["geo_id","date","metric","score"]]],ignore_index=True)
        scored=demand._score(x,weights,set(demand.METRICS)); d=scored[["geo_id","date","demand_dimension"]].drop_duplicates().assign(policy=policy)
        a,z=demand._axis(dimensions,d[["geo_id","date","demand_dimension"]],axis_weights); a["policy"]=policy; z["policy"]=policy
        dim.append(d); axis.append(a); detail.append(scored.assign(policy=policy))
    return pd.concat(dim),pd.concat(axis),pd.concat(detail)


def _parity(run: Path, chron: pd.DataFrame, dim: pd.DataFrame, axis: pd.DataFrame, incumbent: str) -> pd.DataFrame:
    rows=[]; c=chron.loc[chron.policy.eq(incumbent)]
    pf=demand._load(run,"features"); pn=demand._load(run,"normalized_features"); pm=demand._metric_long(demand._load(run,"aligned_metric_scores"))
    def compare(name,left,right,keys,lcol,rcol):
        m=left[keys+[lcol]].merge(right[keys+[rcol]],on=keys,suffixes=("_new","_prod")); err=(m[f"{lcol}_new"]-m[f"{rcol}_prod"]).abs().max()
        rows.append({"field":name,"max_abs_error":err,"tolerance":TOL,"status":"pass" if pd.notna(err) and err<=TOL else "fail"})
    for f in ("level","short","long"):
        q=c.loc[c.feature_key.eq(c.canonical_metric_key+"_"+f)] if "feature_key" in c else c
        prod=pf.loc[pf[demand._col(pf,"feature_key")].isin([m+"_"+f for m in LAUS])].rename(columns={demand._col(pf,"date","evaluation_date"):"date",demand._col(pf,"feature_key"):"feature_key",demand._col(pf,"raw_feature_value","value"):"raw_feature_value"})
        left=c.assign(feature_key=c.canonical_metric_key+"_"+f)
        compare(f"raw {f}",left,prod,["geo_id","date","feature_key"],f+"_raw","raw_feature_value")
        prod_n=pn.loc[pn[demand._col(pn,"feature_key")].isin([m+"_"+f for m in LAUS])].rename(columns={demand._col(pn,"date","evaluation_date"):"date",demand._col(pn,"feature_key"):"feature_key",demand._col(pn,"feature_score","score"):"feature_score"})
        compare(f"normalized {f}",left,prod_n,["geo_id","date","feature_key"],f+"_score","feature_score")
    compare("LAUS metric score",c,pm,["geo_id","date","metric"],"metric_score","score")
    pdim=demand._series(demand._load(run,"dimension_scores"),dimension="demand").rename(columns={"value":"score"})
    paxis=demand._series(demand._load(run,"axis_scores"),axis="demand").rename(columns={"value":"score"})
    compare("Demand dimension score",dim.loc[dim.policy.eq(incumbent)],pdim,["geo_id","date"],"demand_dimension","score")
    compare("Demand axis score",axis.loc[axis.policy.eq(incumbent)],paxis,["geo_id","date"],"demand_axis","score")
    out=pd.DataFrame(rows)
    if out.status.ne("pass").any(): raise ValueError("production parity failed; evidence suppressed\n"+out.to_string(index=False))
    return out


def build(run: Path, root: Path, stage: str, selected_ma: int | None = None) -> dict[str,pd.DataFrame]:
    started=time.time(); production_contract(root); registry=policy_registry(stage,selected_ma); source=_source(run)
    _,metric_weights=demand.production_contract(root); chron=[]
    if stage=="ma": specs=[(p,ma,PRODUCTION_WEIGHTS) for p,ma in MA_POLICIES.items()]; incumbent="LAUS-MA6"
    else: specs=[(p,selected_ma,w) for p,w in WEIGHT_POLICIES.items()]; incumbent="LAUS-W-25-35-40"
    for p,ma,w in specs: chron.append(_chronology(source,p,int(ma),w))
    chron=pd.concat(chron,ignore_index=True); dim,axis,detail=_downstream(run,chron,metric_weights)
    parity=_parity(run,chron,dim,axis,incumbent) if (stage=="ma" or selected_ma==6) else pd.DataFrame([{"field":"production reference MA6 retained separately","max_abs_error":np.nan,"tolerance":TOL,"status":"not_applicable"}])
    feature=[]
    for f in ("level","short","long"):
        feature.append(_stability(chron,"feature_"+f,f+"_score",["policy","geo_id","metric"]))
    feature=pd.concat(feature,ignore_index=True); metric=_stability(chron,"metric","metric_score",["policy","geo_id","metric"])
    dstab=_stability(dim,"dimension","demand_dimension",["policy","geo_id"]); astab=_stability(axis,"axis","demand_axis",["policy","geo_id"])
    mt=_turns(chron,"metric_score","metric"); dt=_turns(dim,"demand_dimension","dimension"); at=_turns(axis,"demand_axis","axis")
    movement=[]; contributions=[]
    for (p,g,m),q in chron.sort_values("date").groupby(["policy","geo_id","metric"]):
        denom=sum(q[f+"_contribution"].abs().sum() for f in ("level","short","long")); dden=sum(q[f+"_contribution"].diff().abs().sum() for f in ("level","short","long"))
        row={"policy":p,"geo_id":g,"metric":m}
        deltas=pd.DataFrame({f:q[f+"_contribution"].diff() for f in ("level","short","long")})
        dominant=deltas.abs().idxmax(axis=1)
        for f in ("level","short","long"):
            row[f+"_absolute_contribution_share"]=q[f+"_contribution"].abs().sum()/denom if denom else np.nan
            row[f+"_movement_share"]=deltas[f].abs().sum()/dden if dden else np.nan
            row[f+"_dominant_driver_share"]=dominant.eq(f).mean()
        contributions.append(row); movement.append(deltas.assign(policy=p,geo_id=g,metric=m,date=q.date.to_numpy()))
    contributions=pd.DataFrame(contributions); movement=pd.concat(movement,ignore_index=True)
    reversals=[]
    for (p,m),q in chron.groupby(["policy","metric"]):
        r=reversal_events(q.assign(move=q.groupby("geo_id").metric_score.diff()),"move")
        for h,x in r.groupby("horizon_months"): reversals.append({"policy":p,"metric":m,"horizon_months":h,"events":len(x),"reversal_rate":x.reversed.mean()})
    reversals=pd.DataFrame(reversals)
    matches=[]
    for series,turns in (("dimension",dt),("axis",at)):
      for p in registry.policy:
       for geo in GEOS:
        a=turns.loc[turns.policy.eq(incumbent)&turns.geo_id.eq(geo)]; b=turns.loc[turns.policy.eq(p)&turns.geo_id.eq(geo)]
        z=match_turning_points(a,b); z=z.assign(policy=p,geo_id=geo,series=series); matches.append(z)
    matches=pd.concat(matches,ignore_index=True) if matches else pd.DataFrame()
    recent=pd.concat([dim.assign(series="dimension",value=dim.demand_dimension),axis.assign(series="axis",value=axis.demand_axis)]).sort_values("date").groupby(["policy","geo_id","series"]).tail(36)
    market=dstab.merge(astab,on=["policy","geo_id"],suffixes=("_dimension","_axis"))
    decision=registry.copy(); decision["Decision"]="pending"; decision["evidence_reference"]="stability, reversal, contribution, turn-match and seven-county tables"
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False,"production_policy_changed":False}])
    runtime=pd.DataFrame([{"stage":stage,"selected_ma":selected_ma,"county_count":len(GEOS),"policy_count":len(registry),"elapsed_seconds":time.time()-started,"authoritative_run":run.name}])
    cancellation=detail.groupby(["policy","geo_id","date"]).agg(gross=("contribution",lambda s:s.abs().sum()),net=("demand_dimension","first")).reset_index(); cancellation["cancellation_ratio"]=np.where(cancellation.gross>TOL,1-cancellation.net.abs()/cancellation.gross,np.nan)
    return dict(registry=registry,parity=parity,chronology=chron,feature_stability=feature,reversals=reversals,contributions=contributions,movement=movement,
      metric_stability=metric,metric_turns=mt,dimension_stability=dstab,axis_stability=astab,turn_match=matches,market=market,recent=recent,
      cancellation=cancellation,decision=decision,governance=governance,runtime=runtime)


def write_review(t: dict[str,pd.DataFrame], output: Path, stage: str) -> None:
    output.mkdir(parents=True,exist_ok=True); prefix="laus_ma" if stage=="ma" else "laus_weight"
    mapping={"registry":"policy_registry","parity":"parity_audit","feature_stability":"feature_stability","reversals":"feature_reversal_summary" if stage=="ma" else "metric_reversal_summary",
      "contributions":"feature_contribution_summary","movement":"feature_movement_attribution","metric_stability":"metric_stability","metric_turns":"metric_turning_points",
      "dimension_stability":"dimension_stability","axis_stability":"axis_stability","turn_match":"turn_match","market":"market_consistency","recent":"recent_36m",
      "cancellation":"cancellation_summary","decision":"decision_matrix","governance":"governance_status","runtime":"runtime_summary"}
    required_ma={"registry","parity","feature_stability","reversals","contributions","metric_stability","metric_turns","dimension_stability","axis_stability","turn_match","market","recent","decision","governance","runtime"}
    required_w={"registry","parity","contributions","movement","metric_stability","reversals","metric_turns","dimension_stability","axis_stability","turn_match","cancellation","market","recent","decision","governance","runtime"}
    for key in (required_ma if stage=="ma" else required_w): t[key].to_csv(output/f"{prefix}_{mapping[key]}.csv",index=False)
    sections="".join(f"<h2>{k}</h2>{t[k].to_html(index=False)}" for k in ("decision","parity","governance","metric_stability","market"))
    (output/f"{prefix}_review.html").write_text("<!doctype html><meta charset='utf-8'><h1>LAUS feature architecture review</h1><p>Diagnostic only. Winner: NONE; human decision pending.</p>"+sections,encoding="utf-8")
