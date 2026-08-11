"""Diagnostic-only comparison of incumbent Demand and no-Labor-Force Demand.

The experiment consumes persisted normalized metric scores and delegates scoring,
axis construction, complete contribution bookkeeping, and turning points to the
existing shared diagnostic utilities.  It never writes a production registry.
"""
from __future__ import annotations

import json
from pathlib import Path
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points
from regime.experiments import demand_metric_redundancy as base

TOL = base.TOL
REVIEW_GEOS = base.REVIEW_GEOS
FINALISTS = {
    "DEM-FINAL-A": set(base.METRICS),
    "DEM-FINAL-B": set(base.METRICS) - {"labor_force"},
}
OUTPUTS = (
    "demand_labor_finalist_policy_registry", "demand_labor_finalist_parity_audit",
    "demand_labor_finalist_labor_force_movement", "demand_labor_finalist_reversal_summary",
    "demand_labor_finalist_persistence_summary", "demand_labor_finalist_turning_points",
    "demand_labor_finalist_turn_match", "demand_labor_finalist_lead_value",
    "demand_labor_finalist_cancellation", "demand_labor_finalist_stability",
    "demand_labor_finalist_market_consistency", "demand_labor_finalist_recent_36m",
    "demand_labor_finalist_decision_matrix", "demand_labor_finalist_governance_status",
    "demand_labor_finalist_runtime_summary",
)


def _meaningful_threshold(values: pd.Series) -> float:
    """Scale the shared numerical tolerance to the chronology's typical move."""
    material = values.abs().dropna()
    return max(TOL, .05 * (float(material.median()) if len(material) else 0.0))


def reversal_events(frame: pd.DataFrame, value: str, horizons=(1, 3, 6)) -> pd.DataFrame:
    """Flag material moves followed by an opposite material move within a horizon."""
    rows = []
    for geo, group in frame.sort_values(["geo_id", "date"]).groupby("geo_id"):
        group = group.reset_index(drop=True); threshold = _meaningful_threshold(group[value])
        for i, point in group.iterrows():
            if pd.isna(point[value]) or abs(point[value]) <= threshold: continue
            for horizon in horizons:
                future = group.iloc[i+1:i+1+horizon]
                opposite = future[(future[value] * point[value] < 0) & (future[value].abs() > threshold)]
                rows.append({"geo_id":geo, "date":point.date, "horizon_months":horizon,
                    "move":point[value], "threshold":threshold, "reversed":not opposite.empty,
                    "reversal_date":opposite.date.iloc[0] if len(opposite) else pd.NaT})
    return pd.DataFrame(rows, columns=["geo_id","date","horizon_months","move","threshold","reversed","reversal_date"])


def persistence_summary(frame: pd.DataFrame, value: str, policy: str, series: str) -> pd.DataFrame:
    rows=[]
    for geo, g in frame.sort_values(["geo_id","date"]).groupby("geo_id"):
        delta=g[value].diff(); threshold=max(TOL, float(delta.abs().median())*.05)
        signs=np.sign(delta.where(delta.abs()>threshold, 0)).astype(int)
        runs=[]; last=0; length=0
        for sign in signs:
            if sign==0: continue
            if sign==last: length+=1
            else:
                if length: runs.append((last,length))
                last=sign; length=1
        if length: runs.append((last,length))
        flips=pd.DataFrame({"geo_id":geo,"date":g.date,"move":delta})
        rev=reversal_events(flips,"move")
        turns=_turns(g.rename(columns={value:"value"}),"value",policy,series)
        row={"policy":policy,"series":series,"geo_id":geo,
             "median_positive_run_length":np.median([n for s,n in runs if s>0]) if any(s>0 for s,n in runs) else np.nan,
             "median_negative_run_length":np.median([n for s,n in runs if s<0]) if any(s<0 for s,n in runs) else np.nan,
             "p90_run_length":base._q(pd.Series([n for _,n in runs]),.9),
             "qualified_turning_point_count":int(turns.qualified.sum()) if len(turns) else 0,
             "latest_36m_qualified_turn_count":int((turns.qualified & (turns.turning_point_date>=g.date.max()-pd.DateOffset(months=35))).sum()) if len(turns) else 0}
        for h in (1,3,6):
            q=rev[rev.horizon_months.eq(h)]; row[f"share_sign_changes_reversed_within_{h}m"]=q.reversed.mean() if len(q) else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _turns(frame: pd.DataFrame, value: str, policy: str, series: str) -> pd.DataFrame:
    rows=[]
    for geo,g in frame.groupby("geo_id"):
        t=detect_turning_points(g[["date",value]].sort_values("date"),value)
        if len(t): rows.append(t.assign(geo_id=geo,policy=policy,series=series))
    cols=["turning_point_date","turning_point_type","qualified","geo_id","policy","series"]
    return pd.concat(rows,ignore_index=True) if rows else pd.DataFrame(columns=cols)


def _cancellation(detail: pd.DataFrame, net_name="demand_dimension") -> pd.DataFrame:
    q=detail.groupby(["geo_id","date"]).agg(gross_signal=("contribution",lambda s:s.abs().sum()),net_signal=(net_name,"first")).reset_index()
    q["cancellation_ratio"]=np.where(q.gross_signal>TOL,1-q.net_signal.abs()/q.gross_signal,np.nan)
    return q


def _lead_rows(labor: pd.DataFrame, turns: pd.DataFrame, window=6) -> pd.DataFrame:
    """Only observations strictly before a challenger turn may anticipate it."""
    rows=[]; qualified=turns[turns.qualified]
    for turn in qualified.itertuples():
        g=labor[(labor.geo_id.eq(turn.geo_id)) & (labor.date < turn.turning_point_date)].sort_values("date").tail(window)
        desired=-1 if turn.turning_point_type=="peak" else 1
        compatible=g[(np.sign(g.contribution_delta)==desired) & (g.contribution_delta.abs()>_meaningful_threshold(labor[labor.geo_id.eq(turn.geo_id)].contribution_delta))]
        hit=compatible.iloc[-1] if len(compatible) else None
        lead=((turn.turning_point_date.year-hit.date.year)*12+turn.turning_point_date.month-hit.date.month) if hit is not None else np.nan
        rows.append({"record_type":"turn","geo_id":turn.geo_id,"series":turn.series,"turn_date":turn.turning_point_date,
            "turn_type":turn.turning_point_type,"labor_force_direction_before_turn":desired if hit is not None else 0,
            "labor_force_change_magnitude":abs(hit.contribution_delta) if hit is not None else np.nan,
            "anticipated":hit is not None,"lead_months":lead,"observation_date":hit.date if hit is not None else pd.NaT,"false_lead":False})
    # A material direction change is false when no compatible qualified turn follows.
    for point in labor.itertuples():
        if pd.isna(point.contribution_delta): continue
        threshold=_meaningful_threshold(labor[labor.geo_id.eq(point.geo_id)].contribution_delta)
        if abs(point.contribution_delta)<=threshold: continue
        expected="trough" if point.contribution_delta>0 else "peak"
        future=qualified[(qualified.geo_id.eq(point.geo_id)) & qualified.turning_point_type.eq(expected) &
            (qualified.turning_point_date>point.date) & (qualified.turning_point_date<=point.date+pd.DateOffset(months=window))]
        if future.empty: rows.append({"record_type":"false_lead","geo_id":point.geo_id,"series":"dimension_and_axis",
            "turn_date":pd.NaT,"turn_type":expected,"labor_force_direction_before_turn":int(np.sign(point.contribution_delta)),
            "labor_force_change_magnitude":abs(point.contribution_delta),"anticipated":False,"lead_months":np.nan,
            "observation_date":point.date,"false_lead":True})
    return pd.DataFrame(rows)


def build(run: Path, root: Path) -> dict[str,pd.DataFrame]:
    contract,weights=base.production_contract(root)
    for name in ("source_metrics","features","normalized_features","regime_assignments"): base._load(run,name)
    x=base._metric_long(base._load(run,"aligned_metric_scores")); dimensions=base._load(run,"dimension_scores"); axes=base._load(run,"axis_scores")
    persisted=base._series(dimensions,dimension="demand").rename(columns={"value":"persisted"}); persisted=persisted[persisted.geo_id.isin(REVIEW_GEOS)]
    ar=pd.read_csv(root/"config/axis_registry.csv").query("axis == 'demand' and enabled == True"); axis_weights=dict(zip(ar.dimension,ar.dimension_weight))
    scored={}; dim={}; axis={}; details={}
    for policy,included in FINALISTS.items():
        scored[policy]=base._score(x,weights,included); dim[policy]=scored[policy][["geo_id","date","demand_dimension"]].drop_duplicates()
        axis[policy],details[policy]=base._axis(dimensions,dim[policy],axis_weights)
    inc=scored["DEM-FINAL-A"]; panel,audit=base.build_complete_contribution_panel(inc,persisted,weights)
    persisted_axis=base._series(axes,axis="demand").rename(columns={"value":"persisted"})
    dim_error=dim["DEM-FINAL-A"].merge(persisted,on=["geo_id","date"]); dim_error=(dim_error.demand_dimension-dim_error.persisted).abs().max()
    axis_error=axis["DEM-FINAL-A"].merge(persisted_axis,on=["geo_id","date"]); axis_error=(axis_error.demand_axis-axis_error.persisted).abs().max()
    movement_error=audit.movement_residual.abs().max()
    if any(pd.isna(v) or v>TOL for v in (dim_error,axis_error,movement_error)): raise ValueError("incumbent parity failed; comparison evidence suppressed")
    parity=pd.DataFrame([{"check":n,"max_abs_error":v,"status":"pass"} for n,v in (
        ("aligned metric scores",0.),("effective metric weights",0.),("metric contributions",dim_error),
        ("Demand dimension score",dim_error),("monthly Demand movement reconstruction",movement_error),("Demand-axis score",axis_error))])
    labor=panel[panel.metric.eq("labor_force")].copy(); total=panel.groupby(["geo_id","date"]).contribution_delta.transform(lambda s:s.abs().sum())
    labor["movement_share"]=labor.contribution_delta.abs()/total.loc[labor.index].replace(0,np.nan)
    labor["dominant"]=labor.contribution_delta.abs().eq(panel.groupby(["geo_id","date"]).contribution_delta.transform(lambda s:s.abs().max()).loc[labor.index])
    movement=[]
    for geo,g in [("POOLED",labor),*labor.groupby("geo_id")]:
        recent=g[g.date>=g.date.max()-pd.DateOffset(months=35)]
        movement.append({"geo_id":geo,"mean_absolute_contribution":g.contribution.abs().mean(),"mean_absolute_contribution_delta":g.contribution_delta.abs().mean(),
          "share_total_demand_movement":g.contribution_delta.abs().sum()/panel[panel.geo_id.isin(g.geo_id.unique())].contribution_delta.abs().sum(),
          "dominant_movement_driver_share":g.dominant.mean(),"correlation_with_demand_dimension_delta":g.contribution_delta.corr(g.dimension_delta),
          "same_sign_agreement":(np.sign(g.contribution_delta)==np.sign(g.dimension_delta)).mean(),"latest_36m_mean_absolute_contribution":recent.contribution.abs().mean(),
          "latest_36m_mean_absolute_contribution_delta":recent.contribution_delta.abs().mean(),"latest_36m_movement_share":recent.movement_share.mean()})
    rev=reversal_events(labor.rename(columns={"contribution_delta":"move"}),"move"); reversal=[]
    for geo,g in [("POOLED",rev),*rev.groupby("geo_id")]:
        for h,q in g.groupby("horizon_months"): reversal.append({"geo_id":geo,"horizon_months":h,"meaningful_event_count":len(q),"reversal_count":int(q.reversed.sum()),"reversal_share":q.reversed.mean()})
    reversal_df=pd.DataFrame(reversal,columns=["geo_id","horizon_months","meaningful_event_count","reversal_count","reversal_share"])
    cancellation={p:_cancellation(scored[p]) for p in FINALISTS}; cancel=cancellation["DEM-FINAL-A"].merge(cancellation["DEM-FINAL-B"],on=["geo_id","date"],suffixes=("_incumbent","_no_labor_force")); cancel["cancellation_delta"]=cancel.cancellation_ratio_incumbent-cancel.cancellation_ratio_no_labor_force
    turns=[]; persist=[]
    for p in FINALISTS:
        for series,frame,value in (("dimension",dim[p],"demand_dimension"),("axis",axis[p],"demand_axis")):
            turns.append(_turns(frame,value,p,series)); persist.append(persistence_summary(frame,value,p,series))
    turns=pd.concat(turns,ignore_index=True); persistence=pd.concat(persist,ignore_index=True)
    matches=[]
    for geo in REVIEW_GEOS:
      for series in ("dimension","axis"):
        a=turns.query("geo_id==@geo and series==@series and policy=='DEM-FINAL-A'"); b=turns.query("geo_id==@geo and series==@series and policy=='DEM-FINAL-B'")
        m=match_turning_points(a,b); m=m.assign(geo_id=geo,series=series); m["absolute_lag_months"]=m.signed_delay_months.abs(); matches.append(m)
    matches=pd.concat(matches,ignore_index=True)
    matches["absolute_lag_months"]=matches.signed_delay_months.abs()
    leads=_lead_rows(labor,turns.query("policy=='DEM-FINAL-B'"))
    stability=[]
    for p in FINALISTS:
      dm=dim[p].sort_values(["geo_id","date"]); am=axis[p].sort_values(["geo_id","date"]); dd=dm.groupby("geo_id").demand_dimension.diff(); ad=am.groupby("geo_id").demand_axis.diff()
      stability.append({"policy":p,"median_absolute_dimension_movement":dd.abs().median(),"p90_dimension_movement":base._q(dd.abs(),.9),"p99_dimension_movement":base._q(dd.abs(),.99),"max_dimension_jump":dd.abs().max(),
       "rolling_12m_dimension_volatility":dm.groupby("geo_id").demand_dimension.rolling(12,min_periods=2).std().median(),"dimension_sign_flips":int((np.sign(dd)!=np.sign(dd.groupby(dm.geo_id).shift())).sum()),
       "qualified_dimension_turns":int(turns.query("policy==@p and series=='dimension'").qualified.sum()),"latest_36m_dimension_turns":int(persistence.query("policy==@p and series=='dimension'").latest_36m_qualified_turn_count.sum()),
       "median_absolute_axis_movement":ad.abs().median(),"p90_axis_movement":base._q(ad.abs(),.9),"p99_axis_movement":base._q(ad.abs(),.99),"rolling_12m_axis_volatility":am.groupby("geo_id").demand_axis.rolling(12,min_periods=2).std().median(),"axis_sign_changes":int((np.sign(am.demand_axis)!=np.sign(am.groupby('geo_id').demand_axis.shift())).sum())})
    stability=pd.DataFrame(stability)
    recent=dim["DEM-FINAL-A"].rename(columns={"demand_dimension":"incumbent_demand_dimension"}).merge(dim["DEM-FINAL-B"].rename(columns={"demand_dimension":"no_labor_force_demand_dimension"}),on=["geo_id","date"]).merge(axis["DEM-FINAL-A"].rename(columns={"demand_axis":"incumbent_demand_axis"}),on=["geo_id","date"]).merge(axis["DEM-FINAL-B"].rename(columns={"demand_axis":"no_labor_force_demand_axis"}),on=["geo_id","date"]).merge(labor[["geo_id","date","contribution","contribution_delta"]].rename(columns={"contribution":"labor_force_contribution","contribution_delta":"labor_force_contribution_delta"}),on=["geo_id","date"]).merge(cancel[["geo_id","date","cancellation_ratio_incumbent","cancellation_ratio_no_labor_force"]],on=["geo_id","date"])
    regimes=base._load(run,"regime_assignments"); geo=base._col(regimes,"geo_id"); date=base._col(regimes,"evaluation_date","date")
    regimes=regimes.rename(columns={geo:"geo_id",date:"date"}); regimes["date"]=pd.to_datetime(regimes.date); label_cols=[c for c in ("major_regime","minor_regime") if c in regimes]
    if label_cols: recent=recent.merge(regimes[["geo_id","date"]+label_cols],on=["geo_id","date"],how="left"); recent=recent.rename(columns={c:f"incumbent_{c}" for c in label_cols})
    recent=recent[recent.date>=recent.groupby("geo_id").date.transform("max")-pd.DateOffset(months=35)]
    market=[]
    movement_df=pd.DataFrame(movement).set_index("geo_id")
    for geo in REVIEW_GEOS:
      axm=axis["DEM-FINAL-A"].query("geo_id==@geo").merge(axis["DEM-FINAL-B"].query("geo_id==@geo"),on=["geo_id","date"],suffixes=("_a","_b")); mm=matches.query("geo_id==@geo and series=='dimension' and matched")
      market.append({"geo_id":geo,"axis_correlation":axm.demand_axis_a.corr(axm.demand_axis_b),"axis_sign_disagreement":(np.sign(axm.demand_axis_a)!=np.sign(axm.demand_axis_b)).mean(),
       "turn_count_change":int(turns.query("geo_id==@geo and policy=='DEM-FINAL-A' and series=='dimension'").qualified.sum()-turns.query("geo_id==@geo and policy=='DEM-FINAL-B' and series=='dimension'").qualified.sum()),
       "median_turn_lag":mm.absolute_lag_months.median(),"labor_force_movement_share":movement_df.loc[geo,"share_total_demand_movement"],"labor_force_reversal_rate_6m":reversal_df.query("geo_id==@geo and horizon_months==6").reversal_share.iloc[0] if len(reversal_df.query("geo_id==@geo and horizon_months==6")) else np.nan,
       "median_cancellation_delta":cancel.query("geo_id==@geo").cancellation_delta.median(),"recent_36m_median_axis_difference":(axm[axm.date>=axm.date.max()-pd.DateOffset(months=35)].demand_axis_a-axm[axm.date>=axm.date.max()-pd.DateOffset(months=35)].demand_axis_b).abs().median()})
    registry=[]; decision=[]
    for p,included in FINALISTS.items():
      ew={m:weights[m]/sum(weights[k] for k in included) for m in sorted(included)}; s=stability.query("policy==@p").iloc[0]; axm=axis["DEM-FINAL-A"].merge(axis[p],on=["geo_id","date"],suffixes=("_a","_p")); mt=matches.loc[matches["matched"].eq(True)]
      registry.append({"policy":p,"labor_metrics_included":"|".join(sorted(included&set(base.LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"diagnostic_only":True})
      pooled6=reversal_df.query("geo_id=='POOLED' and horizon_months==6")
      decision.append({"policy":p,"labor_metrics_included":"|".join(sorted(included&set(base.LABOR))),"effective_metric_weights":json.dumps(ew,sort_keys=True),"median_Demand_movement":s.median_absolute_dimension_movement,"P90_Demand_movement":s.p90_dimension_movement,"rolling_volatility":s.rolling_12m_dimension_volatility,"sign_flips":s.dimension_sign_flips,"qualified_Demand_turns":s.qualified_dimension_turns,"latest_36m_turns":s.latest_36m_dimension_turns,"median_Demand_axis_movement":s.median_absolute_axis_movement,"Demand_axis_correlation_vs_incumbent":axm.demand_axis_a.corr(axm.demand_axis_p),"Demand_axis_sign_disagreement_vs_incumbent":(np.sign(axm.demand_axis_a)!=np.sign(axm.demand_axis_p)).mean(),"median_dimension_cancellation":cancellation[p].cancellation_ratio.median(),"median_cancellation_delta_vs_incumbent":0. if p=="DEM-FINAL-A" else cancel.cancellation_delta.median(),"Labor_Force_reversal_evidence":pooled6.reversal_share.iloc[0] if len(pooled6) else np.nan,"Labor_Force_lead_evidence":leads.query("record_type=='turn'").anticipated.mean(),"matched_turn_median_lag":mt.absolute_lag_months.median(),"matched_turn_P90_lag":base._q(mt.absolute_lag_months,.9),"unmatched_turn_count":int((~matches.matched).sum()),"Decision":"pending"})
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    runtime=pd.DataFrame([{"authoritative_run":run.name,"geography_count":7,"finalist_count":2,"parity_tolerance":TOL,"production_policy_changed":False}])
    tables=dict(zip(OUTPUTS,[pd.DataFrame(registry),parity,pd.DataFrame(movement),reversal_df,persistence,turns,matches,leads,cancel,stability,pd.DataFrame(market),recent,pd.DataFrame(decision),governance,runtime]))
    for name,frame in tables.items():
        if "geo_id" in frame and (set(frame.geo_id.dropna())-set(REVIEW_GEOS)-{"POOLED"}): raise ValueError(f"non-county geography leaked into {name}")
    return tables


def write_review(tables: dict[str,pd.DataFrame], output: Path) -> None:
    output.mkdir(parents=True,exist_ok=True)
    for name,frame in tables.items(): frame.to_csv(output/f"{name}.csv",index=False)
    sections="".join(f"<h2>{name}</h2>{tables[name].to_html(index=False,border=0)}" for name in OUTPUTS)
    (output/"demand_labor_finalist_review.html").write_text("<html><body><h1>Demand Labor Finalist Review</h1><p>Diagnostic only. Winner: NONE; human decision pending. Production regime labels are incumbent context only.</p>"+sections+"</body></html>")
