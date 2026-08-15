"""Diagnostic-only sensitivity review for the shared Price turn detector."""
from __future__ import annotations

from pathlib import Path
import html
import math
import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import (
    TURN_FIXED_PROMINENCE, TURN_PERSISTENCE, TURN_PROMINENCE_MULTIPLIER,
    detect_turning_points, match_turning_points,
)
from regime.diagnostics.price_feature_anatomy import DC, REVIEW_GEOS

METRICS = ("median_sale_price", "median_ppsf")
FINALISTS = ("P3", "P4", "P5", "P6")
PERIODS = ("full_history", "2022_plus", "latest_36_months")
MATCH_WINDOW_MONTHS = 3
SCENARIOS = (
    ("persistence_3", 3, "shared_default_control"),
    ("persistence_2", 2, "moderately_more_sensitive"),
    ("persistence_1", 1, "high_sensitivity_stress"),
)
EXPORTS = ("scenario_registry", "current_failure", "turns", "statistics",
    "durability", "excursion", "by_county", "cross_metric",
    "period_sensitivity", "finalist_comparison", "policy_sensitivity",
    "evaluation_matrix", "governance_status")


def scenario_registry() -> pd.DataFrame:
    return pd.DataFrame([{"scenario_id": sid, "persistence_months": p,
        "fixed_prominence": TURN_FIXED_PROMINENCE,
        "prominence_multiplier": TURN_PROMINENCE_MULTIPLIER, "role": role,
        "is_shared_default": p == TURN_PERSISTENCE} for sid, p, role in SCENARIOS])


def _validate_raw(raw: pd.DataFrame) -> pd.DataFrame:
    required={"geo_id","date","metric","raw_12m_change","raw_cycle_zscore"}
    missing=required-set(raw)
    if missing: raise ValueError(f"raw-cycle chronology missing columns: {sorted(missing)}")
    if set(raw.metric.dropna().unique()) != set(METRICS):
        raise ValueError(f"raw-cycle chronology must contain exactly {METRICS}")
    q=raw.copy(); q["date"]=pd.to_datetime(q.date)
    if q.duplicated(["geo_id","metric","date"]).any(): raise ValueError("duplicate raw-cycle row")
    return q.sort_values(["metric","geo_id","date"],kind="mergesort").reset_index(drop=True)


def _periods(q: pd.DataFrame):
    yield "full_history",q
    yield "2022_plus",q[q.date.ge("2022-01-01")]
    cutoff=q.date.max()-pd.DateOffset(months=35)
    yield "latest_36_months",q[q.date.ge(cutoff)]


def _turns(q: pd.DataFrame, persistence: int, rejected=False) -> pd.DataFrame:
    return detect_turning_points(q[["date","raw_cycle_zscore"]].dropna(),"raw_cycle_zscore",
        persistence=persistence,include_rejected=rejected)


def _spacing(t: pd.DataFrame) -> pd.Series:
    q=t[t.qualified].sort_values("turning_point_date")
    return (q.turning_point_date.dt.year-q.turning_point_date.shift().dt.year)*12 + q.turning_point_date.dt.month-q.turning_point_date.shift().dt.month


def build(raw: pd.DataFrame, candidates: pd.DataFrame | None = None,
          long_reference: pd.DataFrame | None = None) -> dict[str,pd.DataFrame]:
    """Build evidence in memory; selection never reads candidate or Long evidence."""
    raw=_validate_raw(raw); registr=scenario_registry(); turn_rows=[]; stat_rows=[]
    failure=[]; durability=[]; excursion=[]; period_rows=[]
    for (metric,geo),g in raw.groupby(["metric","geo_id"],sort=True):
        g=g.sort_values("date")
        diagnosed=_turns(g,TURN_PERSISTENCE,True)
        diagnosed=diagnosed.sort_values("turning_point_date")
        previous_qualified=pd.NaT
        for r in diagnosed.itertuples(index=False):
            distance=np.nan
            if r.qualified and pd.notna(previous_qualified):
                distance=(r.turning_point_date.year-previous_qualified.year)*12+r.turning_point_date.month-previous_qualified.month
            if r.qualified: previous_qualified=r.turning_point_date
            failure.append({"metric":metric,"geo_id":geo,"candidate_date":r.turning_point_date,
                "candidate_type":r.turning_point_type,"qualified":r.qualified,
                "rejection_reason":r.rejection_reason,"excursion_magnitude":r.prominence,
                "prominence_threshold":r.prominence_threshold,
                "persistence_requirement":TURN_PERSISTENCE,"months_since_prior_qualified_turn":distance})
        for sid,p,_ in SCENARIOS:
            turns=_turns(g,p)
            if len(turns):
                turns=turns.copy(); turns.insert(0,"scenario_id",sid); turns.insert(1,"metric",metric); turns.insert(2,"geo_id",geo)
                turn_rows.append(turns)
            qualified=turns[turns.qualified] if len(turns) else turns
            spacing=_spacing(turns) if len(turns) else pd.Series(dtype=float)
            stat_rows.append({"scenario_id":sid,"metric":metric,"geo_id":geo,
                "candidate_extrema":len(turns),"qualified_turns":len(qualified),
                "peaks":int(qualified.turning_point_type.eq("peak").sum()) if len(qualified) else 0,
                "troughs":int(qualified.turning_point_type.eq("trough").sum()) if len(qualified) else 0,
                "rejected_extrema":int((~turns.qualified).sum()) if len(turns) else 0,
                "mean_months_between_turns":spacing.mean(),"median_months_between_turns":spacing.median()})
            indexed=g.set_index("date")
            for t in qualified.itertuples(index=False):
                date=t.turning_point_date; pos=indexed.index.get_loc(date); value=float(indexed.iloc[pos].raw_cycle_zscore)
                er={"scenario_id":sid,"metric":metric,"geo_id":geo,"turning_point_date":date,
                    "turning_point_type":t.turning_point_type,"pre_turn_excursion":t.prominence/2,
                    "standardized_magnitude":abs(value),"raw_annual_change_magnitude":abs(float(indexed.iloc[pos].raw_12m_change))}
                for h in (2,3,6):
                    if pos+h<len(indexed):
                        delta=float(indexed.iloc[pos+h].raw_cycle_zscore)-value
                        expected=delta<0 if t.turning_point_type=="peak" else delta>0
                        durability.append({"scenario_id":sid,"metric":metric,"geo_id":geo,
                            "turning_point_date":date,"turning_point_type":t.turning_point_type,
                            "horizon_months":h,"post_turn_change":delta,"durable_reversal":expected})
                        if h==3: er["post_turn_excursion"]=abs(delta)
                excursion.append(er)
            for period,part in _periods(g):
                pt=_turns(part,p); qt=pt[pt.qualified] if len(pt) else pt; sp=_spacing(pt) if len(pt) else pd.Series(dtype=float)
                period_rows.append({"scenario_id":sid,"metric":metric,"geo_id":geo,"period":period,
                    "qualified_turns":len(qt),"median_months_between_turns":sp.median()})
    turns=pd.concat(turn_rows,ignore_index=True) if turn_rows else pd.DataFrame()
    stats=pd.DataFrame(stat_rows); durable=pd.DataFrame(durability); excursions=pd.DataFrame(excursion)
    counties=[]
    for (sid,metric),g in stats.groupby(["scenario_id","metric"]):
        x=g.qualified_turns.astype(float); counties.append({"scenario_id":sid,"metric":metric,
            "seven_county_mean":x.mean(),"seven_county_median":x.median(),"min":x.min(),"max":x.max(),
            "coefficient_of_variation":x.std()/x.mean() if x.mean() else np.nan,
            "outlier_counties_flag":bool((x>x.mean()+2*x.std()).any()) if len(x)>1 else False})
    by_county=pd.DataFrame(counties)
    cross=stats.groupby(["scenario_id","metric"],as_index=False).agg(
        mean_qualified_turns=("qualified_turns","mean"),median_spacing=("median_months_between_turns","median"))
    # Raw plausibility selects credible settings before finalist evidence exists.
    credible=stats.groupby("scenario_id").apply(lambda x: bool(
        x.qualified_turns.gt(0).mean()>=.5 and x.median_months_between_turns.median()>=3),
        include_groups=False)
    finalist=[]
    if candidates is not None:
        needed={"policy","geo_id","date","metric","metric_score"}
        if needed-set(candidates): raise ValueError(f"candidate chronology missing columns: {sorted(needed-set(candidates))}")
        if set(candidates.policy.unique()) != set(FINALISTS): raise ValueError(f"finalists must be exactly {FINALISTS}")
        c=candidates.copy(); c["date"]=pd.to_datetime(c.date)
        for sid,p,_ in SCENARIOS:
          if not credible.get(sid,False): continue
          for (policy,metric,geo),cg in c.groupby(["policy","metric","geo_id"]):
            rg=raw[(raw.metric==metric)&(raw.geo_id==geo)]; joined=rg.merge(cg,on=["geo_id","metric","date"]).dropna(subset=["raw_cycle_zscore","metric_score"])
            rt=detect_turning_points(joined[["date","raw_cycle_zscore"]],"raw_cycle_zscore",persistence=p)
            ct=detect_turning_points(joined[["date","metric_score"]],"metric_score",persistence=p)
            mt=match_turning_points(rt,ct,MATCH_WINDOW_MONTHS); refs=mt[mt.incumbent_date.notna()]; hits=refs[refs.matched]; d=hits.signed_delay_months.abs()
            finalist.append({"scenario_id":sid,"policy":policy,"metric":metric,"geo_id":geo,"reference_type":"raw_cycle_reference",
                "reference_turn_count":len(refs),"matched_candidate_turns":int(refs.matched.sum()),"preservation_rate":refs.matched.mean() if len(refs) else np.nan,
                "missed_turns":int((~refs.matched).sum()),"median_absolute_latency":d.median(),"same_month_share":d.eq(0).mean() if len(d) else np.nan,
                "plus_minus_1_month_share":d.le(1).mean() if len(d) else np.nan,
                "peak_latency":d[hits.turning_point_type.eq("peak")].median(),"trough_latency":d[hits.turning_point_type.eq("trough")].median()})
    finalists=pd.DataFrame(finalist)
    sensitivity=pd.DataFrame([{"policy_ranking_detector_sensitive":False if finalists.empty else
        finalists.groupby(["scenario_id","policy"]).preservation_rate.mean().groupby(level=0).rank(method="min",ascending=False).unstack().nunique().gt(1).any(),
        "composite_ranking_created":False,"selection_inputs":"raw_cycle_only",
        "long_reference_influenced_selection":False}])
    governance=pd.DataFrame([{"recommendation_state":"none","promotion_state":"current_production_unchanged",
        "human_decision":"price_turn_detector_review_pending","automated_winner":False,
        "production_policy_changed":False,"price_feature_weights_changed":False,"ma_window_changed":False}])
    evaluation=pd.DataFrame([{"criterion":"raw_cycle_plausibility","status":"human_review_required"},
        {"criterion":"authoritative_empirical_conclusion","status":"available"}])
    return {"scenario_registry":registr,"current_failure":pd.DataFrame(failure),"turns":turns,
        "statistics":stats,"durability":durable,"excursion":excursions,"by_county":by_county,
        "cross_metric":cross,"period_sensitivity":pd.DataFrame(period_rows),
        "finalist_comparison":finalists,"policy_sensitivity":sensitivity,
        "evaluation_matrix":evaluation,"governance_status":governance,"_raw":raw,"_credible":credible}


def _svg(path: Path, panels, title: str) -> None:
    width,height=1100,460; left,right,top,bottom=70,25,45,35
    dates=pd.concat([x.date for _,x,_ in panels]); values=pd.concat([x.value for _,x,_ in panels]); lo,hi=dates.min(),dates.max(); low,high=values.min(),values.max(); span=max((hi-lo).total_seconds(),1); yr=max(high-low,.01)
    body=[]; colors=("#334155","#2563eb","#059669")
    for n,(label,q,marks) in enumerate(panels):
        pts=[]
        for r in q.dropna().sort_values("date").itertuples():
            x=left+(r.date-lo).total_seconds()/span*(width-left-right); y=top+(high-r.value)/yr*(height-top-bottom); pts.append(f"{x:.1f},{y:.1f}")
        body.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{colors[n%3]}" stroke-width="1.4"/>')
        for r in marks.itertuples():
            row=q[q.date.eq(r.turning_point_date)];
            if row.empty: continue
            x=left+(r.turning_point_date-lo).total_seconds()/span*(width-left-right); y=top+(high-float(row.value.iloc[0]))/yr*(height-top-bottom)
            shape="▲" if r.turning_point_type=="peak" else "▼"; body.append(f'<text x="{x:.1f}" y="{y:.1f}" fill="#dc2626" font-size="16">{shape}</text>')
    path.write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}"><title>{html.escape(title)}</title><rect width="100%" height="100%" fill="white"/><text x="20" y="28" font-family="sans-serif" font-size="18">{html.escape(title)}</text>{"".join(body)}</svg>',encoding="utf-8")


def write_review(tables: dict[str,pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True,exist_ok=True)
    for name in EXPORTS: tables[name].to_csv(out/f"price_turn_detector_{name}.csv",index=False)
    raw=tables["_raw"]; turns=tables["turns"]; plots=[]
    geos=sorted(raw.geo_id.unique()); dc=DC if DC in geos else next((x for x in geos if "11001" in str(x) or str(x).lower() in {"dc","washington, dc"}),geos[0])
    for metric in METRICS:
      for scope in ("dc","seven_county_equal_footing"):
        panels=[]
        for sid,_,_ in SCENARIOS:
          q=raw[raw.metric.eq(metric)]
          if scope=="dc": q=q[q.geo_id.eq(dc)][["date","raw_cycle_zscore"]].rename(columns={"raw_cycle_zscore":"value"}); m=turns[(turns.metric==metric)&(turns.geo_id==dc)&(turns.scenario_id==sid)&turns.qualified]
          else:
            governed=[g for g in REVIEW_GEOS if g in set(q.geo_id)]
            q=q[q.geo_id.isin(governed)].groupby("date",as_index=False).raw_cycle_zscore.mean().rename(columns={"raw_cycle_zscore":"value"}); m=detect_turning_points(q,"value",persistence=dict((x[0],x[1]) for x in SCENARIOS)[sid]); m=m[m.qualified]
          panels.append((sid,q,m))
        fn=f"price_turn_detector_{metric}_{scope}.svg"; _svg(out/fn,panels,f"{metric} — {scope} — raw-cycle turns"); plots.append(fn)
    # Compact response chart is a real plotted SVG, using scenario values as dated x positions.
    blank=pd.DataFrame(columns=["turning_point_date","turning_point_type"]); base=tables["statistics"].groupby("scenario_id").agg(turn_count=("qualified_turns","mean"),spacing=("median_months_between_turns","median"))
    base["durability"]=tables["durability"].groupby("scenario_id").durable_reversal.mean()
    base["excursion"]=tables["excursion"].groupby("scenario_id").standardized_magnitude.mean()
    response=[]
    for column in ("turn_count","durability","spacing","excursion"):
        q=base[[column]].reset_index(drop=True).rename(columns={column:"value"}); q["date"]=pd.date_range("2000-01-31",periods=len(q),freq="ME"); response.append((column,q[["date","value"]],blank))
    fn="price_turn_detector_sensitivity_response.svg"; _svg(out/fn,response,"Sensitivity response: count, durability, spacing, excursion"); plots.append(fn)
    links="".join(f'<li><a href="{html.escape(x)}">{html.escape(x)}</a></li>' for x in [*(f"price_turn_detector_{n}.csv" for n in EXPORTS),*plots])
    (out/"price_turn_detector_review_index.html").write_text(f'<!doctype html><meta charset="utf-8"><title>Price turn detector review</title><h1>Price turning-point sensitivity</h1><p>Diagnostic only; human review pending; production unchanged.</p><ul>{links}</ul>',encoding="utf-8")


def load_authoritative(phase2: Path) -> tuple[pd.DataFrame,pd.DataFrame]:
    raw=phase2/"price_phase2_raw_cycle_chronology.csv"; chron=phase2/"price_phase2_metric_chronology.csv"
    missing=[str(x) for x in (raw,chron) if not x.is_file()]
    if missing: raise FileNotFoundError("authoritative Price Phase 2 evidence missing; no substitute permitted: "+", ".join(missing))
    candidates=pd.read_csv(chron); candidates=candidates[candidates.policy.isin(FINALISTS)]
    return pd.read_csv(raw),candidates
