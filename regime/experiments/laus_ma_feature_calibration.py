"""Bounded diagnostic-only LAUS MA x feature-weight calibration.

The builder varies only MA6/MA9 and governed Level/Short/Long weights. It reuses
shared calendar smoothing, normalization, as-of alignment, dimension scoring,
and Demand-axis contribution functions and has no production write path.
"""
from __future__ import annotations

from pathlib import Path
import html

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points
from regime.experiments import laus_feature_architecture as shared
from regime.experiments.demand_signal_attenuation import (
    GEOS, LABOR, STRUCTURAL, _col, _contribution_layer, _load, _scope,
    cancellation, effective_contributions, recent_36,
)
from regime.experiments.laus_ma_window_calibration import (
    _calibration_contract, align_challenger_laus_scores,
)
from regime.experiments.structural_cyclical_demand_architecture import (
    _metric_weights, realized_metric_weights,
)

RUN_ID = "macro_regime_v1_0_1_candidate_20260810"
MA_WINDOWS = (6, 9)
FEATURE_WEIGHTS = {
    "W0": (0.25, 0.35, 0.40), "W1": (0.40, 0.25, 0.35),
    "W2": (0.50, 0.20, 0.30), "W3": (0.60, 0.15, 0.25),
    "W4": (0.70, 0.15, 0.15), "W5": (0.80, 0.10, 0.10),
}
LABELS = {p: f"{p} — {int(w[0]*100)}/{int(w[1]*100)}/{int(w[2]*100)}"
          for p, w in FEATURE_WEIGHTS.items()}
GOVERNANCE = {"recommendation_state":"none",
    "promotion_state":"current_production_unchanged",
    "human_decision":"calibration_pending", "automated_winner":False,
    "production_policy_changed":False}
FIXED_LABOR_MEMBERSHIP = "LF-IN"
FIXED_BALANCE = "BAL-S25-C75"
FEATURES = ("level", "short", "long")
DC = "district_of_columbia_dc__county"
REQUIRED_EXPORTS = (
    "laus_ma_feature_scenario_registry", "laus_ma_feature_metric_chronology",
    "laus_ma_feature_contributions", "laus_ma_feature_metric_statistics",
    "laus_ma_feature_cyclical_statistics", "laus_ma_feature_core_demand_statistics",
    "laus_ma_feature_demand_axis_statistics", "laus_ma_feature_ma_marginal_effects",
    "laus_ma_feature_weight_response_curve", "laus_ma_feature_interactions",
    "laus_ma_feature_by_county", "laus_ma_feature_evaluation_matrix",
    "laus_ma_feature_governance_status",
)
VISUAL_FAMILIES = (
    "raw_ma_dc", "raw_ma_seven_county_standardized", "feature_scores_by_metric_ma",
    "representative_feature_contributions", "cyclical_ma_and_weight_fixed",
    "core_demand_ma_and_weight_fixed", "response_surfaces",
)


def scenario_registry() -> pd.DataFrame:
    rows=[]
    for ma in MA_WINDOWS:
        for policy, weights in FEATURE_WEIGHTS.items():
            rows.append({"scenario_id":f"MA{ma}__{policy}", "ma_policy":f"MA{ma}",
                "ma_months":ma, "weight_policy":policy, "policy_label":LABELS[policy],
                **{f"{f}_weight":w for f,w in zip(FEATURES,weights)},
                "labor_force_membership":FIXED_LABOR_MEMBERSHIP,
                "balance_policy":FIXED_BALANCE, **GOVERNANCE})
    out=pd.DataFrame(rows); expected={f"MA{m}__W{i}" for m in MA_WINDOWS for i in range(6)}
    if len(out)!=12 or set(out.scenario_id)!=expected: raise AssertionError("exactly 12 factor scenarios required")
    if not np.allclose(out[[f+"_weight" for f in FEATURES]].sum(axis=1),1,atol=0,rtol=0):
        raise AssertionError("feature weights must sum exactly to one")
    return out


def construct_laus_features(source: pd.DataFrame, ma_months: int) -> pd.DataFrame:
    if ma_months not in MA_WINDOWS: raise ValueError(f"ma_months must be one of {MA_WINDOWS}")
    return shared._features(source,ma_months)


def metric_chronology(source: pd.DataFrame) -> pd.DataFrame:
    chunks=[]
    for row in scenario_registry().itertuples(index=False):
        frame=shared._chronology(source,row.scenario_id,row.ma_months,
            tuple(getattr(row,f+"_weight") for f in FEATURES)).rename(columns={"policy":"scenario_id"})
        available=frame[[f+"_score" for f in FEATURES]].notna()
        frame["available_feature_weight_sum"]=sum(available[f+"_score"]*getattr(row,f+"_weight") for f in FEATURES)
        chunks.append(frame)
    return pd.concat(chunks,ignore_index=True)


def require_authoritative_run(run: Path) -> Path:
    run=run.resolve()
    if run.name!=RUN_ID: raise ValueError(f"authoritative run identity must be {RUN_ID}")
    if not run.is_dir(): raise FileNotFoundError(f"authoritative persisted run unavailable: {run}")
    return run


def _turn_count(g: pd.DataFrame, value: str) -> int:
    found=detect_turning_points(g[["date",value]].dropna().sort_values("date"),value)
    return int(found.qualified.eq(True).sum()) if len(found) else 0


def _stats(g: pd.DataFrame, value: str) -> dict[str,float]:
    q=g[["date",value]].dropna().sort_values("date"); x=q[value]; d=x.diff(); direction=np.sign(d).replace(0,np.nan).dropna()
    signs=np.sign(x).replace(0,np.nan).dropna()
    return {"observations":len(x), "standard_deviation":x.std(), "range":x.max()-x.min(),
        "mean_absolute_monthly_change":d.abs().mean(),
        "reversal_count":int(max(0,direction.ne(direction.shift()).sum()-1)),
        "turning_point_count":_turn_count(q,value),
        "zero_crossing_count":int(max(0,signs.ne(signs.shift()).sum()-1)),
        "persistence":float(direction.eq(direction.shift()).iloc[1:].mean()) if len(direction)>1 else np.nan}


def _periods(frame: pd.DataFrame):
    yield "full_history",frame
    yield "2022_plus",frame.loc[pd.to_datetime(frame.date)>=pd.Timestamp("2022-01-01")]
    yield "latest_36_months",recent_36(frame)


def _series_statistics(frame: pd.DataFrame, value: str, keys: list[str], extras=None) -> pd.DataFrame:
    rows=[]
    for period,q in _periods(frame):
        for key,g in q.groupby(keys,dropna=False):
            key=(key,) if not isinstance(key,tuple) else key
            row=dict(zip(keys,key)); row.update({"period":period,**_stats(g,value)})
            if extras: row.update(extras(g))
            rows.append(row)
    return pd.DataFrame(rows)


def _raw_ma(source: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    panels=[]
    for geo_metric,g in source.groupby(["geo_id","canonical_metric_key"]):
        calendar=pd.DataFrame({"date":pd.date_range(g.date.min(),g.date.max(),freq="ME")})
        q=calendar.merge(g[["date","raw_value"]],on="date",how="left").sort_values("date")
        q["geo_id"],q["metric"]=geo_metric
        for ma in MA_WINDOWS:
            f=construct_laus_features(source.loc[(source.geo_id==geo_metric[0]) &
                (source.canonical_metric_key==geo_metric[1])],ma)
            level=f.loc[f.feature_type.eq("level"),["date","raw_feature_value"]]
            q=q.merge(level.rename(columns={"raw_feature_value":f"ma{ma}"}),on="date",how="left")
        panels.append(q)
    panel=pd.concat(panels,ignore_index=True)
    rows=[]
    for (geo,metric),g in panel.groupby(["geo_id","metric"]):
        raw=_stats(g.rename(columns={"raw_value":"value"}),"value")
        for series,col in (("raw","raw_value"),("ma6","ma6"),("ma9","ma9")):
            s=_stats(g.rename(columns={col:"value"}),"value"); s.update({"geo_id":geo,"metric":metric,"series":series})
            s["correlation_to_raw"]=g[col].corr(g.raw_value)
            for k in ("standard_deviation","range","mean_absolute_monthly_change","reversal_count","turning_point_count"):
                s[f"attenuation_from_raw_{k}"]=s[k]/raw[k] if raw[k] else np.nan
            rows.append(s)
    return panel,pd.DataFrame(rows)


def _build(run: Path, root: Path):
    mr,ar=_calibration_contract(root); source=shared._source(run); registry=scenario_registry()
    persisted=_load(run,"aligned_metric_scores").rename(columns={
        _col(_load(run,"aligned_metric_scores"),"canonical_metric_key","metric_key","metric"):"metric",
        _col(_load(run,"aligned_metric_scores"),"aligned_metric_score","metric_score","score"):"score"})
    persisted["metric"]=persisted.metric.replace({"laus_labor_force":"labor_force","laus_employment":"employment"})
    persisted=_scope(persisted,"aligned_metric_scores",["geo_id","date","metric"])
    metric=metric_chronology(source); aligned=[]
    for sid,g in metric.groupby("scenario_id"):
        q=align_challenger_laus_scores(g,persisted).rename(columns={"score":"metric_score","metric_date":"source_date"})
        aligned.append(q.assign(scenario_id=sid))
    aligned=pd.concat(aligned,ignore_index=True)
    mechanics=metric.drop(columns=["metric_score"],errors="ignore")
    metric=aligned.merge(mechanics,on=["scenario_id","geo_id","date","metric"],how="left",validate="one_to_one")
    meta=registry[["scenario_id","ma_months","weight_policy","policy_label"]]
    metric=metric.merge(meta,on="scenario_id",validate="many_to_one")
    contributions=[]
    for f in FEATURES:
        cols=["scenario_id","geo_id","date","metric","source_date","metric_age_days",
              "available_feature_weight_sum",f+"_score",f+"_contribution",
              "configured_"+f+"_weight","effective_"+f+"_weight"]
        q=metric[cols].rename(columns={f+"_score":"normalized_feature_score",f+"_contribution":"weighted_contribution",
            "configured_"+f+"_weight":"configured_feature_weight","effective_"+f+"_weight":"effective_feature_weight"})
        contributions.append(q.assign(feature=f))
    contributions=pd.concat(contributions,ignore_index=True)
    base=_metric_weights(mr); structural=persisted.loc[persisted.metric.isin(STRUCTURAL),["geo_id","date","metric","score"]]
    downstream=[]
    for sid,labor in aligned.groupby("scenario_id"):
        weights=realized_metric_weights(base,FIXED_LABOR_MEMBERSHIP,FIXED_BALANCE)
        panel=pd.concat([structural,labor[["geo_id","date","metric","metric_score"]].rename(columns={"metric_score":"score"})])
        for (geo,date),g in panel.groupby(["geo_id","date"]):
            calc=effective_contributions(g.score,g.metric.map(weights)); q=g.assign(contribution=calc.weighted_feature_contribution.to_numpy())
            s=q.loc[q.metric.isin(STRUCTURAL),"contribution"]; c=q.loc[q.metric.isin(LABOR),"contribution"]
            _,_,cc=cancellation(c); _,_,corec=cancellation(q.contribution); ss=s.sum(min_count=1); cs=c.sum(min_count=1)
            downstream.append({"scenario_id":sid,"geo_id":geo,"date":date,"structural_score":ss,
                "cyclical_score":cs,"core_demand_score":q.contribution.sum(min_count=1),
                "cyclical_cancellation":cc,"structural_cyclical_cancellation":corec,
                "structural_cyclical_sign_agreement":np.sign(ss)==np.sign(cs)})
    downstream=pd.DataFrame(downstream).merge(meta,on="scenario_id",validate="many_to_one")
    dims=_load(run,"dimension_scores"); dims=dims.rename(columns={_col(dims,"dimension_score","score"):"score"})
    dims=_scope(dims,"dimension_scores",["geo_id","date","dimension"])
    fixed=dims.loc[dims.dimension.str.lower().isin(["price","affordability","capital_markets"])]
    axes=[]
    for sid,g in downstream.groupby("scenario_id"):
        inp=pd.concat([g[["geo_id","date","core_demand_score"]].rename(columns={"core_demand_score":"score"}).assign(dimension="demand"),fixed])
        _,monthly=_contribution_layer(inp,ar.loc[ar.axis.str.lower().eq("demand")],"axis","dimension","score",_col(ar,"dimension_weight","weight"))
        axes.append(monthly.loc[monthly.axis.str.lower().eq("demand"),["geo_id","date","net_score"]].assign(scenario_id=sid))
    axis=pd.concat(axes).rename(columns={"net_score":"demand_axis_score"})
    downstream=downstream.merge(axis,on=["scenario_id","geo_id","date"],validate="one_to_one")
    return registry,source,metric,contributions,downstream

def _agreement(a: pd.DataFrame,b: pd.DataFrame,value: str) -> dict[str,float]:
    q=a[["date",value]].merge(b[["date",value]],on="date",suffixes=("_a","_b")).dropna()
    if q.empty: return {"mean_absolute_score_difference":np.nan,"chronology_correlation":np.nan,
        "direction_agreement":np.nan,"sign_agreement":np.nan,"matched_turning_point_timing_months":np.nan}
    x,y=q[value+"_a"],q[value+"_b"]; dx,dy=x.diff(),y.diff()
    ta=detect_turning_points(q[["date",value+"_a"]].rename(columns={value+"_a":value}),value)
    tb=detect_turning_points(q[["date",value+"_b"]].rename(columns={value+"_b":value}),value)
    da=pd.PeriodIndex(pd.to_datetime(ta.loc[ta.qualified,"turning_point_date"]),freq="M").astype(int)
    db=pd.PeriodIndex(pd.to_datetime(tb.loc[tb.qualified,"turning_point_date"]),freq="M").astype(int)
    timing=float(np.mean([np.min(np.abs(db-i)) for i in da])) if len(da) and len(db) else np.nan
    return {"mean_absolute_score_difference":(x-y).abs().mean(),"chronology_correlation":x.corr(y),
        "direction_agreement":np.sign(dx).eq(np.sign(dy)).iloc[1:].mean(),
        "sign_agreement":np.sign(x).eq(np.sign(y)).mean(),"matched_turning_point_timing_months":timing}


def _comparisons(frame: pd.DataFrame,value: str,registry: pd.DataFrame):
    pooled=frame.groupby(["scenario_id","date"],as_index=False)[value].mean()
    stats=_series_statistics(pooled,value,["scenario_id"]); full=stats.loc[stats.period.eq("full_history")]
    ma=[]
    for policy in FEATURE_WEIGHTS:
        a=pooled.loc[pooled.scenario_id.eq(f"MA6__{policy}")]; b=pooled.loc[pooled.scenario_id.eq(f"MA9__{policy}")]
        sa=full.loc[full.scenario_id.eq(f"MA6__{policy}")].iloc[0]; sb=full.loc[full.scenario_id.eq(f"MA9__{policy}")].iloc[0]
        row={"weight_policy":policy,"from_scenario":f"MA6__{policy}","to_scenario":f"MA9__{policy}",**_agreement(b,a,value)}
        for col in ("standard_deviation","reversal_count","turning_point_count","persistence"):
            row[col+"_difference_ma9_minus_ma6"]=sb[col]-sa[col]
        ma.append(row)
    response=[]
    for horizon in MA_WINDOWS:
        for reference_kind,pairs in (("adjacent",zip(range(5),range(1,6))),
            ("relative_w0",((0,i) for i in range(6))), ("relative_w5",((5,i) for i in range(6)))):
            for left,right in pairs:
                a_id=f"MA{horizon}__W{left}"; b_id=f"MA{horizon}__W{right}"
                sa=full.loc[full.scenario_id.eq(a_id)].iloc[0]; sb=full.loc[full.scenario_id.eq(b_id)].iloc[0]
                row={"ma_months":horizon,"comparison_type":reference_kind,"from_scenario":a_id,"to_scenario":b_id,
                     **_agreement(pooled.loc[pooled.scenario_id.eq(b_id)],pooled.loc[pooled.scenario_id.eq(a_id)],value)}
                for col in ("standard_deviation","reversal_count","turning_point_count","persistence"):
                    row[col+"_change"]=sb[col]-sa[col]
                response.append(row)
    return pd.DataFrame(ma),pd.DataFrame(response),stats


def _with_equal_footing_pool(frame: pd.DataFrame, values: list[str],
                             standardize: tuple[str,...]=()) -> pd.DataFrame:
    """Append complete-seven-county monthly means without replacing DC rows."""
    keys=[c for c in ("scenario_id","ma_months","weight_policy","metric","date") if c in frame]
    work=frame.copy()
    for value in standardize:
        standardization_keys=[c for c in ("scenario_id","metric","geo_id") if c in work]
        work[value]=work.groupby(standardization_keys)[value].transform(
            lambda x:(x-x.mean())/x.std())
    complete=work.groupby(keys).filter(lambda g:g.geo_id.nunique()==len(GEOS))
    pooled=complete.groupby(keys,as_index=False)[values].mean(numeric_only=True)
    pooled["geo_id"]="seven_county_equal_footing"
    return pd.concat([frame,pooled],ignore_index=True,sort=False)


def marginal_effects(statistics: pd.DataFrame) -> pd.DataFrame:
    required={"ma_months","weight_policy"}
    if not required.issubset(statistics): raise ValueError(f"statistics missing keys: {sorted(required-set(statistics))}")
    numeric=[c for c in statistics.select_dtypes(include="number") if c!="ma_months"]
    group=[c for c in statistics if c not in numeric+["ma_months","scenario_id"]]
    left=statistics.loc[statistics.ma_months.eq(6),group+numeric]
    right=statistics.loc[statistics.ma_months.eq(9),group+numeric]
    paired=left.merge(right,on=group,suffixes=("_ma6","_ma9"),validate="one_to_one")
    for col in numeric: paired[f"{col}_difference_ma9_minus_ma6"]=paired.pop(f"{col}_ma9")-paired.pop(f"{col}_ma6")
    paired["comparison_id"]=paired.weight_policy.map(lambda p:f"MA6__{p}__to__MA9__{p}")
    return paired.sort_values(group,kind="stable").reset_index(drop=True)


def _plots(output: Path,source: pd.DataFrame,raw_panel: pd.DataFrame,metric: pd.DataFrame,
           contributions: pd.DataFrame,downstream: pd.DataFrame,metric_stats: pd.DataFrame,
           core_stats: pd.DataFrame):
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    visual=output/"visual_review"; visual.mkdir()
    colors=plt.cm.viridis(np.linspace(.05,.95,6))
    # Raw/MA: DC and county-first standardized equal-footing seven-county pool.
    for name,g in raw_panel.groupby("metric"):
        for scope,frame in (("dc",g.loc[g.geo_id.eq(DC)]),("seven_county_standardized",g)):
            q=frame.copy()
            if scope.startswith("seven"):
                for col in ("raw_value","ma6","ma9"):
                    q[col]=q.groupby("geo_id")[col].transform(lambda x:(x-x.mean())/x.std())
                q=q.groupby("date").filter(lambda x:x.geo_id.nunique()==7).groupby("date",as_index=False)[["raw_value","ma6","ma9"]].mean()
            fig,ax=plt.subplots(figsize=(10,4))
            for col,label in (("raw_value","Raw"),("ma6","MA6"),("ma9","MA9")): ax.plot(q.date,q[col],label=label)
            ax.set_title(f"{scope} — {name} — Raw / MA6 / MA9"); ax.legend(); fig.tight_layout()
            fig.savefig(visual/f"raw_ma__{scope}__{name}.svg"); plt.close(fig)
    pooled=metric.groupby(["scenario_id","ma_months","weight_policy","policy_label","metric","date"],as_index=False).metric_score.mean()
    for (name,ma),q in pooled.groupby(["metric","ma_months"]):
        fig,ax=plt.subplots(figsize=(10,4))
        for (policy,label),x in q.groupby(["weight_policy","policy_label"]): ax.plot(x.date,x.metric_score,label=label,color=colors[int(policy[1:])])
        ax.set_title(f"{name} — MA{ma} feature-score policies"); ax.legend(ncol=2); fig.tight_layout()
        fig.savefig(visual/f"feature_scores__{name}__MA{ma}.svg"); plt.close(fig)
    rep=contributions.merge(scenario_registry()[["scenario_id","ma_months","weight_policy"]])
    rep=rep.loc[rep.weight_policy.isin(["W0","W2","W3","W5"])].groupby(["ma_months","weight_policy","metric","feature","date"],as_index=False).weighted_contribution.mean()
    for (ma,policy,name),q in rep.groupby(["ma_months","weight_policy","metric"]):
        fig,ax=plt.subplots(figsize=(10,4))
        for feature,x in q.groupby("feature"): ax.plot(x.date,x.weighted_contribution,label=feature.title())
        ax.set_title(f"{name} — MA{ma} {LABELS[policy]} contributions"); ax.legend(); fig.tight_layout()
        fig.savefig(visual/f"contributions__{name}__MA{ma}__{policy}.svg"); plt.close(fig)
    pool=downstream.groupby(["scenario_id","ma_months","weight_policy","date"],as_index=False)[["cyclical_score","core_demand_score"]].mean()
    for value in ("cyclical_score","core_demand_score"):
        for ma,q in pool.groupby("ma_months"):
            fig,axes=plt.subplots(3,2,figsize=(12,9),sharex=True)
            for policy,ax in zip(FEATURE_WEIGHTS,axes.flat):
                x=q.loc[q.weight_policy.eq(policy)]; ax.plot(x.date,x[value]); ax.set_title(LABELS[policy])
            fig.suptitle(f"{value} — MA{ma}, weight-fixed panels"); fig.tight_layout(); fig.savefig(visual/f"downstream__{value}__MA{ma}__weights.svg"); plt.close(fig)
        fig,axes=plt.subplots(3,2,figsize=(12,9),sharex=True)
        for policy,ax in zip(FEATURE_WEIGHTS,axes.flat):
            for ma,x in pool.loc[pool.weight_policy.eq(policy)].groupby("ma_months"): ax.plot(x.date,x[value],label=f"MA{ma}")
            ax.set_title(LABELS[policy]); ax.legend()
        fig.suptitle(f"{value} — MA6 vs MA9 at fixed weight"); fig.tight_layout(); fig.savefig(visual/f"downstream__{value}__ma_fixed.svg"); plt.close(fig)
    surface=metric_stats.loc[metric_stats.period.eq("full_history")].groupby(["ma_months","weight_policy"],as_index=False).mean(numeric_only=True)
    core=core_stats.loc[core_stats.period.eq("full_history")].groupby(["ma_months","weight_policy"],as_index=False).mean(numeric_only=True)
    measures=(("reversal_count",surface),("persistence",surface),("standard_deviation",surface),
              ("turning_point_count",surface),("raw_chronology_correlation",surface),
              ("core_demand_reversal_count",core.rename(columns={"reversal_count":"core_demand_reversal_count"})))
    for measure,data in measures:
        table=data.pivot(index="ma_months",columns="weight_policy",values=measure).reindex(columns=FEATURE_WEIGHTS)
        fig,ax=plt.subplots(figsize=(8,2.5)); im=ax.imshow(table,aspect="auto",cmap="viridis")
        ax.set_xticks(range(6),FEATURE_WEIGHTS); ax.set_yticks(range(2),["MA6","MA9"])
        for i in range(2):
            for j in range(6): ax.text(j,i,f"{table.iloc[i,j]:.2f}",ha="center",va="center",color="white")
        ax.set_title(measure.replace("_"," ").title()); fig.colorbar(im,ax=ax); fig.tight_layout(); fig.savefig(visual/f"surface__{measure}.svg"); plt.close(fig)

def build_review(run: Path,output: Path,root: Path|None=None) -> Path:
    """Generate every governed evidence table and visual, or fail before output."""
    run=require_authoritative_run(run); root=(root or Path(__file__).resolve().parents[2]).resolve()
    registry,source,metric,contributions,downstream=_build(run,root)
    if set(metric.geo_id)!=set(GEOS): raise ValueError("exact seven-county basis was not preserved")
    raw_panel,raw_stats=_raw_ma(source)
    raw=source.rename(columns={"canonical_metric_key":"metric"})
    metric=metric.merge(raw[["geo_id","date","metric","raw_value"]],on=["geo_id","date","metric"],how="left",validate="many_to_one")
    metric_analysis=_with_equal_footing_pool(metric,["metric_score","raw_value"],
        standardize=("raw_value",))
    baselines=metric_analysis.pivot_table(index=["geo_id","date","metric"],columns="scenario_id",values="metric_score")
    def metric_extra(g):
        idx=pd.MultiIndex.from_frame(g[["geo_id","date","metric"]]); base=baselines.reindex(idx)
        return {"raw_chronology_correlation":g.metric_score.corr(g.raw_value),
            "w0_baseline_correlation":g.metric_score.corr(base[f"MA{int(g.ma_months.iloc[0])}__W0"].to_numpy()),
            "production_w5_correlation":g.metric_score.corr(base["MA9__W5"].to_numpy())}
    metric_stats=_series_statistics(metric_analysis,"metric_score",["scenario_id","ma_months","weight_policy","geo_id","metric"],metric_extra)
    downstream_analysis=_with_equal_footing_pool(downstream,["structural_score","cyclical_score","core_demand_score",
        "demand_axis_score","cyclical_cancellation","structural_cyclical_cancellation",
        "structural_cyclical_sign_agreement"])
    cyc_stats=_series_statistics(downstream_analysis,"cyclical_score",["scenario_id","ma_months","weight_policy","geo_id"],
        lambda g:{"cancellation":g.cyclical_cancellation.mean(),"sign_changes":_stats(g,"cyclical_score")["zero_crossing_count"],
            "chronology_correlation":g.cyclical_score.corr(g.structural_score),
            "direction_agreement":np.sign(g.cyclical_score.diff()).eq(np.sign(g.structural_score.diff())).iloc[1:].mean()})
    core_stats=_series_statistics(downstream_analysis,"core_demand_score",["scenario_id","ma_months","weight_policy","geo_id"],
        lambda g:{"structural_cyclical_cancellation":g.structural_cyclical_cancellation.mean(),
            "sign_agreement":g.structural_cyclical_sign_agreement.mean(),
            "direction_agreement":np.sign(g.core_demand_score.diff()).eq(np.sign(g.cyclical_score.diff())).iloc[1:].mean(),
            "cyclical_to_core_amplitude_retention":g.core_demand_score.std()/g.cyclical_score.std() if g.cyclical_score.std() else np.nan,
            "cyclical_to_core_reversal_retention":_stats(g,"core_demand_score")["reversal_count"]/_stats(g,"cyclical_score")["reversal_count"] if _stats(g,"cyclical_score")["reversal_count"] else np.nan,
            "cyclical_to_core_turning_point_retention":_turn_count(g,"core_demand_score")/_turn_count(g,"cyclical_score") if _turn_count(g,"cyclical_score") else np.nan})
    axis_stats=_series_statistics(downstream_analysis,"demand_axis_score",["scenario_id","ma_months","weight_policy","geo_id"],
        lambda g:{"core_demand_to_axis_amplitude_retention":g.demand_axis_score.std()/g.core_demand_score.std() if g.core_demand_score.std() else np.nan,
            "core_demand_to_axis_reversal_retention":_stats(g,"demand_axis_score")["reversal_count"]/_stats(g,"core_demand_score")["reversal_count"] if _stats(g,"core_demand_score")["reversal_count"] else np.nan,
            "direction_agreement":np.sign(g.demand_axis_score.diff()).eq(np.sign(g.core_demand_score.diff())).iloc[1:].mean()})
    ma_effects,response,pooled_core_stats=_comparisons(downstream,"core_demand_score",registry)
    # Difference-in-differences exposes whether the W0->Wx effect changes with MA.
    interactions=[]
    for policy in FEATURE_WEIGHTS:
        for measure in ("standard_deviation","reversal_count","turning_point_count","persistence"):
            q=pooled_core_stats.loc[(pooled_core_stats.period=="full_history")]
            def val(ma,p): return q.loc[q.scenario_id.eq(f"MA{ma}__{p}"),measure].iloc[0]
            interactions.append({"weight_policy":policy,"measure":measure,
                "ma6_weight_effect_from_w0":val(6,policy)-val(6,"W0"),
                "ma9_weight_effect_from_w0":val(9,policy)-val(9,"W0"),
                "interaction_difference_in_differences":(val(9,policy)-val(9,"W0"))-(val(6,policy)-val(6,"W0"))})
    interactions=pd.DataFrame(interactions)
    by_county=metric_stats.merge(core_stats,on=["scenario_id","ma_months","weight_policy","geo_id","period"],suffixes=("_metric","_core"),how="outer")
    evaluation=core_stats.loc[core_stats.period.eq("full_history")].merge(
        metric_stats.loc[metric_stats.period.eq("full_history")].groupby(["scenario_id","ma_months","weight_policy"],as_index=False).mean(numeric_only=True),
        on=["scenario_id","ma_months","weight_policy"],suffixes=("_core","_metric"))
    governance=pd.DataFrame([{**GOVERNANCE,"authoritative_run":RUN_ID,"decision_basis":"seven_county_equal_footing",
        "governed_county_count":7,"dc_included":True,"labor_force_membership":FIXED_LABOR_MEMBERSHIP,
        "balance_policy":FIXED_BALANCE}])
    exports={"laus_ma_feature_scenario_registry":registry,
        "laus_ma_feature_metric_chronology":metric,
        "laus_ma_feature_contributions":contributions,
        "laus_ma_feature_metric_statistics":metric_stats,
        "laus_ma_feature_cyclical_statistics":cyc_stats,
        "laus_ma_feature_core_demand_statistics":core_stats,
        "laus_ma_feature_demand_axis_statistics":axis_stats,
        "laus_ma_feature_ma_marginal_effects":ma_effects,
        "laus_ma_feature_weight_response_curve":response,
        "laus_ma_feature_interactions":interactions,
        "laus_ma_feature_by_county":by_county,
        "laus_ma_feature_evaluation_matrix":evaluation,
        "laus_ma_feature_governance_status":governance,
        "laus_ma_feature_raw_ma_statistics":raw_stats}
    missing=set(REQUIRED_EXPORTS)-set(exports)
    if missing: raise AssertionError(f"required evidence exports not implemented: {sorted(missing)}")
    output=output.resolve()
    if output.exists(): raise FileExistsError(f"review output already exists: {output}")
    output.mkdir(parents=True)
    for name,frame in exports.items(): frame.to_csv(output/f"{name}.csv",index=False)
    _plots(output,source,raw_panel,metric,contributions,downstream,metric_stats,core_stats)
    links="".join(f'<li><a href="{html.escape(name)}.csv">{html.escape(name)}.csv</a></li>' for name in exports)
    visuals="".join(f'<li><a href="visual_review/{html.escape(path.name)}">{html.escape(path.name)}</a></li>' for path in sorted((output/"visual_review").glob("*.svg")))
    (output/"laus_ma_feature_review.html").write_text("<!doctype html><meta charset='utf-8'><title>LAUS MA × feature-weight review</title><h1>LAUS MA × feature-weight review</h1><p>Diagnostic only. No recommendation, automated winner, or production change.</p><h2>Evidence</h2><ul>"+links+"</ul><h2>Visuals</h2><ul>"+visuals+"</ul>",encoding="utf-8")
    return output


__all__=["FEATURE_WEIGHTS","FIXED_BALANCE","FIXED_LABOR_MEMBERSHIP","GOVERNANCE",
    "MA_WINDOWS","REQUIRED_EXPORTS","RUN_ID","VISUAL_FAMILIES","align_challenger_laus_scores","build_review",
    "construct_laus_features","marginal_effects","metric_chronology",
    "require_authoritative_run","scenario_registry"]
