"""Persisted-evidence-only LAUS finalist stability and turn diagnostics.

This module is deliberately downstream of the long-weight calibration.  It
never constructs a LAUS feature or invokes a production run.  Its public
builder validates the immutable review bundle before creating an output
directory, so missing or incomplete evidence fails closed.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path
import html

import numpy as np
import pandas as pd

from regime.diagnostics.capital_markets_ma import detect_turning_points
from regime.experiments.demand_signal_attenuation import GEOS

POLICIES = {"B2": (.45, .15, .40), "B3": (.40, .15, .45),
            "L0": (.35, .20, .45), "L1": (.35, .15, .50)}
MA_WINDOWS = (6, 9)
SCENARIOS = tuple(f"MA{ma}__{policy}" for ma in MA_WINDOWS for policy in POLICIES)
SERIES = ("labor_force", "employment", "laus_unemployment_rate",
          "cyclical", "core_demand")
PERIODS = ("full_history", "2022_plus", "latest_36_months")
CONTROLLED_PAIRS = (("B2", "B3"), ("B3", "L1"), ("B3", "L0"), ("L0", "L1"))
GOVERNANCE = {"recommendation_state": "none",
    "promotion_state": "current_production_unchanged",
    "human_decision": "finalist_review_pending", "automated_winner": False,
    "production_policy_changed": False}
REQUIRED_INPUTS = ("laus_long_weight_metric_chronology.csv",
    "laus_long_weight_downstream_chronology.csv",
    "laus_long_weight_feature_anatomy.csv", "laus_long_weight_cyclical_statistics.csv",
    "laus_long_weight_core_demand_statistics.csv", "laus_long_weight_by_county.csv",
    "laus_long_weight_controlled_comparisons.csv",
    "laus_long_weight_family_response_curves.csv")
REQUIRED_EXPORTS = ("laus_finalist_scenario_registry", "laus_finalist_reversal_events",
    "laus_finalist_reversal_summary", "laus_finalist_turning_points",
    "laus_finalist_consensus_turns", "laus_finalist_turn_latency",
    "laus_finalist_turn_preservation", "laus_finalist_stability_pairwise",
    "laus_finalist_controlled_comparisons", "laus_finalist_ma_comparisons",
    "laus_finalist_cross_metric_consistency", "laus_finalist_by_county",
    "laus_finalist_period_sensitivity", "laus_finalist_evaluation_matrix",
    "laus_finalist_governance_status")


def scenario_registry() -> pd.DataFrame:
    rows = []
    for ma in MA_WINDOWS:
        for policy, weights in POLICIES.items():
            rows.append({"scenario_id": f"MA{ma}__{policy}", "ma_months": ma,
                "weight_policy": policy, "level_weight": weights[0],
                "short_weight": weights[1], "long_weight": weights[2],
                "labor_force_membership": "LF-IN", "balance_policy": "BAL-S25-C75",
                **GOVERNANCE})
    return pd.DataFrame(rows)


def validate_persisted_bundle(root: Path) -> dict[str, pd.DataFrame]:
    """Read required evidence and validate finalist identities by factors."""
    root = root.resolve()
    missing = [name for name in REQUIRED_INPUTS if not (root / name).is_file()]
    if missing:
        raise FileNotFoundError(f"required persisted LAUS evidence absent: {missing}")
    frames = {name[:-4]: pd.read_csv(root / name) for name in REQUIRED_INPUTS}
    empty = [name for name, frame in frames.items() if frame.empty]
    if empty:
        raise ValueError(f"required persisted LAUS evidence empty: {empty}")
    chronology = frames["laus_long_weight_metric_chronology"]
    required = {"scenario_id", "geo_id", "date", "metric", "metric_score",
                "ma_months", "weight_policy"}
    if not required.issubset(chronology):
        raise ValueError(f"metric chronology missing columns: {sorted(required-set(chronology))}")
    registry = scenario_registry().set_index("scenario_id")
    observed = chronology.loc[chronology.scenario_id.isin(SCENARIOS),
        ["scenario_id", "ma_months", "weight_policy"]].drop_duplicates()
    if set(observed.scenario_id) != set(SCENARIOS) or len(observed) != 8:
        raise ValueError("persisted chronology does not contain exactly eight finalists")
    for row in observed.itertuples(index=False):
        expected = registry.loc[row.scenario_id]
        if row.ma_months != expected.ma_months or row.weight_policy != expected.weight_policy:
            raise ValueError(f"factor identity mismatch for {row.scenario_id}")
    downstream = frames["laus_long_weight_downstream_chronology"]
    downstream_required = {"scenario_id", "geo_id", "date", "cyclical_score",
                           "core_demand_score"}
    if not downstream_required.issubset(downstream):
        raise ValueError("downstream chronology missing columns: "
                         f"{sorted(downstream_required-set(downstream))}")
    finalists = downstream.loc[downstream.scenario_id.isin(SCENARIOS)]
    if set(finalists.scenario_id) != set(SCENARIOS):
        raise ValueError("downstream chronology does not contain exactly eight finalist identities")
    if set(finalists.geo_id) != set(GEOS):
        raise ValueError("downstream finalist chronology does not preserve governed geography scope")
    return frames


def _month_delta(later, earlier) -> int:
    later, earlier = pd.Timestamp(later), pd.Timestamp(earlier)
    return (later.year-earlier.year)*12 + later.month-earlier.month


def reversal_events(frame: pd.DataFrame, value: str = "score") -> pd.DataFrame:
    """Return continuous evidence for actual-level reversal recovery."""
    q = frame[["date", value]].dropna().copy().sort_values("date", kind="stable")
    q["date"] = pd.to_datetime(q.date)
    q["change"] = q[value].diff()
    q["direction"] = np.sign(q.change)
    std = q[value].std()
    median_change = q.change.abs().median()
    rows = []
    for i in range(2, len(q)):
        before, after = q.iloc[i-1], q.iloc[i]
        if before.direction == 0 or after.direction == 0 or before.direction == after.direction:
            continue
        baseline = float(before[value]); direction = int(after.direction)
        future = q.iloc[i+1:i+4]
        def undone(horizon):
            x = future.iloc[:horizon][value]
            return bool((x >= baseline).any()) if direction < 0 else bool((x <= baseline).any())
        def excursion(horizon):
            x = pd.concat([pd.Series([after[value]]), future.iloc[:max(0,horizon-1)][value]], ignore_index=True)
            extreme = x.min() if direction < 0 else x.max()
            return abs(float(extreme)-baseline)
        e2, e3 = excursion(2), excursion(3)
        tail = q.iloc[i+1:][value]
        recovered = ((tail >= baseline).any() if direction < 0 else (tail <= baseline).any())
        rows.append({"reversal_date": after.date, "score_at_reversal": after[value],
            "pre_reversal_level": baseline, "pre_reversal_monthly_direction": int(before.direction),
            "immediate_post_reversal_movement": after.change,
            "maximum_excursion_2m": e2, "maximum_excursion_3m": e3,
            "whipsaw_2m": undone(2), "durable_2m": not undone(2),
            "whipsaw_3m": undone(3), "durable_3m": not undone(3),
            "eventual_recovery": bool(recovered), "eventual_outcome": "recovery" if recovered else "continuation",
            "excursion_2m_share_std": e2/std if std else np.nan,
            "excursion_3m_share_std": e3/std if std else np.nan,
            "excursion_2m_to_median_abs_change": e2/median_change if median_change else np.nan,
            "excursion_3m_to_median_abs_change": e3/median_change if median_change else np.nan})
    return pd.DataFrame(rows)


def reversal_summary(events: pd.DataFrame) -> dict:
    total = len(events)
    return {"total_reversal_count": total,
        "whipsaw_2m_count": int(events.whipsaw_2m.sum()) if total else 0,
        "whipsaw_2m_share": float(events.whipsaw_2m.mean()) if total else np.nan,
        "durable_2m_count": int(events.durable_2m.sum()) if total else 0,
        "whipsaw_3m_count": int(events.whipsaw_3m.sum()) if total else 0,
        "whipsaw_3m_share": float(events.whipsaw_3m.mean()) if total else np.nan,
        "durable_3m_count": int(events.durable_3m.sum()) if total else 0}


def cluster_consensus_turns(points: pd.DataFrame, support: int = 6,
                            window_months: int = 3) -> pd.DataFrame:
    """Cluster same-type turns, counting each finalist at most once."""
    columns = ["consensus_id", "turning_point_type", "reference_date", "supporting_finalists",
        "earliest_detection", "latest_detection", "date_dispersion_months", "support_threshold"]
    if points.empty:
        return pd.DataFrame(columns=columns)
    work = points.copy(); work["turning_point_date"] = pd.to_datetime(work.turning_point_date)
    rows = []
    for kind, group in work.groupby("turning_point_type", sort=True):
        remaining = group.sort_values("turning_point_date", kind="stable").copy()
        cluster_number = 0
        while len(remaining):
            seed = remaining.iloc[0].turning_point_date
            candidates = remaining.loc[remaining.turning_point_date.map(lambda d: abs(_month_delta(d, seed)) <= window_months)]
            candidates = candidates.assign(distance=candidates.turning_point_date.map(lambda d: abs(_month_delta(d, seed))))
            candidates = candidates.sort_values(["scenario_id", "distance", "turning_point_date"]).drop_duplicates("scenario_id")
            used = set(candidates.index); remaining = remaining.drop(index=list(used))
            if candidates.scenario_id.nunique() < support:
                continue
            dates = candidates.turning_point_date.sort_values(); ordinal = dates.map(lambda d:d.to_period("M").ordinal)
            reference = pd.Period(ordinal=int(np.median(ordinal)), freq="M").to_timestamp("M")
            cluster_number += 1
            rows.append({"consensus_id": f"{kind}_{cluster_number}", "turning_point_type": kind,
                "reference_date": reference, "supporting_finalists": candidates.scenario_id.nunique(),
                "earliest_detection": dates.iloc[0], "latest_detection": dates.iloc[-1],
                "date_dispersion_months": _month_delta(dates.iloc[-1], dates.iloc[0]),
                "support_threshold": support})
    return pd.DataFrame(rows, columns=columns)


def match_consensus(points: pd.DataFrame, consensus: pd.DataFrame, window: int = 3) -> pd.DataFrame:
    rows = []
    for scenario in SCENARIOS:
        candidate = points.loc[points.scenario_id.eq(scenario)]
        used = set()
        for turn in consensus.itertuples(index=False):
            choices=[]
            for idx, point in candidate.loc[candidate.turning_point_type.eq(turn.turning_point_type)].iterrows():
                if idx not in used and abs(_month_delta(point.turning_point_date, turn.reference_date)) <= window:
                    choices.append((abs(_month_delta(point.turning_point_date, turn.reference_date)), point.turning_point_date, idx))
            if choices:
                _, date, idx = min(choices); used.add(idx); latency = _month_delta(date, turn.reference_date)
            else: date, latency = pd.NaT, np.nan
            rows.append({"scenario_id":scenario, "consensus_id":turn.consensus_id,
                "turning_point_type":turn.turning_point_type, "consensus_reference_date":turn.reference_date,
                "candidate_turn_date":date, "latency_months":latency, "absolute_latency_months":abs(latency) if pd.notna(latency) else np.nan,
                "missed":pd.isna(date)})
    return pd.DataFrame(rows)


def _period(frame, period):
    if period == "full_history": return frame
    dates = pd.to_datetime(frame.date)
    if period == "2022_plus": return frame.loc[dates >= "2022-01-01"]
    cutoff = dates.max().to_period("M") - 35
    return frame.loc[dates.dt.to_period("M") >= cutoff]


def _chronologies(frames):
    metric = frames["laus_long_weight_metric_chronology"].copy()
    metric = metric.loc[metric.scenario_id.isin(SCENARIOS)]
    metric = metric.rename(columns={"metric_score":"score", "metric":"series"})
    downstream = frames["laus_long_weight_downstream_chronology"].copy()
    parts=[metric[["scenario_id","geo_id","date","series","score"]]]
    for col, name in (("cyclical_score","cyclical"),("core_demand_score","core_demand")):
        q=downstream.loc[downstream.scenario_id.isin(SCENARIOS),["scenario_id","geo_id","date",col]].rename(columns={col:"score"})
        parts.append(q.assign(series=name))
    out=pd.concat(parts,ignore_index=True); out["date"]=pd.to_datetime(out.date)
    if set(out.series) != set(SERIES): raise ValueError("all five required chronology families must be persisted")
    return out


def analyze(frames: dict[str,pd.DataFrame]) -> dict[str,pd.DataFrame]:
    chronology=_chronologies(frames); event_rows=[]; summary_rows=[]; point_rows=[]
    for keys,g in chronology.groupby(["scenario_id","geo_id","series"],sort=True):
        for period in PERIODS:
            ev=reversal_events(_period(g,period))
            if len(ev):
                for key,val in zip(("scenario_id","geo_id","series"),keys): ev[key]=val
                ev["period"]=period; event_rows.append(ev)
            summary_rows.append({"scenario_id":keys[0],"geo_id":keys[1],"series":keys[2],"period":period,
                                 **reversal_summary(ev)})
        turns=detect_turning_points(g[["date","score"]],"score")
        if len(turns):
            turns=turns.loc[turns.qualified].copy()
            for key,val in zip(("scenario_id","geo_id","series"),keys): turns[key]=val
            point_rows.append(turns)
    events=pd.concat(event_rows,ignore_index=True) if event_rows else pd.DataFrame()
    summaries=pd.DataFrame(summary_rows)
    points=pd.concat(point_rows,ignore_index=True) if point_rows else pd.DataFrame(columns=["scenario_id","geo_id","series","turning_point_date","turning_point_type"])
    consensus_rows=[]; latency_rows=[]
    for (geo,series),g in points.groupby(["geo_id","series"]):
        for threshold in (5,6,7):
            c=cluster_consensus_turns(g,threshold); c["geo_id"],c["series"]=geo,series; consensus_rows.append(c)
            if threshold==6:
                latency=match_consensus(g,c); latency["geo_id"],latency["series"]=geo,series; latency_rows.append(latency)
    consensus=pd.concat(consensus_rows,ignore_index=True) if consensus_rows else pd.DataFrame()
    latency=pd.concat(latency_rows,ignore_index=True) if latency_rows else pd.DataFrame()
    preservation=[]
    for keys,g in latency.groupby(["scenario_id","geo_id","series"]):
        candidate=len(points.loc[(points.scenario_id==keys[0])&(points.geo_id==keys[1])&(points.series==keys[2])])
        matched=int((~g.missed).sum()); total=len(g)
        preservation.append(dict(zip(("scenario_id","geo_id","series"),keys),candidate_turns=candidate,
            consensus_turns_detected=matched,consensus_turns_missed=total-matched,
            non_consensus_turns_generated=max(0,candidate-matched),
            precision_like_share=matched/candidate if candidate else np.nan,
            recall_like_share=matched/total if total else np.nan))
    preservation=pd.DataFrame(preservation)
    evaluation=summaries.loc[summaries.period.eq("full_history")].merge(preservation,on=["scenario_id","geo_id","series"],how="left")
    if len(latency): evaluation=evaluation.merge(latency.groupby(["scenario_id","geo_id","series"],as_index=False).absolute_latency_months.median().rename(columns={"absolute_latency_months":"median_absolute_turn_latency"}),on=["scenario_id","geo_id","series"],how="left")
    pairwise=[]
    for (ma,geo,series), family in chronology.assign(ma_months=chronology.scenario_id.str.extract(r"MA(\d+)")[0].astype(int)).groupby(["ma_months","geo_id","series"]):
        for left,right in combinations(POLICIES,2):
            a=family.loc[family.scenario_id.eq(f"MA{ma}__{left}"),["date","score"]]
            b=family.loc[family.scenario_id.eq(f"MA{ma}__{right}"),["date","score"]]
            q=a.merge(b,on="date",suffixes=("_left","_right")).dropna(); dl,dr=q.score_left.diff(),q.score_right.diff()
            pairwise.append({"ma_months":ma,"geo_id":geo,"series":series,"left_policy":left,"right_policy":right,
                "chronology_correlation":q.score_left.corr(q.score_right),"direction_agreement":np.sign(dl).eq(np.sign(dr)).iloc[1:].mean(),
                "sign_agreement":np.sign(q.score_left).eq(np.sign(q.score_right)).mean(),"mean_absolute_score_difference":(q.score_left-q.score_right).abs().mean(),
                "maximum_absolute_score_difference":(q.score_left-q.score_right).abs().max(),"standard_deviation_difference":q.score_right.std()-q.score_left.std(),
                "mean_absolute_monthly_change_difference":dr.abs().mean()-dl.abs().mean()})
    pairwise=pd.DataFrame(pairwise)
    base=evaluation.groupby(["scenario_id","series"],as_index=False).mean(numeric_only=True)
    controlled=[]
    for ma in MA_WINDOWS:
        for left,right in CONTROLLED_PAIRS:
            a=base.loc[base.scenario_id.eq(f"MA{ma}__{left}")]; b=base.loc[base.scenario_id.eq(f"MA{ma}__{right}")]
            for series in SERIES:
                x=a.loc[a.series.eq(series)]; y=b.loc[b.series.eq(series)]
                if len(x) and len(y):
                    row={"ma_months":ma,"comparison":f"{left}_vs_{right}","from_scenario":f"MA{ma}__{left}","to_scenario":f"MA{ma}__{right}","series":series}
                    for col in ("whipsaw_2m_count","whipsaw_3m_count","durable_2m_count","durable_3m_count","median_absolute_turn_latency","consensus_turns_missed"):
                        row[col+"_change"]=y.iloc[0].get(col,np.nan)-x.iloc[0].get(col,np.nan)
                    controlled.append(row)
    controlled=pd.DataFrame(controlled)
    ma_rows=[]
    for policy in POLICIES:
        for series in SERIES:
            a=base.loc[(base.scenario_id==f"MA6__{policy}")&(base.series==series)]
            b=base.loc[(base.scenario_id==f"MA9__{policy}")&(base.series==series)]
            if len(a) and len(b):
                row={"weight_policy":policy,"series":series,"from_scenario":f"MA6__{policy}","to_scenario":f"MA9__{policy}"}
                for col in ("total_reversal_count","whipsaw_2m_count","whipsaw_3m_count","durable_2m_count","durable_3m_count","median_absolute_turn_latency","consensus_turns_missed"):
                    row[col+"_change_ma9_minus_ma6"]=b.iloc[0].get(col,np.nan)-a.iloc[0].get(col,np.nan)
                ma_rows.append(row)
    ma=pd.DataFrame(ma_rows)
    cross=base.loc[base.series.isin(SERIES[:3])].copy(); cross["common_weighting_human_review_required"]=False
    by_county=evaluation.copy(); sensitivity=summaries.copy()
    governance=pd.DataFrame([{**GOVERNANCE,"persisted_evidence_only":True,"finalist_count":8,
        "labor_force_membership":"LF-IN","balance_policy":"BAL-S25-C75","dc_reported_separately":True}])
    return {"laus_finalist_scenario_registry":scenario_registry(),"laus_finalist_reversal_events":events,
        "laus_finalist_reversal_summary":summaries,"laus_finalist_turning_points":points,
        "laus_finalist_consensus_turns":consensus,"laus_finalist_turn_latency":latency,
        "laus_finalist_turn_preservation":preservation,"laus_finalist_stability_pairwise":pairwise,
        "laus_finalist_controlled_comparisons":controlled,"laus_finalist_ma_comparisons":ma,
        "laus_finalist_cross_metric_consistency":cross,"laus_finalist_by_county":by_county,
        "laus_finalist_period_sensitivity":sensitivity,"laus_finalist_evaluation_matrix":evaluation,
        "laus_finalist_governance_status":governance}


def _plots(output: Path, exports: dict[str,pd.DataFrame], chronology: pd.DataFrame) -> None:
    """Render non-spaghetti, decision-facing SVG diagnostics."""
    import matplotlib; matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    visual=output/"visual_review"; visual.mkdir()
    summary=exports["laus_finalist_reversal_summary"]
    full=summary.loc[summary.period.eq("full_history")].copy()
    full["ma_months"]=full.scenario_id.str.extract(r"MA(\d+)")[0].astype(int)
    full["weight_policy"]=full.scenario_id.str.split("__").str[1]
    for (series,ma),q in full.groupby(["series","ma_months"]):
        q=q.groupby("weight_policy",as_index=False).mean(numeric_only=True).set_index("weight_policy").reindex(POLICIES)
        for horizon in (2,3):
            fig,ax=plt.subplots(figsize=(7,4)); q[[f"whipsaw_{horizon}m_count",f"durable_{horizon}m_count"]].plot.bar(ax=ax)
            ax.set_title(f"{series} — MA{ma} — {horizon}m reversal decomposition"); ax.set_ylabel("county-mean events"); fig.tight_layout()
            fig.savefig(visual/f"reversals__{series}__MA{ma}__{horizon}m.svg"); plt.close(fig)
    latency=exports["laus_finalist_turn_latency"]
    if len(latency):
        latency=latency.assign(ma_months=latency.scenario_id.str.extract(r"MA(\d+)")[0].astype(int))
        for series,q in latency.loc[~latency.missed].groupby("series"):
            fig,axes=plt.subplots(1,2,figsize=(12,4),sharey=True)
            for ax,(ma,g) in zip(axes,q.groupby("ma_months")):
                groups=[g.loc[g.scenario_id.eq(f"MA{ma}__{p}"),"latency_months"] for p in POLICIES]
                ax.boxplot(groups,tick_labels=list(POLICIES)); ax.axhline(0,color="black",lw=.8); ax.set_title(f"MA{ma}")
            fig.suptitle(f"{series} consensus-turn latency (months)"); fig.tight_layout(); fig.savefig(visual/f"latency__{series}.svg"); plt.close(fig)
    consensus=exports["laus_finalist_consensus_turns"]
    primary=consensus.loc[consensus.support_threshold.eq(6)] if len(consensus) else consensus
    pooled=chronology.groupby(["scenario_id","series","date"],as_index=False).score.mean()
    for series,q in pooled.groupby("series"):
        fig,ax=plt.subplots(figsize=(11,4))
        for sid,g in q.groupby("scenario_id"): ax.plot(g.date,g.score,color="#9aa0a6",alpha=.22,lw=.8)
        turns=primary.loc[primary.series.eq(series)] if len(primary) else primary
        for date in turns.reference_date.unique(): ax.axvline(pd.Timestamp(date),color="#b3261e",alpha=.65,lw=1)
        ax.set_title(f"{series} — eight finalists and 6-of-8 consensus turns"); fig.tight_layout(); fig.savefig(visual/f"consensus__{series}.svg"); plt.close(fig)
    # Dedicated, readable small multiples for every controlled policy pair.
    for left,right in CONTROLLED_PAIRS:
        for series,q in pooled.groupby("series"):
            fig,axes=plt.subplots(1,2,figsize=(12,4),sharey=True)
            for ax,ma in zip(axes,MA_WINDOWS):
                for policy in (left,right):
                    g=q.loc[q.scenario_id.eq(f"MA{ma}__{policy}")]; ax.plot(g.date,g.score,label=policy)
                ax.set_title(f"MA{ma}"); ax.legend()
            fig.suptitle(f"{left} vs {right} — {series}"); fig.tight_layout(); fig.savefig(visual/f"controlled__{left}_vs_{right}__{series}.svg"); plt.close(fig)
    evaluation=exports["laus_finalist_evaluation_matrix"].groupby("scenario_id",as_index=False).mean(numeric_only=True)
    if len(evaluation):
        panels=(("whipsaw_2m_share","median_absolute_turn_latency","Whipsaw share vs latency"),
                ("recall_like_share","total_reversal_count","Durable-turn recall vs reversals"))
        for x,y,title in panels:
            if {x,y}.issubset(evaluation):
                fig,ax=plt.subplots(figsize=(7,5)); ax.scatter(evaluation[x],evaluation[y])
                for row in evaluation.itertuples(): ax.annotate(row.scenario_id,(getattr(row,x),getattr(row,y)))
                ax.set(xlabel=x.replace("_"," "),ylabel=y.replace("_"," "),title=title); fig.tight_layout(); fig.savefig(visual/f"frontier__{x}__{y}.svg"); plt.close(fig)


def build_review(source: Path, output: Path) -> Path:
    frames=validate_persisted_bundle(source); exports=analyze(frames)
    output=output.resolve()
    if output.exists(): raise FileExistsError(f"review output already exists: {output}")
    output.mkdir(parents=True)
    for name in REQUIRED_EXPORTS: exports[name].to_csv(output/f"{name}.csv",index=False)
    _plots(output,exports,_chronologies(frames))
    links="".join(f'<li><a href="{html.escape(name)}.csv">{html.escape(name)}.csv</a></li>' for name in REQUIRED_EXPORTS)
    visuals="".join(f'<li><a href="visual_review/{html.escape(p.name)}">{html.escape(p.name)}</a></li>' for p in sorted((output/"visual_review").glob("*.svg")))
    (output/"laus_finalist_review.html").write_text("<!doctype html><meta charset='utf-8'><title>LAUS finalist stability review</title><h1>LAUS finalist stability × turning-responsiveness diagnostic</h1><p>Persisted evidence only. No automated winner or production change.</p><h2>Evidence</h2><ul>"+links+"</ul><h2>Visuals</h2><ul>"+visuals+"</ul>",encoding="utf-8")
    return output


__all__=["CONTROLLED_PAIRS","GOVERNANCE","MA_WINDOWS","POLICIES","REQUIRED_EXPORTS",
    "SCENARIOS","build_review","cluster_consensus_turns","match_consensus","reversal_events",
    "reversal_summary","scenario_registry","validate_persisted_bundle"]
