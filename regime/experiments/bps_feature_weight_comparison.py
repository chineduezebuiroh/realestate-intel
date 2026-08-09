"""Diagnostic-only BPS feature-weight comparison with the lag-6 contract frozen."""
from __future__ import annotations

import html
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from regime.diagnostics.bps_permit_volatility import (
    GEOGRAPHIES, TOLERANCE, _production_contract,
    build_evidence as build_incumbent_evidence,
)
from regime.experiments.bps_short_horizon_comparison import (
    _movement, _responsiveness, _turns,
)

POLICIES = {
    "BPS-W-50-25-25": (.50, .25, .25),
    "BPS-W-60-20-20": (.60, .20, .20),
    "BPS-W-70-15-15": (.70, .15, .15),
    "BPS-W-80-10-10": (.80, .10, .10),
    "BPS-W-90-05-05": (.90, .05, .05),
}
FEATURES = ("level", "short", "long")
INCUMBENT = "BPS-W-50-25-25"
DECISION_ID = "bps_feature_weight_family_lag6_frozen_2026_08_09"
SUPPLY_METRIC_WEIGHT = .20


def policy_registry() -> pd.DataFrame:
    return pd.DataFrame([{
        "policy_id": policy, "level_weight": weights[0], "short_weight": weights[1],
        "long_weight": weights[2], "level_formula": "MA12(raw bps_total_units)",
        "short_formula": "MA12 / lag6(MA12) - 1", "long_formula": "MA12 / lag12(MA12) - 1",
        "short_horizon": "lag6", "long_horizon": "lag12", "transform_family": "ratio",
        "normalization_method": "expanding_percentile", "normalization_polarity": "positive",
        "supply_metric_weight": SUPPLY_METRIC_WEIGHT, "scope": "BPS-only",
        "only_policy_difference": "feature_weights", "production_status": "production" if policy == INCUMBENT else "diagnostic",
    } for policy, weights in POLICIES.items()])


def _chronology(source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    incumbent = build_incumbent_evidence(source, "feature-weight-parity")["chronology"].rename(columns={
        "ma12_structural_level": "ma12_level", "short_feature": "short_raw_feature",
        "long_feature": "long_raw_feature"})
    required = {"geo_id", "date", "raw_bps_total_units", "ma12_level", "short_raw_feature",
                "long_raw_feature", "normalized_level_score", "normalized_short_score",
                "normalized_long_score", "metric_score"}
    if not required.issubset(incumbent):
        raise ValueError(f"production chronology lacks required fields: {sorted(required-set(incumbent))}")
    frames=[]
    for policy, weights in POLICIES.items():
        frame=incumbent.drop(columns=["metric_score"], errors="ignore").copy(); frame.insert(0,"policy_id",policy)
        available=frame[[f"normalized_{f}_score" for f in FEATURES]].notna()
        total=sum(available[f"normalized_{f}_score"]*w for f,w in zip(FEATURES,weights))
        for f,w in zip(FEATURES,weights):
            frame[f"effective_{f}_weight"]=np.where(available[f"normalized_{f}_score"],w/total.replace(0,np.nan),0.)
            frame[f"{f}_contribution"]=frame[f"normalized_{f}_score"].fillna(0)*frame[f"effective_{f}_weight"]
        frame["metric_score"]=frame[[f"{f}_contribution" for f in FEATURES]].sum(axis=1).where(total.gt(0))
        frames.append(frame)
    chron=pd.concat(frames,ignore_index=True)
    error=(chron[[f"{f}_contribution" for f in FEATURES]].sum(axis=1)-chron.metric_score).abs().dropna()
    if len(error) and error.max()>TOLERANCE: raise AssertionError("metric-score reconstruction failed")
    return chron,incumbent


def _stability(chron: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    rows=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        x=g.metric_score.dropna(); d=x.diff().dropna(); signs=np.sign(x).replace(0,np.nan).dropna()
        rows.append({"policy_id":policy,"geo_id":geo,"median_abs_mom":d.abs().median(),
            "p90_abs_mom":d.abs().quantile(.9),"p99_abs_mom":d.abs().quantile(.99),
            "max_abs_jump":d.abs().max(),"sign_flips":max(int(signs.ne(signs.shift()).sum()-1),0),
            "rolling_12m_volatility":x.rolling(12,min_periods=2).std().median()})
    detail=pd.DataFrame(rows); summary=detail.groupby("policy_id",as_index=False).median(numeric_only=True); summary["geo_id"]="pooled_median"
    return detail,summary


def _suppression(chron: pd.DataFrame, movement: pd.DataFrame) -> pd.DataFrame:
    rows=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        deltas=pd.DataFrame({f:g[f"normalized_{f}_score"].diff() for f in FEATURES}); deltas["metric"]=g.metric_score.diff()
        valid=deltas.notna().all(axis=1); d=deltas[valid]; level=np.sign(d.level); short=np.sign(d.short); long=np.sign(d.long); metric=np.sign(d.metric)
        level_only=(metric==level)&(metric!=short)&(metric!=long)
        momentum_agree=(short==long)&short.ne(0); follows_level=momentum_agree&(metric==level)&(metric!=short)
        momentum=d[["short","long"]].abs().max(axis=1); threshold=momentum.quantile(.90)
        metric_threshold=d.metric.abs().quantile(.50); large=momentum.ge(threshold)
        rows.append({"policy_id":policy,"geo_id":geo,
            "level_agreement_both_momentum_disagreement_share":level_only.mean() if len(d) else np.nan,
            "momentum_agree_metric_follows_level_share":follows_level[momentum_agree].mean() if momentum_agree.any() else np.nan,
            "large_momentum_event_threshold":threshold,"metric_materiality_threshold":metric_threshold,
            "large_momentum_events_muted_share":d.loc[large,"metric"].abs().lt(metric_threshold).mean() if large.any() else np.nan})
    return pd.DataFrame(rows)


def build_evidence(source: pd.DataFrame, source_run_id: str) -> dict[str,pd.DataFrame]:
    _production_contract()
    chron,reference=_chronology(source)
    if set(chron.geo_id)!=set(GEOGRAPHIES): raise ValueError("exact governed seven-geography panel is required")
    stability,stability_summary=_stability(chron)
    turns,turn_summary,turn_audit,references=_turns(chron)
    movement,contrib,drivers,extreme=_movement(chron)
    responsiveness=_responsiveness(chron,turns,references).query("target_feature == 'metric_score'").reset_index(drop=True)
    selected=chron.query("policy_id == @INCUMBENT").set_index(["geo_id","date"]); ref=reference.set_index(["geo_id","date"])
    parity=[]
    for col in ["ma12_level","short_raw_feature","long_raw_feature","normalized_level_score","normalized_short_score","normalized_long_score","metric_score"]:
        delta=(selected[col]-ref[col]).abs(); maximum=delta.max()
        parity.append({"field":col,"max_abs_difference":maximum,"tolerance":TOLERANCE,"status":"pass" if maximum<=TOLERANCE else "fail"})
    parity=pd.DataFrame(parity)
    if parity.status.ne("pass").any(): raise AssertionError("50/25/25 production parity failed")
    base=chron.query("policy_id == @INCUMBENT").set_index(["geo_id","date"])
    for policy,g in chron.groupby("policy_id"):
        candidate=g.set_index(["geo_id","date"])
        for col in ["ma12_level","short_raw_feature","long_raw_feature","normalized_level_score","normalized_short_score","normalized_long_score"]:
            if (candidate[col]-base[col]).abs().max()>TOLERANCE: raise AssertionError("feature chronology differs across policies")
    comparisons=[]
    for policy,g in chron.groupby("policy_id"):
        c=g.set_index(["geo_id","date"])
        for geo,x in c.groupby(level="geo_id"):
            b=base.loc[geo,"metric_score"]; y=x.metric_score.droplevel("geo_id"); valid=pd.concat([b,y],axis=1,keys=["base","policy"]).dropna(); diff=(valid.policy-valid.base).abs(); recent=valid.tail(36)
            comparisons.append({"incumbent_policy":INCUMBENT,"challenger_policy":policy,"geo_id":geo,
                "median_absolute_difference":diff.median(),"p90_absolute_difference":diff.quantile(.9),"p99_absolute_difference":diff.quantile(.99),
                "max_absolute_difference":diff.max(),"score_correlation":valid.base.corr(valid.policy),
                "sign_disagreement_share":(np.sign(valid.base)!=np.sign(valid.policy)).mean(),
                "latest_36m_disagreement":(np.sign(recent.base)!=np.sign(recent.policy)).mean()})
    suppression=_suppression(chron,movement)
    recent=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        r=g.tail(36); m=movement.query("policy_id == @policy and geo_id == @geo").tail(35); denom=sum(m[f"{f}_contribution_delta"].abs() for f in FEATURES).sum()
        t=turn_summary.query("policy_id == @policy and geo_id == @geo and feature == 'metric_score'")
        row={"policy_id":policy,"geo_id":geo,"metric_median_abs_mom":r.metric_score.diff().abs().median(),"metric_p90_abs_mom":r.metric_score.diff().abs().quantile(.9),
             "rolling_volatility":r.metric_score.rolling(12,min_periods=2).std().median(),"turn_count":int(t.latest_36m_turning_points.iloc[0]),"largest_jump":r.metric_score.diff().abs().max()}
        for f in FEATURES: row[f"{f}_contribution_share"]=m[f"{f}_contribution_delta"].abs().sum()/denom if denom else np.nan
        recent.append(row)
    recent=pd.DataFrame(recent); decision=[]
    for policy,weights in POLICIES.items():
        s=stability_summary.query("policy_id == @policy").iloc[0]; c=contrib.query("policy_id == @policy"); t=turn_summary.query("policy_id == @policy and feature == 'metric_score'"); r=responsiveness.query("policy_id == @policy"); rec=recent.query("policy_id == @policy"); sup=suppression.query("policy_id == @policy")
        decision.append({"Policy":policy,"Level weight":weights[0],"Short weight":weights[1],"Long weight":weights[2],"Metric median abs MoM":s.median_abs_mom,"Metric P90":s.p90_abs_mom,"Metric P99":s.p99_abs_mom,"Metric max jump":s.max_abs_jump,"Metric sign flips":s.sign_flips,"Metric turning points":t.turning_points.sum(),"Latest-36m metric turns":t.latest_36m_turning_points.sum(),"Recent-36m metric volatility":rec.rolling_volatility.median(),"Level absolute movement contribution share":c.level_absolute_movement_contribution_share.median(),"Short absolute movement contribution share":c.short_absolute_movement_contribution_share.median(),"Long absolute movement contribution share":c.long_absolute_movement_contribution_share.median(),"Median responsiveness lag months":r.median_absolute_lag_months.median(),"P90 responsiveness lag months":r.p90_absolute_lag_months.median(),"Share within 3 months":r.share_within_3_months.mean(),"Momentum suppression evidence":sup.large_momentum_events_muted_share.median(),"Largest metric jump":extreme.query("policy_id == @policy").abs_metric_delta.max(),"Decision":"pending"})
    status=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"recommendation_state":"none","promotion_state":"none","human_decision":"pending","automated_winner":False}])
    return {"policy_registry":policy_registry(),"policy_chronology":chron,"stability":stability,"stability_summary":stability_summary,"contribution_summary":contrib,"metric_driver_audit":drivers,"metric_movement_attribution":movement,"turning_points":turns,"turning_point_summary":turn_summary,"responsiveness_audit":responsiveness,"extreme_jump_attribution":extreme,"recent_36m_summary":recent,"metric_score_comparison":pd.DataFrame(comparisons),"momentum_suppression_audit":suppression,"decision_matrix":pd.DataFrame(decision),"parity_audit":parity,"human_decision_status":status,"turn_detection_audit":turn_audit}


def _visual(frame: pd.DataFrame,title: str,path: Path) -> None:
    image=Image.new("RGB",(1400,900),"white"); draw=ImageDraw.Draw(image); draw.text((20,15),title,fill="black")
    panels=[("ma12_level","MA12 structural level"),("normalized_level_score","Normalized feature context"),("metric_score","Final metric policies"),("level_contribution","Level contribution")]
    colors=dict(zip(POLICIES,["#111827","#94a3b8","#7c3aed","#60a5fa","#dc2626"]))
    for pi,(col,label) in enumerate(panels):
        top=50+pi*205; bottom=top+170; left=75; right=1370; draw.rectangle((left,top,right,bottom),outline="#aaa"); draw.text((left+5,top+4),label,fill="black")
        vals=frame[col].dropna(); lo,hi=(vals.min(),vals.max()) if len(vals) else (0,1); hi=hi if hi!=lo else lo+1
        for j,(policy,g) in enumerate(frame.groupby("policy_id")):
            g=g.sort_values("date"); pts=[(left+i*(right-left)/max(len(g)-1,1),bottom-8-(v-lo)/(hi-lo)*(bottom-top-30)) for i,v in enumerate(g[col]) if pd.notna(v)]
            if len(pts)>1: draw.line(pts,fill=colors[policy],width=4 if policy in {INCUMBENT,"BPS-W-90-05-05"} else 2)
            if pi==2: draw.text((left+j*250,top+20),policy,fill=colors[policy])
    image.save(path)


def write_bundle(evidence: dict[str,pd.DataFrame],output_dir: Path,source_run_id: str) -> int:
    output_dir.mkdir(parents=True,exist_ok=True); visuals=output_dir/"visuals"; visuals.mkdir(exist_ok=True)
    for key,frame in evidence.items(): frame.to_csv(output_dir/f"bps_feature_weight_{key}.csv",index=False)
    names=[]
    for geo in GEOGRAPHIES:
        name=f"{geo}__bps_feature_weight_comparison.png"; _visual(evidence["policy_chronology"].query("geo_id == @geo"),geo,visuals/name); names.append(name)
    comparison="seven_geo_bps_feature_weight_comparison.png"; _visual(evidence["policy_chronology"],"Seven-geography BPS feature-weight comparison",visuals/comparison)
    statement="This experiment changes only BPS feature weights. Lag6 short horizon, lag12 long horizon, MA12 level, ratio transforms, normalization, Supply metric weight, and Supply architecture are frozen. No production policy is promoted automatically."
    order=["policy_registry","decision_matrix","stability_summary","contribution_summary","turning_point_summary","responsiveness_audit","momentum_suppression_audit","extreme_jump_attribution","recent_36m_summary","metric_score_comparison"]
    body=[f"<h1>BPS Feature-Weight Review</h1><p><strong>{html.escape(statement)}</strong></p>"]+[f"<h2>{k.replace('_',' ').title()}</h2>{evidence[k].to_html(index=False)}" for k in order]
    body.append(f"<h2>Visuals</h2><img src='visuals/{comparison}'>"+"".join(f"<img src='visuals/{n}'>" for n in names)); body.append("<h2>Governance</h2>"+evidence["human_decision_status"].to_html(index=False))
    (output_dir/"bps_feature_weight_review.html").write_text("<!doctype html><meta charset='utf-8'><style>body{font-family:sans-serif}img{max-width:100%}table{font-size:10px}</style>"+"".join(body),encoding="utf-8")
    runtime=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"geography_count":7,"policy_count":5,"visual_count":8,"parity_tolerance":TOLERANCE,"output_file_count":len(list(output_dir.rglob('*')))+1}]); runtime.to_csv(output_dir/"bps_feature_weight_runtime_summary.csv",index=False)
    return len(list(output_dir.rglob("*")))
