"""Diagnostic-only comparison of BPS ratio and arithmetic-difference transforms."""
from __future__ import annotations

import html
from pathlib import Path
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from regime._01_feature_engine import build_feature_matrix_with_lineage
from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.bps_permit_volatility import (
    GEOGRAPHIES, FEATURES, TOLERANCE, _canonical_input, _production_contract,
    build_evidence as build_incumbent_evidence,
)
from regime.diagnostics.capital_markets_ma import detect_turning_points

POLICIES = {
    "BPS-T-RATIO": ("ratio", "MA12 / lag3(MA12) - 1", "MA12 / lag12(MA12) - 1"),
    "BPS-T-DIFF": ("arithmetic_difference", "MA12 - lag3(MA12)", "MA12 - lag12(MA12)"),
}
WEIGHTS = {"level": .50, "short": .25, "long": .25}
SUPPLY_METRIC_WEIGHT = .20


def policy_registry() -> pd.DataFrame:
    return pd.DataFrame([{"policy_id": p, "transform_family": v[0], "level_formula": "MA12(raw bps_total_units)",
        "short_formula": v[1], "long_formula": v[2], "short_horizon": "lag3", "long_horizon": "lag12",
        "level_weight": .5, "short_weight": .25, "long_weight": .25,
        "normalization_method": "expanding_percentile", "normalization_polarity": "positive",
        "supply_metric_weight": SUPPLY_METRIC_WEIGHT} for p, v in POLICIES.items()])


def _policy_chronologies(source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = _canonical_input(source)
    incumbent = build_incumbent_evidence(source, "parity")["chronology"].rename(columns={
        "ma12_structural_level":"ma12_level", "short_feature":"short_raw_feature", "long_feature":"long_raw_feature"})
    incumbent.insert(0, "policy_id", "BPS-T-RATIO")
    incumbent_reference = incumbent.copy()
    observations = raw.assign(metric_origin="bps_total_units")
    features, _ = build_feature_matrix_with_lineage(
        canonical_observations=observations, derived_metric_lineage=pd.DataFrame())
    features = features[features.feature_key.isin(FEATURES)].copy()
    ma = incumbent[["geo_id", "date", "ma12_level"]].copy()
    ma["date"] = pd.to_datetime(ma.date)
    ma = ma.sort_values(["geo_id", "date"])
    # Match by calendar month, rather than treating a missing observation as if
    # it shortened the governed horizon.
    ma["month"] = ma.date.dt.to_period("M")
    month_level = ma.set_index(["geo_id", "month"]).ma12_level
    for name, lag in (("short", 3), ("long", 12)):
        lag_index = pd.MultiIndex.from_arrays([ma.geo_id, ma.month-lag])
        ma[name] = ma.ma12_level.to_numpy()-month_level.reindex(lag_index).to_numpy()
    values = {"bps_total_units_level":"ma12_level", "bps_total_units_short":"short", "bps_total_units_long":"long"}
    lookup = ma.set_index(["geo_id", "date"])
    for key, col in values.items():
        mask = features.feature_key.eq(key)
        idx = pd.MultiIndex.from_frame(features.loc[mask, ["geo_id", "date"]])
        features.loc[mask, "raw_feature_value"] = lookup[col].reindex(idx).to_numpy()
    normalized = normalize_features(features)
    f = features.pivot(index=["geo_id","date"], columns="feature_key", values="raw_feature_value")
    n = normalized.pivot(index=["geo_id","date"], columns="feature_key", values="feature_score")
    diff = raw.pivot(index=["geo_id","date"], columns="canonical_metric_key", values="value").join(f).join(n, rsuffix="_score").reset_index().rename(columns={
        "permit_activity":"raw_bps_total_units", "bps_total_units_level":"ma12_level", "bps_total_units_short":"short_raw_feature",
        "bps_total_units_long":"long_raw_feature", "bps_total_units_level_score":"normalized_level_score",
        "bps_total_units_short_score":"normalized_short_score", "bps_total_units_long_score":"normalized_long_score"})
    diff.insert(0, "policy_id", "BPS-T-DIFF")
    for frame in (incumbent, diff):
        available = frame[[f"normalized_{x}_score" for x in WEIGHTS]].notna()
        total = sum(available[f"normalized_{x}_score"] * WEIGHTS[x] for x in WEIGHTS)
        frame["feature_weight_sum"] = total
        for family, weight in WEIGHTS.items():
            frame[f"effective_{family}_weight"] = np.where(available[f"normalized_{family}_score"], weight / total.replace(0, np.nan), 0.)
            frame[f"{family}_contribution"] = frame[f"normalized_{family}_score"].fillna(0) * frame[f"effective_{family}_weight"]
        frame["metric_score"] = frame[[f"{x}_contribution" for x in WEIGHTS]].sum(axis=1).where(total.gt(0))
        error = (frame[[f"{x}_contribution" for x in WEIGHTS]].sum(axis=1)-frame.metric_score).abs().dropna()
        if len(error) and error.max() > TOLERANCE: raise AssertionError("metric-score reconstruction failed")
    columns = ["policy_id","geo_id","date","raw_bps_total_units","ma12_level","short_raw_feature","long_raw_feature",
        "normalized_level_score","normalized_short_score","normalized_long_score","effective_level_weight","effective_short_weight",
        "effective_long_weight","level_contribution","short_contribution","long_contribution","metric_score"]
    return pd.concat([incumbent[columns], diff[columns]], ignore_index=True), incumbent_reference


def _stability(chron: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows=[]
    for (policy, geo), g in chron.groupby(["policy_id","geo_id"]):
        for feature in ("normalized_short_score","normalized_long_score","metric_score"):
            x=g.sort_values("date")[feature].dropna(); d=x.diff().dropna(); signs=np.sign(x).replace(0,np.nan).dropna()
            rows.append({"policy_id":policy,"geo_id":geo,"feature":feature,"median_abs_mom":d.abs().median(),
                "p90_abs_mom":d.abs().quantile(.9),"p99_abs_mom":d.abs().quantile(.99),"max_abs_jump":d.abs().max(),
                "sign_flips":int(signs.ne(signs.shift()).sum()-1 if len(signs) else 0),
                "rolling_12m_volatility":x.rolling(12,min_periods=2).std().median()})
    detail=pd.DataFrame(rows)
    summary=detail.groupby(["policy_id","feature"],as_index=False).agg({c:"median" for c in ["median_abs_mom","p90_abs_mom","p99_abs_mom","max_abs_jump","sign_flips","rolling_12m_volatility"]})
    summary["geo_id"]="pooled_median"
    return detail, summary


def _turns(chron: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame]:
    rows=[]; summaries=[]
    for (policy,geo),g in chron.groupby(["policy_id","geo_id"]):
        for feature in ("normalized_short_score","normalized_long_score","metric_score"):
            found=detect_turning_points(g,feature)
            if not found.empty: found=found[found.qualified]
            dates=pd.to_datetime(found.turning_point_date) if len(found) else pd.Series(dtype="datetime64[ns]")
            for r in found.to_dict("records"): rows.append({"policy_id":policy,"geo_id":geo,"feature":feature,**r})
            spacing=dates.sort_values().diff().dt.days/30.4375
            cutoff=pd.to_datetime(g.date).max()-pd.DateOffset(months=36)
            summaries.append({"policy_id":policy,"geo_id":geo,"feature":feature,"turning_points":len(found),
                "peak_count":int((found.turning_point_type=="peak").sum()) if len(found) else 0,
                "trough_count":int((found.turning_point_type=="trough").sum()) if len(found) else 0,
                "median_turn_spacing_months":spacing.median(),"latest_36m_turning_points":int((dates>=cutoff).sum())})
    return pd.DataFrame(rows),pd.DataFrame(summaries)


def _movement(chron: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,pd.DataFrame]:
    rows=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        x=g[["date","metric_score","level_contribution","short_contribution","long_contribution"]].copy()
        x.insert(0,"geo_id",geo); x.insert(0,"policy_id",policy); x["metric_delta"]=x.metric_score.diff()
        for f in WEIGHTS: x[f"{f}_contribution_delta"]=x[f"{f}_contribution"].diff()
        x["reconstructed_metric_delta"]=sum(x[f"{f}_contribution_delta"] for f in WEIGHTS)
        x["absolute_reconstruction_error"]=(x.metric_delta-x.reconstructed_metric_delta).abs(); rows.append(x)
    movement=pd.concat(rows,ignore_index=True).dropna(subset=["metric_delta"])
    if movement.absolute_reconstruction_error.max()>TOLERANCE: raise AssertionError("metric-delta reconstruction failed")
    cs=[]; drivers=[]
    for (policy,geo),g in movement.groupby(["policy_id","geo_id"]):
        denom=sum(g[f"{f}_contribution_delta"].abs() for f in WEIGHTS).sum()
        row={"policy_id":policy,"geo_id":geo}
        for f in WEIGHTS:
            d=g[f"{f}_contribution_delta"]; row[f"{f}_absolute_movement_contribution_share"]=d.abs().sum()/denom if denom else np.nan
            drivers.append({"policy_id":policy,"geo_id":geo,"feature_family":f,"correlation_with_metric_delta":d.corr(g.metric_delta),
                "signed_agreement_share":float((np.sign(d[d.ne(0)&g.metric_delta.ne(0)])==np.sign(g.loc[d.ne(0)&g.metric_delta.ne(0),"metric_delta"])).mean())})
        cs.append(row)
    movement["dominant_driver"]=movement[[f"{f}_contribution_delta" for f in WEIGHTS]].abs().idxmax(axis=1).str.replace("_contribution_delta","",regex=False)
    extreme=movement.assign(abs_metric_delta=movement.metric_delta.abs()).sort_values(["policy_id","geo_id","abs_metric_delta"],ascending=[True,True,False]).groupby(["policy_id","geo_id"]).head(20)
    return movement,pd.DataFrame(cs),pd.DataFrame(drivers),extreme


def build_evidence(source: pd.DataFrame, source_run_id: str) -> dict[str,pd.DataFrame]:
    _production_contract(); chron, incumbent = _policy_chronologies(source)
    stability, stability_summary=_stability(chron); turns,turn_summary=_turns(chron); movement,contrib,drivers,extreme=_movement(chron)
    wide=chron.pivot(index=["geo_id","date"],columns="policy_id")
    ratio=wide.xs("BPS-T-RATIO",axis=1,level=1); diff=wide.xs("BPS-T-DIFF",axis=1,level=1)
    parity_rows=[]
    original=incumbent.set_index(["geo_id","date"])
    for col in ["ma12_level","short_raw_feature","long_raw_feature","normalized_level_score","normalized_short_score","normalized_long_score","metric_score"]:
        delta=(ratio[col]-original[col]).abs(); parity_rows.append({"field":col,"max_abs_difference":delta.max(),"tolerance":TOLERANCE,"status":"pass" if delta.max()<=TOLERANCE else "fail"})
    parity=pd.DataFrame(parity_rows)
    if parity.status.ne("pass").any(): raise AssertionError("incumbent parity failed")
    direction=[]
    for horizon in ("short","long"):
        valid=ratio[f"{horizon}_raw_feature"].notna()&diff[f"{horizon}_raw_feature"].notna()
        disagree=np.sign(ratio.loc[valid,f"{horizon}_raw_feature"]).ne(np.sign(diff.loc[valid,f"{horizon}_raw_feature"]))
        direction.append({"horizon":horizon,"comparison_count":int(valid.sum()),"zero_neutral_count":int((ratio.loc[valid,f"{horizon}_raw_feature"]==0).sum()),"sign_disagreement_count":int(disagree.sum()),"status":"pass" if not disagree.any() else "fail"})
    directional=pd.DataFrame(direction)
    if directional.sign_disagreement_count.sum(): raise AssertionError("directional parity failed")
    denom=[]
    for geo,g in chron.query("policy_id == 'BPS-T-RATIO'").sort_values("date").groupby("geo_id"):
        d=diff.loc[geo]; r=ratio.loc[geo]
        corr3=r.short_raw_feature.abs().corr(r.ma12_level.shift(3)); corr12=r.long_raw_feature.abs().corr(r.ma12_level.shift(12))
        for date in r.index:
            denom.append({"geo_id":geo,"date":date,"lag3_ma12_denominator":r.ma12_level.shift(3).loc[date],
                "lag12_ma12_denominator":r.ma12_level.shift(12).loc[date],"ratio_short_magnitude":abs(r.short_raw_feature.loc[date]),
                "ratio_long_magnitude":abs(r.long_raw_feature.loc[date]),"arithmetic_short_magnitude":abs(d.short_raw_feature.loc[date]),
                "arithmetic_long_magnitude":abs(d.long_raw_feature.loc[date]),
                "correlation_abs_ratio_short_with_lag3_ma12_denominator":corr3,
                "correlation_abs_ratio_long_with_lag12_ma12_denominator":corr12})
    denominator=pd.DataFrame(denom)
    fairness=[]
    for (policy,geo),g in chron.groupby(["policy_id","geo_id"]):
        fairness.append({"policy_id":policy,"geo_id":geo,"median_ma12_level":g.ma12_level.median(),"mean_ma12_level":g.ma12_level.mean(),
            "median_abs_raw_short":g.short_raw_feature.abs().median(),"p90_abs_raw_short":g.short_raw_feature.abs().quantile(.9),
            "median_abs_raw_long":g.long_raw_feature.abs().median(),"p90_abs_raw_long":g.long_raw_feature.abs().quantile(.9),
            "raw_short_mean":g.short_raw_feature.mean(),"raw_short_p10":g.short_raw_feature.quantile(.1),"raw_short_p99":g.short_raw_feature.quantile(.99),
            "raw_long_mean":g.long_raw_feature.mean(),"raw_long_p10":g.long_raw_feature.quantile(.1),"raw_long_p99":g.long_raw_feature.quantile(.99),
            "normalized_short_mean":g.normalized_short_score.mean(),"normalized_short_median":g.normalized_short_score.median(),"normalized_short_p10":g.normalized_short_score.quantile(.1),"normalized_short_p90":g.normalized_short_score.quantile(.9),"normalized_short_p99":g.normalized_short_score.quantile(.99),
            "normalized_long_mean":g.normalized_long_score.mean(),"normalized_long_median":g.normalized_long_score.median(),"normalized_long_p10":g.normalized_long_score.quantile(.1),"normalized_long_p90":g.normalized_long_score.quantile(.9),"normalized_long_p99":g.normalized_long_score.quantile(.99),
            "normalized_short_volatility":g.normalized_short_score.diff().abs().median(),"normalized_long_volatility":g.normalized_long_score.diff().abs().median(),
            "metric_score_volatility":g.metric_score.diff().abs().median()})
    fairness=pd.DataFrame(fairness)
    for policy in POLICIES:
        m=fairness.policy_id.eq(policy)
        for outcome in ("normalized_short_volatility","normalized_long_volatility","metric_score_volatility"):
            fairness.loc[m,f"panel_correlation_median_level_with_{outcome}"]=fairness.loc[m,"median_ma12_level"].corr(fairness.loc[m,outcome])
    comparisons=[]
    for geo in [*GEOGRAPHIES,"pooled"]:
        rr=ratio.metric_score if geo=="pooled" else ratio.loc[geo].metric_score; dd=diff.metric_score if geo=="pooled" else diff.loc[geo].metric_score
        delta=(rr-dd).abs(); valid=rr.notna()&dd.notna(); latest=delta[valid].tail(36)
        comparisons.append({"geo_id":geo,"median_absolute_policy_difference":delta.median(),"p90_absolute_policy_difference":delta.quantile(.9),
            "p99_absolute_policy_difference":delta.quantile(.99),"maximum_absolute_policy_difference":delta.max(),"policy_score_correlation":rr.corr(dd),
            "sign_disagreement_share":float((np.sign(rr[valid])!=np.sign(dd[valid])).mean()),"latest_36m_median_absolute_difference":latest.median(),"latest_36m_maximum_absolute_difference":latest.max()})
    comparison=pd.DataFrame(comparisons)
    decision=[]
    for policy,(family,sformula,lformula) in POLICIES.items():
        ss=stability_summary.query("policy_id == @policy").set_index("feature"); ts=turn_summary.query("policy_id == @policy").groupby("feature").sum(numeric_only=True); cs=contrib.query("policy_id == @policy")
        decision.append({"Policy":policy,"Transform family":family,"Short formula":sformula,"Long formula":lformula,"Feature weights":"50 / 25 / 25",
            "Normalized short median abs MoM":ss.loc["normalized_short_score","median_abs_mom"],"Normalized short P90":ss.loc["normalized_short_score","p90_abs_mom"],"Normalized short P99":ss.loc["normalized_short_score","p99_abs_mom"],"Normalized short sign flips":ss.loc["normalized_short_score","sign_flips"],"Normalized short turning points":ts.loc["normalized_short_score","turning_points"],
            "Normalized long median abs MoM":ss.loc["normalized_long_score","median_abs_mom"],"Normalized long P90":ss.loc["normalized_long_score","p90_abs_mom"],"Normalized long P99":ss.loc["normalized_long_score","p99_abs_mom"],
            "Metric median abs MoM":ss.loc["metric_score","median_abs_mom"],"Metric P90":ss.loc["metric_score","p90_abs_mom"],"Metric P99":ss.loc["metric_score","p99_abs_mom"],"Metric sign flips":ss.loc["metric_score","sign_flips"],"Metric turning points":ts.loc["metric_score","turning_points"],"Latest-36m metric turns":ts.loc["metric_score","latest_36m_turning_points"],
            "Short absolute movement contribution share":cs.short_absolute_movement_contribution_share.mean(),"Long absolute movement contribution share":cs.long_absolute_movement_contribution_share.mean(),"Largest metric jump":extreme.query("policy_id == @policy").metric_delta.abs().max(),
            "Scale-fairness evidence":"see bps_transform_scale_fairness_audit.csv","Directional parity status":"pass","Decision":"pending"})
    status=pd.DataFrame([{"source_run_id":source_run_id,"recommendation_state":"none","promotion_state":"none","human_decision":"pending"}])
    return {"policy_registry":policy_registry(),"policy_chronology":chron,"stability":stability,"stability_summary":stability_summary,
        "turning_points":turns,"turning_point_summary":turn_summary,"contribution_summary":contrib,"metric_driver_audit":drivers,
        "metric_movement_attribution":movement,"extreme_jump_attribution":extreme,"directional_parity_audit":directional,
        "ratio_denominator_audit":denominator,"scale_fairness_audit":fairness,"metric_score_comparison":comparison,
        "decision_matrix":pd.DataFrame(decision),"parity_audit":parity,"human_decision_status":status}


def _visual(chron: pd.DataFrame, title: str, path: Path) -> None:
    image=Image.new("RGB",(1400,900),"white"); draw=ImageDraw.Draw(image); draw.text((20,15),title,fill="black")
    panels=[(["ma12_level"],"MA12 structural level"),(["short_raw_feature","long_raw_feature"],"Raw transforms (policy-specific units; separate axes not implied)"),(["normalized_short_score","normalized_long_score"],"Normalized short / long"),(["metric_score"],"Final metric score")]
    colors={"BPS-T-RATIO":"#2563eb","BPS-T-DIFF":"#dc2626"}
    for pi,(cols,label) in enumerate(panels):
        top=50+pi*205; bottom=top+170; left=75; right=1370; draw.rectangle((left,top,right,bottom),outline="#aaa"); draw.text((left+5,top+4),label,fill="black")
        vals=chron[cols].stack().dropna(); lo,hi=(vals.min(),vals.max()) if len(vals) else (0,1); hi=hi if hi!=lo else lo+1
        for policy,g in chron.groupby("policy_id"):
            g=g.sort_values("date"); n=max(len(g)-1,1)
            for ci,col in enumerate(cols):
                pts=[(left+i*(right-left)/n,bottom-8-(v-lo)/(hi-lo)*(bottom-top-30)) for i,v in enumerate(g[col]) if pd.notna(v)]
                if len(pts)>1: draw.line(pts,fill=colors[policy],width=2 if ci==0 else 1)
            draw.text((left+250*list(POLICIES).index(policy),top+20),policy,fill=colors[policy])
    image.save(path)


def write_bundle(evidence: dict[str,pd.DataFrame], output_dir: Path, source_run_id: str) -> int:
    output_dir.mkdir(parents=True,exist_ok=True); visuals=output_dir/"visuals"; visuals.mkdir(exist_ok=True)
    for key,frame in evidence.items(): frame.to_csv(output_dir/f"bps_transform_{key}.csv",index=False)
    names=[]
    for geo in GEOGRAPHIES:
        name=f"{geo}__bps_transform_comparison.png"; _visual(evidence["policy_chronology"].query("geo_id == @geo"),geo,visuals/name); names.append(name)
    comparison="seven_geo_bps_transform_comparison.png"; _visual(evidence["policy_chronology"],"Seven-geography BPS transform comparison",visuals/comparison)
    statement="This experiment tests only BPS short/long transform family. Weights, horizons, MA12 level, normalization, metric weight, and Supply architecture are frozen. No production policy is promoted automatically."
    order=["policy_registry","decision_matrix","stability_summary","turning_point_summary","contribution_summary","ratio_denominator_audit","scale_fairness_audit","extreme_jump_attribution","directional_parity_audit"]
    sections=[f"<h1>BPS Transform Review</h1><p><strong>{html.escape(statement)}</strong></p>"]+[f"<h2>{k.replace('_',' ').title()}</h2>{evidence[k].to_html(index=False)}" for k in order]
    sections.append(f"<img src='visuals/{comparison}'>"+"".join(f"<img src='visuals/{n}'>" for n in names)); sections.append("<h2>Governance</h2>"+evidence["human_decision_status"].to_html(index=False))
    (output_dir/"bps_transform_review.html").write_text("<!doctype html><meta charset='utf-8'><style>body{font-family:sans-serif}img{max-width:100%}table{font-size:10px}</style>"+"".join(sections),encoding="utf-8")
    runtime=pd.DataFrame([{"source_run_id":source_run_id,"geography_count":7,"policy_count":2,"visual_count":8,"output_file_count":len(list(output_dir.rglob('*')))+1}]); runtime.to_csv(output_dir/"bps_transform_runtime_summary.csv",index=False)
    return len(list(output_dir.rglob("*")))
