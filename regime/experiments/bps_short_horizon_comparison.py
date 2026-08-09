"""Diagnostic-only comparison of BPS lag-1, lag-3, and lag-6 short horizons."""
from __future__ import annotations

import html
from pathlib import Path

import numpy as np
import pandas as pd
from PIL import Image, ImageDraw

from regime._01_feature_engine import build_feature_matrix_with_lineage
from regime._02_feature_normalizer import normalize_features
from regime.diagnostics.bps_permit_volatility import (
    FEATURES, GEOGRAPHIES, TOLERANCE, _canonical_input, _production_contract,
    build_evidence as build_incumbent_evidence,
)
from regime.diagnostics.capital_markets_ma import detect_turning_points, match_turning_points

POLICIES = {"BPS-H-LAG1": 1, "BPS-H-LAG3": 3, "BPS-H-LAG6": 6}
WEIGHTS = {"level": .50, "short": .25, "long": .25}
SUPPLY_METRIC_WEIGHT = .20
DECISION_ID = "bps_short_horizon_lag6_2026_08_09"
SELECTED_POLICY = "BPS-H-LAG6"


def policy_registry() -> pd.DataFrame:
    return pd.DataFrame([{
        "policy_id": policy, "short_horizon": f"lag{lag}", "long_horizon": "lag12",
        "transform_family": "ratio", "level_formula": "MA12(raw bps_total_units)",
        "short_formula": f"MA12 / lag{lag}(MA12) - 1", "long_formula": "MA12 / lag12(MA12) - 1",
        "level_weight": .5, "short_weight": .25, "long_weight": .25,
        "normalization_method": "expanding_percentile", "normalization_polarity": "positive",
        "supply_metric_weight": SUPPLY_METRIC_WEIGHT, "scope": "BPS-only",
        "selection_status": "selected" if policy == SELECTED_POLICY else "not_selected",
        "production_status": "production" if policy == SELECTED_POLICY else "diagnostic",
    } for policy, lag in POLICIES.items()])


def _chronologies(source: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    raw = _canonical_input(source)
    incumbent = build_incumbent_evidence(source, "parity")["chronology"].rename(columns={
        "ma12_structural_level": "ma12_level", "short_feature": "short_raw_feature",
        "long_feature": "long_raw_feature"})
    reference = incumbent.copy()
    observations = raw.assign(metric_origin="bps_total_units")
    base_features, _ = build_feature_matrix_with_lineage(
        canonical_observations=observations, derived_metric_lineage=pd.DataFrame())
    base_features = base_features[base_features.feature_key.isin(FEATURES)].copy()
    ma = incumbent[["geo_id", "date", "ma12_level"]].copy()
    ma["date"] = pd.to_datetime(ma.date); ma["month"] = ma.date.dt.to_period("M")
    levels = ma.set_index(["geo_id", "month"]).ma12_level
    frames = []
    for policy, lag in POLICIES.items():
        features = base_features.copy(); lag_index = pd.MultiIndex.from_arrays([ma.geo_id, ma.month-lag])
        short = ma.ma12_level.to_numpy()/levels.reindex(lag_index).to_numpy()-1
        lookup = pd.Series(short, index=pd.MultiIndex.from_frame(ma[["geo_id", "date"]]))
        mask = features.feature_key.eq("bps_total_units_short")
        features.loc[mask, "raw_feature_value"] = lookup.reindex(pd.MultiIndex.from_frame(features.loc[mask, ["geo_id", "date"]])).to_numpy()
        normalized = normalize_features(features)
        f = features.pivot(index=["geo_id","date"], columns="feature_key", values="raw_feature_value")
        n = normalized.pivot(index=["geo_id","date"], columns="feature_key", values="feature_score")
        frame = raw.pivot(index=["geo_id","date"], columns="canonical_metric_key", values="value").join(f).join(n, rsuffix="_score").reset_index().rename(columns={
            "permit_activity":"raw_bps_total_units", "bps_total_units_level":"ma12_level",
            "bps_total_units_short":"short_raw_feature", "bps_total_units_long":"long_raw_feature",
            "bps_total_units_level_score":"normalized_level_score", "bps_total_units_short_score":"normalized_short_score",
            "bps_total_units_long_score":"normalized_long_score"})
        frame.insert(0, "policy_id", policy)
        available = frame[[f"normalized_{x}_score" for x in WEIGHTS]].notna()
        weight_sum = sum(available[f"normalized_{x}_score"]*WEIGHTS[x] for x in WEIGHTS)
        for family, weight in WEIGHTS.items():
            frame[f"effective_{family}_weight"] = np.where(available[f"normalized_{family}_score"], weight/weight_sum.replace(0,np.nan), 0.)
            frame[f"{family}_contribution"] = frame[f"normalized_{family}_score"].fillna(0)*frame[f"effective_{family}_weight"]
        frame["metric_score"] = frame[[f"{x}_contribution" for x in WEIGHTS]].sum(axis=1).where(weight_sum.gt(0))
        frames.append(frame)
    columns = ["policy_id","geo_id","date","raw_bps_total_units","ma12_level","short_raw_feature","long_raw_feature",
        "normalized_level_score","normalized_short_score","normalized_long_score","effective_level_weight","effective_short_weight",
        "effective_long_weight","level_contribution","short_contribution","long_contribution","metric_score"]
    chronology = pd.concat([x[columns] for x in frames], ignore_index=True)
    error = (chronology[[f"{x}_contribution" for x in WEIGHTS]].sum(axis=1)-chronology.metric_score).abs().dropna()
    if len(error) and error.max() > TOLERANCE: raise AssertionError("metric-score reconstruction failed")
    return chronology, reference


def _stability(chron: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        for feature in ("normalized_short_score","normalized_long_score","metric_score"):
            x=g[feature].dropna(); d=x.diff().dropna(); signs=np.sign(x).replace(0,np.nan).dropna()
            rows.append({"policy_id":policy,"geo_id":geo,"feature":feature,"median_abs_mom":d.abs().median(),"p90_abs_mom":d.abs().quantile(.9),
                "p99_abs_mom":d.abs().quantile(.99),"max_abs_jump":d.abs().max(),"sign_flips":max(int(signs.ne(signs.shift()).sum()-1),0),
                "rolling_12m_volatility":x.rolling(12,min_periods=2).std().median()})
    detail=pd.DataFrame(rows); summary=detail.groupby(["policy_id","feature"],as_index=False).median(numeric_only=True); summary["geo_id"]="pooled_median"
    return detail,summary


def _scale_valid_turns(frame: pd.DataFrame, value: str) -> pd.DataFrame:
    """Run persistent detection with scale-valid 12-month prominence.

    Candidate dates still require the shared detector's three incoming and
    three outgoing same-direction observations.  Qualification measures the
    candidate's unitless excursion from its 12-month shoulders, rather than
    only the six tiny changes immediately adjacent to a smooth extremum.
    """
    work=frame[["date",value]].copy(); work[value]=pd.to_numeric(work[value],errors="coerce")
    finite=np.isfinite(work[value]); scale=work.loc[finite,value].std(ddof=0)
    if not finite.any() or not np.isfinite(scale) or scale <= 0:
        return pd.DataFrame(columns=["turning_point_date","turning_point_type","qualified"])
    work.loc[finite,value]=(work.loc[finite,value]-work.loc[finite,value].mean())/scale
    found=detect_turning_points(work,value)
    if not len(found): return found
    series=work.set_index(pd.to_datetime(work.date))[value]
    prominence=[]
    for date in pd.to_datetime(found.turning_point_date):
        center=series.get(date,np.nan); before=series.get(date-pd.DateOffset(months=12),np.nan); after=series.get(date+pd.DateOffset(months=12),np.nan)
        prominence.append(abs(center-before)+abs(center-after) if np.isfinite(center) and np.isfinite(before) and np.isfinite(after) else np.nan)
    found["prominence"]=prominence; found["qualified"]=found.prominence.gt(found.prominence_threshold)
    return found[found.qualified].copy()


def _series_audit(policy: str, geo: str, name: str, frame: pd.DataFrame, turns: pd.DataFrame) -> dict:
    work=frame[["date",name]].copy().sort_values("date"); values=pd.to_numeric(work[name],errors="coerce")
    finite=values[np.isfinite(values)]; a=finite.to_numpy()
    extrema=int(((((a[1:-1]>a[:-2])&(a[1:-1]>a[2:]))|((a[1:-1]<a[:-2])&(a[1:-1]<a[2:]))).sum())) if len(a)>2 else 0
    return {"geo_id":geo,"policy_id":policy,"series_name":name,"row_count":len(work),"finite_count":len(finite),
        "null_count":int(values.isna().sum()),"duplicate_date_count":int(pd.to_datetime(work.date).duplicated().sum()),
        "first_date":pd.to_datetime(work.date).min(),"last_date":pd.to_datetime(work.date).max(),
        "min_value":finite.min() if len(finite) else np.nan,"max_value":finite.max() if len(finite) else np.nan,
        "std_value":finite.std(ddof=0) if len(finite) else np.nan,"simple_local_extrema_count":extrema,
        "qualified_turn_count":len(turns),"turn_detection_status":"qualified_turns" if len(turns) else "no_qualified_turns"}


def _turns(chron: pd.DataFrame) -> tuple[pd.DataFrame,pd.DataFrame,pd.DataFrame,dict[str,pd.DataFrame]]:
    rows=[]; summaries=[]; audits=[]; references={}
    for (policy,geo),g in chron.groupby(["policy_id","geo_id"]):
        structural=_scale_valid_turns(g,"ma12_level")
        if geo in references:
            if not structural.reset_index(drop=True).equals(references[geo].reset_index(drop=True)): raise AssertionError("structural reference turns differ by policy")
        else: references[geo]=structural
        audits.append(_series_audit(policy,geo,"ma12_level",g,structural))
        for feature in ("normalized_short_score","metric_score"):
            found=_scale_valid_turns(g,feature); audits.append(_series_audit(policy,geo,feature,g,found))
            dates=pd.to_datetime(found.turning_point_date) if len(found) else pd.Series(dtype="datetime64[ns]")
            rows.extend({"policy_id":policy,"geo_id":geo,"feature":feature,**r} for r in found.to_dict("records"))
            spacing=dates.sort_values().diff().dt.days/30.4375; cutoff=pd.to_datetime(g.date).max()-pd.DateOffset(months=36)
            summaries.append({"policy_id":policy,"geo_id":geo,"feature":feature,"turning_points":len(found),"peak_count":int((found.turning_point_type=="peak").sum()) if len(found) else 0,
                "trough_count":int((found.turning_point_type=="trough").sum()) if len(found) else 0,"median_turn_spacing_months":spacing.median(),"latest_36m_turning_points":int((dates>=cutoff).sum())})
    detail=pd.DataFrame(rows)
    if detail.empty:
        detail=pd.DataFrame(columns=["policy_id","geo_id","feature","turning_point_date","turning_point_type","qualified"])
    return detail,pd.DataFrame(summaries),pd.DataFrame(audits),references


def _movement(chron: pd.DataFrame):
    frames=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        x=g[["date","metric_score","level_contribution","short_contribution","long_contribution"]].copy(); x.insert(0,"geo_id",geo); x.insert(0,"policy_id",policy)
        x["metric_delta"]=x.metric_score.diff()
        for f in WEIGHTS: x[f"{f}_contribution_delta"]=x[f"{f}_contribution"].diff()
        x["reconstructed_metric_delta"]=sum(x[f"{f}_contribution_delta"] for f in WEIGHTS); x["absolute_reconstruction_error"]=(x.metric_delta-x.reconstructed_metric_delta).abs(); frames.append(x)
    movement=pd.concat(frames).dropna(subset=["metric_delta"])
    if movement.absolute_reconstruction_error.max()>TOLERANCE: raise AssertionError("metric-delta reconstruction failed")
    shares=[]; drivers=[]
    for (policy,geo),g in movement.groupby(["policy_id","geo_id"]):
        denom=sum(g[f"{f}_contribution_delta"].abs() for f in WEIGHTS).sum(); row={"policy_id":policy,"geo_id":geo}
        for f in WEIGHTS:
            d=g[f"{f}_contribution_delta"]; nz=d.ne(0)&g.metric_delta.ne(0); row[f"{f}_absolute_movement_contribution_share"]=d.abs().sum()/denom if denom else np.nan
            drivers.append({"policy_id":policy,"geo_id":geo,"feature_family":f,"correlation_with_metric_delta":d.corr(g.metric_delta),"same_month_signed_agreement_share":float((np.sign(d[nz])==np.sign(g.loc[nz,"metric_delta"])).mean()) if nz.any() else np.nan})
        shares.append(row)
    movement["dominant_driver"]=movement[[f"{f}_contribution_delta" for f in WEIGHTS]].abs().idxmax(axis=1).str.split("_").str[0]
    extreme=movement.assign(abs_metric_delta=movement.metric_delta.abs()).sort_values(["policy_id","geo_id","abs_metric_delta"],ascending=[True,True,False]).groupby(["policy_id","geo_id"]).head(20)
    return movement,pd.DataFrame(shares),pd.DataFrame(drivers),extreme


def _responsiveness(chron: pd.DataFrame, turns: pd.DataFrame, references: dict[str,pd.DataFrame]) -> pd.DataFrame:
    """Match each reference to the nearest unused same-type turn within ±12 months."""
    rows=[]
    for (policy,geo),g in chron.groupby(["policy_id","geo_id"]):
        structural=references[geo]
        for target in ("normalized_short_score","metric_score"):
            candidates=turns.query("policy_id == @policy and geo_id == @geo and feature == @target")
            matched=match_turning_points(structural,candidates,window_months=12)
            signed=matched.loc[matched.matched,"signed_delay_months"].astype(float); absolute=signed.abs()
            rows.append({"policy_id":policy,"geo_id":geo,"target_feature":target,"reference_definition":"qualified scale-standardized MA12 level turns","matching_window_months":12,
                "reference_turn_count":len(structural),"policy_turn_count":len(candidates),"matched_turn_count":len(signed),
                "unmatched_reference_turn_count":len(structural)-len(signed),"unmatched_policy_turn_count":len(candidates)-len(signed),
                "median_signed_lag_months":signed.median(),"median_absolute_lag_months":absolute.median(),"p90_absolute_lag_months":absolute.quantile(.9),
                "share_within_1_month":absolute.le(1).mean() if len(absolute) else np.nan,"share_within_3_months":absolute.le(3).mean() if len(absolute) else np.nan,"share_within_6_months":absolute.le(6).mean() if len(absolute) else np.nan})
    result=pd.DataFrame(rows)
    if any(len(x)==0 for x in references.values()): raise AssertionError("every sufficient geography must have a structural reference turn")
    if result.matched_turn_count.sum()==0: raise AssertionError("responsiveness produced zero matches")
    if (result.matched_turn_count.gt(result.reference_turn_count)|result.matched_turn_count.gt(result.policy_turn_count)).any(): raise AssertionError("turn matching is not one-to-one")
    shares=result[["share_within_1_month","share_within_3_months","share_within_6_months"]].dropna()
    if ((shares.lt(0)|shares.gt(1)).any().any() or (shares.iloc[:,0]>shares.iloc[:,1]).any() or (shares.iloc[:,1]>shares.iloc[:,2]).any()): raise AssertionError("invalid responsiveness shares")
    return result


def build_evidence(source: pd.DataFrame, source_run_id: str) -> dict[str,pd.DataFrame]:
    _production_contract(); chron,reference=_chronologies(source); stability,stability_summary=_stability(chron); turns,turn_summary,turn_audit,references=_turns(chron); movement,contrib,drivers,extreme=_movement(chron)
    selected=chron.query("policy_id == @SELECTED_POLICY").set_index(["geo_id","date"]); ref=reference.set_index(["geo_id","date"]); parity=[]
    for col in ["ma12_level","short_raw_feature","long_raw_feature","normalized_level_score","normalized_short_score","normalized_long_score","metric_score"]:
        delta=(selected[col]-ref[col]).abs(); parity.append({"decision_id":DECISION_ID,"production_policy":SELECTED_POLICY,"diagnostic_policy":SELECTED_POLICY,"field":col,"max_abs_difference":delta.max(),"tolerance":TOLERANCE,"status":"pass" if delta.max()<=TOLERANCE else "fail"})
    parity=pd.DataFrame(parity)
    if parity.status.ne("pass").any(): raise AssertionError("lag6 production parity failed")
    responsiveness=_responsiveness(chron,turns,references); direction=[]
    wide=chron.pivot(index=["geo_id","date"],columns="policy_id",values="short_raw_feature")
    for a,b in (("BPS-H-LAG1","BPS-H-LAG3"),("BPS-H-LAG3","BPS-H-LAG6"),("BPS-H-LAG1","BPS-H-LAG6")):
        for geo,g in wide.groupby(level="geo_id"):
            valid=g[a].notna()&g[b].notna(); direction.append({"policy_a":a,"policy_b":b,"geo_id":geo,"comparison_count":int(valid.sum()),"directional_agreement_share":float((np.sign(g.loc[valid,a])==np.sign(g.loc[valid,b])).mean())})
        valid=wide[a].notna()&wide[b].notna(); direction.append({"policy_a":a,"policy_b":b,"geo_id":"pooled","comparison_count":int(valid.sum()),"directional_agreement_share":float((np.sign(wide.loc[valid,a])==np.sign(wide.loc[valid,b])).mean())})
    comparisons=[]
    for a,b in (("BPS-H-LAG1","BPS-H-LAG3"),("BPS-H-LAG3","BPS-H-LAG6"),("BPS-H-LAG1","BPS-H-LAG6")):
        scores=chron.pivot(index=["geo_id","date"],columns="policy_id",values="metric_score")
        for geo,g in scores.groupby(level="geo_id"):
            valid=g[[a,b]].dropna(); d=(valid[a]-valid[b]).abs(); recent=valid.tail(36)
            comparisons.append({"policy_a":a,"policy_b":b,"geo_id":geo,"median_absolute_difference":d.median(),"p90_absolute_difference":d.quantile(.9),"p99_absolute_difference":d.quantile(.99),"max_absolute_difference":d.max(),"score_correlation":valid[a].corr(valid[b]),"sign_disagreement_share":float((np.sign(valid[a])!=np.sign(valid[b])).mean()),"latest_36m_disagreement":float((recent[a]-recent[b]).abs().median())})
    recent=[]
    for (policy,geo),g in chron.sort_values("date").groupby(["policy_id","geo_id"]):
        r=g.tail(36); m=movement.query("policy_id == @policy and geo_id == @geo").tail(35); t=turn_summary.query("policy_id == @policy and geo_id == @geo and feature == 'metric_score'")
        denom=sum(m[f"{f}_contribution_delta"].abs() for f in WEIGHTS).sum()
        recent.append({"policy_id":policy,"geo_id":geo,"metric_score_volatility":r.metric_score.diff().std(),"short_score_volatility":r.normalized_short_score.diff().std(),"turn_count":int(t.latest_36m_turning_points.iloc[0]),"short_contribution_share":m.short_contribution_delta.abs().sum()/denom if denom else np.nan,"largest_jump":r.metric_score.diff().abs().max()})
    decision=[]
    for policy,lag in POLICIES.items():
        ss=stability_summary.query("policy_id == @policy and feature == 'normalized_short_score'").iloc[0]; ms=stability_summary.query("policy_id == @policy and feature == 'metric_score'").iloc[0]
        ts=turn_summary.query("policy_id == @policy and feature == 'normalized_short_score'"); tm=turn_summary.query("policy_id == @policy and feature == 'metric_score'"); rr=responsiveness.query("policy_id == @policy and target_feature == 'normalized_short_score'"); dd=drivers.query("policy_id == @policy and feature_family == 'short'")
        decision.append({"Policy":policy,"Short horizon":f"lag{lag}","Long horizon":"lag12","Feature weights":"50/25/25","Normalized short median abs MoM":ss.median_abs_mom,"Normalized short P90":ss.p90_abs_mom,"Normalized short P99":ss.p99_abs_mom,"Normalized short sign flips":ss.sign_flips,"Normalized short turning points":ts.turning_points.sum(),"Metric median abs MoM":ms.median_abs_mom,"Metric P90":ms.p90_abs_mom,"Metric P99":ms.p99_abs_mom,"Metric max jump":ms.max_abs_jump,"Metric sign flips":ms.sign_flips,"Metric turning points":tm.turning_points.sum(),"Latest-36m metric turns":tm.latest_36m_turning_points.sum(),"Short absolute movement contribution share":contrib.query("policy_id == @policy").short_absolute_movement_contribution_share.median(),"Short-metric correlation":dd.correlation_with_metric_delta.median(),"Reference turn count":rr.reference_turn_count.sum(),"Matched turn count":rr.matched_turn_count.sum(),"Median signed responsiveness lag months":rr.median_signed_lag_months.median(),"Median absolute responsiveness lag months":rr.median_absolute_lag_months.median(),"P90 absolute responsiveness lag months":rr.p90_absolute_lag_months.median(),"Share within 1 month":rr.share_within_1_month.mean(),"Share within 3 months":rr.share_within_3_months.mean(),"Share within 6 months":rr.share_within_6_months.mean(),"Largest metric jump":extreme.query("policy_id == @policy").abs_metric_delta.max(),"Recent-36m metric volatility":pd.DataFrame(recent).query("policy_id == @policy").metric_score_volatility.median(),"Decision":"selected" if policy == SELECTED_POLICY else "not_selected"})
    status=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"selected_policy":SELECTED_POLICY,"recommendation_state":"selected","promotion_state":"promoted","human_decision":"approved","bps_short_horizon_calibration_state":"closed","automated_winner":False}])
    config_diff=pd.DataFrame([
        {"feature_key":"bps_total_units_level","field":"window","before":"12m","after":"12m","change_status":"unchanged"},
        {"feature_key":"bps_total_units_short","field":"window","before":"12m/lag3m","after":"12m/lag6m","change_status":"changed"},
        {"feature_key":"bps_total_units_long","field":"window","before":"12m/lag12m","after":"12m/lag12m","change_status":"unchanged"},
        {"feature_key":"bps_total_units","field":"feature_weights","before":"0.50/0.25/0.25","after":"0.50/0.25/0.25","change_status":"unchanged"},
        {"feature_key":"bps_total_units","field":"transform_family","before":"ratio","after":"ratio","change_status":"unchanged"},
        {"feature_key":"bps_total_units","field":"normalization","before":"positive expanding-percentile","after":"positive expanding-percentile","change_status":"unchanged"},
        {"feature_key":"bps_total_units","field":"supply_metric_weight","before":"0.20","after":"0.20","change_status":"unchanged"},
    ])
    return {"policy_registry":policy_registry(),"policy_chronology":chron,"stability":stability,"stability_summary":stability_summary,"turning_points":turns,"turning_point_summary":turn_summary,"turn_detection_audit":turn_audit,"contribution_summary":contrib,"metric_driver_audit":drivers,"metric_movement_attribution":movement,"responsiveness_audit":responsiveness,"directional_agreement":pd.DataFrame(direction),"extreme_jump_attribution":extreme,"recent_36m_summary":pd.DataFrame(recent),"metric_score_comparison":pd.DataFrame(comparisons),"decision_matrix":pd.DataFrame(decision),"parity_audit":parity,"human_decision_status":status,"promotion_policy_registry":policy_registry(),"promotion_config_diff":config_diff,"promotion_parity_audit":parity,"promotion_human_decision_status":status}


def _visual(frame: pd.DataFrame, title: str, path: Path) -> None:
    image=Image.new("RGB",(1400,900),"white"); draw=ImageDraw.Draw(image); draw.text((20,15),title,fill="black")
    panels=[("ma12_level","MA12 structural level"),("short_raw_feature","Raw short feature"),("normalized_short_score","Normalized short score"),("metric_score","Final metric score")]
    colors={"BPS-H-LAG1":"#d97706","BPS-H-LAG3":"#111827","BPS-H-LAG6":"#2563eb"}
    for pi,(col,label) in enumerate(panels):
        top=50+pi*205; bottom=top+170; left=75; right=1370; draw.rectangle((left,top,right,bottom),outline="#aaa"); draw.text((left+5,top+4),label,fill="black")
        vals=frame[col].dropna(); lo,hi=(vals.min(),vals.max()) if len(vals) else (0,1); hi=hi if hi!=lo else lo+1
        for j,(policy,g) in enumerate(frame.groupby("policy_id")):
            g=g.sort_values("date"); n=max(len(g)-1,1); pts=[(left+i*(right-left)/n,bottom-8-(v-lo)/(hi-lo)*(bottom-top-30)) for i,v in enumerate(g[col]) if pd.notna(v)]
            if len(pts)>1: draw.line(pts,fill=colors[policy],width=4 if policy==SELECTED_POLICY else 2)
            draw.text((left+j*240,top+20),policy+(" (production)" if policy==SELECTED_POLICY else ""),fill=colors[policy])
    image.save(path)


def write_bundle(evidence: dict[str,pd.DataFrame], output_dir: Path, source_run_id: str) -> int:
    output_dir.mkdir(parents=True,exist_ok=True); visuals=output_dir/"visuals"; visuals.mkdir(exist_ok=True)
    for key,frame in evidence.items(): frame.to_csv(output_dir/f"bps_short_horizon_{key}.csv",index=False)
    names=[]
    for geo in GEOGRAPHIES:
        name=f"{geo}__bps_short_horizon_comparison.png"; _visual(evidence["policy_chronology"].query("geo_id == @geo"),geo,visuals/name); names.append(name)
    comparison="seven_geo_bps_short_horizon_comparison.png"; _visual(evidence["policy_chronology"],"Seven-geography BPS short-horizon comparison",visuals/comparison)
    statement="The diagnostic tested only the BPS short horizon. Human decision selected BPS-H-LAG6 for production; transform family, long horizon, MA12 level, weights, normalization, Supply metric weight, and Supply architecture remain frozen. There is no automated winner."
    order=["policy_registry","decision_matrix","stability_summary","responsiveness_audit","turn_detection_audit","turning_point_summary","contribution_summary","extreme_jump_attribution","recent_36m_summary","metric_score_comparison"]
    body=[f"<h1>BPS Short-Horizon Review</h1><p><strong>{html.escape(statement)}</strong></p>"]+[f"<h2>{k.replace('_',' ').title()}</h2>{evidence[k].to_html(index=False)}" for k in order]
    body.append(f"<h2>Seven-Geo Comparison</h2><img src='visuals/{comparison}'>"+"".join(f"<img src='visuals/{n}'>" for n in names)); body.append("<h2>Governance</h2>"+evidence["human_decision_status"].to_html(index=False))
    (output_dir/"bps_short_horizon_review.html").write_text("<!doctype html><meta charset='utf-8'><style>body{font-family:sans-serif}img{max-width:100%}table{font-size:10px}</style>"+"".join(body),encoding="utf-8")
    runtime=pd.DataFrame([{"decision_id":DECISION_ID,"source_run_id":source_run_id,"selected_policy":SELECTED_POLICY,"geography_count":7,"policy_count":3,"visual_count":8,"parity_tolerance":TOLERANCE,"output_file_count":len(list(output_dir.rglob('*')))+2}]); runtime.to_csv(output_dir/"bps_short_horizon_runtime_summary.csv",index=False); runtime.to_csv(output_dir/"bps_short_horizon_promotion_runtime_summary.csv",index=False)
    return len(list(output_dir.rglob("*")))
